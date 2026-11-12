//! Composition root: assembles the `axum` router and the Kinesis consumer.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::middleware;
use axum::routing::get;
use axum::Router;
use tokio_util::sync::CancellationToken;

use crate::admin::AdminFeed;
use crate::admin_routes::admin_router;
use crate::auth::{require_gateway_secret, SecretState};
use crate::config::Config;
use crate::kinesis_consumer::{AwsRecordsSource, SecurityEventConsumer};

/// Region used when `AWS_REGION` is not set (the lab runs in `us-east-1`).
const DEFAULT_AWS_REGION: &str = "us-east-1";
/// Endpoint override, only ever set against LocalStack.
const ENDPOINT_URL_VAR: &str = "AWS_ENDPOINT_URL";

/// Builds the router: `GET /health` (unauthenticated) and the admin routes
/// (`GET /metrics`, `GET /events`) behind the shared-secret middleware.
/// `shutdown` ends the open SSE streams when the service stops.
pub fn build_router(config: &Config, feed: Arc<AdminFeed>, shutdown: CancellationToken) -> Router {
    let state = SecretState(Arc::new(config.gateway_shared_secret.clone()));
    let protected = admin_router(feed, shutdown)
        .fallback(|| async { StatusCode::NOT_FOUND })
        .layer(middleware::from_fn_with_state(
            state,
            require_gateway_secret,
        ));

    Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .merge(protected)
}

/// Builds the Kinesis consumer feeding `feed`, or `None` (with a warning)
/// when `SECURITY_EVENTS_STREAM_NAME` is not set: the service then serves an
/// empty aggregator.
pub async fn build_consumer(config: &Config, feed: &AdminFeed) -> Option<SecurityEventConsumer> {
    let Some(stream_name) = config.security_events_stream_name.as_deref() else {
        tracing::warn!(
            "SECURITY_EVENTS_STREAM_NAME is not set: the security event stream is not consumed \
             and the admin feed stays empty"
        );
        return None;
    };
    let region = config.aws_region.as_deref().unwrap_or(DEFAULT_AWS_REGION);
    let endpoint = std::env::var(ENDPOINT_URL_VAR)
        .ok()
        .filter(|url| !url.trim().is_empty());
    tracing::info!(stream = stream_name, "consuming the security event stream");
    let source =
        AwsRecordsSource::connect(region, endpoint.as_deref(), stream_name.to_string()).await;
    Some(SecurityEventConsumer::new(
        Arc::new(source),
        Arc::clone(&feed.aggregator),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::GATEWAY_SECRET_HEADER;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::ffi::OsString;
    use tower::ServiceExt;

    fn router() -> Router {
        let config = Config::from_lookup(|name| match name {
            "HTTP_HOST" => Some(OsString::from("127.0.0.1")),
            "HTTP_PORT" => Some(OsString::from("8080")),
            "GATEWAY_SHARED_SECRET" => Some(OsString::from("s3cret")),
            _ => None,
        })
        .expect("valid config");
        build_router(
            &config,
            Arc::new(AdminFeed::default()),
            CancellationToken::new(),
        )
    }

    #[tokio::test]
    async fn health_is_ok_without_the_secret() {
        let response = router()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn unknown_routes_without_the_secret_are_unauthorized() {
        let response = router()
            .oneshot(Request::builder().uri("/nope").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn unknown_routes_with_the_secret_are_not_found() {
        let response = router()
            .oneshot(
                Request::builder()
                    .uri("/nope")
                    .header(GATEWAY_SECRET_HEADER, "s3cret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
