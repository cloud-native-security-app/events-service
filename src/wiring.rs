//! Composition root: assembles the `axum` router.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::middleware;
use axum::routing::get;
use axum::Router;

use crate::auth::{require_gateway_secret, SecretState};
use crate::config::Config;

/// Builds the router: `GET /health` (unauthenticated) and, later, the
/// routes guarded by the shared-secret middleware.
pub fn build_router(config: &Config) -> Router {
    let state = SecretState(Arc::new(config.gateway_shared_secret.clone()));
    let protected = Router::new()
        .fallback(|| async { StatusCode::NOT_FOUND })
        .layer(middleware::from_fn_with_state(
            state,
            require_gateway_secret,
        ));

    Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .merge(protected)
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
        build_router(&config)
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
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn unknown_routes_with_the_secret_are_not_found() {
        let response = router()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header(GATEWAY_SECRET_HEADER, "s3cret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
