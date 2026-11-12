//! Shared-secret authentication for calls coming from the gateway.

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use secrecy::{ExposeSecret, SecretString};
use subtle::ConstantTimeEq;

/// Header carrying the shared secret (same name the gateway sends to user-service).
pub const GATEWAY_SECRET_HEADER: &str = "X-Gateway-Secret";

/// Middleware state: the expected shared secret.
#[derive(Clone)]
pub struct SecretState(pub std::sync::Arc<SecretString>);

/// Rejects requests without a valid [`GATEWAY_SECRET_HEADER`] with `401`.
pub async fn require_gateway_secret(
    State(state): State<SecretState>,
    request: Request,
    next: Next,
) -> Response {
    let valid = request
        .headers()
        .get(GATEWAY_SECRET_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|provided| secrets_match(provided, &state.0));
    if valid {
        next.run(request).await
    } else {
        StatusCode::UNAUTHORIZED.into_response()
    }
}

/// Constant-time comparison; a length mismatch is rejected up front.
fn secrets_match(provided: &str, expected: &SecretString) -> bool {
    let expected = expected.expose_secret().as_bytes();
    let provided = provided.as_bytes();
    expected.len() == provided.len() && bool::from(expected.ct_eq(provided))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::middleware;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    const SECRET: &str = "test-shared-secret";

    fn app() -> Router {
        let state = SecretState(std::sync::Arc::new(SecretString::from(SECRET)));
        Router::new()
            .route("/protected", get(|| async { "ok" }))
            .route_layer(middleware::from_fn_with_state(
                state,
                require_gateway_secret,
            ))
    }

    async fn status(header: Option<&str>) -> StatusCode {
        let mut req = Request::builder().uri("/protected");
        if let Some(value) = header {
            req = req.header(GATEWAY_SECRET_HEADER, value);
        }
        app()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn rejects_a_request_without_the_secret_header() {
        assert_eq!(status(None).await, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_a_wrong_secret() {
        assert_eq!(status(Some("wrong")).await, StatusCode::UNAUTHORIZED);
        assert_eq!(
            status(Some("test-shared-secreT")).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn accepts_the_right_secret() {
        assert_eq!(status(Some(SECRET)).await, StatusCode::OK);
    }
}
