//! HTTP surface of the admin feed: `GET /metrics` and `GET /events` sit behind
//! the shared-secret middleware, `/metrics` serves the snapshot, `/events`
//! opens with a `snapshot` event and ends when the service shuts down.

use std::ffi::OsString;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use events_service::admin::{AdminAggregator, AdminFeed, ManualClock};
use events_service::auth::GATEWAY_SECRET_HEADER;
use events_service::config::Config;
use events_service::wiring::build_router;
use futures_util::StreamExt;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const SECRET: &str = "s3cret";

fn app() -> (Router, Arc<AdminAggregator>, CancellationToken) {
    let config = Config::from_lookup(|name| match name {
        "HTTP_HOST" => Some(OsString::from("127.0.0.1")),
        "HTTP_PORT" => Some(OsString::from("8080")),
        "GATEWAY_SHARED_SECRET" => Some(OsString::from(SECRET)),
        _ => None,
    })
    .expect("valid config");
    let aggregator = Arc::new(AdminAggregator::new(Arc::new(ManualClock::new(0))));
    let feed = Arc::new(AdminFeed::new(
        aggregator.clone(),
        Duration::from_secs(3600),
    ));
    let shutdown = CancellationToken::new();
    (
        build_router(&config, feed, shutdown.clone()),
        aggregator,
        shutdown,
    )
}

fn get(uri: &str, secret: Option<&str>) -> Request<Body> {
    let mut request = Request::builder().uri(uri);
    if let Some(secret) = secret {
        request = request.header(GATEWAY_SECRET_HEADER, secret);
    }
    request.body(Body::empty()).unwrap()
}

#[tokio::test]
async fn admin_routes_reject_requests_without_the_shared_secret() {
    for uri in ["/metrics", "/events"] {
        let (router, _, _) = app();
        let response = router.oneshot(get(uri, None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
    }
}

#[tokio::test]
async fn metrics_returns_the_snapshot_shape() {
    let (router, _, _) = app();
    let response = router.oneshot(get("/metrics", Some(SECRET))).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    for field in [
        "seq",
        "kpis",
        "scans_in_flight",
        "critical_findings",
        "recent_events",
    ] {
        assert!(body.get(field).is_some(), "missing {field}: {body}");
    }
    assert_eq!(body["seq"], 0);
}

#[tokio::test]
async fn events_opens_with_a_snapshot_and_then_streams_deltas() {
    let (router, aggregator, _) = app();
    let response = router.oneshot(get("/events", Some(SECRET))).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "text/event-stream",
        "SSE content type"
    );
    let mut stream = response.into_body().into_data_stream();
    let first = String::from_utf8(stream.next().await.unwrap().unwrap().to_vec()).unwrap();
    assert!(first.starts_with("event: snapshot\n"), "{first}");
    assert!(first.contains("data: {"), "{first}");

    let event = events_service::event_contract::parse_record(
        br#"{"event_id":"00000000-0000-4000-8000-000000000001","event_type":"auth.login","occurred_at":"2026-11-10T12:00:00Z","source":"gateway","actor_sub":"u1","payload":{}}"#,
    )
    .unwrap();
    aggregator.ingest(&event);
    let second = String::from_utf8(stream.next().await.unwrap().unwrap().to_vec()).unwrap();
    assert!(second.starts_with("event: delta\n"), "{second}");
}

#[tokio::test]
async fn events_stream_ends_when_the_service_shuts_down() {
    let (router, _, shutdown) = app();
    let response = router.oneshot(get("/events", Some(SECRET))).await.unwrap();
    let mut stream = response.into_body().into_data_stream();
    let _snapshot = stream.next().await.unwrap().unwrap();

    shutdown.cancel();
    let end = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("the stream must end after the shutdown signal");
    assert!(end.is_none(), "the SSE stream must close on shutdown");
}
