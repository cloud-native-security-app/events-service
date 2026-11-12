//! End-to-end test of the admin feed against a real LocalStack started with
//! `testcontainers` (no mocks, no AWS account): events put on the Kinesis
//! stream are read by the consumer (`AwsRecordsSource` +
//! `SecurityEventConsumer`, `GetShardIterator` + `GetRecords`) and
//! reflected by `GET /metrics` of the real router. A garbage record is also
//! put and counted.
//!
//! Run with `cargo test -- --ignored` (requires Docker).

mod common;

use std::ffi::OsString;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_sdk_kinesis::primitives::Blob;
use axum::body::{to_bytes, Body};
use axum::http::Request;
use events_service::admin::{AdminAggregator, AdminFeed, SystemClock};
use events_service::auth::GATEWAY_SECRET_HEADER;
use events_service::config::Config;
use events_service::kinesis_consumer::{AwsRecordsSource, SecurityEventConsumer};
use events_service::wiring::build_router;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const STREAM: &str = "security-events";
const SECRET: &str = "lab-only-shared-secret";

/// Polls `condition` until `timeout`; fails with `what` when it never holds.
async fn wait_until(what: &str, timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn put(localstack: &common::LocalStack, partition_key: &str, body: Vec<u8>) {
    localstack
        .kinesis
        .put_record()
        .stream_name(STREAM)
        .partition_key(partition_key)
        .data(Blob::new(body))
        .send()
        .await
        .expect("put_record");
}

fn event(
    id: u64,
    event_type: &str,
    at: &str,
    actor: Option<&str>,
    scan: Option<&str>,
    payload: Value,
) -> Vec<u8> {
    let mut event = json!({
        "event_id": format!("00000000-0000-4000-8000-{id:012}"),
        "event_type": event_type,
        "occurred_at": at,
        "source": "gateway",
        "payload": payload
    });
    if let Some(actor) = actor {
        event["actor_sub"] = json!(actor);
    }
    if let Some(scan) = scan {
        event["scan_id"] = json!(scan);
    }
    serde_json::to_vec(&event).unwrap()
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}

/// Restart scenario: records published while no consumer was running are
/// read by a NEW consumer (warm start, `AT_TIMESTAMP` 15 minutes back).
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_new_consumer_recovers_the_events_published_before_it_started() {
    let localstack = common::LocalStack::start().await;
    localstack.create_stream(STREAM).await;
    let at = now_rfc3339();
    for (id, actor) in [(1, "admin-1"), (2, "admin-2")] {
        put(
            &localstack,
            "restart",
            event(id, "auth.login", &at, Some(actor), None, json!({})),
        )
        .await;
    }
    put(
        &localstack,
        "restart",
        event(
            3,
            "scan.failed",
            &at,
            None,
            Some("scan-restart"),
            json!({"reason_code": "ssh_timeout"}),
        ),
    )
    .await;

    // The "restarted" instance: nothing was seen before this point.
    let aggregator = Arc::new(AdminAggregator::new(Arc::new(SystemClock)));
    let shutdown = CancellationToken::new();
    let consumer = SecurityEventConsumer::new(
        Arc::new(AwsRecordsSource::from_client(
            localstack.kinesis.clone(),
            STREAM.to_string(),
        )),
        aggregator.clone(),
    );
    let consumer_task = tokio::spawn(consumer.run(shutdown.clone()));

    wait_until(
        "the pre-start events are replayed",
        Duration::from_secs(30),
        || aggregator.snapshot().recent_events.len() == 3,
    )
    .await;
    let kpis = aggregator.snapshot().kpis;
    assert_eq!(kpis.active_users, 2);
    assert_eq!(kpis.failed_last_window, 1);

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), consumer_task)
        .await
        .expect("the consumer must stop")
        .expect("no panic");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn events_published_to_kinesis_reach_the_metrics_endpoint() {
    let localstack = common::LocalStack::start().await;
    localstack.create_stream(STREAM).await;

    let aggregator = Arc::new(AdminAggregator::new(Arc::new(SystemClock)));
    let feed = Arc::new(AdminFeed::new(aggregator.clone(), Duration::from_secs(60)));
    let shutdown = CancellationToken::new();
    let config = Config::from_lookup(|name| match name {
        "HTTP_HOST" => Some(OsString::from("127.0.0.1")),
        "HTTP_PORT" => Some(OsString::from("8080")),
        "GATEWAY_SHARED_SECRET" => Some(OsString::from(SECRET)),
        _ => None,
    })
    .expect("valid config");
    let app = build_router(&config, feed, shutdown.clone());

    let consumer = SecurityEventConsumer::new(
        Arc::new(AwsRecordsSource::from_client(
            localstack.kinesis.clone(),
            STREAM.to_string(),
        )),
        aggregator.clone(),
    );
    let consumer_task = tokio::spawn(consumer.run(shutdown.clone()));

    // A probe (no effect on the KPIs) is published until it shows up in the
    // log, so the consumer is known to be reading before the real events go.
    let ready_deadline = Instant::now() + Duration::from_secs(30);
    let mut probe_id = 900;
    while aggregator.snapshot().recent_events.is_empty() {
        assert!(
            Instant::now() < ready_deadline,
            "the consumer never got ready"
        );
        probe_id += 1;
        put(
            &localstack,
            "probe",
            event(
                probe_id,
                "message.deadlettered",
                "2026-11-10T12:00:00Z",
                None,
                None,
                json!({"queue": "probe", "reason_code": "probe"}),
            ),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(600)).await;
    }

    let at = now_rfc3339();
    let nmap = |id: u64, event_type: &str, at: &str, payload: Value| {
        let mut body: Value =
            serde_json::from_slice(&event(id, event_type, at, None, Some("scan-e2e"), payload))
                .unwrap();
        body["source"] = json!("nmap-service");
        serde_json::to_vec(&body).unwrap()
    };
    let mut bodies = vec![
        event(1, "auth.login", &at, Some("admin-1"), None, json!({})),
        event(
            2,
            "scan.requested",
            &at,
            Some("admin-1"),
            Some("scan-e2e"),
            json!({"target": "198.51.100.4"}),
        ),
        event(
            3,
            "ratelimit.hit",
            &at,
            Some("admin-1"),
            None,
            json!({"route": "/api/scans", "limit": 10, "window_seconds": 60}),
        ),
        nmap(0xa1, "scan.started", &at, json!({})),
        nmap(
            0xa2,
            "finding.critical",
            &at,
            json!({"port": 22, "protocol": "tcp", "cve_id": "CVE-2023-38408",
                   "severity": "critical", "confidence": "confirmed"}),
        ),
        nmap(
            0xa3,
            "scan.completed",
            &at,
            json!({"duration_ms": 1200, "finding_counts":
                {"critical": 1, "high": 0, "medium": 0, "low": 0, "info": 0, "unknown": 0}}),
        ),
    ];
    bodies.push(b"this is not a security event".to_vec());
    for body in bodies {
        put(&localstack, "scan-e2e", body).await;
    }

    wait_until("every event aggregated", Duration::from_secs(30), || {
        let kpis = aggregator.snapshot().kpis;
        kpis.completed_last_window == 1 && kpis.malformed_records == 1
    })
    .await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header(GATEWAY_SECRET_HEADER, SECRET)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    let kpis = &body["kpis"];
    assert_eq!(
        kpis["scans_in_flight"], 0,
        "the scan already completed: {body}"
    );
    assert_eq!(kpis["completed_last_window"], 1);
    assert_eq!(kpis["failed_last_window"], 0);
    assert_eq!(kpis["failure_rate"], 0.0);
    assert_eq!(kpis["active_users"], 1);
    assert_eq!(kpis["rate_limit_hits"], 1);
    assert_eq!(kpis["malformed_records"], 1);
    assert_eq!(body["critical_findings"][0]["cve_id"], "CVE-2023-38408");
    assert_eq!(body["critical_findings"][0]["scan_id"], "scan-e2e");
    let types: Vec<&str> = body["recent_events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["event_type"].as_str().unwrap())
        .collect();
    for expected in [
        "auth.login",
        "scan.requested",
        "scan.started",
        "scan.completed",
    ] {
        assert!(
            types.contains(&expected),
            "{expected} missing from {types:?}"
        );
    }

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), consumer_task)
        .await
        .expect("the consumer must stop")
        .expect("no panic");
}
