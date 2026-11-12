//! Admin HTTP surface: `GET /metrics` (JSON snapshot) and `GET /events` (SSE).
//!
//! Both routes are mounted behind the shared-secret middleware by
//! [`crate::wiring::build_router`]. Authorization of the *admin* stays in the
//! gateway: this service only trusts the shared secret.
//!
//! ## SSE protocol of `GET /events`
//!
//! | Event | Data | When |
//! |---|---|---|
//! | `snapshot` | [`AdminSnapshot`] | first; after a client lag; and every [`AdminFeed::snapshot_interval`] |
//! | `delta` | [`AdminDelta`] | for every new event (or malformed record) |
//!
//! Plus `: ping` keep-alive comments every 15 s. The snapshot and the
//! subscription are taken atomically
//! ([`crate::admin::AdminAggregator::subscribe_with_snapshot`]), so the first `delta` is the
//! one right after the snapshot (consecutive `seq`).
//!
//! The stream ends when the service is shutting down (the shutdown token is
//! cancelled) so graceful shutdown never waits for a long-lived connection.
//!
//! ## Design decisions
//!
//! - **Periodic snapshot**: the 15-minute window expires without any event
//!   arriving, so the KPIs of an idle client would go stale. This used to ride
//!   on the gateway's role re-check tick; it now lives here so the gateway can
//!   pass the stream through.
//! - **Slow client**: when the `broadcast` receiver falls behind its capacity
//!   ([`crate::admin::DELTA_CHANNEL_CAPACITY`]) there is no error: a fresh
//!   snapshot is taken with a new receiver and the stream continues.
//! - **No PII**: see [`crate::admin`].

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::stream::{self, Stream};
use tokio::sync::broadcast;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::admin::{AdminDelta, AdminFeed, AdminSnapshot};

/// How often the server sends a `: ping` comment to keep the connection alive
/// through proxies.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Shared state of the admin handlers.
#[derive(Clone)]
struct RouteState {
    feed: Arc<AdminFeed>,
    shutdown: CancellationToken,
}

/// Admin routes (`/metrics`, `/events`). The caller layers the shared-secret
/// middleware on top.
pub fn admin_router(feed: Arc<AdminFeed>, shutdown: CancellationToken) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .route("/events", get(events))
        .with_state(RouteState { feed, shutdown })
}

async fn metrics(State(state): State<RouteState>) -> Json<AdminSnapshot> {
    Json(state.feed.aggregator.snapshot())
}

async fn events(
    State(state): State<RouteState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    Sse::new(events_stream(state.feed, state.shutdown))
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE_INTERVAL).text("ping"))
}

struct StreamState {
    feed: Arc<AdminFeed>,
    shutdown: CancellationToken,
    receiver: broadcast::Receiver<AdminDelta>,
    ticker: tokio::time::Interval,
    /// Events already built and still to send, in order.
    pending: VecDeque<Event>,
}

fn snapshot_event(snapshot: &AdminSnapshot) -> Event {
    Event::default()
        .event("snapshot")
        .json_data(snapshot)
        .unwrap_or_else(|_| serialization_error_event())
}

fn delta_event(delta: &AdminDelta) -> Event {
    Event::default()
        .event("delta")
        .json_data(delta)
        .unwrap_or_else(|_| serialization_error_event())
}

fn serialization_error_event() -> Event {
    Event::default()
        .event("error")
        .data("no se pudo serializar el evento")
}

/// Builds the stream of one client. Separate from the handler so lag and
/// shutdown can be tested without HTTP.
pub(crate) fn events_stream(
    feed: Arc<AdminFeed>,
    shutdown: CancellationToken,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let (snapshot, receiver) = feed.aggregator.subscribe_with_snapshot();
    let period = feed.snapshot_interval;
    let mut ticker = tokio::time::interval_at(Instant::now() + period, period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let state = StreamState {
        feed,
        shutdown,
        receiver,
        ticker,
        pending: VecDeque::from([snapshot_event(&snapshot)]),
    };

    stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                return Some((Ok(event), state));
            }
            tokio::select! {
                () = state.shutdown.cancelled() => return None,
                received = state.receiver.recv() => match received {
                    Ok(delta) => state.pending.push_back(delta_event(&delta)),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::debug!(skipped, "admin SSE client lagged; resending a snapshot");
                        resync(&mut state);
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                },
                _ = state.ticker.tick() => resync(&mut state),
            }
        }
    })
}

/// Replaces the receiver with a new one and queues a snapshot consistent with
/// it (drops the lagged deltas, which the snapshot already reflects).
fn resync(state: &mut StreamState) {
    let (snapshot, receiver) = state.feed.aggregator.subscribe_with_snapshot();
    state.receiver = receiver;
    state.pending.clear();
    state.pending.push_back(snapshot_event(&snapshot));
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

    use super::*;
    use crate::admin::{AdminAggregator, ManualClock, DELTA_CHANNEL_CAPACITY};
    use crate::security_event::SecurityEvent;

    fn login(n: u64) -> SecurityEvent {
        serde_json::from_value(serde_json::json!({
            "event_id": format!("00000000-0000-4000-8000-{n:012}"),
            "event_type": "auth.login",
            "occurred_at": "2026-11-10T12:00:00Z",
            "source": "gateway",
            "actor_sub": format!("user-{n}"),
            "payload": {}
        }))
        .unwrap()
    }

    fn feed(interval: Duration) -> (Arc<AdminAggregator>, Arc<AdminFeed>) {
        let aggregator = Arc::new(AdminAggregator::new(Arc::new(ManualClock::new(0))));
        let feed = Arc::new(AdminFeed::new(aggregator.clone(), interval));
        (aggregator, feed)
    }

    #[tokio::test]
    async fn a_lagging_client_gets_a_fresh_snapshot_instead_of_an_error() {
        let (aggregator, feed) = feed(Duration::from_secs(3600));
        let mut stream = std::pin::pin!(events_stream(feed, CancellationToken::new()));

        let first = format!("{:?}", stream.next().await.unwrap().unwrap());
        assert!(first.contains("snapshot"), "{first}");

        // The client does not read while the channel overflows.
        for n in 0..(DELTA_CHANNEL_CAPACITY as u64 + 20) {
            aggregator.ingest(&login(n));
        }

        let next = format!("{:?}", stream.next().await.unwrap().unwrap());
        assert!(
            next.contains("snapshot"),
            "after the lag a snapshot arrives: {next}"
        );
        assert!(
            next.contains(&format!("\\\"seq\\\":{}", DELTA_CHANNEL_CAPACITY + 20)),
            "the snapshot reflects everything ingested: {next}"
        );

        aggregator.ingest(&login(10_000));
        let after = format!("{:?}", stream.next().await.unwrap().unwrap());
        assert!(after.contains("delta"), "deltas follow: {after}");
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_client_gets_a_periodic_snapshot() {
        let (_aggregator, feed) = feed(Duration::from_secs(60));
        let mut stream = std::pin::pin!(events_stream(feed, CancellationToken::new()));
        let _first = stream.next().await.unwrap().unwrap();

        let tick = format!("{:?}", stream.next().await.unwrap().unwrap());
        assert!(tick.contains("snapshot"), "{tick}");
    }

    #[tokio::test]
    async fn the_stream_ends_when_the_shutdown_token_is_cancelled() {
        let (_aggregator, feed) = feed(Duration::from_secs(3600));
        let shutdown = CancellationToken::new();
        let mut stream = std::pin::pin!(events_stream(feed, shutdown.clone()));
        let _first = stream.next().await.unwrap().unwrap();

        shutdown.cancel();
        assert!(stream.next().await.is_none());
    }
}
