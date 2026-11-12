//! `events-service`: read side of the `security-events` Kinesis stream.
//!
//! Internal service behind the gateway. It owns the Kinesis consumer
//! ([`kinesis_consumer`]), the aggregator ([`admin`]) and the admin HTTP
//! surface ([`admin_routes`]: `GET /metrics`, `GET /events`), all behind the
//! shared-secret middleware ([`auth`]). `GET /health` is the only open route.
//!
//! ## Shutdown
//!
//! `SIGTERM` (how ECS stops a task) and `Ctrl-C` cancel one
//! [`CancellationToken`]. The token stops the consumer loop and ends every
//! open SSE stream, then the HTTP server drains and the process exits.

#![deny(missing_docs)]

pub mod admin;
pub mod admin_routes;
pub mod auth;
pub mod config;
pub mod event_contract;
pub mod kinesis_consumer;
pub mod security_event;
pub mod wiring;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// How long the HTTP server may keep draining after the shutdown signal.
const HTTP_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the consumer task may take to stop before it is aborted.
const CONSUMER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Starts the service: loads [`config::Config`] from the environment, starts
/// the consumer (when a stream is configured) and serves the router on
/// `HTTP_HOST:HTTP_PORT` until `SIGTERM` or `Ctrl-C`.
///
/// # Errors
///
/// Returns a [`RunError`] when the configuration is invalid or the HTTP
/// server cannot bind or keeps failing.
pub async fn run() -> Result<(), RunError> {
    let config = config::Config::from_env()?;
    let feed = Arc::new(admin::AdminFeed::default());
    let shutdown = CancellationToken::new();

    let consumer = wiring::build_consumer(&config, &feed)
        .await
        .map(|consumer| tokio::spawn(consumer.run(shutdown.clone())));
    let app = wiring::build_router(&config, Arc::clone(&feed), shutdown.clone());

    let listener = match TcpListener::bind((config.http_host.as_str(), config.http_port)).await {
        Ok(listener) => listener,
        Err(source) => {
            stop_consumer(&shutdown, consumer).await;
            return Err(RunError::Bind(source));
        }
    };
    tracing::info!(host = %config.http_host, port = config.http_port, "events-service listening");

    serve_until(listener, app, shutdown, consumer, shutdown_signal()).await
}

/// Serves `app` on `listener` until `signal` completes, then cancels
/// `shutdown` (stopping the consumer and the SSE streams), drains the HTTP
/// server and waits for `consumer` to stop.
///
/// # Errors
///
/// Returns [`RunError::Serve`] when the HTTP server stops with an error.
pub async fn serve_until(
    listener: TcpListener,
    app: axum::Router,
    shutdown: CancellationToken,
    consumer: Option<JoinHandle<()>>,
    signal: impl Future<Output = ()> + Send + 'static,
) -> Result<(), RunError> {
    let signal_token = shutdown.clone();
    let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
        signal.await;
        tracing::info!("shutdown signal received; stopping the server and the consumer");
        signal_token.cancel();
    });
    let served = tokio::select! {
        result = serve => result,
        () = async {
            shutdown.cancelled().await;
            tokio::time::sleep(HTTP_DRAIN_TIMEOUT).await;
        } => {
            tracing::warn!("HTTP connections still open after the drain timeout; closing them");
            Ok(())
        }
    };
    stop_consumer(&shutdown, consumer).await;
    served.map_err(RunError::Serve)
}

/// Completes on `Ctrl-C` (`SIGINT`) or, on Unix, `SIGTERM`.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "could not install the Ctrl-C handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "could not install the SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

/// Cancels `token` and waits up to [`CONSUMER_SHUTDOWN_TIMEOUT`] for the
/// consumer task; aborts it when it does not stop (or panicked).
async fn stop_consumer(token: &CancellationToken, consumer: Option<JoinHandle<()>>) {
    token.cancel();
    let Some(mut handle) = consumer else { return };
    match tokio::time::timeout(CONSUMER_SHUTDOWN_TIMEOUT, &mut handle).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::error!(%error, "the consumer task panicked"),
        Err(_) => {
            tracing::warn!("the consumer did not stop in time; aborting it");
            handle.abort();
        }
    }
}

/// Error returned by [`run`] when the service cannot start.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// The configuration could not be loaded from the environment.
    #[error("could not load configuration: {0}")]
    Config(#[from] config::ConfigError),
    /// The HTTP listener could not bind the configured host and port.
    #[error("could not bind the HTTP listener: {0}")]
    Bind(std::io::Error),
    /// The HTTP server stopped with an error.
    #[error("the HTTP server stopped with an error: {0}")]
    Serve(std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::admin::AdminFeed;
    use crate::kinesis_consumer::{
        RecordsBatch, RecordsSource, SecurityEventConsumer, SourceError,
    };

    /// A stream with one shard and no records.
    struct IdleSource;

    #[async_trait::async_trait]
    impl RecordsSource for IdleSource {
        async fn list_open_shards(&self) -> Result<Vec<String>, SourceError> {
            Ok(vec!["shard-0".to_string()])
        }
        async fn latest_iterator(&self, _shard_id: &str) -> Result<String, SourceError> {
            Ok("it".to_string())
        }
        async fn get_records(&self, iterator: &str) -> Result<RecordsBatch, SourceError> {
            Ok(RecordsBatch {
                records: vec![],
                next_iterator: Some(iterator.to_string()),
            })
        }
    }

    #[tokio::test]
    async fn the_shutdown_signal_stops_the_consumer_and_the_server() {
        let feed = Arc::new(AdminFeed::default());
        let shutdown = CancellationToken::new();
        let consumer =
            SecurityEventConsumer::new(Arc::new(IdleSource), Arc::clone(&feed.aggregator));
        let stopped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stopped);
        let token = shutdown.clone();
        let handle = tokio::spawn(async move {
            consumer.run(token).await;
            flag.store(true, Ordering::SeqCst);
        });
        let config = config::Config::from_lookup(|name| match name {
            "HTTP_HOST" => Some("127.0.0.1".into()),
            "HTTP_PORT" => Some("0".into()),
            "GATEWAY_SHARED_SECRET" => Some("s".into()),
            _ => None,
        })
        .unwrap();
        let app = wiring::build_router(&config, feed, shutdown.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel::<()>();

        let server = tokio::spawn(serve_until(
            listener,
            app,
            shutdown,
            Some(handle),
            async move {
                let _ = signal_rx.await;
            },
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !stopped.load(Ordering::SeqCst),
            "the consumer runs until the signal"
        );

        signal_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("serve_until must return after the signal")
            .unwrap()
            .unwrap();
        assert!(stopped.load(Ordering::SeqCst), "the consumer stopped");
    }
}
