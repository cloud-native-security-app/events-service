//! Thin binary wrapper: initializes `tracing` and delegates to [`events_service::run`].

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    if let Err(error) = events_service::run().await {
        tracing::error!(%error, "events-service failed to start");
        std::process::exit(1);
    }
}
