//! `events-service`: read side of the `security-events` Kinesis stream.
//!
//! Internal service behind the gateway. This scaffold only provides the
//! configuration, the unauthenticated `/health` route and the shared-secret
//! middleware; the consumer, aggregator and admin SSE arrive in later tasks.

#![deny(missing_docs)]

pub mod auth;
pub mod config;
pub mod wiring;

/// Starts the service: loads [`config::Config`] from the environment, builds
/// the router and serves it on `HTTP_HOST:HTTP_PORT`.
///
/// # Errors
///
/// Returns a [`RunError`] when the configuration is invalid or the HTTP
/// server cannot bind or keeps failing.
pub async fn run() -> Result<(), RunError> {
    let config = config::Config::from_env()?;
    let app = wiring::build_router(&config);

    let listener = tokio::net::TcpListener::bind((config.http_host.as_str(), config.http_port))
        .await
        .map_err(RunError::Bind)?;
    tracing::info!(host = %config.http_host, port = config.http_port, "events-service listening");

    axum::serve(listener, app).await.map_err(RunError::Serve)
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
