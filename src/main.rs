mod circuit;
mod config;
mod group;
mod health;
mod metrics;
mod parser;
mod pool;
mod protocol;
mod router;
mod session;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::signal;
use tokio::task::JoinSet;
use tracing::{error, info, warn, Level};
use tracing_subscriber::EnvFilter;

use circuit::{ConcurrencyController, LimitConfig};
use config::Config;
use group::GroupManager;
use session::Session;

/// Global connection counter for generating unique session IDs
static CONNECTION_COUNTER: AtomicU32 = AtomicU32::new(1);

/// Graceful shutdown timeout (wait for connections to close)
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

    // Load configuration
    let config = load_or_default_config();

    // Create group manager (groups are required)
    let group_manager = Arc::new(GroupManager::new(&config).await);
    info!(
        groups = ?group_manager.group_names(),
        "Groups configured"
    );

    // Create concurrency controller for rate limiting (legacy, will migrate to per-instance)
    let limit_config = LimitConfig::from(&config.circuit);
    let concurrency_controller = Arc::new(ConcurrencyController::new(limit_config));

    info!(
        enabled = config.circuit.enabled,
        max_concurrent = config.circuit.max_concurrent_per_user_shard,
        queue_size = config.circuit.queue_size,
        queue_timeout_ms = config.circuit.queue_timeout_ms,
        "Circuit breaker configured"
    );

    let addr = format!("{}:{}", config.server.listen_addr, config.server.listen_port);
    let listener = TcpListener::bind(&addr).await?;

    info!(addr = %addr, "Athena MySQL proxy listening");

    // Start metrics server in background
    let metrics_addr = format!(
        "{}:{}",
        config.server.listen_addr,
        config.server.listen_port + 1000
    );
    info!(metrics_addr = %metrics_addr, "Metrics server starting");
    tokio::spawn(async move {
        if let Err(e) = metrics::start_metrics_server(&metrics_addr).await {
            error!(error = %e, "Metrics server failed");
        }
    });

    // Track active sessions for graceful shutdown
    let mut sessions: JoinSet<()> = JoinSet::new();

    // Main accept loop with graceful shutdown support
    loop {
        tokio::select! {
            // Handle shutdown signals
            _ = shutdown_signal() => {
                info!("Shutdown signal received, stopping accept loop");
                break;
            }

            // Accept new connections
            accept_result = listener.accept() => {
                let (stream, peer_addr) = match accept_result {
                    Ok(v) => v,
                    Err(e) => {
                        error!(error = %e, "Failed to accept connection");
                        continue;
                    }
                };

                let session_id = CONNECTION_COUNTER.fetch_add(1, Ordering::SeqCst);
                let group_manager = group_manager.clone();
                let concurrency_controller = concurrency_controller.clone();

                info!(session_id = session_id, peer = %peer_addr, "New connection");
                metrics::metrics().record_connection_accepted();

                sessions.spawn(async move {
                    let session = Session::with_group_manager(
                        session_id,
                        group_manager,
                        concurrency_controller,
                    );
                    if let Err(e) = session.run(stream).await {
                        warn!(session_id = session_id, error = %e, "Session ended with error");
                    } else {
                        info!(session_id = session_id, "Session ended");
                    }
                    metrics::metrics().record_connection_closed();
                });
            }
        }
    }

    // Graceful shutdown: wait for active sessions to complete
    let active_count = sessions.len();
    if active_count > 0 {
        info!(
            active_sessions = active_count,
            timeout_secs = GRACEFUL_SHUTDOWN_TIMEOUT.as_secs(),
            "Waiting for active sessions to complete"
        );

        let shutdown_deadline = tokio::time::Instant::now() + GRACEFUL_SHUTDOWN_TIMEOUT;

        loop {
            if sessions.is_empty() {
                info!("All sessions completed gracefully");
                break;
            }

            tokio::select! {
                _ = tokio::time::sleep_until(shutdown_deadline) => {
                    let remaining = sessions.len();
                    warn!(
                        remaining_sessions = remaining,
                        "Graceful shutdown timeout, aborting remaining sessions"
                    );
                    sessions.abort_all();
                    break;
                }

                Some(result) = sessions.join_next() => {
                    if let Err(e) = result {
                        if !e.is_cancelled() {
                            error!(error = %e, "Session task panicked");
                        }
                    }
                    let remaining = sessions.len();
                    if remaining > 0 {
                        info!(remaining_sessions = remaining, "Session completed during shutdown");
                    }
                }
            }
        }
    }

    info!("Athena MySQL proxy shutdown complete");
    Ok(())
}

/// Wait for shutdown signal (SIGTERM or SIGINT)
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

fn load_or_default_config() -> Config {
    // Try to load from config file
    let config_paths = ["config/athena.toml", "athena.toml"];

    for path in config_paths {
        match config::load_config(path) {
            Ok(config) => {
                info!(path = path, "Loaded configuration");
                return config;
            }
            Err(e) => {
                warn!(path = path, error = %e, "Failed to load config");
            }
        }
    }

    info!("Using default configuration");
    Config::default()
}
