mod circuit;
mod config;
mod parser;
mod pool;
mod protocol;
mod router;
mod session;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::{error, info, warn, Level};
use tracing_subscriber::EnvFilter;

use circuit::{ConcurrencyController, LimitConfig};
use config::Config;
use pool::{PoolManager, StatelessPoolConfig};
use session::Session;

/// Global connection counter for generating unique session IDs
static CONNECTION_COUNTER: AtomicU32 = AtomicU32::new(1);

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

    // Create pool manager
    let pool_manager = Arc::new(PoolManager::new(
        config.backend.clone(),
        StatelessPoolConfig::default(),
    ));

    // Create concurrency controller for rate limiting
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

    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "Failed to accept connection");
                continue;
            }
        };

        let session_id = CONNECTION_COUNTER.fetch_add(1, Ordering::SeqCst);
        let pool_manager = pool_manager.clone();
        let concurrency_controller = concurrency_controller.clone();

        info!(session_id = session_id, peer = %peer_addr, "New connection");

        tokio::spawn(async move {
            let session = Session::new(session_id, pool_manager, concurrency_controller);
            if let Err(e) = session.run(stream).await {
                warn!(session_id = session_id, error = %e, "Session ended with error");
            } else {
                info!(session_id = session_id, "Session ended");
            }
        });
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
