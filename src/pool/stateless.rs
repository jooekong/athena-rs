use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::config::BackendConfig;

use super::connection::{ConnectionError, PooledConnection};

/// Configuration for stateless pool
#[derive(Debug, Clone)]
pub struct StatelessPoolConfig {
    /// Maximum number of idle connections to keep
    pub max_idle: usize,
    /// Maximum connection age before recycling
    pub max_age: Duration,
    /// Maximum idle time before closing
    pub max_idle_time: Duration,
}

impl Default for StatelessPoolConfig {
    fn default() -> Self {
        Self {
            max_idle: 10,
            max_age: Duration::from_secs(3600),      // 1 hour
            max_idle_time: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// A pool for stateless (non-transactional) connections
///
/// Connections are borrowed and returned after each request.
/// The pool maintains a set of idle connections for reuse.
pub struct StatelessPool {
    /// Pool configuration
    config: StatelessPoolConfig,
    /// Backend configuration
    backend_config: Arc<BackendConfig>,
    /// Idle connections
    idle: Mutex<VecDeque<PooledConnection>>,
    /// Current database for connections
    database: Option<String>,
}

impl StatelessPool {
    /// Create a new stateless pool
    pub fn new(
        backend_config: Arc<BackendConfig>,
        pool_config: StatelessPoolConfig,
        database: Option<String>,
    ) -> Self {
        Self {
            config: pool_config,
            backend_config,
            idle: Mutex::new(VecDeque::new()),
            database,
        }
    }

    /// Get a connection from the pool
    ///
    /// Returns an idle connection if available, otherwise creates a new one.
    pub async fn get(&self) -> Result<PooledConnection, ConnectionError> {
        // Try to get an idle connection
        {
            let mut idle = self.idle.lock().await;
            while let Some(mut conn) = idle.pop_front() {
                // Check if connection is still usable
                if conn.is_expired(self.config.max_age) {
                    debug!("Connection expired, discarding");
                    continue;
                }

                if conn.is_idle_too_long(self.config.max_idle_time) {
                    debug!("Connection idle too long, discarding");
                    continue;
                }

                // Check if database matches
                if self.database != conn.database {
                    if let Some(ref db) = self.database {
                        if conn.change_database(db).await.is_err() {
                            warn!("Failed to change database, discarding connection");
                            continue;
                        }
                    }
                }

                conn.acquire();
                debug!("Reusing idle connection");
                return Ok(conn);
            }
        }

        // Create a new connection
        debug!("Creating new connection");
        let mut conn = PooledConnection::connect(&self.backend_config, self.database.clone()).await?;
        conn.acquire();
        Ok(conn)
    }

    /// Return a connection to the pool
    ///
    /// If the pool is full, the connection is dropped.
    /// Connection is reset before returning to pool to prevent state leakage.
    pub async fn put(&self, mut conn: PooledConnection) {
        conn.release();

        // Check if connection is still usable
        if !conn.is_usable() {
            debug!("Connection not usable, discarding");
            return;
        }

        if conn.is_expired(self.config.max_age) {
            debug!("Connection expired, discarding");
            return;
        }

        // Reset connection state to prevent session state leakage
        // COM_RESET_CONNECTION clears session variables, temp tables, prepared statements, etc.
        if !conn.reset().await {
            warn!("Failed to reset connection, discarding");
            return;
        }

        let mut idle = self.idle.lock().await;

        // Check if pool is full
        if idle.len() >= self.config.max_idle {
            debug!("Pool full, discarding connection");
            return;
        }

        idle.push_back(conn);
        debug!(idle_count = idle.len(), "Returned connection to pool");
    }

    /// Get current number of idle connections
    pub async fn idle_count(&self) -> usize {
        self.idle.lock().await.len()
    }

    /// Close all idle connections
    pub async fn close_all(&self) {
        let mut idle = self.idle.lock().await;
        for conn in idle.drain(..) {
            drop(conn);
        }
        debug!("Closed all idle connections");
    }

    /// Get backend address (host:port) for this pool
    pub fn backend_addr(&self) -> String {
        format!("{}:{}", self.backend_config.host, self.backend_config.port)
    }
}
