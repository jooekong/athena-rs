use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_queue::SegQueue;
use tracing::debug;

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
///
/// This pool is lock-free for high concurrency performance.
/// Each pool is defined by endpoint:port/database+user, so all connections
/// in the pool share the same database and credentials.
pub struct StatelessPool {
    /// Pool configuration
    config: StatelessPoolConfig,
    /// Backend configuration
    backend_config: Arc<BackendConfig>,
    /// Idle connections (lock-free queue)
    idle: SegQueue<PooledConnection>,
    /// Current idle count (approximate, for pool-full check)
    idle_count: AtomicUsize,
    /// Database for this pool (used when creating new connections)
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
            idle: SegQueue::new(),
            idle_count: AtomicUsize::new(0),
            database,
        }
    }

    /// Get a connection from the pool
    ///
    /// Returns an idle connection if available, otherwise creates a new one.
    /// This method is lock-free.
    pub async fn get(&self) -> Result<PooledConnection, ConnectionError> {
        // Try to get an idle connection (lock-free)
        while let Some(mut conn) = self.idle.pop() {
            self.idle_count.fetch_sub(1, Ordering::Relaxed);

            // Check if connection is still usable
            if conn.is_expired(self.config.max_age) {
                debug!("Connection expired, discarding");
                continue;
            }

            if conn.is_idle_too_long(self.config.max_idle_time) {
                debug!("Connection idle too long, discarding");
                continue;
            }

            conn.acquire();
            debug!("Reusing idle connection");
            return Ok(conn);
        }

        // Create a new connection
        debug!("Creating new connection");
        let mut conn =
            PooledConnection::connect(&self.backend_config, self.database.clone()).await?;
        conn.acquire();
        Ok(conn)
    }

    /// Return a connection to the pool
    ///
    /// If the pool is full, the connection is dropped.
    /// This method is lock-free.
    ///
    /// Note: No reset() is called because:
    /// 1. Session variables are not supported (intercepted at proxy level)
    /// 2. Each pool is defined by endpoint:port/database+user, no database switching needed
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

        // Best-effort pool size limit. Due to lock-free design, actual idle count
        // may slightly exceed max_idle under high concurrency. This is acceptable
        // as it only affects memory usage marginally.
        if self.idle_count.load(Ordering::Relaxed) >= self.config.max_idle {
            debug!("Pool full, discarding connection");
            return;
        }

        self.idle.push(conn);
        self.idle_count.fetch_add(1, Ordering::Relaxed);
        debug!(
            idle_count = self.idle_count.load(Ordering::Relaxed),
            "Returned connection to pool"
        );
    }

    /// Get current number of idle connections (approximate)
    pub fn idle_count(&self) -> usize {
        self.idle_count.load(Ordering::Relaxed)
    }

    /// Close all idle connections
    pub fn close_all(&self) {
        while self.idle.pop().is_some() {
            self.idle_count.fetch_sub(1, Ordering::Relaxed);
        }
        debug!("Closed all idle connections");
    }

    /// Get backend address (host:port) for this pool
    pub fn backend_addr(&self) -> String {
        format!("{}:{}", self.backend_config.host, self.backend_config.port)
    }
}
