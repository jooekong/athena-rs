use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::config::BackendConfig;

use super::connection::{ConnectionError, PooledConnection};

/// A pool for transaction-bound connections
///
/// Each session that starts a transaction gets a dedicated connection
/// that is held until the transaction completes.
pub struct TransactionPool {
    /// Backend configuration
    backend_config: Arc<BackendConfig>,
    /// Connections bound to sessions (session_id -> connection)
    bound: Mutex<HashMap<u32, PooledConnection>>,
}

impl TransactionPool {
    /// Create a new transaction pool
    pub fn new(backend_config: Arc<BackendConfig>) -> Self {
        Self {
            backend_config,
            bound: Mutex::new(HashMap::new()),
        }
    }

    /// Get the bound connection for a session, or create a new one
    pub async fn get_or_create(
        &self,
        session_id: u32,
        database: Option<String>,
    ) -> Result<(), ConnectionError> {
        let mut bound = self.bound.lock().await;

        if bound.contains_key(&session_id) {
            debug!(session_id = session_id, "Session already has bound connection");
            return Ok(());
        }

        // Create a new connection for this session
        debug!(session_id = session_id, "Creating bound connection for session");
        let mut conn = PooledConnection::connect(&self.backend_config, database).await?;
        conn.acquire();
        bound.insert(session_id, conn);
        Ok(())
    }

    /// Get the bound connection for a session
    pub async fn get(&self, session_id: u32) -> Option<()> {
        let bound = self.bound.lock().await;
        if bound.contains_key(&session_id) {
            Some(())
        } else {
            None
        }
    }

    /// Access the bound connection mutably
    pub async fn with_connection<F, R>(&self, session_id: u32, f: F) -> Option<R>
    where
        F: FnOnce(&mut PooledConnection) -> R,
    {
        let mut bound = self.bound.lock().await;
        bound.get_mut(&session_id).map(f)
    }

    /// Release the bound connection for a session
    ///
    /// This should be called when the transaction ends (COMMIT/ROLLBACK).
    /// The connection is dropped since transaction connections should not be reused.
    pub async fn release(&self, session_id: u32) {
        let mut bound = self.bound.lock().await;
        if let Some(mut conn) = bound.remove(&session_id) {
            // Reset connection state before dropping
            if conn.reset().await {
                debug!(session_id = session_id, "Reset and released bound connection");
            } else {
                warn!(session_id = session_id, "Failed to reset bound connection");
            }
        }
    }

    /// Check if a session has a bound connection
    pub async fn has_bound(&self, session_id: u32) -> bool {
        self.bound.lock().await.contains_key(&session_id)
    }

    /// Get number of bound connections
    pub async fn bound_count(&self) -> usize {
        self.bound.lock().await.len()
    }

    /// Force remove a session's connection (e.g., on session close)
    pub async fn force_release(&self, session_id: u32) {
        let mut bound = self.bound.lock().await;
        if bound.remove(&session_id).is_some() {
            debug!(session_id = session_id, "Force released bound connection");
        }
    }

    /// Send packet through bound connection
    pub async fn send(
        &self,
        session_id: u32,
        packet: crate::protocol::Packet,
    ) -> Result<(), ConnectionError> {
        let mut bound = self.bound.lock().await;
        if let Some(conn) = bound.get_mut(&session_id) {
            conn.send(packet).await
        } else {
            Err(ConnectionError::Disconnected)
        }
    }

    /// Receive packet from bound connection
    pub async fn recv(&self, session_id: u32) -> Result<crate::protocol::Packet, ConnectionError> {
        let mut bound = self.bound.lock().await;
        if let Some(conn) = bound.get_mut(&session_id) {
            conn.recv().await
        } else {
            Err(ConnectionError::Disconnected)
        }
    }
}
