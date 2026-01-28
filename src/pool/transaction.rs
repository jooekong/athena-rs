use std::collections::HashMap;

use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::config::BackendConfig;

use super::connection::{ConnectionError, PooledConnection};
use super::stateless::StatelessPool;

/// A pool for transaction-bound connections
///
/// Each session that starts a transaction gets a dedicated connection
/// that is held until the transaction completes.
pub struct TransactionPool {
    /// Connections bound to sessions (session_id -> connection)
    bound: Mutex<HashMap<u32, PooledConnection>>,
}

impl Default for TransactionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionPool {
    /// Create a new transaction pool
    pub fn new() -> Self {
        Self {
            bound: Mutex::new(HashMap::new()),
        }
    }

    /// Get the bound connection for a session, or create a new one
    ///
    /// The backend_config determines which shard the transaction connects to.
    /// The connection is created with autocommit=0 for transaction semantics.
    pub async fn get_or_create(
        &self,
        session_id: u32,
        backend_config: &BackendConfig,
        database: Option<String>,
    ) -> Result<(), ConnectionError> {
        let mut bound = self.bound.lock().await;

        if bound.contains_key(&session_id) {
            debug!(session_id = session_id, "Session already has bound connection");
            return Ok(());
        }

        // Create a new connection for this session using the specified backend
        debug!(session_id = session_id, host = %backend_config.host, "Creating bound connection for session");
        let mut conn = PooledConnection::connect(backend_config, database).await?;
        conn.acquire();

        // Set autocommit=0 for transaction semantics
        // This allows implicit transaction without explicit BEGIN
        Self::set_autocommit(&mut conn, false).await?;

        bound.insert(session_id, conn);
        Ok(())
    }

    /// Set autocommit mode on the connection
    async fn set_autocommit(conn: &mut PooledConnection, on: bool) -> Result<(), ConnectionError> {
        use crate::protocol::{is_err_packet, is_ok_packet, Packet};

        let value = if on { "1" } else { "0" };
        let mut payload = vec![0x03]; // COM_QUERY
        payload.extend_from_slice(format!("SET autocommit={}", value).as_bytes());
        let packet = Packet {
            sequence_id: 0,
            payload: payload.into(),
        };

        conn.send(packet).await?;

        let response = conn.recv().await?;
        if is_err_packet(&response.payload) {
            return Err(ConnectionError::Protocol(format!(
                "Failed to set autocommit={}",
                value
            )));
        }
        if !is_ok_packet(&response.payload) {
            return Err(ConnectionError::Protocol(format!(
                "Expected OK after SET autocommit={}",
                value
            )));
        }

        debug!("Set autocommit={} on connection", value);
        Ok(())
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
    /// For normally committed/rolled-back transactions, the connection is clean
    /// and can be returned to the stateless pool after restoring autocommit=1.
    /// For abnormal exits (transaction not completed), the connection is discarded.
    pub async fn release(
        &self,
        session_id: u32,
        transaction_completed: bool,
        stateless_pool: Option<&StatelessPool>,
    ) {
        if let Some(mut conn) = self.bound.lock().await.remove(&session_id) {
            if transaction_completed {
                // Transaction was properly committed/rolled back, connection is clean
                if Self::set_autocommit(&mut conn, true).await.is_ok() {
                    if let Some(pool) = stateless_pool {
                        debug!(session_id, "Returning clean transaction connection to pool");
                        pool.put(conn).await;
                        return;
                    }
                }
            }
            // Abnormal exit or failed to restore autocommit - discard connection
            debug!(session_id, transaction_completed, "Discarding transaction connection");
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

    /// Get capabilities of the bound connection
    pub async fn capabilities(&self, session_id: u32) -> Option<u32> {
        let bound = self.bound.lock().await;
        bound.get(&session_id).map(|conn| conn.capabilities())
    }
}
