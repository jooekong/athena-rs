use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::debug;

use crate::config::BackendConfig;

use super::connection::{ConnectionError, PooledConnection};
use super::stateless::{StatelessPool, StatelessPoolConfig};
use super::transaction::TransactionPool;

/// Identifier for a shard/backend
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardId(pub String);

impl ShardId {
    pub fn default_shard() -> Self {
        Self("default".to_string())
    }
}

/// Configuration for a shard backend
#[derive(Debug, Clone)]
pub struct ShardBackend {
    /// Shard identifier
    pub shard_id: ShardId,
    /// Master backend configuration
    pub master: BackendConfig,
    /// Slave backend configurations (for read replicas)
    pub slaves: Vec<BackendConfig>,
}

/// Pool manager that manages connection pools for multiple shards
pub struct PoolManager {
    /// Stateless pools per shard (for master)
    stateless_pools: RwLock<HashMap<ShardId, Arc<StatelessPool>>>,
    /// Stateless pools for slaves per shard
    slave_pools: RwLock<HashMap<ShardId, Vec<Arc<StatelessPool>>>>,
    /// Transaction pool (shared, connections are bound to sessions)
    transaction_pool: Arc<TransactionPool>,
    /// Backend configurations per shard
    backends: RwLock<HashMap<ShardId, ShardBackend>>,
    /// Pool configuration
    pool_config: StatelessPoolConfig,
    /// Slave selection counter (round-robin)
    slave_counter: std::sync::atomic::AtomicUsize,
}

impl PoolManager {
    /// Create a new pool manager with a single default backend
    pub fn new(backend_config: BackendConfig, pool_config: StatelessPoolConfig) -> Self {
        let backend_config = Arc::new(backend_config);
        let transaction_pool = Arc::new(TransactionPool::new());

        let mut stateless_pools = HashMap::new();
        let default_pool = Arc::new(StatelessPool::new(
            backend_config.clone(),
            pool_config.clone(),
            None,
        ));
        stateless_pools.insert(ShardId::default_shard(), default_pool);

        let mut backends = HashMap::new();
        backends.insert(
            ShardId::default_shard(),
            ShardBackend {
                shard_id: ShardId::default_shard(),
                master: (*backend_config).clone(),
                slaves: vec![],
            },
        );

        Self {
            stateless_pools: RwLock::new(stateless_pools),
            slave_pools: RwLock::new(HashMap::new()),
            transaction_pool,
            backends: RwLock::new(backends),
            pool_config,
            slave_counter: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Add a shard backend
    pub async fn add_shard(&self, backend: ShardBackend) {
        let shard_id = backend.shard_id.clone();
        let master_config = Arc::new(backend.master.clone());

        // Create master pool
        let master_pool = Arc::new(StatelessPool::new(
            master_config.clone(),
            self.pool_config.clone(),
            None,
        ));

        // Create slave pools
        let slave_pools: Vec<Arc<StatelessPool>> = backend
            .slaves
            .iter()
            .map(|slave_config| {
                Arc::new(StatelessPool::new(
                    Arc::new(slave_config.clone()),
                    self.pool_config.clone(),
                    None,
                ))
            })
            .collect();

        {
            let mut pools = self.stateless_pools.write().await;
            pools.insert(shard_id.clone(), master_pool);
        }

        if !slave_pools.is_empty() {
            let mut slaves = self.slave_pools.write().await;
            slaves.insert(shard_id.clone(), slave_pools);
        }

        {
            let mut backends = self.backends.write().await;
            backends.insert(shard_id.clone(), backend);
        }

        debug!(shard_id = ?shard_id, "Added shard backend");
    }

    /// Get a connection from the master pool for a shard
    pub async fn get_master(
        &self,
        shard_id: &ShardId,
        database: Option<String>,
    ) -> Result<PooledConnection, ConnectionError> {
        let pools = self.stateless_pools.read().await;
        let pool = if let Some(pool) = pools.get(shard_id) {
            pool.clone()
        } else {
            // Fall back to default if shard not found
            pools
                .get(&ShardId::default_shard())
                .ok_or(ConnectionError::Disconnected)?
                .clone()
        };
        drop(pools); // Release lock before async operation

        let mut conn = pool.get().await?;

        // Switch database if needed
        if let Some(ref db) = database {
            if conn.database.as_ref() != Some(db) {
                conn.change_database(db).await?;
            }
        }

        Ok(conn)
    }

    /// Get a connection from a slave pool for a shard (round-robin)
    ///
    /// Falls back to master if no slaves available.
    pub async fn get_slave(
        &self,
        shard_id: &ShardId,
        database: Option<String>,
    ) -> Result<PooledConnection, ConnectionError> {
        let slaves = self.slave_pools.read().await;
        if let Some(slave_pools) = slaves.get(shard_id) {
            if !slave_pools.is_empty() {
                // Round-robin selection
                let idx = self
                    .slave_counter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    % slave_pools.len();
                let pool = slave_pools[idx].clone();
                drop(slaves); // Release lock before async operation

                let mut conn = pool.get().await?;

                // Switch database if needed
                if let Some(ref db) = database {
                    if conn.database.as_ref() != Some(db) {
                        conn.change_database(db).await?;
                    }
                }

                return Ok(conn);
            }
        }
        drop(slaves); // Release lock before fallback

        // Fall back to master
        self.get_master(shard_id, database).await
    }

    /// Return a connection to the master pool
    pub async fn put_master(&self, shard_id: &ShardId, conn: PooledConnection) {
        let pools = self.stateless_pools.read().await;
        if let Some(pool) = pools.get(shard_id) {
            pool.put(conn).await;
        } else if let Some(pool) = pools.get(&ShardId::default_shard()) {
            pool.put(conn).await;
        }
    }

    /// Return a connection to a slave pool
    ///
    /// Finds the correct slave pool by matching the connection's backend address.
    pub async fn put_slave(&self, shard_id: &ShardId, conn: PooledConnection) {
        let conn_addr = conn.backend_addr().to_string();

        // Find the slave pool that matches this connection's backend address
        let matching_pool = {
            let slaves = self.slave_pools.read().await;
            if let Some(slave_pools) = slaves.get(shard_id) {
                slave_pools
                    .iter()
                    .find(|pool| pool.backend_addr() == conn_addr)
                    .cloned()
            } else {
                None
            }
        };

        if let Some(pool) = matching_pool {
            pool.put(conn).await;
        } else {
            // Connection didn't match any slave pool, discard it
            debug!(
                shard = %shard_id.0,
                addr = %conn_addr,
                "Slave connection didn't match any pool, discarding"
            );
            drop(conn);
        }
    }

    /// Get connection based on route target (Master or Slave)
    pub async fn get_for_target(
        &self,
        shard_id: &ShardId,
        target: crate::router::RouteTarget,
        database: Option<String>,
    ) -> Result<PooledConnection, ConnectionError> {
        match target {
            crate::router::RouteTarget::Master => self.get_master(shard_id, database).await,
            crate::router::RouteTarget::Slave => self.get_slave(shard_id, database).await,
        }
    }

    /// Return connection based on route target
    pub async fn put_for_target(
        &self,
        shard_id: &ShardId,
        target: crate::router::RouteTarget,
        conn: PooledConnection,
    ) {
        match target {
            crate::router::RouteTarget::Master => self.put_master(shard_id, conn).await,
            crate::router::RouteTarget::Slave => self.put_slave(shard_id, conn).await,
        }
    }

    /// Get the transaction pool
    pub fn transaction_pool(&self) -> &Arc<TransactionPool> {
        &self.transaction_pool
    }

    /// Get or create a transaction connection for a session
    ///
    /// Routes to the correct shard's master backend.
    pub async fn begin_transaction(
        &self,
        session_id: u32,
        shard_id: &ShardId,
        database: Option<String>,
    ) -> Result<(), ConnectionError> {
        // Get backend config for the specified shard
        let backends = self.backends.read().await;
        let backend = backends
            .get(shard_id)
            .or_else(|| backends.get(&ShardId::default_shard()))
            .ok_or(ConnectionError::Disconnected)?;

        self.transaction_pool
            .get_or_create(session_id, &backend.master, database)
            .await
    }

    /// End a transaction and release the connection
    pub async fn end_transaction(&self, session_id: u32) {
        self.transaction_pool.release(session_id).await;
    }

    /// Check if session has an active transaction
    pub async fn has_transaction(&self, session_id: u32) -> bool {
        self.transaction_pool.has_bound(session_id).await
    }

    /// Get pool statistics
    pub async fn stats(&self) -> PoolStats {
        let mut total_idle = 0;
        {
            let pools = self.stateless_pools.read().await;
            for pool in pools.values() {
                total_idle += pool.idle_count().await;
            }
        }

        let transaction_bound = self.transaction_pool.bound_count().await;

        PoolStats {
            total_idle_connections: total_idle,
            transaction_bound_connections: transaction_bound,
        }
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_idle_connections: usize,
    pub transaction_bound_connections: usize,
}
