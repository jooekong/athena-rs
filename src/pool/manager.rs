use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::debug;

use crate::config::BackendConfig;

use super::connection::{ConnectionError, PooledConnection};
use super::stateless::{StatelessPool, StatelessPoolConfig};
use super::transaction::TransactionPool;

/// Index of a shard in the shard pool vector (0-based)
pub type ShardIndex = usize;

/// Database group identifier for routing
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DbGroupId {
    /// Shard by index (0, 1, 2, ...)
    Shard(ShardIndex),
    /// Home group for non-sharded tables
    Home,
}

impl DbGroupId {
    pub fn shard(index: ShardIndex) -> Self {
        DbGroupId::Shard(index)
    }

    pub fn home() -> Self {
        DbGroupId::Home
    }

    pub fn is_shard(&self) -> bool {
        matches!(self, DbGroupId::Shard(_))
    }

    pub fn is_home(&self) -> bool {
        matches!(self, DbGroupId::Home)
    }

    pub fn shard_index(&self) -> Option<ShardIndex> {
        match self {
            DbGroupId::Shard(idx) => Some(*idx),
            DbGroupId::Home => None,
        }
    }
}

impl fmt::Display for DbGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbGroupId::Shard(idx) => write!(f, "shard_{}", idx),
            DbGroupId::Home => write!(f, "home"),
        }
    }
}

/// A group of slave pools with round-robin selection
struct SlavePoolGroup {
    pools: Vec<Arc<StatelessPool>>,
    /// Address to pool index for O(1) lookup when returning connections
    addr_to_index: HashMap<String, usize>,
    counter: AtomicUsize,
}

impl SlavePoolGroup {
    fn new(pools: Vec<Arc<StatelessPool>>) -> Self {
        let addr_to_index: HashMap<String, usize> = pools
            .iter()
            .enumerate()
            .map(|(i, p)| (p.backend_addr().to_string(), i))
            .collect();
        Self {
            pools,
            addr_to_index,
            counter: AtomicUsize::new(0),
        }
    }

    fn select(&self) -> Option<Arc<StatelessPool>> {
        if self.pools.is_empty() {
            return None;
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.pools.len();
        Some(self.pools[idx].clone())
    }

    fn find_by_addr(&self, addr: &str) -> Option<Arc<StatelessPool>> {
        self.addr_to_index
            .get(addr)
            .and_then(|&idx| self.pools.get(idx))
            .cloned()
    }
}

/// Backend configuration for a database group
#[derive(Debug, Clone)]
pub struct DbGroupBackend {
    pub master: BackendConfig,
    pub slaves: Vec<BackendConfig>,
}

/// Pool manager using Vec-based storage for O(1) index access
pub struct PoolManager {
    /// Shard master pools (indexed by ShardIndex)
    shard_master_pools: RwLock<Vec<Arc<StatelessPool>>>,
    /// Shard slave pool groups (indexed by ShardIndex)
    shard_slave_pools: RwLock<Vec<Option<SlavePoolGroup>>>,
    /// Shard backend configs (indexed by ShardIndex)
    shard_backends: RwLock<Vec<DbGroupBackend>>,

    /// Home group master pool (for non-sharded tables)
    home_master_pool: RwLock<Option<Arc<StatelessPool>>>,
    /// Home group slave pool
    home_slave_pool: RwLock<Option<SlavePoolGroup>>,
    /// Home group backend config
    home_backend: RwLock<Option<DbGroupBackend>>,

    /// Transaction pool (connections bound to sessions)
    transaction_pool: Arc<TransactionPool>,
    /// Pool configuration
    pool_config: StatelessPoolConfig,
}

impl PoolManager {
    /// Create a new pool manager with a default backend as home
    pub fn new(backend_config: BackendConfig, pool_config: StatelessPoolConfig) -> Self {
        let backend_config_arc = Arc::new(backend_config.clone());
        let transaction_pool = Arc::new(TransactionPool::new());

        let default_pool = Arc::new(StatelessPool::new(
            backend_config_arc,
            pool_config.clone(),
            None,
        ));

        let home_backend = DbGroupBackend {
            master: backend_config,
            slaves: vec![],
        };

        Self {
            shard_master_pools: RwLock::new(Vec::new()),
            shard_slave_pools: RwLock::new(Vec::new()),
            shard_backends: RwLock::new(Vec::new()),
            home_master_pool: RwLock::new(Some(default_pool)),
            home_slave_pool: RwLock::new(None),
            home_backend: RwLock::new(Some(home_backend)),
            transaction_pool,
            pool_config,
        }
    }

    /// Add a shard at a specific index
    pub async fn add_shard(&self, index: ShardIndex, backend: DbGroupBackend) {
        let master_config = Arc::new(backend.master.clone());

        let master_pool = Arc::new(StatelessPool::new(
            master_config,
            self.pool_config.clone(),
            None,
        ));

        let slave_pools: Vec<Arc<StatelessPool>> = backend
            .slaves
            .iter()
            .map(|cfg| Arc::new(StatelessPool::new(Arc::new(cfg.clone()), self.pool_config.clone(), None)))
            .collect();

        let slave_group = if slave_pools.is_empty() {
            None
        } else {
            Some(SlavePoolGroup::new(slave_pools))
        };

        // Extend vectors if needed
        {
            let mut masters = self.shard_master_pools.write().await;
            while masters.len() <= index {
                masters.push(Arc::new(StatelessPool::new(
                    Arc::new(BackendConfig::default()),
                    self.pool_config.clone(),
                    None,
                )));
            }
            masters[index] = master_pool;
        }

        {
            let mut slaves = self.shard_slave_pools.write().await;
            while slaves.len() <= index {
                slaves.push(None);
            }
            slaves[index] = slave_group;
        }

        {
            let mut backends = self.shard_backends.write().await;
            while backends.len() <= index {
                backends.push(DbGroupBackend {
                    master: BackendConfig::default(),
                    slaves: vec![],
                });
            }
            backends[index] = backend;
        }

        debug!(index, "Added shard");
    }

    /// Set the home group backend
    pub async fn set_home(&self, backend: DbGroupBackend) {
        let master_config = Arc::new(backend.master.clone());

        let master_pool = Arc::new(StatelessPool::new(
            master_config,
            self.pool_config.clone(),
            None,
        ));

        let slave_group = if backend.slaves.is_empty() {
            None
        } else {
            let pools: Vec<Arc<StatelessPool>> = backend
                .slaves
                .iter()
                .map(|cfg| Arc::new(StatelessPool::new(Arc::new(cfg.clone()), self.pool_config.clone(), None)))
                .collect();
            Some(SlavePoolGroup::new(pools))
        };

        *self.home_master_pool.write().await = Some(master_pool);
        *self.home_slave_pool.write().await = slave_group;
        *self.home_backend.write().await = Some(backend);

        debug!("Set home group");
    }

    /// Get a master connection
    pub async fn get_master(&self, group_id: &DbGroupId) -> Result<PooledConnection, ConnectionError> {
        let pool = match group_id {
            DbGroupId::Shard(index) => {
                let pools = self.shard_master_pools.read().await;
                pools.get(*index).cloned().or_else(|| None)
            }
            DbGroupId::Home => self.home_master_pool.read().await.clone(),
        };

        // Fallback to home if shard not found
        let pool = pool.or_else(|| {
            // Can't await here, so we return None and handle below
            None
        });

        let pool = if pool.is_none() && matches!(group_id, DbGroupId::Shard(_)) {
            self.home_master_pool.read().await.clone()
        } else {
            pool
        };

        pool.ok_or(ConnectionError::Disconnected)?.get().await
    }

    /// Get a slave connection (round-robin, falls back to master)
    pub async fn get_slave(&self, group_id: &DbGroupId) -> Result<PooledConnection, ConnectionError> {
        let pool = match group_id {
            DbGroupId::Shard(index) => {
                let slaves = self.shard_slave_pools.read().await;
                slaves.get(*index).and_then(|g| g.as_ref()).and_then(|g| g.select())
            }
            DbGroupId::Home => {
                let slave_group = self.home_slave_pool.read().await;
                slave_group.as_ref().and_then(|g| g.select())
            }
        };

        if let Some(pool) = pool {
            pool.get().await
        } else {
            self.get_master(group_id).await
        }
    }

    /// Return a master connection
    pub async fn put_master(&self, group_id: &DbGroupId, conn: PooledConnection) {
        match group_id {
            DbGroupId::Shard(index) => {
                let pools = self.shard_master_pools.read().await;
                if let Some(pool) = pools.get(*index) {
                    pool.put(conn).await;
                }
            }
            DbGroupId::Home => {
                if let Some(pool) = self.home_master_pool.read().await.as_ref() {
                    pool.put(conn).await;
                }
            }
        }
    }

    /// Return a slave connection
    pub async fn put_slave(&self, group_id: &DbGroupId, conn: PooledConnection) {
        let conn_addr = conn.backend_addr().to_string();

        let matching_pool = match group_id {
            DbGroupId::Shard(index) => {
                let slaves = self.shard_slave_pools.read().await;
                slaves.get(*index).and_then(|g| g.as_ref()).and_then(|g| g.find_by_addr(&conn_addr))
            }
            DbGroupId::Home => {
                let slave_group = self.home_slave_pool.read().await;
                slave_group.as_ref().and_then(|g| g.find_by_addr(&conn_addr))
            }
        };

        if let Some(pool) = matching_pool {
            pool.put(conn).await;
        } else {
            debug!(group = %group_id, addr = %conn_addr, "Slave connection didn't match any pool, discarding");
        }
    }

    /// Get connection by route target
    pub async fn get_for_target(
        &self,
        group_id: &DbGroupId,
        target: crate::router::RouteTarget,
    ) -> Result<PooledConnection, ConnectionError> {
        match target {
            crate::router::RouteTarget::Master => self.get_master(group_id).await,
            crate::router::RouteTarget::Slave => self.get_slave(group_id).await,
        }
    }

    /// Return connection by route target
    pub async fn put_for_target(
        &self,
        group_id: &DbGroupId,
        target: crate::router::RouteTarget,
        conn: PooledConnection,
    ) {
        match target {
            crate::router::RouteTarget::Master => self.put_master(group_id, conn).await,
            crate::router::RouteTarget::Slave => self.put_slave(group_id, conn).await,
        }
    }

    /// Begin transaction
    pub async fn begin_transaction(
        &self,
        session_id: u32,
        group_id: &DbGroupId,
        database: Option<String>,
    ) -> Result<(), ConnectionError> {
        let backend = match group_id {
            DbGroupId::Shard(index) => {
                let backends = self.shard_backends.read().await;
                backends.get(*index).cloned()
            }
            DbGroupId::Home => self.home_backend.read().await.clone(),
        };

        let backend = backend.ok_or(ConnectionError::Disconnected)?;
        self.transaction_pool
            .get_or_create(session_id, &backend.master, database)
            .await
    }

    /// End transaction
    pub async fn end_transaction(
        &self,
        session_id: u32,
        group_id: Option<&DbGroupId>,
        transaction_completed: bool,
    ) {
        let pool = match group_id {
            Some(DbGroupId::Shard(index)) => {
                let pools = self.shard_master_pools.read().await;
                pools.get(*index).cloned()
            }
            Some(DbGroupId::Home) | None => self.home_master_pool.read().await.clone(),
        };

        self.transaction_pool
            .release(session_id, transaction_completed, pool.as_deref())
            .await;
    }

    /// Get transaction pool
    pub fn transaction_pool(&self) -> &Arc<TransactionPool> {
        &self.transaction_pool
    }

    /// Get shard count
    pub async fn shard_count(&self) -> usize {
        self.shard_master_pools.read().await.len()
    }
}
