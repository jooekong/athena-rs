//! Group manager implementation

use std::sync::Arc;

use dashmap::DashMap;
use tracing::{debug, error, info};

use crate::config::{Config, DBGroupConfig, DBInstanceConfig, GroupConfig, BackendConfig};
use crate::health::InstanceRegistry;
use crate::pool::{DbGroupBackend, PoolManager, StatelessPoolConfig};
use crate::router::RouterConfig;

/// Context for a single group (tenant)
///
/// Contains everything needed to process requests for this group:
/// - Pool manager for backend connections
/// - Router config for SQL routing
/// - Proxy-level authentication (user/password)
pub struct GroupContext {
    /// Group name (= database name from client's perspective)
    pub name: String,
    /// Proxy authentication username
    pub auth_user: String,
    /// Proxy authentication password
    pub auth_password: String,
    /// Pool manager for this group's backends
    pub pool_manager: Arc<PoolManager>,
    /// Router configuration (sharding rules)
    pub router_config: RouterConfig,
}

impl GroupContext {
    /// Validate proxy-level authentication
    ///
    /// Returns true if username and password match the group's auth config
    pub fn validate_auth(&self, username: &str, password: &str) -> bool {
        self.auth_user == username && self.auth_password == password
    }
}

/// Manages all groups and their resources
///
/// Provides:
/// - Group lookup by name (username maps 1:1 to group name)
/// - Shared health registry across all groups
pub struct GroupManager {
    /// Groups by name
    groups: DashMap<String, Arc<GroupContext>>,
    /// Shared health registry (de-duplicates health checks across groups)
    health_registry: Arc<InstanceRegistry>,
}

impl GroupManager {
    /// Create a new group manager from configuration (async)
    ///
    /// Requires at least one group to be configured.
    pub async fn new(config: &Config) -> Self {
        if config.groups.is_empty() {
            panic!("No groups configured! At least one group must be defined in configuration.");
        }

        let health_registry = Arc::new(InstanceRegistry::with_config(config.health.clone()));
        let groups = DashMap::new();
        let pool_config = StatelessPoolConfig::default();

        // Build groups from config
        for group_config in &config.groups {
            let context = Self::build_group_context(
                group_config,
                &health_registry,
                &pool_config,
            )
            .await;
            info!(group = %group_config.name, "Registered group");
            groups.insert(group_config.name.clone(), Arc::new(context));
        }

        Self {
            groups,
            health_registry,
        }
    }

    /// Build context for a single group
    async fn build_group_context(
        group_config: &GroupConfig,
        health_registry: &Arc<InstanceRegistry>,
        pool_config: &StatelessPoolConfig,
    ) -> GroupContext {
        // Build router config from sharding rules
        let mut router_config = RouterConfig::new();
        for rule in &group_config.sharding_rules {
            router_config.add_rule(rule.clone());
        }

        // Build pool manager
        let pool_manager = Self::build_pool_manager(
            &group_config.db_groups,
            health_registry,
            pool_config,
        )
        .await;

        GroupContext {
            name: group_config.name.clone(),
            auth_user: group_config.user.clone(),
            auth_password: group_config.password.clone(),
            pool_manager: Arc::new(pool_manager),
            router_config,
        }
    }

    /// Build pool manager for a group's db_groups
    ///
    /// db_groups are added by index order (0, 1, 2, ...).
    /// The first db_group is also used as the home group fallback.
    async fn build_pool_manager(
        db_groups: &[DBGroupConfig],
        health_registry: &Arc<InstanceRegistry>,
        pool_config: &StatelessPoolConfig,
    ) -> PoolManager {
        // Use first master as default backend (for PoolManager::new)
        let default_backend = db_groups
            .first()
            .and_then(|dg| dg.primary_master())
            .map(|inst| inst.to_backend_config())
            .unwrap_or_else(|| {
                // This is a configuration error - no db_groups or no masters configured
                // Log error but don't panic - let the connection fail at runtime
                // with a clear error message
                error!(
                    "No db_groups configured or no master in first db_group! \
                     Connections will fail. Please check your configuration."
                );
                BackendConfig::default()
            });

        let pool_manager = PoolManager::new(default_backend, pool_config.clone());

        // Add all shards by index order
        for (index, db_group) in db_groups.iter().enumerate() {
            // Collect masters and slaves
            let masters: Vec<&DBInstanceConfig> = db_group.masters();
            let slaves: Vec<&DBInstanceConfig> = db_group.slaves();

            // Use primary master
            let master = masters
                .first()
                .map(|m| m.to_backend_config())
                .unwrap_or_else(|| {
                    // This is a configuration error - shard has no master
                    error!(
                        db_group = %db_group.name,
                        "No master configured for db_group! Queries to this db_group will fail."
                    );
                    BackendConfig::default()
                });

            // Convert slaves
            let slave_configs: Vec<BackendConfig> =
                slaves.iter().map(|s| s.to_backend_config()).collect();

            // Create new-style DbGroupBackend
            let db_group_backend = DbGroupBackend {
                master,
                slaves: slave_configs,
            };

            pool_manager.add_shard(index, db_group_backend).await;

            // Register instances for health checks
            for inst in &db_group.instances {
                health_registry.register(inst);
                debug!(
                    addr = %inst.addr(),
                    role = ?inst.role,
                    db_group = %db_group.name,
                    index = index,
                    "Registered instance for health checks"
                );
            }

            debug!(
                db_group = %db_group.name,
                index = index,
                "Added db_group at index"
            );
        }

        pool_manager
    }

    /// Get group by name
    ///
    /// Returns None if group not found
    pub fn get(&self, name: &str) -> Option<Arc<GroupContext>> {
        self.groups.get(name).map(|r| r.value().clone())
    }

    /// Get group by database name
    ///
    /// Group name = database name (client connects with database that maps to group)
    pub fn get_by_database(&self, database: &str) -> Option<Arc<GroupContext>> {
        self.get(database)
    }

    /// Get group for a username
    ///
    /// Username maps 1:1 to group name.
    pub fn get_for_user(&self, username: &str) -> Option<Arc<GroupContext>> {
        self.get(username)
    }

    /// Get the shared health registry
    pub fn health_registry(&self) -> Arc<InstanceRegistry> {
        self.health_registry.clone()
    }

    /// Get list of all group names
    pub fn group_names(&self) -> Vec<String> {
        self.groups.iter().map(|r| r.key().clone()).collect()
    }
}
