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
            // Validate group config
            if let Err(e) = group_config.validate() {
                error!("{}", e);
                continue;
            }

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
            &group_config.home_group,
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
    /// - The db_group matching `home_group` name is set as the home group
    /// - Each db_group with `shard_indices` registers those indices to point to this db_group
    async fn build_pool_manager(
        db_groups: &[DBGroupConfig],
        home_group_name: &str,
        health_registry: &Arc<InstanceRegistry>,
        pool_config: &StatelessPoolConfig,
    ) -> PoolManager {
        // Find home group by name
        let home_group = db_groups.iter().find(|dg| dg.name == home_group_name);

        let default_backend = home_group
            .and_then(|dg| dg.primary_master())
            .map(|inst| inst.to_backend_config())
            .unwrap_or_else(|| {
                // Fall back to first db_group's master if no home group
                db_groups
                    .first()
                    .and_then(|dg| dg.primary_master())
                    .map(|inst| inst.to_backend_config())
                    .unwrap_or_else(|| {
                        error!(
                            "No db_groups configured or no master found! \
                             Connections will fail. Please check your configuration."
                        );
                        BackendConfig::default()
                    })
            });

        let pool_manager = PoolManager::new(default_backend, pool_config.clone());

        for db_group in db_groups.iter() {
            // Build DbGroupBackend
            let db_group_backend = Self::build_db_group_backend(db_group);

            // Register instances for health checks
            for inst in &db_group.instances {
                health_registry.register(inst);
                debug!(
                    addr = %inst.addr(),
                    role = ?inst.role,
                    db_group = %db_group.name,
                    limiter = ?inst.limiter,
                    "Registered instance for health checks"
                );
            }

            // Home group: matched by name
            if db_group.name == home_group_name {
                pool_manager.set_home(db_group_backend).await;
                debug!(db_group = %db_group.name, "Set as home group");
            } else if !db_group.shard_indices.is_empty() {
                // Shard group: register each shard_index to this db_group
                for &shard_idx in &db_group.shard_indices {
                    pool_manager.add_shard(shard_idx, db_group_backend.clone()).await;
                    debug!(
                        db_group = %db_group.name,
                        shard_index = shard_idx,
                        "Registered shard index"
                    );
                }
            } else {
                // db_group with no shard_indices and not home - warn
                error!(
                    db_group = %db_group.name,
                    "db_group has no shard_indices and is not home_group, skipping"
                );
            }
        }

        pool_manager
    }

    /// Build DbGroupBackend from DBGroupConfig
    fn build_db_group_backend(db_group: &DBGroupConfig) -> DbGroupBackend {
        let masters: Vec<&DBInstanceConfig> = db_group.masters();
        let slaves: Vec<&DBInstanceConfig> = db_group.slaves();
        if !db_group.has_slaves() {
            debug!(db_group = %db_group.name, "No slave instances configured");
        }

        let master = masters
            .first()
            .map(|m| m.to_backend_config())
            .unwrap_or_else(|| {
                error!(
                    db_group = %db_group.name,
                    "No master configured for db_group! Queries will fail."
                );
                BackendConfig::default()
            });

        let slave_configs: Vec<BackendConfig> =
            slaves.iter().map(|s| s.to_backend_config()).collect();

        DbGroupBackend {
            master,
            slaves: slave_configs,
        }
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

    /// Get the shared health registry
    pub fn health_registry(&self) -> Arc<InstanceRegistry> {
        self.health_registry.clone()
    }

    /// Get list of all group names
    pub fn group_names(&self) -> Vec<String> {
        self.groups.iter().map(|r| r.key().clone()).collect()
    }
}
