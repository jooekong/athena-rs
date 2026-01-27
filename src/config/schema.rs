use serde::Deserialize;

use crate::router::ShardingRule;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    /// Default backend (used when no group is configured)
    pub backend: BackendConfig,
    /// Groups configuration (multi-tenant support)
    #[serde(default)]
    pub groups: Vec<GroupConfig>,
    /// Legacy circuit config (deprecated, use group-level config instead)
    #[serde(default)]
    pub circuit: CircuitConfig,
    /// Health check configuration
    #[serde(default)]
    pub health: HealthCheckConfig,
    #[serde(default)]
    pub sharding: Vec<ShardingRule>,
}

// ============================================================================
// Health Check Configuration
// ============================================================================

/// Health check configuration for backend instances
#[derive(Debug, Clone, Deserialize)]
pub struct HealthCheckConfig {
    /// Whether health checks are enabled
    #[serde(default = "default_health_enabled")]
    pub enabled: bool,
    /// Interval between checks (milliseconds)
    #[serde(default = "default_check_interval_ms")]
    pub check_interval_ms: u64,
    /// Number of consecutive failures before marking unhealthy
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    /// Timeout for each health check (milliseconds)
    #[serde(default = "default_check_timeout_ms")]
    pub check_timeout_ms: u64,
}

fn default_health_enabled() -> bool {
    true
}

fn default_check_interval_ms() -> u64 {
    5000
}

fn default_failure_threshold() -> u32 {
    5
}

fn default_check_timeout_ms() -> u64 {
    3000
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: default_health_enabled(),
            check_interval_ms: default_check_interval_ms(),
            failure_threshold: default_failure_threshold(),
            check_timeout_ms: default_check_timeout_ms(),
        }
    }
}

// ============================================================================
// Group / DBGroup / DBInstance Configuration
// ============================================================================

/// Group: Client-facing logical database (1:1 with tenant/user)
///
/// A Group represents a virtual database from the client's perspective.
/// Each tenant has their own Group, which maps to one or more DBGroups.
#[derive(Debug, Clone, Deserialize)]
pub struct GroupConfig {
    /// Unique group name (used as identifier)
    pub name: String,
    /// Database groups (shards) in this group
    #[serde(default)]
    pub db_groups: Vec<DBGroupConfig>,
    /// Sharding rules for this group
    #[serde(default)]
    pub sharding_rules: Vec<ShardingRule>,
}

/// DBGroup: Backend cluster (one shard's physical implementation)
///
/// A DBGroup contains multiple DBInstances (master + slaves).
/// In a sharded setup, each shard has its own DBGroup.
#[derive(Debug, Clone, Deserialize)]
pub struct DBGroupConfig {
    /// Shard identifier (e.g., "shard_0", "shard_1")
    pub shard_id: String,
    /// Database instances in this group
    pub instances: Vec<DBInstanceConfig>,
}

/// DBInstance: Individual MySQL server with its own rate limiter
///
/// Each DBInstance has:
/// - Connection info (host, port, credentials)
/// - Role (master/slave)
/// - Rate limiting configuration (embedded limiter)
#[derive(Debug, Clone, Deserialize)]
pub struct DBInstanceConfig {
    /// Hostname or IP
    pub host: String,
    /// Port number
    pub port: u16,
    /// MySQL username
    pub user: String,
    /// MySQL password
    pub password: String,
    /// Default database
    #[serde(default)]
    pub database: Option<String>,
    /// Instance role
    #[serde(default)]
    pub role: DBInstanceRole,
    /// Rate limiting configuration for this instance
    #[serde(default)]
    pub limiter: LimiterConfig,
}

/// Role of a DBInstance in a DBGroup
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DBInstanceRole {
    #[default]
    Master,
    Slave,
}

/// Rate limiter configuration for a single DBInstance
#[derive(Debug, Clone, Deserialize)]
pub struct LimiterConfig {
    /// Whether rate limiting is enabled for this instance
    #[serde(default = "default_limiter_enabled")]
    pub enabled: bool,
    /// Maximum concurrent requests to this instance
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
    /// Maximum queue size (requests waiting for a slot)
    #[serde(default = "default_queue_size")]
    pub max_queue_size: usize,
    /// Timeout in milliseconds for waiting in queue
    #[serde(default = "default_queue_timeout_ms")]
    pub queue_timeout_ms: u64,
}

fn default_limiter_enabled() -> bool {
    true
}

fn default_max_concurrent() -> usize {
    100
}

fn default_queue_size() -> usize {
    50
}

fn default_queue_timeout_ms() -> u64 {
    5000
}

impl Default for LimiterConfig {
    fn default() -> Self {
        Self {
            enabled: default_limiter_enabled(),
            max_concurrent: default_max_concurrent(),
            max_queue_size: default_queue_size(),
            queue_timeout_ms: default_queue_timeout_ms(),
        }
    }
}

// ============================================================================
// Legacy Configuration (for backward compatibility)
// ============================================================================

/// Legacy circuit breaker configuration
/// Deprecated: Use LimiterConfig in DBInstanceConfig instead
#[derive(Debug, Clone, Deserialize)]
pub struct CircuitConfig {
    #[serde(default = "default_circuit_enabled")]
    pub enabled: bool,
    #[serde(default = "default_legacy_max_concurrent")]
    pub max_concurrent_per_user_shard: usize,
    #[serde(default = "default_legacy_queue_size")]
    pub queue_size: usize,
    #[serde(default = "default_queue_timeout_ms")]
    pub queue_timeout_ms: u64,
}

fn default_circuit_enabled() -> bool {
    true
}

fn default_legacy_max_concurrent() -> usize {
    10
}

fn default_legacy_queue_size() -> usize {
    100
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            enabled: default_circuit_enabled(),
            max_concurrent_per_user_shard: default_legacy_max_concurrent(),
            queue_size: default_legacy_queue_size(),
            queue_timeout_ms: default_queue_timeout_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub listen_addr: String,
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,
}

fn default_listen_port() -> u16 {
    3307
}

#[derive(Debug, Clone, Deserialize)]
pub struct BackendConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    #[serde(default)]
    pub database: Option<String>,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 3306,
            user: "root".to_string(),
            password: String::new(),
            database: None,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                listen_addr: "127.0.0.1".to_string(),
                listen_port: 3307,
            },
            backend: BackendConfig {
                host: "127.0.0.1".to_string(),
                port: 3306,
                user: "root".to_string(),
                password: String::new(),
                database: None,
            },
            groups: Vec::new(),
            circuit: CircuitConfig::default(),
            health: HealthCheckConfig::default(),
            sharding: Vec::new(),
        }
    }
}

impl DBInstanceConfig {
    /// Get the address string (host:port)
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Check if this instance is a master
    pub fn is_master(&self) -> bool {
        self.role == DBInstanceRole::Master
    }

    /// Check if this instance is a slave
    pub fn is_slave(&self) -> bool {
        self.role == DBInstanceRole::Slave
    }

    /// Convert to BackendConfig for pool manager
    pub fn to_backend_config(&self) -> BackendConfig {
        BackendConfig {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
        }
    }
}

impl From<&BackendConfig> for DBInstanceConfig {
    /// Convert legacy BackendConfig to DBInstanceConfig
    fn from(config: &BackendConfig) -> Self {
        Self {
            host: config.host.clone(),
            port: config.port,
            user: config.user.clone(),
            password: config.password.clone(),
            database: config.database.clone(),
            role: DBInstanceRole::Master,
            limiter: LimiterConfig::default(),
        }
    }
}

impl DBGroupConfig {
    /// Get all master instances in this group
    pub fn masters(&self) -> Vec<&DBInstanceConfig> {
        self.instances.iter().filter(|i| i.is_master()).collect()
    }

    /// Get all slave instances in this group
    pub fn slaves(&self) -> Vec<&DBInstanceConfig> {
        self.instances.iter().filter(|i| i.is_slave()).collect()
    }

    /// Get the primary master (first master instance)
    ///
    /// This is the default target for write operations.
    pub fn primary_master(&self) -> Option<&DBInstanceConfig> {
        self.instances.iter().find(|i| i.is_master())
    }

    /// Check if this group has any slave instances
    pub fn has_slaves(&self) -> bool {
        self.instances.iter().any(|i| i.is_slave())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"
[server]
listen_addr = "0.0.0.0"

[backend]
host = "mysql.local"
port = 3306
user = "app"
password = "secret"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.server.listen_addr, "0.0.0.0");
        assert_eq!(config.server.listen_port, 3307); // default
        assert_eq!(config.backend.host, "mysql.local");
        assert!(config.circuit.enabled); // default
        assert!(config.sharding.is_empty());
        assert!(config.groups.is_empty());
    }

    #[test]
    fn test_parse_config_with_groups() {
        let toml = r#"
[server]
listen_addr = "127.0.0.1"

[backend]
host = "localhost"
port = 3306
user = "root"
password = ""

[[groups]]
name = "tenant_a"

[[groups.db_groups]]
shard_id = "shard_0"

[[groups.db_groups.instances]]
host = "mysql-1"
port = 3306
user = "root"
password = "secret"
role = "master"

[groups.db_groups.instances.limiter]
max_concurrent = 100
max_queue_size = 50

[[groups.db_groups.instances]]
host = "mysql-2"
port = 3306
user = "root"
password = "secret"
role = "slave"

[groups.db_groups.instances.limiter]
max_concurrent = 200
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.groups.len(), 1);
        assert_eq!(config.groups[0].name, "tenant_a");
        assert_eq!(config.groups[0].db_groups.len(), 1);
        assert_eq!(config.groups[0].db_groups[0].shard_id, "shard_0");
        assert_eq!(config.groups[0].db_groups[0].instances.len(), 2);

        let master = &config.groups[0].db_groups[0].instances[0];
        assert_eq!(master.host, "mysql-1");
        assert!(master.is_master());
        assert_eq!(master.limiter.max_concurrent, 100);
        assert_eq!(master.limiter.max_queue_size, 50);

        let slave = &config.groups[0].db_groups[0].instances[1];
        assert_eq!(slave.host, "mysql-2");
        assert!(slave.is_slave());
        assert_eq!(slave.limiter.max_concurrent, 200);
    }

    #[test]
    fn test_parse_config_with_sharding() {
        let toml = r#"
[server]
listen_addr = "127.0.0.1"
listen_port = 3307

[backend]
host = "localhost"
port = 3306
user = "root"
password = ""

[[sharding]]
name = "user_shard"
table_pattern = "users"
shard_column = "user_id"
algorithm = "mod"
shard_count = 4

[[sharding]]
name = "order_shard"
table_pattern = "orders"
shard_column = "order_id"
algorithm = "hash"
shard_count = 8
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.sharding.len(), 2);
        assert_eq!(config.sharding[0].name, "user_shard");
        assert_eq!(config.sharding[0].table_pattern, "users");
        assert_eq!(config.sharding[0].shard_column, "user_id");
        assert_eq!(config.sharding[0].algorithm, "mod");
        assert_eq!(config.sharding[0].shard_count, 4);
        assert_eq!(config.sharding[1].algorithm, "hash");
        assert_eq!(config.sharding[1].shard_count, 8);
    }

    #[test]
    fn test_parse_config_with_circuit() {
        let toml = r#"
[server]
listen_addr = "127.0.0.1"

[backend]
host = "localhost"
port = 3306
user = "root"
password = ""

[circuit]
enabled = false
max_concurrent_per_user_shard = 20
queue_size = 50
queue_timeout_ms = 3000
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(!config.circuit.enabled);
        assert_eq!(config.circuit.max_concurrent_per_user_shard, 20);
        assert_eq!(config.circuit.queue_size, 50);
        assert_eq!(config.circuit.queue_timeout_ms, 3000);
    }

    #[test]
    fn test_limiter_config_defaults() {
        let limiter = LimiterConfig::default();
        assert!(limiter.enabled);
        assert_eq!(limiter.max_concurrent, 100);
        assert_eq!(limiter.max_queue_size, 50);
        assert_eq!(limiter.queue_timeout_ms, 5000);
    }

    #[test]
    fn test_circuit_config_defaults() {
        let circuit = CircuitConfig::default();
        assert!(circuit.enabled);
        assert_eq!(circuit.max_concurrent_per_user_shard, 10);
        assert_eq!(circuit.queue_size, 100);
        assert_eq!(circuit.queue_timeout_ms, 5000);
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.server.listen_addr, "127.0.0.1");
        assert_eq!(config.server.listen_port, 3307);
        assert_eq!(config.backend.host, "127.0.0.1");
        assert_eq!(config.backend.port, 3306);
        assert!(config.sharding.is_empty());
        assert!(config.groups.is_empty());
    }

    #[test]
    fn test_db_instance_config_from_backend() {
        let backend = BackendConfig {
            host: "mysql.local".to_string(),
            port: 3306,
            user: "root".to_string(),
            password: "secret".to_string(),
            database: Some("test".to_string()),
        };
        let instance = DBInstanceConfig::from(&backend);
        assert_eq!(instance.host, "mysql.local");
        assert_eq!(instance.port, 3306);
        assert!(instance.is_master());
        assert!(instance.limiter.enabled);
    }

    #[test]
    fn test_db_group_config_masters_slaves() {
        let db_group = DBGroupConfig {
            shard_id: "shard_0".to_string(),
            instances: vec![
                DBInstanceConfig {
                    host: "master-1".to_string(),
                    port: 3306,
                    user: "root".to_string(),
                    password: "".to_string(),
                    database: None,
                    role: DBInstanceRole::Master,
                    limiter: LimiterConfig::default(),
                },
                DBInstanceConfig {
                    host: "master-2".to_string(),
                    port: 3306,
                    user: "root".to_string(),
                    password: "".to_string(),
                    database: None,
                    role: DBInstanceRole::Master,
                    limiter: LimiterConfig::default(),
                },
                DBInstanceConfig {
                    host: "slave-1".to_string(),
                    port: 3306,
                    user: "root".to_string(),
                    password: "".to_string(),
                    database: None,
                    role: DBInstanceRole::Slave,
                    limiter: LimiterConfig::default(),
                },
            ],
        };

        let masters = db_group.masters();
        assert_eq!(masters.len(), 2);
        assert_eq!(masters[0].host, "master-1");
        assert_eq!(masters[1].host, "master-2");

        let slaves = db_group.slaves();
        assert_eq!(slaves.len(), 1);
        assert_eq!(slaves[0].host, "slave-1");

        let primary = db_group.primary_master().unwrap();
        assert_eq!(primary.host, "master-1");

        assert!(db_group.has_slaves());
    }

    #[test]
    fn test_db_group_config_no_slaves() {
        let db_group = DBGroupConfig {
            shard_id: "shard_0".to_string(),
            instances: vec![DBInstanceConfig {
                host: "master-1".to_string(),
                port: 3306,
                user: "root".to_string(),
                password: "".to_string(),
                database: None,
                role: DBInstanceRole::Master,
                limiter: LimiterConfig::default(),
            }],
        };

        assert!(!db_group.has_slaves());
        assert!(db_group.slaves().is_empty());
        assert!(db_group.primary_master().is_some());
    }

    #[test]
    fn test_health_check_config_defaults() {
        let health = HealthCheckConfig::default();
        assert!(health.enabled);
        assert_eq!(health.check_interval_ms, 5000);
        assert_eq!(health.failure_threshold, 5);
        assert_eq!(health.check_timeout_ms, 3000);
    }

    #[test]
    fn test_parse_config_with_health() {
        let toml = r#"
[server]
listen_addr = "127.0.0.1"

[backend]
host = "localhost"
port = 3306
user = "root"
password = ""

[health]
enabled = true
check_interval_ms = 10000
failure_threshold = 3
check_timeout_ms = 5000
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.health.enabled);
        assert_eq!(config.health.check_interval_ms, 10000);
        assert_eq!(config.health.failure_threshold, 3);
        assert_eq!(config.health.check_timeout_ms, 5000);
    }
}
