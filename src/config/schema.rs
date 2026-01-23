use serde::Deserialize;

use crate::router::ShardingRule;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub backend: BackendConfig,
    #[serde(default)]
    pub circuit: CircuitConfig,
    #[serde(default)]
    pub sharding: Vec<ShardingRule>,
}

/// Circuit breaker / rate limiting configuration
#[derive(Debug, Clone, Deserialize)]
pub struct CircuitConfig {
    /// Whether circuit breaker is enabled
    #[serde(default = "default_circuit_enabled")]
    pub enabled: bool,
    /// Maximum concurrent requests per (user, shard) pair
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_per_user_shard: usize,
    /// Maximum queue size for waiting requests
    #[serde(default = "default_queue_size")]
    pub queue_size: usize,
    /// Timeout in milliseconds for waiting in queue
    #[serde(default = "default_queue_timeout_ms")]
    pub queue_timeout_ms: u64,
}

fn default_circuit_enabled() -> bool {
    true
}

fn default_max_concurrent() -> usize {
    10
}

fn default_queue_size() -> usize {
    100
}

fn default_queue_timeout_ms() -> u64 {
    5000
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            enabled: default_circuit_enabled(),
            max_concurrent_per_user_shard: default_max_concurrent(),
            queue_size: default_queue_size(),
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
            circuit: CircuitConfig::default(),
            sharding: Vec::new(),
        }
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
    }
}
