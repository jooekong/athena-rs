use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub backend: BackendConfig,
    #[serde(default)]
    pub circuit: CircuitConfig,
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
        }
    }
}
