//! Integration test entry point
//!
//! Run with: ATHENA_RUN_INTEGRATION_TESTS=1 cargo test --test integration
//!
//! Environment variables:
//! - ATHENA_RUN_INTEGRATION_TESTS: Set to "1" to enable integration tests
//! - ATHENA_TEST_PROXY_HOST: Proxy host (default: 127.0.0.1)
//! - ATHENA_TEST_PROXY_PORT: Proxy port (default: 3307)
//! - ATHENA_TEST_PROXY_USER: Proxy user (default: app_user)
//! - ATHENA_TEST_PROXY_PASS: Proxy password (default: test123)
//! - ATHENA_TEST_SHARD_COUNT: Number of shards (default: 4)

mod limiter;
mod rw_split;
mod sharding;
mod transaction;

use mysql::{Error as MySqlError, OptsBuilder, Pool, PooledConn};
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::hash::{Hash, Hasher};

/// Check if integration tests should run
pub fn should_run_integration_tests() -> bool {
    env::var("ATHENA_RUN_INTEGRATION_TESTS")
        .map(|v| v == "1")
        .unwrap_or(false)
}

/// Skip test if integration tests are not enabled
#[macro_export]
macro_rules! skip_if_not_enabled {
    () => {
        if !crate::should_run_integration_tests() {
            eprintln!("Skipping integration test (set ATHENA_RUN_INTEGRATION_TESTS=1 to run)");
            return;
        }
    };
}

/// Get proxy connection config from environment
pub fn get_proxy_config() -> ProxyTestConfig {
    ProxyTestConfig {
        host: env::var("ATHENA_TEST_PROXY_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
        port: env::var("ATHENA_TEST_PROXY_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(3307),
        user: env::var("ATHENA_TEST_PROXY_USER").unwrap_or_else(|_| "app_user".to_string()),
        password: env::var("ATHENA_TEST_PROXY_PASS").unwrap_or_else(|_| "test123".to_string()),
        database: env::var("ATHENA_TEST_PROXY_DB").unwrap_or_else(|_| "athena_rs_test".to_string()),
    }
}

/// Get shard count from environment (default 4)
pub fn get_shard_count() -> usize {
    env::var("ATHENA_TEST_SHARD_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4)
}

/// Proxy test configuration
#[derive(Debug, Clone)]
pub struct ProxyTestConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl ProxyTestConfig {
    /// Create a connection pool to the proxy
    pub fn pool(&self) -> Pool {
        let opts = OptsBuilder::new()
            .ip_or_hostname(Some(&self.host))
            .tcp_port(self.port)
            .user(Some(&self.user))
            .pass(Some(&self.password))
            .db_name(Some(&self.database));
        Pool::new(opts).expect("Failed to create connection pool")
    }

    /// Get a single connection to the proxy
    pub fn conn(&self) -> PooledConn {
        self.pool().get_conn().expect("Failed to get connection")
    }
}

/// Calculate shard index for a string value (matches production DefaultHasher logic)
pub fn calculate_shard(value: &str, shard_count: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() % shard_count as u64) as usize
}

/// Find two user_ids that hash to different shards
pub fn find_different_shard_user_ids(shard_count: usize) -> (String, String, usize, usize) {
    let user_a = "test_user_a".to_string();
    let shard_a = calculate_shard(&user_a, shard_count);

    // Find another user that hashes to a different shard
    for i in 0..1000 {
        let user_b = format!("test_user_b_{}", i);
        let shard_b = calculate_shard(&user_b, shard_count);
        if shard_b != shard_a {
            return (user_a, user_b, shard_a, shard_b);
        }
    }
    panic!("Could not find two user_ids hashing to different shards");
}

/// Assert that a MySQL error matches expected code and message substring
pub fn assert_mysql_error(result: Result<(), MySqlError>, expected_code: u16, expected_msg: &str) {
    match result {
        Ok(_) => panic!(
            "Expected MySQL error {} with message containing '{}', but query succeeded",
            expected_code, expected_msg
        ),
        Err(MySqlError::MySqlError(ref e)) => {
            assert_eq!(
                e.code, expected_code,
                "Expected error code {}, got {}. Message: {}",
                expected_code, e.code, e.message
            );
            assert!(
                e.message.contains(expected_msg),
                "Expected message containing '{}', got: {}",
                expected_msg,
                e.message
            );
        }
        Err(e) => panic!(
            "Expected MySQL error {} with message containing '{}', got different error: {:?}",
            expected_code, expected_msg, e
        ),
    }
}

/// Assert that a query result is a MySQL error with given code and message
pub fn assert_query_error<T: std::fmt::Debug>(
    result: Result<T, MySqlError>,
    expected_code: u16,
    expected_msg: &str,
) {
    match result {
        Ok(v) => panic!(
            "Expected MySQL error {} with message containing '{}', but got: {:?}",
            expected_code, expected_msg, v
        ),
        Err(MySqlError::MySqlError(ref e)) => {
            assert_eq!(
                e.code, expected_code,
                "Expected error code {}, got {}. Message: {}",
                expected_code, e.code, e.message
            );
            assert!(
                e.message.contains(expected_msg),
                "Expected message containing '{}', got: {}",
                expected_msg,
                e.message
            );
        }
        Err(e) => panic!(
            "Expected MySQL error {} with message containing '{}', got different error: {:?}",
            expected_code, expected_msg, e
        ),
    }
}

/// Legacy config for backward compatibility
#[derive(Debug, Clone)]
pub struct MysqlTestConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

/// Get legacy MySQL connection config from environment
pub fn get_mysql_config() -> MysqlTestConfig {
    MysqlTestConfig {
        host: env::var("ATHENA_TEST_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
        port: env::var("ATHENA_TEST_MYSQL_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(3306),
        user: env::var("ATHENA_TEST_MYSQL_USER").unwrap_or_else(|_| "root".to_string()),
        password: env::var("ATHENA_TEST_MYSQL_PASS").unwrap_or_default(),
        database: env::var("ATHENA_TEST_MYSQL_DB").unwrap_or_else(|_| "test".to_string()),
    }
}
