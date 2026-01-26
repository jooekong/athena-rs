//! Integration test entry point
//!
//! Run with: ATHENA_RUN_INTEGRATION_TESTS=1 cargo test --test integration

mod transaction;
mod limiter;

use std::env;

/// Check if integration tests should run
pub fn should_run_integration_tests() -> bool {
    env::var("ATHENA_RUN_INTEGRATION_TESTS").map(|v| v == "1").unwrap_or(false)
}

/// Get MySQL connection config from environment
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

/// MySQL test configuration
#[derive(Debug, Clone)]
pub struct MysqlTestConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl MysqlTestConfig {
    /// Get connection string for mysql client
    #[allow(dead_code)]
    pub fn connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }

    /// Get command line args for mysql CLI
    #[allow(dead_code)]
    pub fn mysql_cli_args(&self) -> Vec<String> {
        let mut args = vec![
            format!("-h{}", self.host),
            format!("-P{}", self.port),
            format!("-u{}", self.user),
        ];
        if !self.password.is_empty() {
            args.push(format!("-p{}", self.password));
        }
        args.push(self.database.clone());
        args
    }
}
