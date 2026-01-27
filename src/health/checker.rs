//! Health checker utilities
//!
//! The actual health check tasks are now spawned by InstanceRegistry.
//! This module provides configuration and utility types.

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::HealthCheckConfig;

use super::registry::InstanceRegistry;

/// Health checker manager
///
/// Holds the registry and configuration. The actual health check tasks
/// are spawned by the registry when instances are registered.
pub struct HealthChecker {
    /// Instance registry (shared with other components)
    pub registry: Arc<InstanceRegistry>,
    /// Configuration
    pub config: HealthCheckConfig,
}

impl HealthChecker {
    /// Create a new health checker with registry that uses the given config
    pub fn new(config: HealthCheckConfig) -> Self {
        let registry = Arc::new(InstanceRegistry::with_config(config.clone()));
        Self { registry, config }
    }

    /// Create from an existing registry
    pub fn with_registry(registry: Arc<InstanceRegistry>, config: HealthCheckConfig) -> Self {
        Self { registry, config }
    }

    /// Get the registry
    pub fn registry(&self) -> Arc<InstanceRegistry> {
        self.registry.clone()
    }

    /// Start background task that waits for shutdown
    ///
    /// Note: The actual health check tasks are spawned by the registry
    /// when instances are registered. This just handles graceful shutdown.
    pub fn start(self: Arc<Self>, shutdown: CancellationToken) -> JoinHandle<()> {
        if !self.config.enabled {
            info!("Health checks are disabled");
        } else {
            info!(
                interval_ms = self.config.check_interval_ms,
                "Health checker ready (tasks spawned on instance registration)"
            );
        }

        tokio::spawn(async move {
            shutdown.cancelled().await;
            info!("Health checker shutting down");
            // Registry tasks will be cancelled when registry is dropped
            // or when individual instances are unregistered
        })
    }
}

/// Error during health check
#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    #[error("Connection failed: {0}")]
    Connection(String),
    #[error("Ping failed")]
    Ping,
    #[error("Role detection failed: {0}")]
    RoleDetection(String),
    #[error("Invalid address: {0}")]
    InvalidAddr(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_config_default() {
        let config = HealthCheckConfig::default();
        assert!(config.enabled);
        assert_eq!(config.check_interval_ms, 5000);
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.check_timeout_ms, 3000);
    }

    #[test]
    fn test_health_checker_new() {
        let config = HealthCheckConfig::default();
        let checker = HealthChecker::new(config.clone());
        assert_eq!(checker.config.check_interval_ms, config.check_interval_ms);
    }
}
