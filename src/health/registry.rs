//! Instance registry for health check deduplication
//!
//! Multiple tenants/groups may share the same physical DBInstance.
//! This registry ensures we only run one health check per unique host:port.
//!
//! Each registered instance gets its own long-running health check task.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::RwLock;
use rand::Rng as _;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{DBInstanceConfig, HealthCheckConfig};

use super::master::MasterDetector;
use super::state::InstanceHealth;

/// Registry for tracking DBInstance health across multiple tenants
///
/// Uses reference counting to handle multiple groups registering the same instance.
/// Each unique instance gets its own long-running health check task.
pub struct InstanceRegistry {
    /// Instance health state (addr -> health)
    instances: DashMap<String, Arc<RwLock<InstanceHealth>>>,
    /// Reference counts (addr -> ref_count)
    ref_counts: DashMap<String, usize>,
    /// Cancellation tokens for health check tasks (addr -> token)
    cancellation_tokens: DashMap<String, CancellationToken>,
    /// Health check configuration
    config: HealthCheckConfig,
}

impl Default for InstanceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl InstanceRegistry {
    /// Create a new empty registry with default config
    pub fn new() -> Self {
        Self::with_config(HealthCheckConfig::default())
    }

    /// Create a new registry with custom config
    pub fn with_config(config: HealthCheckConfig) -> Self {
        Self {
            instances: DashMap::new(),
            ref_counts: DashMap::new(),
            cancellation_tokens: DashMap::new(),
            config,
        }
    }

    /// Register an instance for health checking
    ///
    /// If the instance is already registered (by another tenant), increments ref count.
    /// If this is a new instance, spawns a long-running health check task.
    /// Returns the shared health state.
    pub fn register(&self, config: &DBInstanceConfig) -> Arc<RwLock<InstanceHealth>> {
        let addr = config.addr();

        // Create window config from failure_threshold
        let window_config = super::state::WindowConfig::from_failure_threshold(
            self.config.failure_threshold,
        );

        // Atomically get or create health state, tracking if we inserted
        let mut is_new = false;
        let health = self
            .instances
            .entry(addr.clone())
            .or_insert_with(|| {
                is_new = true;
                info!(addr = %addr, "Registering new instance for health checks");
                Arc::new(RwLock::new(InstanceHealth::with_config(
                    addr.clone(),
                    config.user.clone(),
                    config.password.clone(),
                    window_config.clone(),
                )))
            })
            .clone();

        // Increment reference count
        self.ref_counts
            .entry(addr.clone())
            .and_modify(|c| *c += 1)
            .or_insert(1);

        // Spawn health check task for new instances (only if we just inserted)
        if is_new && self.config.enabled {
            self.spawn_check_task(addr.clone(), health.clone());
        }

        debug!(addr = %addr, ref_count = ?self.ref_counts.get(&addr).map(|r| *r), "Instance registered");
        health
    }

    /// Spawn a long-running health check task for an instance
    fn spawn_check_task(&self, addr: String, health: Arc<RwLock<InstanceHealth>>) {
        let cancel_token = CancellationToken::new();
        self.cancellation_tokens
            .insert(addr.clone(), cancel_token.clone());

        let config = self.config.clone();
        let check_interval = Duration::from_millis(config.check_interval_ms);
        let check_timeout = Duration::from_millis(config.check_timeout_ms);

        let task_addr = addr.clone();
        tokio::spawn(async move {
            // Random initial delay to stagger checks (0-100% of interval)
            let initial_delay = rand::thread_rng().gen_range(0..check_interval.as_millis() as u64);
            tokio::time::sleep(Duration::from_millis(initial_delay)).await;

            // Persistent connection for this instance
            let mut conn: Option<crate::pool::PooledConnection> = None;

            let mut ticker = tokio::time::interval(check_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        debug!(addr = %task_addr, "Health check task cancelled");
                        break;
                    }
                    _ = ticker.tick() => {
                        Self::do_check(&task_addr, &health, &mut conn, check_timeout).await;
                    }
                }
            }
        });

        debug!(addr = %addr, "Spawned health check task");
    }

    /// Perform a single health check
    async fn do_check(
        addr: &str,
        health: &Arc<RwLock<InstanceHealth>>,
        conn: &mut Option<crate::pool::PooledConnection>,
        check_timeout: Duration,
    ) {
        let (user, password) = {
            let h = health.read();
            (h.user.clone(), h.password.clone())
        };

        let result = tokio::time::timeout(
            check_timeout,
            Self::check_with_conn(addr, &user, &password, conn),
        )
        .await;

        match result {
            Ok(Ok(role)) => {
                let mut h = health.write();
                let changed = h.record_success(Some(role));
                if changed {
                    info!(
                        addr = %addr,
                        role = ?role,
                        status = ?h.status,
                        "Instance status changed"
                    );
                } else {
                    debug!(addr = %addr, role = ?role, "Health check passed");
                }
            }
            Ok(Err(e)) => {
                // Connection failed, clear cached connection
                *conn = None;
                let mut h = health.write();
                let changed = h.record_failure();
                if changed {
                    warn!(addr = %addr, error = %e, status = ?h.status, "Instance status changed");
                } else {
                    debug!(addr = %addr, error = %e, "Health check failed");
                }
            }
            Err(_) => {
                // Timeout, clear cached connection
                *conn = None;
                let mut h = health.write();
                let changed = h.record_failure();
                if changed {
                    warn!(addr = %addr, "Instance status changed (timeout)");
                } else {
                    debug!(addr = %addr, "Health check timed out");
                }
            }
        }
    }

    /// Check with persistent connection, reconnecting if needed
    async fn check_with_conn(
        addr: &str,
        user: &str,
        password: &str,
        conn: &mut Option<crate::pool::PooledConnection>,
    ) -> Result<crate::config::DBInstanceRole, String> {
        // Try existing connection first
        if let Some(ref mut c) = conn {
            match MasterDetector::ping_and_detect_role(c).await {
                Ok(role) => return Ok(role),
                Err(e) => {
                    debug!(addr = %addr, error = %e, "Cached connection failed, reconnecting");
                    *conn = None;
                }
            }
        }

        // Create new connection
        let (host, port) = Self::parse_addr(addr)?;
        let backend_config = crate::config::BackendConfig {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
            database: None,
        };

        let mut new_conn = crate::pool::PooledConnection::connect(&backend_config, None)
            .await
            .map_err(|e| e.to_string())?;

        let role = MasterDetector::ping_and_detect_role(&mut new_conn)
            .await
            .map_err(|e| e.to_string())?;

        // Cache connection for reuse
        *conn = Some(new_conn);
        Ok(role)
    }

    /// Parse "host:port" into components
    fn parse_addr(addr: &str) -> Result<(&str, u16), String> {
        let parts: Vec<&str> = addr.rsplitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid address format: {}", addr));
        }
        let port = parts[0]
            .parse::<u16>()
            .map_err(|_| format!("Invalid port: {}", parts[0]))?;
        let host = parts[1];
        Ok((host, port))
    }

    /// Unregister an instance
    ///
    /// Decrements reference count. If count reaches 0, cancels the health check task
    /// and removes the instance.
    pub fn unregister(&self, addr: &str) {
        let should_remove = {
            let mut entry = match self.ref_counts.get_mut(addr) {
                Some(e) => e,
                None => return,
            };
            *entry -= 1;
            *entry == 0
        };

        if should_remove {
            // Cancel the health check task
            if let Some((_, token)) = self.cancellation_tokens.remove(addr) {
                token.cancel();
            }

            self.ref_counts.remove(addr);
            self.instances.remove(addr);
            info!(addr = %addr, "Instance unregistered and health check task cancelled");
        } else {
            debug!(addr = %addr, ref_count = ?self.ref_counts.get(addr).map(|r| *r), "Instance unregistered (still has references)");
        }
    }

    /// Get all instances that need health checking
    pub fn all_instances(&self) -> Vec<Arc<RwLock<InstanceHealth>>> {
        self.instances.iter().map(|r| r.value().clone()).collect()
    }

    /// Check if an instance is healthy
    pub fn is_healthy(&self, addr: &str) -> bool {
        self.instances
            .get(addr)
            .map(|h| h.read().is_healthy())
            .unwrap_or(false)
    }

    /// Check if an instance is available (healthy or unknown)
    pub fn is_available(&self, addr: &str) -> bool {
        self.instances
            .get(addr)
            .map(|h| h.read().is_available())
            .unwrap_or(true) // Default to available if not registered
    }

    /// Get the health state for an instance
    pub fn get(&self, addr: &str) -> Option<Arc<RwLock<InstanceHealth>>> {
        self.instances.get(addr).map(|r| r.value().clone())
    }

    /// Get the number of registered instances
    pub fn len(&self) -> usize {
        self.instances.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.instances.is_empty()
    }

    /// Get statistics about the registry
    pub fn stats(&self) -> RegistryStats {
        let mut healthy = 0;
        let mut unhealthy = 0;
        let mut unknown = 0;

        for entry in self.instances.iter() {
            let health = entry.value().read();
            match health.status {
                super::state::HealthStatus::Healthy => healthy += 1,
                super::state::HealthStatus::Unhealthy => unhealthy += 1,
                super::state::HealthStatus::Unknown => unknown += 1,
            }
        }

        RegistryStats {
            total: self.instances.len(),
            healthy,
            unhealthy,
            unknown,
        }
    }
}

/// Statistics about the instance registry
#[derive(Debug, Clone)]
pub struct RegistryStats {
    pub total: usize,
    pub healthy: usize,
    pub unhealthy: usize,
    pub unknown: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DBInstanceRole, LimiterConfig};

    fn create_test_registry() -> InstanceRegistry {
        // Use disabled config for tests (no tokio runtime needed)
        let mut config = HealthCheckConfig::default();
        config.enabled = false;
        InstanceRegistry::with_config(config)
    }

    fn create_test_config(host: &str, port: u16) -> DBInstanceConfig {
        DBInstanceConfig {
            host: host.to_string(),
            port,
            user: "root".to_string(),
            password: "".to_string(),
            database: None,
            role: DBInstanceRole::Master,
            limiter: LimiterConfig::default(),
        }
    }

    #[test]
    fn test_register_single() {
        let registry = create_test_registry();
        let config = create_test_config("localhost", 3306);

        let health = registry.register(&config);
        assert_eq!(registry.len(), 1);

        let h = health.read();
        assert_eq!(h.addr, "localhost:3306");
    }

    #[test]
    fn test_register_duplicate_increments_refcount() {
        let registry = create_test_registry();
        let config = create_test_config("localhost", 3306);

        // Register twice
        let health1 = registry.register(&config);
        let health2 = registry.register(&config);

        // Should be same instance
        assert_eq!(registry.len(), 1);
        assert!(Arc::ptr_eq(&health1, &health2));

        // Ref count should be 2
        assert_eq!(*registry.ref_counts.get("localhost:3306").unwrap(), 2);
    }

    #[test]
    fn test_unregister_decrements_refcount() {
        let registry = create_test_registry();
        let config = create_test_config("localhost", 3306);

        registry.register(&config);
        registry.register(&config);
        assert_eq!(*registry.ref_counts.get("localhost:3306").unwrap(), 2);

        registry.unregister("localhost:3306");
        assert_eq!(*registry.ref_counts.get("localhost:3306").unwrap(), 1);
        assert_eq!(registry.len(), 1);

        registry.unregister("localhost:3306");
        assert!(registry.ref_counts.get("localhost:3306").is_none());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_different_instances() {
        let registry = create_test_registry();
        let config1 = create_test_config("mysql-1", 3306);
        let config2 = create_test_config("mysql-2", 3306);

        registry.register(&config1);
        registry.register(&config2);

        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn test_is_available_default() {
        let registry = create_test_registry();

        // Unknown instances are available by default
        assert!(registry.is_available("nonexistent:3306"));

        let config = create_test_config("localhost", 3306);
        let health = registry.register(&config);

        // Newly registered (Unknown status) is available
        assert!(registry.is_available("localhost:3306"));

        // Make it healthy (need enough successes for sliding window)
        // Default window: min_samples=3, healthy_threshold=5
        for _ in 0..5 {
            health.write().record_success(None);
        }
        assert!(registry.is_available("localhost:3306"));
        assert!(registry.is_healthy("localhost:3306"));
    }

    #[test]
    fn test_stats() {
        let registry = create_test_registry();
        let config1 = create_test_config("mysql-1", 3306);
        let config2 = create_test_config("mysql-2", 3306);
        let config3 = create_test_config("mysql-3", 3306);

        let h1 = registry.register(&config1);
        let h2 = registry.register(&config2);
        let _h3 = registry.register(&config3);

        // Need enough samples for sliding window to transition states
        // Default: unhealthy_threshold=5, healthy_threshold=5, min_samples=3
        for _ in 0..5 {
            h1.write().record_success(None);
        }
        for _ in 0..5 {
            h2.write().record_failure();
        }
        // h3 remains Unknown

        let stats = registry.stats();
        assert_eq!(stats.total, 3);
        assert_eq!(stats.healthy, 1);
        assert_eq!(stats.unhealthy, 1);
        assert_eq!(stats.unknown, 1);
    }
}
