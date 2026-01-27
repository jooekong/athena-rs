//! Instance selection strategies for read-write splitting
//!
//! This module provides strategies for selecting DBInstance from a DBGroup
//! based on the routing target (Master/Slave).

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::config::{DBGroupConfig, DBInstanceConfig};
use crate::router::RouteTarget;

/// Strategy for selecting an instance from a list
pub trait InstanceSelector: Send + Sync {
    /// Select an instance from the given list
    ///
    /// Returns None if the list is empty
    fn select<'a>(&self, instances: &[&'a DBInstanceConfig]) -> Option<&'a DBInstanceConfig>;
}

/// Select the first instance (default strategy for masters)
#[derive(Debug, Default)]
pub struct FirstSelector;

impl InstanceSelector for FirstSelector {
    fn select<'a>(&self, instances: &[&'a DBInstanceConfig]) -> Option<&'a DBInstanceConfig> {
        instances.first().copied()
    }
}

/// Round-robin selection (default strategy for slaves)
#[derive(Debug, Default)]
pub struct RoundRobinSelector {
    counter: AtomicUsize,
}

impl RoundRobinSelector {
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl InstanceSelector for RoundRobinSelector {
    fn select<'a>(&self, instances: &[&'a DBInstanceConfig]) -> Option<&'a DBInstanceConfig> {
        if instances.is_empty() {
            return None;
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % instances.len();
        Some(instances[idx])
    }
}

/// Read-write router that selects instances based on routing target
///
/// # Example
///
/// ```ignore
/// let router = ReadWriteRouter::default();
/// let instance = router.select(&db_group, RouteTarget::Slave);
/// ```
pub struct ReadWriteRouter {
    /// Strategy for selecting master instances
    master_selector: Box<dyn InstanceSelector>,
    /// Strategy for selecting slave instances
    slave_selector: Box<dyn InstanceSelector>,
}

impl Default for ReadWriteRouter {
    fn default() -> Self {
        Self {
            master_selector: Box::new(FirstSelector),
            slave_selector: Box::new(RoundRobinSelector::new()),
        }
    }
}

impl ReadWriteRouter {
    /// Create a new router with custom selectors
    pub fn new(
        master_selector: Box<dyn InstanceSelector>,
        slave_selector: Box<dyn InstanceSelector>,
    ) -> Self {
        Self {
            master_selector,
            slave_selector,
        }
    }

    /// Select an instance from the DBGroup based on the routing target
    ///
    /// # Behavior
    /// - `RouteTarget::Master`: Select from master instances using master_selector
    /// - `RouteTarget::Slave`: Select from slave instances using slave_selector,
    ///   fallback to master if no slaves available
    pub fn select<'a>(
        &self,
        db_group: &'a DBGroupConfig,
        target: RouteTarget,
    ) -> Option<&'a DBInstanceConfig> {
        match target {
            RouteTarget::Master => {
                let masters = db_group.masters();
                self.master_selector.select(&masters)
            }
            RouteTarget::Slave => {
                let slaves = db_group.slaves();
                if slaves.is_empty() {
                    // Fallback to master if no slaves
                    let masters = db_group.masters();
                    self.master_selector.select(&masters)
                } else {
                    self.slave_selector.select(&slaves)
                }
            }
        }
    }

    /// Select an instance, returning the actual target used
    ///
    /// This is useful for knowing whether a fallback occurred
    pub fn select_with_target<'a>(
        &self,
        db_group: &'a DBGroupConfig,
        target: RouteTarget,
    ) -> Option<(&'a DBInstanceConfig, RouteTarget)> {
        match target {
            RouteTarget::Master => {
                let masters = db_group.masters();
                self.master_selector
                    .select(&masters)
                    .map(|i| (i, RouteTarget::Master))
            }
            RouteTarget::Slave => {
                let slaves = db_group.slaves();
                if slaves.is_empty() {
                    // Fallback to master
                    let masters = db_group.masters();
                    self.master_selector
                        .select(&masters)
                        .map(|i| (i, RouteTarget::Master))
                } else {
                    self.slave_selector
                        .select(&slaves)
                        .map(|i| (i, RouteTarget::Slave))
                }
            }
        }
    }

    /// Select a healthy instance from the DBGroup
    ///
    /// Filters out unhealthy instances before selection.
    /// Uses the InstanceRegistry to check health status.
    pub fn select_healthy<'a>(
        &self,
        db_group: &'a DBGroupConfig,
        target: RouteTarget,
        registry: &crate::health::InstanceRegistry,
    ) -> Option<&'a DBInstanceConfig> {
        match target {
            RouteTarget::Master => {
                let masters = db_group.masters();
                let healthy: Vec<_> = masters
                    .into_iter()
                    .filter(|m| registry.is_available(&m.addr()))
                    .collect();
                self.master_selector.select(&healthy)
            }
            RouteTarget::Slave => {
                let slaves = db_group.slaves();
                let healthy_slaves: Vec<_> = slaves
                    .into_iter()
                    .filter(|s| registry.is_available(&s.addr()))
                    .collect();

                if healthy_slaves.is_empty() {
                    // Fallback to healthy master
                    let masters = db_group.masters();
                    let healthy_masters: Vec<_> = masters
                        .into_iter()
                        .filter(|m| registry.is_available(&m.addr()))
                        .collect();
                    self.master_selector.select(&healthy_masters)
                } else {
                    self.slave_selector.select(&healthy_slaves)
                }
            }
        }
    }

    /// Select a healthy instance, returning the actual target used
    pub fn select_healthy_with_target<'a>(
        &self,
        db_group: &'a DBGroupConfig,
        target: RouteTarget,
        registry: &crate::health::InstanceRegistry,
    ) -> Option<(&'a DBInstanceConfig, RouteTarget)> {
        match target {
            RouteTarget::Master => {
                let masters = db_group.masters();
                let healthy: Vec<_> = masters
                    .into_iter()
                    .filter(|m| registry.is_available(&m.addr()))
                    .collect();
                self.master_selector
                    .select(&healthy)
                    .map(|i| (i, RouteTarget::Master))
            }
            RouteTarget::Slave => {
                let slaves = db_group.slaves();
                let healthy_slaves: Vec<_> = slaves
                    .into_iter()
                    .filter(|s| registry.is_available(&s.addr()))
                    .collect();

                if healthy_slaves.is_empty() {
                    // Fallback to healthy master
                    let masters = db_group.masters();
                    let healthy_masters: Vec<_> = masters
                        .into_iter()
                        .filter(|m| registry.is_available(&m.addr()))
                        .collect();
                    self.master_selector
                        .select(&healthy_masters)
                        .map(|i| (i, RouteTarget::Master))
                } else {
                    self.slave_selector
                        .select(&healthy_slaves)
                        .map(|i| (i, RouteTarget::Slave))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DBInstanceRole, LimiterConfig};

    fn create_test_instance(host: &str, role: DBInstanceRole) -> DBInstanceConfig {
        DBInstanceConfig {
            host: host.to_string(),
            port: 3306,
            user: "root".to_string(),
            password: "".to_string(),
            database: None,
            role,
            limiter: LimiterConfig::default(),
        }
    }

    fn create_test_db_group() -> DBGroupConfig {
        DBGroupConfig {
            name: "shard_0".to_string(),
            instances: vec![
                create_test_instance("master-1", DBInstanceRole::Master),
                create_test_instance("master-2", DBInstanceRole::Master),
                create_test_instance("slave-1", DBInstanceRole::Slave),
                create_test_instance("slave-2", DBInstanceRole::Slave),
                create_test_instance("slave-3", DBInstanceRole::Slave),
            ],
        }
    }

    #[test]
    fn test_first_selector() {
        let selector = FirstSelector;
        let db_group = create_test_db_group();
        let masters = db_group.masters();

        let selected = selector.select(&masters).unwrap();
        assert_eq!(selected.host, "master-1");

        // Always returns the first
        let selected2 = selector.select(&masters).unwrap();
        assert_eq!(selected2.host, "master-1");
    }

    #[test]
    fn test_round_robin_selector() {
        let selector = RoundRobinSelector::new();
        let db_group = create_test_db_group();
        let slaves = db_group.slaves();

        // Should cycle through slaves
        assert_eq!(selector.select(&slaves).unwrap().host, "slave-1");
        assert_eq!(selector.select(&slaves).unwrap().host, "slave-2");
        assert_eq!(selector.select(&slaves).unwrap().host, "slave-3");
        assert_eq!(selector.select(&slaves).unwrap().host, "slave-1"); // wraps around
    }

    #[test]
    fn test_read_write_router_master() {
        let router = ReadWriteRouter::default();
        let db_group = create_test_db_group();

        let instance = router.select(&db_group, RouteTarget::Master).unwrap();
        assert_eq!(instance.host, "master-1");
        assert!(instance.is_master());
    }

    #[test]
    fn test_read_write_router_slave() {
        let router = ReadWriteRouter::default();
        let db_group = create_test_db_group();

        // Should use round-robin for slaves
        let i1 = router.select(&db_group, RouteTarget::Slave).unwrap();
        let i2 = router.select(&db_group, RouteTarget::Slave).unwrap();
        let i3 = router.select(&db_group, RouteTarget::Slave).unwrap();

        assert!(i1.is_slave());
        assert!(i2.is_slave());
        assert!(i3.is_slave());

        // Different instances (round-robin)
        assert_eq!(i1.host, "slave-1");
        assert_eq!(i2.host, "slave-2");
        assert_eq!(i3.host, "slave-3");
    }

    #[test]
    fn test_read_write_router_slave_fallback() {
        let router = ReadWriteRouter::default();

        // DB group with no slaves
        let db_group = DBGroupConfig {
            name: "shard_0".to_string(),
            instances: vec![create_test_instance("master-1", DBInstanceRole::Master)],
        };

        // Should fallback to master
        let instance = router.select(&db_group, RouteTarget::Slave).unwrap();
        assert_eq!(instance.host, "master-1");
        assert!(instance.is_master());
    }

    #[test]
    fn test_select_with_target_reports_fallback() {
        let router = ReadWriteRouter::default();

        // DB group with no slaves
        let db_group = DBGroupConfig {
            name: "shard_0".to_string(),
            instances: vec![create_test_instance("master-1", DBInstanceRole::Master)],
        };

        // Should report Master as actual target due to fallback
        let (instance, actual_target) = router
            .select_with_target(&db_group, RouteTarget::Slave)
            .unwrap();
        assert_eq!(instance.host, "master-1");
        assert_eq!(actual_target, RouteTarget::Master);
    }

    #[test]
    fn test_empty_instances() {
        let router = ReadWriteRouter::default();
        let db_group = DBGroupConfig {
            name: "shard_0".to_string(),
            instances: vec![],
        };

        assert!(router.select(&db_group, RouteTarget::Master).is_none());
        assert!(router.select(&db_group, RouteTarget::Slave).is_none());
    }

    fn create_test_registry() -> crate::health::InstanceRegistry {
        use crate::config::HealthCheckConfig;
        let mut config = HealthCheckConfig::default();
        config.enabled = false; // Disable spawning for tests
        crate::health::InstanceRegistry::with_config(config)
    }

    #[test]
    fn test_select_healthy_filters_unhealthy() {
        let router = ReadWriteRouter::default();
        let db_group = create_test_db_group();
        let registry = create_test_registry();

        // Register all instances
        for inst in &db_group.instances {
            registry.register(inst);
        }

        // Mark master-1 as unhealthy (need enough failures for sliding window)
        // Default window: unhealthy_threshold=5, min_samples=3
        if let Some(health) = registry.get("master-1:3306") {
            for _ in 0..5 {
                health.write().record_failure();
            }
        }

        // Should select master-2 instead
        let instance = router
            .select_healthy(&db_group, RouteTarget::Master, &registry)
            .unwrap();
        assert_eq!(instance.host, "master-2");
    }

    #[test]
    fn test_select_healthy_fallback_when_all_slaves_unhealthy() {
        let router = ReadWriteRouter::default();
        let db_group = create_test_db_group();
        let registry = create_test_registry();

        // Register all instances
        for inst in &db_group.instances {
            registry.register(inst);
        }

        // Mark all slaves as unhealthy (need enough failures for sliding window)
        for slave in db_group.slaves() {
            if let Some(health) = registry.get(&slave.addr()) {
                for _ in 0..5 {
                    health.write().record_failure();
                }
            }
        }

        // Should fallback to master
        let (instance, actual_target) = router
            .select_healthy_with_target(&db_group, RouteTarget::Slave, &registry)
            .unwrap();
        assert!(instance.is_master());
        assert_eq!(actual_target, RouteTarget::Master);
    }
}
