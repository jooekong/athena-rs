//! Health state definitions for DBInstance monitoring
//!
//! Uses a sliding window to track health check results, providing
//! stable state transitions that avoid flapping on network jitter.

use std::collections::VecDeque;
use std::time::Instant;

use crate::config::DBInstanceRole;

/// Health status of a DBInstance
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Instance is healthy and accepting connections
    Healthy,
    /// Instance is unhealthy (too many failures in window)
    Unhealthy,
    /// Instance status is unknown (not enough samples)
    Unknown,
}

impl Default for HealthStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Result of a single health check
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckResult {
    Success,
    Failure,
}

/// Sliding window configuration for health state transitions
#[derive(Debug, Clone)]
pub struct WindowConfig {
    /// Window size (number of recent checks to consider)
    pub window_size: usize,
    /// Failures needed to transition Healthy → Unhealthy
    pub unhealthy_threshold: usize,
    /// Successes needed to transition Unhealthy → Healthy
    pub healthy_threshold: usize,
    /// Minimum samples before leaving Unknown state
    pub min_samples: usize,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            window_size: 10,
            unhealthy_threshold: 5,  // 5 failures in 10 checks → unhealthy
            healthy_threshold: 5,     // 5 successes in 10 checks → healthy
            min_samples: 3,           // Need at least 3 checks to decide
        }
    }
}

impl WindowConfig {
    /// Create from HealthCheckConfig's failure_threshold
    ///
    /// Maps failure_threshold to unhealthy_threshold, uses same value for healthy_threshold
    pub fn from_failure_threshold(threshold: u32) -> Self {
        let threshold = threshold.max(1) as usize;
        Self {
            window_size: threshold * 2, // Window size is 2x threshold
            unhealthy_threshold: threshold,
            healthy_threshold: threshold, // Same for recovery
            min_samples: (threshold / 2).max(1), // At least half threshold samples
        }
    }
}

/// Health state for a single DBInstance using sliding window
#[derive(Debug)]
pub struct InstanceHealth {
    /// Instance address (host:port)
    pub addr: String,
    /// Current health status
    pub status: HealthStatus,
    /// Sliding window of recent check results
    window: VecDeque<CheckResult>,
    /// Window configuration
    config: WindowConfig,
    /// Detected role (may differ from configured role)
    pub detected_role: Option<DBInstanceRole>,
    /// Last check timestamp
    pub last_check: Option<Instant>,
    /// Last successful check timestamp
    pub last_success: Option<Instant>,
    /// Connection credentials for health checks
    pub(crate) user: String,
    pub(crate) password: String,
}

impl InstanceHealth {
    /// Create a new InstanceHealth with Unknown status
    pub fn new(addr: String, user: String, password: String) -> Self {
        Self::with_config(addr, user, password, WindowConfig::default())
    }

    /// Create with custom window configuration
    pub fn with_config(
        addr: String,
        user: String,
        password: String,
        config: WindowConfig,
    ) -> Self {
        Self {
            addr,
            status: HealthStatus::Unknown,
            window: VecDeque::with_capacity(config.window_size),
            config,
            detected_role: None,
            last_check: None,
            last_success: None,
            user,
            password,
        }
    }

    /// Check if instance is healthy
    pub fn is_healthy(&self) -> bool {
        self.status == HealthStatus::Healthy
    }

    /// Check if instance is available (healthy or unknown)
    pub fn is_available(&self) -> bool {
        matches!(self.status, HealthStatus::Healthy | HealthStatus::Unknown)
    }

    /// Get success count in current window
    pub fn success_count(&self) -> usize {
        self.window.iter().filter(|r| **r == CheckResult::Success).count()
    }

    /// Get failure count in current window
    pub fn failure_count(&self) -> usize {
        self.window.iter().filter(|r| **r == CheckResult::Failure).count()
    }

    /// Record a successful health check
    ///
    /// Returns true if status changed
    pub fn record_success(&mut self, detected_role: Option<DBInstanceRole>) -> bool {
        // Only update detected_role if a new role was detected
        if detected_role.is_some() {
            self.detected_role = detected_role;
        }
        let now = Instant::now();
        self.last_check = Some(now);
        self.last_success = Some(now);

        self.push_result(CheckResult::Success)
    }

    /// Record a failed health check
    ///
    /// Returns true if status changed
    pub fn record_failure(&mut self) -> bool {
        self.last_check = Some(Instant::now());
        self.push_result(CheckResult::Failure)
    }

    /// Push a result to the sliding window and update status
    fn push_result(&mut self, result: CheckResult) -> bool {
        // Add to window, maintaining max size
        if self.window.len() >= self.config.window_size {
            self.window.pop_front();
        }
        self.window.push_back(result);

        // Update status based on window contents
        let old_status = self.status;
        self.status = self.calculate_status();
        
        old_status != self.status
    }

    /// Calculate status based on current window and previous status
    fn calculate_status(&self) -> HealthStatus {
        let sample_count = self.window.len();
        let success_count = self.success_count();
        let failure_count = self.failure_count();

        // Not enough samples to decide
        if sample_count < self.config.min_samples {
            return HealthStatus::Unknown;
        }

        match self.status {
            HealthStatus::Unknown => {
                // From Unknown: need clear signal to transition
                if failure_count >= self.config.unhealthy_threshold {
                    HealthStatus::Unhealthy
                } else if success_count >= self.config.healthy_threshold {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unknown
                }
            }
            HealthStatus::Healthy => {
                // From Healthy: only go Unhealthy if failures exceed threshold
                if failure_count >= self.config.unhealthy_threshold {
                    HealthStatus::Unhealthy
                } else {
                    HealthStatus::Healthy
                }
            }
            HealthStatus::Unhealthy => {
                // From Unhealthy: need sustained success to recover
                if success_count >= self.config.healthy_threshold {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unhealthy
                }
            }
        }
    }

    /// Get time since last check
    pub fn time_since_last_check(&self) -> Option<std::time::Duration> {
        self.last_check.map(|t| t.elapsed())
    }

    /// Get time since last success
    pub fn time_since_last_success(&self) -> Option<std::time::Duration> {
        self.last_success.map(|t| t.elapsed())
    }

    /// Get current window size
    pub fn window_len(&self) -> usize {
        self.window.len()
    }

    /// Get window configuration
    pub fn window_config(&self) -> &WindowConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_window_config() -> WindowConfig {
        WindowConfig {
            window_size: 5,
            unhealthy_threshold: 3,
            healthy_threshold: 3,
            min_samples: 2,
        }
    }

    #[test]
    fn test_instance_health_new() {
        let health = InstanceHealth::new(
            "localhost:3306".to_string(),
            "root".to_string(),
            "".to_string(),
        );
        assert_eq!(health.status, HealthStatus::Unknown);
        assert!(health.is_available());
        assert!(!health.is_healthy());
        assert_eq!(health.window_len(), 0);
    }

    #[test]
    fn test_unknown_to_healthy() {
        let mut health = InstanceHealth::with_config(
            "localhost:3306".to_string(),
            "root".to_string(),
            "".to_string(),
            small_window_config(),
        );

        // Need 3 successes with min_samples=2
        health.record_success(Some(DBInstanceRole::Master));
        assert_eq!(health.status, HealthStatus::Unknown); // Not enough samples

        health.record_success(None);
        assert_eq!(health.status, HealthStatus::Unknown); // 2 successes, need 3

        health.record_success(None);
        assert_eq!(health.status, HealthStatus::Healthy); // Now healthy
        assert!(health.is_healthy());
        assert_eq!(health.detected_role, Some(DBInstanceRole::Master));
    }

    #[test]
    fn test_unknown_to_unhealthy() {
        let mut health = InstanceHealth::with_config(
            "localhost:3306".to_string(),
            "root".to_string(),
            "".to_string(),
            small_window_config(),
        );

        // Need 3 failures
        health.record_failure();
        health.record_failure();
        assert_eq!(health.status, HealthStatus::Unknown);

        health.record_failure();
        assert_eq!(health.status, HealthStatus::Unhealthy);
        assert!(!health.is_available());
    }

    #[test]
    fn test_healthy_to_unhealthy_needs_threshold() {
        let mut health = InstanceHealth::with_config(
            "localhost:3306".to_string(),
            "root".to_string(),
            "".to_string(),
            small_window_config(),
        );

        // Make healthy first
        for _ in 0..3 {
            health.record_success(None);
        }
        assert!(health.is_healthy());

        // Single failure shouldn't change status
        health.record_failure();
        assert!(health.is_healthy());

        // Second failure still healthy
        health.record_failure();
        assert!(health.is_healthy());

        // Third failure triggers unhealthy (window: S,S,S,F,F -> then F)
        // Actually window is now: S,S,F,F,? 
        // Let's add more context
        health.record_failure();
        assert_eq!(health.status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_unhealthy_to_healthy_needs_sustained_success() {
        let mut health = InstanceHealth::with_config(
            "localhost:3306".to_string(),
            "root".to_string(),
            "".to_string(),
            small_window_config(),
        );

        // Make unhealthy
        for _ in 0..3 {
            health.record_failure();
        }
        assert_eq!(health.status, HealthStatus::Unhealthy);

        // Single success shouldn't recover (window: F,F,F,S)
        health.record_success(None);
        assert_eq!(health.status, HealthStatus::Unhealthy);

        // Second success still unhealthy (window: F,F,F,S,S)
        health.record_success(None);
        assert_eq!(health.status, HealthStatus::Unhealthy);

        // Third success triggers healthy (window: F,F,S,S,S - 3 successes >= threshold)
        health.record_success(None);
        assert_eq!(health.status, HealthStatus::Healthy);
    }

    #[test]
    fn test_sliding_window_evicts_old_results() {
        let mut health = InstanceHealth::with_config(
            "localhost:3306".to_string(),
            "root".to_string(),
            "".to_string(),
            small_window_config(), // window_size = 5
        );

        // Fill window with failures
        for _ in 0..5 {
            health.record_failure();
        }
        assert_eq!(health.status, HealthStatus::Unhealthy);
        assert_eq!(health.failure_count(), 5);
        assert_eq!(health.window_len(), 5);

        // Add successes - old failures get evicted
        for _ in 0..5 {
            health.record_success(None);
        }
        assert_eq!(health.failure_count(), 0);
        assert_eq!(health.success_count(), 5);
        assert_eq!(health.status, HealthStatus::Healthy);
    }

    #[test]
    fn test_flapping_protection() {
        let mut health = InstanceHealth::with_config(
            "localhost:3306".to_string(),
            "root".to_string(),
            "".to_string(),
            small_window_config(),
        );

        // Make healthy
        for _ in 0..3 {
            health.record_success(None);
        }
        assert!(health.is_healthy());

        // Intermittent failures shouldn't cause flapping
        // Pattern: S,S,S -> S,S,S,F -> S,S,F,S -> S,F,S,F -> F,S,F,S
        health.record_failure();
        assert!(health.is_healthy()); // Still healthy

        health.record_success(None);
        assert!(health.is_healthy());

        health.record_failure();
        assert!(health.is_healthy()); // Window: S,S,F,S,F - only 2 failures

        health.record_success(None);
        assert!(health.is_healthy()); // Window: S,F,S,F,S - only 2 failures

        // This pattern should keep it healthy despite occasional failures
        assert_eq!(health.failure_count(), 2);
        assert_eq!(health.success_count(), 3);
    }

    #[test]
    fn test_status_change_returns_true() {
        let mut health = InstanceHealth::with_config(
            "localhost:3306".to_string(),
            "root".to_string(),
            "".to_string(),
            small_window_config(),
        );

        // First few don't change status (Unknown -> Unknown)
        assert!(!health.record_success(None));
        assert!(!health.record_success(None));

        // Third success: Unknown -> Healthy
        assert!(health.record_success(None));

        // More successes don't change status
        assert!(!health.record_success(None));

        // Failures that don't cross threshold
        assert!(!health.record_failure());
        assert!(!health.record_failure());

        // Third failure: Healthy -> Unhealthy
        assert!(health.record_failure());
    }
}
