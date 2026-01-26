//! Simplified rate limiter for DBInstance
//!
//! Each DBInstance has its own embedded Limiter, eliminating the need for
//! dynamic key management (DashMap) and complex LimitKey structures.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::config::LimiterConfig;

/// Error type for rate limiting
#[derive(Debug, thiserror::Error)]
pub enum LimitError {
    #[error("Queue full: max queue size {max} exceeded")]
    QueueFull { max: usize },

    #[error("Timeout waiting for available slot after {timeout:?}")]
    Timeout { timeout: Duration },

    #[error("Rate limiting is disabled")]
    Disabled,
}

/// RAII permit that releases the slot when dropped
pub struct Permit {
    _permit: OwnedSemaphorePermit,
}

impl Permit {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self { _permit: permit }
    }
}

/// Simple rate limiter for a single DBInstance
///
/// This limiter is designed to be embedded directly in a DBInstance,
/// eliminating the need for dynamic key management.
///
/// # Example
///
/// ```ignore
/// let limiter = Limiter::new(LimiterConfig::default());
///
/// // Acquire permit before executing query
/// let permit = limiter.acquire().await?;
///
/// // Execute query...
///
/// // Permit is automatically released when dropped
/// drop(permit);
/// ```
pub struct Limiter {
    /// Semaphore for controlling concurrency
    semaphore: Arc<Semaphore>,
    /// Current number of waiting requests
    waiting: AtomicUsize,
    /// Configuration
    config: LimiterConfig,
    /// Statistics
    stats: LimiterStats,
}

/// Limiter statistics
#[derive(Default)]
struct LimiterStats {
    acquired: AtomicUsize,
    rejected_full: AtomicUsize,
    rejected_timeout: AtomicUsize,
}

impl Limiter {
    /// Create a new limiter with the given configuration
    pub fn new(config: LimiterConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent)),
            waiting: AtomicUsize::new(0),
            config,
            stats: LimiterStats::default(),
        }
    }

    /// Create a limiter with default configuration
    pub fn with_defaults() -> Self {
        Self::new(LimiterConfig::default())
    }

    /// Check if rate limiting is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the maximum concurrent requests allowed
    pub fn max_concurrent(&self) -> usize {
        self.config.max_concurrent
    }

    /// Get the number of available permits
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get the number of active (acquired) permits
    pub fn active_count(&self) -> usize {
        self.config.max_concurrent - self.semaphore.available_permits()
    }

    /// Get the number of waiting requests
    pub fn waiting_count(&self) -> usize {
        self.waiting.load(Ordering::Relaxed)
    }

    /// Acquire a permit for executing a request
    ///
    /// Returns a permit that must be held while the request is being processed.
    /// The permit is automatically released when dropped.
    ///
    /// # Errors
    ///
    /// - `LimitError::Disabled` - Rate limiting is disabled
    /// - `LimitError::QueueFull` - Too many requests waiting
    /// - `LimitError::Timeout` - Timed out waiting for a permit
    pub async fn acquire(&self) -> Result<Permit, LimitError> {
        if !self.config.enabled {
            return Err(LimitError::Disabled);
        }

        // Fast path: try to acquire immediately without waiting
        if let Ok(permit) = self.semaphore.clone().try_acquire_owned() {
            self.stats.acquired.fetch_add(1, Ordering::Relaxed);
            debug!("Acquired rate limit permit (fast path)");
            return Ok(Permit::new(permit));
        }

        // Slow path: need to wait, check if queue is full first
        let current_waiting = self.waiting.fetch_add(1, Ordering::SeqCst);
        if current_waiting >= self.config.max_queue_size {
            self.waiting.fetch_sub(1, Ordering::SeqCst);
            self.stats.rejected_full.fetch_add(1, Ordering::Relaxed);
            warn!(
                max_queue = self.config.max_queue_size,
                current_waiting,
                "Rate limit queue full"
            );
            return Err(LimitError::QueueFull {
                max: self.config.max_queue_size,
            });
        }

        // Try to acquire semaphore with timeout
        let queue_timeout = Duration::from_millis(self.config.queue_timeout_ms);
        let result = timeout(queue_timeout, self.semaphore.clone().acquire_owned()).await;

        // Decrement waiting count
        self.waiting.fetch_sub(1, Ordering::SeqCst);

        match result {
            Ok(Ok(permit)) => {
                self.stats.acquired.fetch_add(1, Ordering::Relaxed);
                debug!("Acquired rate limit permit (slow path)");
                Ok(Permit::new(permit))
            }
            Ok(Err(_)) => {
                // Semaphore closed - shouldn't happen
                Err(LimitError::Timeout { timeout: queue_timeout })
            }
            Err(_) => {
                self.stats.rejected_timeout.fetch_add(1, Ordering::Relaxed);
                warn!(timeout = ?queue_timeout, "Rate limit timeout");
                Err(LimitError::Timeout { timeout: queue_timeout })
            }
        }
    }

    /// Try to acquire a permit without waiting
    ///
    /// Returns `None` if no permit is available or rate limiting is disabled.
    pub fn try_acquire(&self) -> Option<Permit> {
        if !self.config.enabled {
            return None;
        }

        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                self.stats.acquired.fetch_add(1, Ordering::Relaxed);
                Some(Permit::new(permit))
            }
            Err(_) => None,
        }
    }

    /// Get limiter statistics
    pub fn stats(&self) -> Stats {
        Stats {
            total_acquired: self.stats.acquired.load(Ordering::Relaxed),
            total_rejected_full: self.stats.rejected_full.load(Ordering::Relaxed),
            total_rejected_timeout: self.stats.rejected_timeout.load(Ordering::Relaxed),
            current_active: self.active_count(),
            current_waiting: self.waiting_count(),
        }
    }
}

/// Public statistics for a limiter
#[derive(Debug, Clone)]
pub struct Stats {
    pub total_acquired: usize,
    pub total_rejected_full: usize,
    pub total_rejected_timeout: usize,
    pub current_active: usize,
    pub current_waiting: usize,
}

// ============================================================================
// Legacy support: Keep old types for backward compatibility during migration
// ============================================================================

/// Legacy: Key for identifying rate limit scope
/// Deprecated: Use Limiter embedded in DBInstance instead
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LimitKey {
    pub user: String,
    pub shard: String,
}

impl LimitKey {
    pub fn new(user: impl Into<String>, shard: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            shard: shard.into(),
        }
    }
}

/// Legacy: RAII permit with key
/// Deprecated: Use Permit instead
pub struct LimitPermit {
    _permit: OwnedSemaphorePermit,
    key: LimitKey,
}

impl LimitPermit {
    fn new(permit: OwnedSemaphorePermit, key: LimitKey) -> Self {
        Self { _permit: permit, key }
    }

    pub fn key(&self) -> &LimitKey {
        &self.key
    }
}

/// Legacy: Configuration for concurrency limits
/// Deprecated: Use LimiterConfig instead
#[derive(Debug, Clone)]
pub struct LimitConfig {
    pub max_concurrent: usize,
    pub max_queue_size: usize,
    pub queue_timeout: Duration,
    pub enabled: bool,
}

impl Default for LimitConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 100,
            max_queue_size: 50,
            queue_timeout: Duration::from_secs(5),
            enabled: true,
        }
    }
}

impl From<&crate::config::CircuitConfig> for LimitConfig {
    fn from(config: &crate::config::CircuitConfig) -> Self {
        Self {
            max_concurrent: config.max_concurrent_per_user_shard,
            max_queue_size: config.queue_size,
            queue_timeout: Duration::from_millis(config.queue_timeout_ms),
            enabled: config.enabled,
        }
    }
}

impl From<crate::config::CircuitConfig> for LimitConfig {
    fn from(config: crate::config::CircuitConfig) -> Self {
        Self::from(&config)
    }
}

/// Legacy: Concurrency controller with dynamic key management
/// Deprecated: Use Limiter embedded in DBInstance instead
pub struct ConcurrencyController {
    limiter: Limiter,
}

impl ConcurrencyController {
    pub fn new(config: LimitConfig) -> Self {
        let limiter_config = LimiterConfig {
            enabled: config.enabled,
            max_concurrent: config.max_concurrent,
            max_queue_size: config.max_queue_size,
            queue_timeout_ms: config.queue_timeout.as_millis() as u64,
        };
        Self {
            limiter: Limiter::new(limiter_config),
        }
    }

    /// Acquire a permit (key is ignored in new implementation)
    pub async fn acquire(&self, key: LimitKey) -> Result<LimitPermit, LimitError> {
        let permit = self.limiter.acquire().await?;
        // Convert internal permit to legacy LimitPermit
        // Note: We need to re-acquire since we can't move out of Permit
        let owned = self.limiter.semaphore.clone().try_acquire_owned()
            .map_err(|_| LimitError::QueueFull { max: 0 })?;
        // Release the permit we just acquired
        drop(permit);
        Ok(LimitPermit::new(owned, key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_acquire_release() {
        let config = LimiterConfig {
            max_concurrent: 2,
            max_queue_size: 10,
            queue_timeout_ms: 1000,
            enabled: true,
        };
        let limiter = Limiter::new(config);

        // Acquire first permit
        let permit1 = limiter.acquire().await.unwrap();
        assert_eq!(limiter.active_count(), 1);

        // Acquire second permit
        let permit2 = limiter.acquire().await.unwrap();
        assert_eq!(limiter.active_count(), 2);

        // Drop permits
        drop(permit1);
        assert_eq!(limiter.active_count(), 1);
        drop(permit2);
        assert_eq!(limiter.active_count(), 0);

        let stats = limiter.stats();
        assert_eq!(stats.total_acquired, 2);
    }

    #[tokio::test]
    async fn test_try_acquire() {
        let config = LimiterConfig {
            max_concurrent: 1,
            max_queue_size: 10,
            queue_timeout_ms: 1000,
            enabled: true,
        };
        let limiter = Limiter::new(config);

        // First try_acquire should succeed
        let permit = limiter.try_acquire().unwrap();
        assert_eq!(limiter.active_count(), 1);

        // Second try_acquire should fail (no waiting)
        assert!(limiter.try_acquire().is_none());

        // After dropping, should succeed again
        drop(permit);
        assert!(limiter.try_acquire().is_some());
    }

    #[tokio::test]
    async fn test_queue_full() {
        let config = LimiterConfig {
            max_concurrent: 1,
            max_queue_size: 1,
            queue_timeout_ms: 100,
            enabled: true,
        };
        let limiter = Arc::new(Limiter::new(config));

        // Acquire first permit
        let _permit1 = limiter.acquire().await.unwrap();

        // Start one waiting request
        let limiter_clone = limiter.clone();
        let handle = tokio::spawn(async move {
            limiter_clone.acquire().await
        });

        // Give it time to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Try another acquire - should fail with QueueFull
        let result = limiter.acquire().await;
        assert!(matches!(result, Err(LimitError::QueueFull { .. })));

        // Clean up
        drop(handle);
    }

    #[tokio::test]
    async fn test_timeout() {
        let config = LimiterConfig {
            max_concurrent: 1,
            max_queue_size: 10,
            queue_timeout_ms: 50,
            enabled: true,
        };
        let limiter = Limiter::new(config);

        // Acquire and hold permit
        let _permit = limiter.acquire().await.unwrap();

        // Second acquire should timeout
        let result = limiter.acquire().await;
        assert!(matches!(result, Err(LimitError::Timeout { .. })));
    }

    #[tokio::test]
    async fn test_disabled() {
        let config = LimiterConfig {
            enabled: false,
            ..Default::default()
        };
        let limiter = Limiter::new(config);

        let result = limiter.acquire().await;
        assert!(matches!(result, Err(LimitError::Disabled)));
    }

    #[tokio::test]
    async fn test_zero_queue_size_first_acquire_succeeds() {
        // With queue_size=0, the first acquire should still succeed
        // because it doesn't need to wait (permit is available)
        let config = LimiterConfig {
            max_concurrent: 1,
            max_queue_size: 0,
            queue_timeout_ms: 100,
            enabled: true,
        };
        let limiter = Limiter::new(config);

        // First acquire should succeed (permit available, no waiting needed)
        let permit = limiter.acquire().await;
        assert!(permit.is_ok(), "First acquire should succeed with available permit");

        // Second acquire should fail with QueueFull (no queue allowed)
        let result = limiter.acquire().await;
        assert!(matches!(result, Err(LimitError::QueueFull { .. })));
    }

    #[tokio::test]
    async fn test_fast_path_no_queue_increment() {
        // When permits are available, we should not increment waiting count
        let config = LimiterConfig {
            max_concurrent: 2,
            max_queue_size: 1,
            queue_timeout_ms: 100,
            enabled: true,
        };
        let limiter = Arc::new(Limiter::new(config));

        // Acquire both permits - should not increment waiting count
        let _permit1 = limiter.acquire().await.unwrap();
        let _permit2 = limiter.acquire().await.unwrap();
        assert_eq!(limiter.waiting_count(), 0);

        // Third acquire should be able to wait (queue_size=1)
        let limiter_clone = limiter.clone();
        let handle = tokio::spawn(async move {
            limiter_clone.acquire().await
        });

        // Give it time to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Fourth acquire should fail with QueueFull (queue is now at capacity)
        let result = limiter.acquire().await;
        assert!(matches!(result, Err(LimitError::QueueFull { .. })));

        // Clean up
        drop(handle);
    }

    #[tokio::test]
    async fn test_stats() {
        let config = LimiterConfig {
            max_concurrent: 2,
            max_queue_size: 10,
            queue_timeout_ms: 1000,
            enabled: true,
        };
        let limiter = Limiter::new(config);

        let _permit1 = limiter.acquire().await.unwrap();
        let _permit2 = limiter.acquire().await.unwrap();

        let stats = limiter.stats();
        assert_eq!(stats.total_acquired, 2);
        assert_eq!(stats.current_active, 2);
        assert_eq!(stats.current_waiting, 0);
    }
}
