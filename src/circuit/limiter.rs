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
}

impl LimitPermit {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self { _permit: permit }
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
    pub async fn acquire(&self, _key: LimitKey) -> Result<LimitPermit, LimitError> {
        let permit = self.limiter.acquire().await?;
        // Convert internal permit to legacy LimitPermit
        // Note: We need to re-acquire since we can't move out of Permit
        let owned = self.limiter.semaphore.clone().try_acquire_owned()
            .map_err(|_| LimitError::QueueFull { max: 0 })?;
        // Release the permit we just acquired
        drop(permit);
        Ok(LimitPermit::new(owned))
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

        // Acquire second permit
        let permit2 = limiter.acquire().await.unwrap();

        // Drop permits
        drop(permit1);
        drop(permit2);
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

}
