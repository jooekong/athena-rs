use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::config::CircuitConfig;

/// Key for identifying rate limit scope
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LimitKey {
    /// Username
    pub user: String,
    /// Shard identifier
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

/// Configuration for concurrency limits
#[derive(Debug, Clone)]
pub struct LimitConfig {
    /// Maximum concurrent requests per key
    pub max_concurrent: usize,
    /// Maximum queue size (requests waiting for a slot)
    pub max_queue_size: usize,
    /// Timeout for waiting in queue
    pub queue_timeout: Duration,
    /// Whether to enable rate limiting
    pub enabled: bool,
}

impl Default for LimitConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 10,
            max_queue_size: 100,
            queue_timeout: Duration::from_secs(30),
            enabled: true,
        }
    }
}

impl From<&CircuitConfig> for LimitConfig {
    fn from(config: &CircuitConfig) -> Self {
        Self {
            max_concurrent: config.max_concurrent_per_user_shard,
            max_queue_size: config.queue_size,
            queue_timeout: Duration::from_millis(config.queue_timeout_ms),
            enabled: config.enabled,
        }
    }
}

impl From<CircuitConfig> for LimitConfig {
    fn from(config: CircuitConfig) -> Self {
        Self::from(&config)
    }
}

/// State for a single limit key
pub struct ConcurrencyLimit {
    /// Semaphore for controlling concurrency
    semaphore: Arc<Semaphore>,
    /// Current number of waiting requests
    waiting: AtomicUsize,
    /// Maximum queue size
    max_queue: usize,
}

impl ConcurrencyLimit {
    fn new(max_concurrent: usize, max_queue: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            waiting: AtomicUsize::new(0),
            max_queue,
        }
    }

    /// Current number of active (acquired) permits
    pub fn active_count(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Current number of waiting requests
    pub fn waiting_count(&self) -> usize {
        self.waiting.load(Ordering::Relaxed)
    }
}

/// RAII permit that releases the slot when dropped
pub struct LimitPermit {
    _permit: OwnedSemaphorePermit,
    key: LimitKey,
}

impl LimitPermit {
    fn new(permit: OwnedSemaphorePermit, key: LimitKey) -> Self {
        Self {
            _permit: permit,
            key,
        }
    }

    /// Get the key this permit belongs to
    pub fn key(&self) -> &LimitKey {
        &self.key
    }
}

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

/// Concurrency controller for rate limiting
pub struct ConcurrencyController {
    /// Limits per key
    limits: DashMap<LimitKey, Arc<ConcurrencyLimit>>,
    /// Configuration
    config: LimitConfig,
    /// Statistics
    stats: ControllerStats,
}

/// Controller statistics
#[derive(Default)]
struct ControllerStats {
    acquired: AtomicUsize,
    rejected_full: AtomicUsize,
    rejected_timeout: AtomicUsize,
}

impl ConcurrencyController {
    /// Create a new controller with the given configuration
    pub fn new(config: LimitConfig) -> Self {
        Self {
            limits: DashMap::new(),
            config,
            stats: ControllerStats::default(),
        }
    }

    /// Try to acquire a permit for the given key
    ///
    /// Returns a permit that must be held while the request is being processed.
    /// The permit is automatically released when dropped.
    pub async fn acquire(&self, key: LimitKey) -> Result<LimitPermit, LimitError> {
        if !self.config.enabled {
            return Err(LimitError::Disabled);
        }

        // Get or create limit for this key
        let limit = self
            .limits
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(ConcurrencyLimit::new(
                    self.config.max_concurrent,
                    self.config.max_queue_size,
                ))
            })
            .clone();

        // Check if queue is full
        let current_waiting = limit.waiting.fetch_add(1, Ordering::SeqCst);
        if current_waiting >= self.config.max_queue_size {
            limit.waiting.fetch_sub(1, Ordering::SeqCst);
            self.stats.rejected_full.fetch_add(1, Ordering::Relaxed);
            warn!(
                user = %key.user,
                shard = %key.shard,
                max_queue = self.config.max_queue_size,
                "Rate limit queue full"
            );
            return Err(LimitError::QueueFull {
                max: self.config.max_queue_size,
            });
        }

        // Try to acquire semaphore with timeout
        let semaphore = limit.semaphore.clone();
        let result = timeout(
            self.config.queue_timeout,
            semaphore.acquire_owned(),
        )
        .await;

        // Decrement waiting count
        limit.waiting.fetch_sub(1, Ordering::SeqCst);

        match result {
            Ok(Ok(permit)) => {
                self.stats.acquired.fetch_add(1, Ordering::Relaxed);
                debug!(
                    user = %key.user,
                    shard = %key.shard,
                    "Acquired rate limit permit"
                );
                Ok(LimitPermit::new(permit, key))
            }
            Ok(Err(_)) => {
                // Semaphore closed - shouldn't happen
                Err(LimitError::Timeout {
                    timeout: self.config.queue_timeout,
                })
            }
            Err(_) => {
                self.stats.rejected_timeout.fetch_add(1, Ordering::Relaxed);
                warn!(
                    user = %key.user,
                    shard = %key.shard,
                    timeout = ?self.config.queue_timeout,
                    "Rate limit timeout"
                );
                Err(LimitError::Timeout {
                    timeout: self.config.queue_timeout,
                })
            }
        }
    }

    /// Try to acquire a permit without waiting
    ///
    /// Returns None if no permit is available.
    pub fn try_acquire(&self, key: LimitKey) -> Option<LimitPermit> {
        if !self.config.enabled {
            return None;
        }

        let limit = self
            .limits
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(ConcurrencyLimit::new(
                    self.config.max_concurrent,
                    self.config.max_queue_size,
                ))
            })
            .clone();

        match limit.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                self.stats.acquired.fetch_add(1, Ordering::Relaxed);
                Some(LimitPermit::new(permit, key))
            }
            Err(_) => None,
        }
    }

    /// Get statistics
    pub fn stats(&self) -> ConcurrencyStats {
        ConcurrencyStats {
            total_acquired: self.stats.acquired.load(Ordering::Relaxed),
            total_rejected_full: self.stats.rejected_full.load(Ordering::Relaxed),
            total_rejected_timeout: self.stats.rejected_timeout.load(Ordering::Relaxed),
            active_keys: self.limits.len(),
        }
    }

    /// Get the limit for a specific key
    pub fn get_limit(&self, key: &LimitKey) -> Option<Arc<ConcurrencyLimit>> {
        self.limits.get(key).map(|r| r.value().clone())
    }

    /// Get or create the limit for a specific key
    pub fn get_or_create_limit(&self, key: LimitKey) -> Arc<ConcurrencyLimit> {
        self.limits
            .entry(key)
            .or_insert_with(|| {
                Arc::new(ConcurrencyLimit::new(
                    self.config.max_concurrent,
                    self.config.max_queue_size,
                ))
            })
            .clone()
    }
}

/// Controller statistics
#[derive(Debug, Clone)]
pub struct ConcurrencyStats {
    pub total_acquired: usize,
    pub total_rejected_full: usize,
    pub total_rejected_timeout: usize,
    pub active_keys: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_acquire_release() {
        let config = LimitConfig {
            max_concurrent: 2,
            max_queue_size: 10,
            queue_timeout: Duration::from_secs(1),
            enabled: true,
        };
        let controller = ConcurrencyController::new(config);

        let key = LimitKey::new("user1", "shard1");

        // Acquire first permit
        let permit1 = controller.acquire(key.clone()).await.unwrap();
        assert_eq!(permit1.key().user, "user1");

        // Acquire second permit
        let permit2 = controller.acquire(key.clone()).await.unwrap();

        // Drop permits
        drop(permit1);
        drop(permit2);

        let stats = controller.stats();
        assert_eq!(stats.total_acquired, 2);
    }

    #[tokio::test]
    async fn test_try_acquire() {
        let config = LimitConfig {
            max_concurrent: 1,
            max_queue_size: 10,
            queue_timeout: Duration::from_secs(1),
            enabled: true,
        };
        let controller = ConcurrencyController::new(config);

        let key = LimitKey::new("user1", "shard1");

        // First try_acquire should succeed
        let permit = controller.try_acquire(key.clone()).unwrap();

        // Second try_acquire should fail (no waiting)
        assert!(controller.try_acquire(key.clone()).is_none());

        // After dropping, should succeed again
        drop(permit);
        assert!(controller.try_acquire(key).is_some());
    }

    #[tokio::test]
    async fn test_queue_full() {
        let config = LimitConfig {
            max_concurrent: 1,
            max_queue_size: 1,
            queue_timeout: Duration::from_millis(100),
            enabled: true,
        };
        let controller = Arc::new(ConcurrencyController::new(config));

        let key = LimitKey::new("user1", "shard1");

        // Acquire first permit
        let _permit1 = controller.acquire(key.clone()).await.unwrap();

        // Start one waiting request
        let controller_clone = controller.clone();
        let key_clone = key.clone();
        let handle = tokio::spawn(async move {
            controller_clone.acquire(key_clone).await
        });

        // Give it time to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Try another acquire - should fail with QueueFull
        let result = controller.acquire(key).await;
        assert!(matches!(result, Err(LimitError::QueueFull { .. })));

        // Clean up
        drop(handle);
    }

    #[tokio::test]
    async fn test_timeout() {
        let config = LimitConfig {
            max_concurrent: 1,
            max_queue_size: 10,
            queue_timeout: Duration::from_millis(50),
            enabled: true,
        };
        let controller = ConcurrencyController::new(config);

        let key = LimitKey::new("user1", "shard1");

        // Acquire and hold permit
        let _permit = controller.acquire(key.clone()).await.unwrap();

        // Second acquire should timeout
        let result = controller.acquire(key).await;
        assert!(matches!(result, Err(LimitError::Timeout { .. })));
    }

    #[tokio::test]
    async fn test_disabled() {
        let config = LimitConfig {
            enabled: false,
            ..Default::default()
        };
        let controller = ConcurrencyController::new(config);

        let key = LimitKey::new("user1", "shard1");
        let result = controller.acquire(key).await;
        assert!(matches!(result, Err(LimitError::Disabled)));
    }

    #[tokio::test]
    async fn test_different_keys() {
        let config = LimitConfig {
            max_concurrent: 1,
            max_queue_size: 10,
            queue_timeout: Duration::from_secs(1),
            enabled: true,
        };
        let controller = ConcurrencyController::new(config);

        let key1 = LimitKey::new("user1", "shard1");
        let key2 = LimitKey::new("user2", "shard1");
        let key3 = LimitKey::new("user1", "shard2");

        // All should succeed because they're different keys
        let _permit1 = controller.acquire(key1).await.unwrap();
        let _permit2 = controller.acquire(key2).await.unwrap();
        let _permit3 = controller.acquire(key3).await.unwrap();

        let stats = controller.stats();
        assert_eq!(stats.total_acquired, 3);
        assert_eq!(stats.active_keys, 3);
    }
}
