mod limiter;

// New simplified API
pub use limiter::LimitError;

// Legacy API (for backward compatibility during migration)
pub use limiter::{ConcurrencyController, LimitConfig, LimitKey, LimitPermit};
