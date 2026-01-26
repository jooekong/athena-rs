mod limiter;

// New simplified API
pub use limiter::{Limiter, LimitError, Permit, Stats};

// Legacy API (for backward compatibility during migration)
pub use limiter::{ConcurrencyController, LimitConfig, LimitKey, LimitPermit};
