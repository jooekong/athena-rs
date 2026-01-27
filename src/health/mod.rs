//! Health check module for DBInstance monitoring
//!
//! This module provides:
//! - Periodic health checks for backend DB instances
//! - Automatic unhealthy instance removal after consecutive failures
//! - Multi-tenant deduplication (shared heartbeat for same host:port)
//! - Master/Slave role detection

mod checker;
mod master;
mod registry;
mod state;

pub use checker::{CheckError, HealthChecker};
pub use crate::config::HealthCheckConfig;
pub use master::{DetectError, MasterDetector};
pub use registry::{InstanceRegistry, RegistryStats};
pub use state::{CheckResult, HealthStatus, InstanceHealth, WindowConfig};
