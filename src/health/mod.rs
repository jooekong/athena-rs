//! Health check module for DBInstance monitoring
//!
//! This module provides:
//! - Periodic health checks for backend DB instances
//! - Automatic unhealthy instance removal after consecutive failures
//! - Multi-tenant deduplication (shared heartbeat for same host:port)
//! - Master/Slave role detection

mod master;
mod registry;
mod state;

pub use registry::InstanceRegistry;
