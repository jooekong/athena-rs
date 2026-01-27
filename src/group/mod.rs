//! Group manager for multi-tenant support
//!
//! Groups-only architecture: each tenant (user) maps to exactly one Group.
//! A Group contains DBGroups (shards), each with master/slave instances.

mod manager;

pub use manager::{GroupContext, GroupManager};
