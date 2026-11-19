//! Shared service singletons.
//!
//! - [`heartbeat`] — keeps `jobs:running:*` leases alive while jobs execute.
//! - [`mongo`] — pooled MongoDB connection.
//! - [`redis`] — Redis client (job queue, lease tracking, pubsub).
//! - [`builder`] — the [`Services`] aggregate that ties the above together
//!   and exposes [`Services::build_file_context`] / [`Services::build_chunk_context`]
//!   for the scheduler.

pub mod builder;
pub mod heartbeat;
pub mod mongo;
pub mod redis;

pub use builder::Services;
