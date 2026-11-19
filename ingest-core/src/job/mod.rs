//! Job types — pure data shapes that get persisted to Mongo and Redis.
//!
//! - [`status`] — the per-job runtime state (`JobStatus`)
//! - [`batch`] — a `Batch` grouping jobs from one YAML
//! - [`file`] — `FileJob` + `FileJobSpec`
//! - [`chunk`] — `ChunkJob`
//! - [`job_outcome`] — `JobOutcome` + `JobEffect` (what a handler returns)
//!
//! The handler trait, envelope, and context live in `handlers::jobs` —
//! those depend on this module but this module does not depend on them.

pub mod batch;
pub mod chunk;
pub mod file;
pub mod job_outcome;
pub mod status;

pub use batch::Batch;
pub use chunk::ChunkJob;
pub use file::{FileJob, FileJobSpec};
pub use job_outcome::{JobEffect, JobOutcome};
pub use status::JobStatus;
