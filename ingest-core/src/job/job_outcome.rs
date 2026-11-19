//! Job outcome types — replaces the 4-variant `JobOutcome` with a cleaner 3-variant shape.

use crate::models::{ChunkRef, Metadata};

/// The outcome of a job execution — 3 variants instead of 4.
///
/// `Done(JobEffect)` carries the side-effect data, so the scheduler
/// knows exactly what to do next without matching on job kind.
pub enum JobOutcome {
    /// Job completed successfully. The scheduler will perform the
    /// appropriate post-execution action based on the effect.
    Done(JobEffect),

    /// Job hit a transient failure. The scheduler will re-enqueue
    /// with the specified backoff.
    Retry { reason: String, backoff_secs: u64 },

    /// Job hit a terminal failure. The scheduler will mark the
    /// job as failed.
    Fail { reason: String },
}

/// The side-effect produced by a successful job execution.
pub enum JobEffect {
    /// A file job stored its metadata and uploaded the file.
    FileStored { metadata: Metadata },

    /// A file job decided to split into chunks. The scheduler
    /// will enqueue the chunks for processing.
    ChunksSpawned { chunks: Vec<crate::job::ChunkJob> },

    /// A chunk job completed and stored its data. The scheduler
    /// will update the chunk counter and finalize if last.
    ChunkStored { chunk_ref: ChunkRef, mime: String },

    /// A file job detected a duplicate hash. The scheduler
    /// will mark the job as completed without metadata.
    DuplicateSkipped,
}
