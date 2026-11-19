//! A `Batch` — the parent grouping of jobs from a single YAML file.
//!
//! Created by `bootstrap::enqueue`, persisted to `files_batches` in Mongo,
//! and used by the gRPC `GetBatchStatus` RPC.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::job::status::JobStatus;

pub type JobId = String;
pub type BatchId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub _id: BatchId,
    pub created_at: DateTime<Utc>,
    pub yaml_path: PathBuf,
    pub status: JobStatus,
    pub job_ids: Vec<JobId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample_batch() -> Batch {
        Batch {
            _id: "batch-1".into(),
            created_at: Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap(),
            yaml_path: PathBuf::from("config.yaml"),
            status: JobStatus::Pending,
            job_ids: vec!["j1".into(), "j2".into()],
        }
    }

    #[test]
    fn test_batch_serde_roundtrip() {
        let batch = sample_batch();
        let json = serde_json::to_string(&batch).unwrap();
        let back: Batch = serde_json::from_str(&json).unwrap();
        assert_eq!(back._id, batch._id);
        assert_eq!(back.job_ids, batch.job_ids);
    }
}
