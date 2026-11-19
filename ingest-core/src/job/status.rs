//! Runtime per-job status — the value of `FileJob.status` and `ChunkJob.status`.
//!
//! Distinct from [`crate::domain::JobStatus`], which is the simple string-keyed
//! filter type for Mongo queries. This one carries the *runtime* payload
//! (worker_id, retry_after, etc.) and is what gets stored in Mongo.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Per-job runtime status, persisted to Mongo.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running {
        worker_id: String,
        started_at: DateTime<Utc>,
    },
    Retrying {
        attempt: u8,
        retry_after: DateTime<Utc>,
    },
    Completed {
        finished_at: DateTime<Utc>,
    },
    Failed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
    Cancelled,
}

impl JobStatus {
    /// Short lowercase string used in Redis state and progress events.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running { .. } => "running",
            Self::Retrying { .. } => "retrying",
            Self::Completed { .. } => "completed",
            Self::Failed { .. } => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_as_str_all_variants() {
        assert_eq!(JobStatus::Pending.as_str(), "pending");
        assert_eq!(
            JobStatus::Running {
                worker_id: "w1".into(),
                started_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            }
            .as_str(),
            "running"
        );
        assert_eq!(
            JobStatus::Retrying {
                attempt: 1,
                retry_after: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            }
            .as_str(),
            "retrying"
        );
        assert_eq!(
            JobStatus::Completed {
                finished_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            }
            .as_str(),
            "completed"
        );
        assert_eq!(
            JobStatus::Failed {
                reason: "x".into(),
                failed_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            }
            .as_str(),
            "failed"
        );
        assert_eq!(JobStatus::Cancelled.as_str(), "cancelled");
    }

    #[test]
    fn test_pending_serde_roundtrip() {
        let s = JobStatus::Pending;
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, "\"pending\"");
        let back: JobStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn test_cancelled_serde_roundtrip() {
        let s = JobStatus::Cancelled;
        let json = serde_json::to_string(&s).unwrap();
        let back: JobStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, s);
    }
}
