//! File job types — what gets persisted to `files_jobs` in Mongo.
//!
//! `FileJobSpec` is a slim view of a YAML `Resource` containing only the
//! fields the worker actually needs. `FileJob` wraps it with the runtime
//! metadata (status, retry count, hash, etc.) the scheduler updates.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    config::ingest_yaml::{Destination, ResourceLevelConfig},
    job::status::JobStatus,
};

pub type JobId = String;
pub type BatchId = String;

/// Slim view of a `Resource` stored in `FileJob`.
/// Contains the YAML-derived fields needed for job execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileJobSpec {
    /// Resource ID (UUID v4).
    pub id: String,
    /// Source URL to download from.
    pub url: Url,
    /// Optional human-readable name.
    pub name: Option<String>,
    /// Destination provider and path.
    pub dest: Option<Destination>,
    /// Per-resource config (compression, headers, auth).
    pub config: Option<ResourceLevelConfig>,
}

/// A single file-download job. One `FileJob` is created per YAML resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileJob {
    pub _id: JobId,
    pub batch_id: BatchId,
    pub spec: FileJobSpec,
    pub priority: i32,
    pub status: JobStatus,
    pub retry_count: u8,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub file_hash: Option<String>,
    pub error: Option<String>,
    /// Per-job chunk size from the ingestion YAML (e.g. "64MB").
    /// Falls back to the server TOML config when absent (upgraded jobs).
    #[serde(default)]
    pub chunk_size: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample_spec() -> FileJobSpec {
        FileJobSpec {
            id: "r1".into(),
            url: Url::parse("https://example.com/file.bin").unwrap(),
            name: Some("file".into()),
            dest: None,
            config: None,
        }
    }

    fn sample_job() -> FileJob {
        FileJob {
            _id: "job-1".into(),
            batch_id: "batch-1".into(),
            spec: sample_spec(),
            priority: 5,
            status: JobStatus::Pending,
            retry_count: 0,
            created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            updated_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            file_hash: None,
            error: None,
            chunk_size: Some("128MB".into()),
        }
    }

    #[test]
    fn test_file_job_spec_default_id_is_required() {
        // FileJobSpec.id is required (no default) — verified by panic if missing.
        let yaml = r#"
            url: https://example.com/x
            name: foo
        "#;
        let result: Result<FileJobSpec, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_file_job_serde_roundtrip() {
        let job = sample_job();
        let json = serde_json::to_string(&job).unwrap();
        let back: FileJob = serde_json::from_str(&json).unwrap();
        assert_eq!(back._id, job._id);
        assert_eq!(back.spec.url, job.spec.url);
        assert_eq!(back.chunk_size, job.chunk_size);
    }

    #[test]
    fn test_file_job_chunk_size_optional_on_legacy_documents() {
        // `chunk_size` has `#[serde(default)]` — older docs without it should still load.
        let json = r#"{
            "_id": "job-1",
            "batch_id": "batch-1",
            "spec": {
                "id": "r1",
                "url": "https://example.com/file.bin"
            },
            "priority": 0,
            "status": "pending",
            "retry_count": 0,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        }"#;
        let job: FileJob = serde_json::from_str(json).unwrap();
        assert!(job.chunk_size.is_none());
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.spec.url.as_str(), "https://example.com/file.bin");
    }
}
