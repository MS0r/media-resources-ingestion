//! Chunk job type — one row per chunk of a chunked file download.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{compression::GenericCompressionStrategy, job::status::JobStatus, storage::Provider};

pub type JobId = String;
pub type BatchId = String;

/// A single chunk's job. Created by the file handler when a download
/// exceeds the chunk threshold; consumed by `ChunkJobHandler`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkJob {
    pub _id: JobId,
    pub parent_job_id: JobId,
    pub file_hash: Option<String>,
    pub chunk_index: u32,
    pub offset_start: u64,
    pub offset_end: u64,
    pub priority: i64,
    pub status: JobStatus,
    pub retry_count: u8,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub chunk_hash: Option<String>,
    pub error: Option<String>,
    pub url: Url,
    pub authorization: Option<String>,
    pub cookie: Option<String>,
    pub dest_path: String,
    #[serde(default)]
    pub storage: Provider,
    pub total_chunks: u32,
    pub total_file_size: u64,
    pub compression_strategy: Option<GenericCompressionStrategy>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample_chunk() -> ChunkJob {
        ChunkJob {
            _id: "chunk-1".into(),
            parent_job_id: "job-1".into(),
            file_hash: None,
            chunk_index: 0,
            offset_start: 0,
            offset_end: 1023,
            priority: 0,
            status: JobStatus::Pending,
            retry_count: 0,
            created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            updated_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            chunk_hash: None,
            error: None,
            url: Url::parse("https://example.com/file.bin").unwrap(),
            authorization: None,
            cookie: None,
            dest_path: "/data/chunk_00000.bin".into(),
            storage: Provider::Local,
            total_chunks: 4,
            total_file_size: 4096,
            compression_strategy: Some(GenericCompressionStrategy::OriginalFormat),
        }
    }

    #[test]
    fn test_chunk_job_serde_roundtrip() {
        let job = sample_chunk();
        let json = serde_json::to_string(&job).unwrap();
        let back: ChunkJob = serde_json::from_str(&json).unwrap();
        assert_eq!(back._id, job._id);
        assert_eq!(back.offset_start, job.offset_start);
        assert_eq!(back.storage, job.storage);
        assert_eq!(back.compression_strategy, job.compression_strategy);
    }

    #[test]
    fn test_chunk_job_storage_defaults_to_local_for_legacy_documents() {
        // `storage` has `#[serde(default)]` — older chunk docs without it should still load.
        let json = r#"{
            "_id": "chunk-1",
            "parent_job_id": "job-1",
            "chunk_index": 0,
            "offset_start": 0,
            "offset_end": 1023,
            "priority": 0,
            "status": "pending",
            "retry_count": 0,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "url": "https://example.com/file.bin",
            "dest_path": "/tmp/chunk_00000.bin",
            "total_chunks": 1,
            "total_file_size": 1024
        }"#;
        let job: ChunkJob = serde_json::from_str(json).unwrap();
        assert_eq!(job.storage, Provider::Local);
    }
}
