use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use url::Url;

use crate::models::{GenericCompressionStrategy, Resource};

type JobId = String;
type BatchId = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub _id: BatchId,
    pub created_at: DateTime<Utc>,
    pub yaml_path: PathBuf,
    pub status: JobStatus,
    pub job_ids: Vec<JobId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileJob {
    pub _id: JobId,
    pub batch_id: BatchId,
    pub resource: Resource,
    pub priority: i32,
    pub status: JobStatus,
    pub retry_count: u8,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub file_hash: Option<String>,
    pub error: Option<String>,
}

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
    pub total_chunks: u32,
    pub total_file_size: u64,
    pub compression_strategy: Option<GenericCompressionStrategy>,
}

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

pub struct DownloadInfo {
    pub filename: String,
    pub extension: String,
    pub content_length: u64,
    pub mime_type: String,
}
