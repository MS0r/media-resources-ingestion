use crate::{models::Resource, services::{mongo::MongoService, redis::RedisService}, storage::StorageProvider, settings::TomlConfig};
use std::{path::PathBuf, sync::Arc};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

type JobId = String;
type BatchId = String;

#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobError>;
}

pub struct JobContext {
    pub job_id: JobId,
    pub storage: Arc<dyn StorageProvider + Send + Sync>,
    pub db: Arc<MongoService>,
    pub redis: Arc<RedisService>,
    pub resource: Arc<Resource>,
    pub config: Arc<TomlConfig>,
}

impl JobContext {
    pub fn new(
        job_id: JobId,
        db: Arc<MongoService>,
        redis: Arc<RedisService>,
        resource: Arc<Resource>,
        config: Arc<TomlConfig>,
    ) -> Self {
        resource.dest.take();
        //TODO
        Self {
            job_id,
            db,
            redis,
            resource,
            config,
        }
    }
}

pub enum JobOutcome {
    Completed,
    SpawnedChunks(Vec<ChunkJob>),  // FileJob signals it created chunk jobs
}

pub enum JobError {
    Retryable(String),   // network timeout, provider blip — will retry
    Fatal(String),       // bad MIME, validation failure — will not retry
}

pub enum JobKind {
    File,
    Chunk,
}

pub struct Batch {
    pub id : BatchId,
    pub created_at: DateTime<Utc>,
    pub yaml_path: PathBuf,
    pub status: JobStatus,
    pub job_ids: Vec<JobId>,
}

pub struct FileJob {
    pub id: JobId,
    pub batch_id: BatchId,        // parsed from YAML — url, name, dest, config
    pub priority: i64,
    pub status: JobStatus,
    pub retry_count: u8,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub file_hash: Option<String>, // populated after download completes
    pub error: Option<String>,
}

pub struct ChunkJob {
    pub id: JobId,
    pub parent_job_id: JobId,      // the FileJob that spawned this
    pub file_hash: String,         // known at spawn time
    pub chunk_index: u32,
    pub offset_start: u64,
    pub offset_end: u64,
    pub status: JobStatus,
    pub retry_count: u8,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub chunk_hash: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JobStatus {
    Pending,
    Running { worker_id: String, started_at: DateTime<Utc> },
    Retrying { attempt: u8, retry_after: DateTime<Utc> },
    Completed { finished_at: DateTime<Utc> },
    Failed { reason: String, failed_at: DateTime<Utc> },
    Cancelled,
}

pub struct FileJobHandler;

#[async_trait]
impl JobHandler for FileJobHandler {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobError> {
        // 1. preflight URL check
        // 2. stream download, compute SHA-256 incrementally
        // 3. check MongoDB for duplicate hash
        // 4. detect MIME from first 512 bytes
        // 5. decide whether to chunk
        // 6a. small file → compress → upload → write metadata → Completed
        // 6b. large file → build chunk plan → SpawnedChunks(vec![...])
    }
}

pub struct ChunkJobHandler;

#[async_trait]
impl JobHandler for ChunkJobHandler {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobError> {
        // 1. slice bytes for this chunk's offset range
        // 2. compress chunk
        // 3. hash chunk
        // 4. upload to storage provider
        // 5. register chunk hash in jobs:chunks:<file_hash> Redis set
        // 6. return Completed
    }
}