use crate::{
    error::BoxedError,
    models::{Metadata, Resource},
    services::{
        mongo::{MongoService, UpsertResult},
        redis::RedisService,
    },
    settings::TomlConfig,
    storage::{Provider, StorageProvider},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, TryStreamExt};
use reqwest::Response;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{io, path::PathBuf, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};
use tokio_util::io::StreamReader;
use url::Url;

type JobId = String;
type BatchId = String;

#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobError>;
}

pub enum JobEnvelope {
    File(FileJob),
    Chunk(ChunkJob),
}

pub struct JobContext {
    pub job_id: JobId,
    pub storage: Arc<dyn StorageProvider>,
    pub db: Arc<MongoService>,
    pub redis: Arc<RedisService>,
    pub config: Arc<TomlConfig>,
    pub job: JobEnvelope,
}

impl JobContext {
    pub fn from_file_job(
        job: FileJob,
        db: Arc<MongoService>,
        redis: Arc<RedisService>,
        config: Arc<TomlConfig>,
    ) -> Self {
        if let Some(dest) = &job.resource.dest
            && let Some(provider) = &dest.provider
        {
            let storage = provider.into_storage();
            return Self {
                job_id: job._id.to_string(),
                storage,
                db,
                redis,
                config,
                job: JobEnvelope::File(job),
            };
        }

        Self {
            job_id: job._id.to_string(),
            storage: Provider::Local.into_storage(),
            db,
            redis,
            config,
            job: JobEnvelope::File(job),
        }
    }

    pub fn from_chunk_job(
        job: ChunkJob,
        db: Arc<MongoService>,
        redis: Arc<RedisService>,
        config: Arc<TomlConfig>,
    ) -> Self {
        Self {
            job_id: job._id.clone(),
            storage: Provider::Local.into_storage(), // chunk storage resolved from manifest
            db,
            redis,
            config,
            job: JobEnvelope::Chunk(job),
        }
    }
    /// Convenience — panics if called on a chunk context
    pub fn file_job(&self) -> &FileJob {
        match &self.job {
            JobEnvelope::File(j) => j,
            JobEnvelope::Chunk(_) => panic!("Called file_job() on a chunk context"),
        }
    }

    pub fn chunk_job(&self) -> &ChunkJob {
        match &self.job {
            JobEnvelope::Chunk(j) => j,
            JobEnvelope::File(_) => panic!("Called chunk_job() on a file context"),
        }
    }
}

pub enum JobOutcome {
    Completed,
    SpawnedChunks(Vec<ChunkJob>), // FileJob signals it created chunk jobs
}

pub enum JobError {
    Retryable(String), // network timeout, provider blip — will retry
    Fatal(String),     // bad MIME, validation failure — will not retry
}

impl From<BoxedError> for JobError {
    fn from(e: BoxedError) -> Self {
        JobError::Retryable(format!("Boxed error: {}", e))
    }
}

impl From<std::io::Error> for JobError {
    fn from(e: std::io::Error) -> Self {
        JobError::Retryable(format!("IO error: {}", e))
    }
}

impl From<reqwest::Error> for JobError {
    fn from(e: reqwest::Error) -> Self {
        if e.is_timeout() || e.is_connect() {
            JobError::Retryable(format!("Network error: {}", e))
        } else {
            JobError::Fatal(format!("HTTP error: {}", e))
        }
    }
}

impl From<&str> for JobError {
    fn from(s: &str) -> Self {
        JobError::Fatal(s.to_string())
    }
}
impl From<String> for JobError {
    fn from(s: String) -> Self {
        JobError::Fatal(s)
    }
}

pub enum JobKind {
    File,
    Chunk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub _id: BatchId,
    pub created_at: DateTime<Utc>,
    pub yaml_path: PathBuf, // PathBuf serialises fine with serde
    pub status: JobStatus,
    pub job_ids: Vec<JobId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileJob {
    pub _id: JobId,
    pub batch_id: BatchId,
    pub resource: Resource, // ← move Resource in here; it belongs with the job
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
    pub file_hash: String,
    pub chunk_index: u32,
    pub offset_start: u64,
    pub offset_end: u64,
    pub priority: i64, // ← add; inherited from parent FileJob
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

pub struct FileJobHandler;

fn expand_path(path: &str, filename: &str) -> PathBuf {
    let expanded_path = shellexpand::tilde(path).to_string();
    PathBuf::from(expanded_path).join(filename)
}

fn filename_from_url(url: &Url) -> Option<String> {
    url.path_segments()
        .and_then(|segments| segments.filter(|s| !s.is_empty()).next_back())
        .map(|s| s.to_string())
}

async fn uploading_resource(response: Response, storage_provider: Arc<dyn StorageProvider>, storage_path: String) -> Result<String, JobError>{
        let stream = response
            .bytes_stream()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e));

        let mut reader = StreamReader::new(stream);
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 64 * 1024];
        let (mut writer, mut upload_reader) = tokio::io::duplex(64 * 1024);

        tokio::spawn(async move {
            let _ = storage_provider
                .upload(&storage_path, &mut upload_reader)
                .await;
        });
        loop {
            let n = reader.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
            writer.write_all(&buffer[..n]).await?;
        }

        drop(writer);

        Ok(hex::encode(hasher.finalize()))
}

fn extract_file_job(job: &JobEnvelope) -> Result<&FileJob, JobError> {
    match job {
        JobEnvelope::File(file_job) => Ok(file_job),
        _ => Err("FileJobHandler received non-file job".into()),
    }
}

struct DownloadInfo {
    filename: String,
    content_length: u64,
    mime_type: String,
}

async fn initiate_download(resource: &Resource) -> Result<(Response, DownloadInfo), JobError> {
    let response = reqwest::get(resource.url.as_str()).await?;

    if !response.status().is_success() {
        return Err(JobError::Fatal(format!(
            "Failed to download file: HTTP {}",
            response.status()
        )));
    }

    let content_length = response.content_length().unwrap_or(0);
    let mime_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .and_then(|ct| ct.split(';').next())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let filename = filename_from_url(&resource.url)
        .unwrap_or_else(|| "downloaded_file".to_string());

    tracing::info!("Downloading from URL: {}", resource.url);

    Ok((response, DownloadInfo {filename, content_length, mime_type }))
}

struct StagingPaths {
    temp: String,
    final_path: String,
    provider: Option<Provider>,
}

impl StagingPaths {
    fn resolve(resource: &Resource, filename: &str, job_id: &str) -> Result<Self, JobError> {
        let dest = resource
            .dest
            .as_ref()
            .and_then(|d| d.path.as_ref())
            .ok_or("Missing destination path")?;

        let provider = resource
            .dest
            .as_ref()
            .and_then(|d| d.provider.as_ref())
            .cloned();

        let temp = expand_path(dest, &format!("{}.tmp_{}", filename, job_id))
            .to_string_lossy()
            .to_string();
        let final_path = expand_path(dest, filename)
            .to_string_lossy()
            .to_string();

        Ok(Self { temp, final_path, provider })
    }
}

async fn stream_to_storage(
    response: Response,
    storage: Arc<dyn StorageProvider>,
    staging: &StagingPaths,
) -> Result<String, JobError> {
    let upload_path = if storage.requires_local_staging() {
        &staging.temp
    } else {
        &staging.final_path
    };
    uploading_resource(response, storage, upload_path.clone()).await
}

async fn handle_new_file(
    ctx: &JobContext,
    file_job: &FileJob,
    download: DownloadInfo,
    staging: StagingPaths,
    hash_hex: String,
) -> Result<JobOutcome, JobError> {
    tracing::info!("New file metadata inserted with hash: {}", hash_hex);

    let threshold_bytes = ctx.config.compression.threshold_mb * 1024 * 1024;
    if download.content_length > threshold_bytes {
        tracing::info!(
            "Large file ({} bytes), spawning chunks",
            download.content_length
        );
        return Ok(JobOutcome::SpawnedChunks(vec![])); // TODO: build chunk plan
    }

    ctx.storage.commit_temp(&staging.temp, &staging.final_path).await?;

    let metadata = Metadata::new(
        hash_hex,
        file_job.resource.url.clone(),
        staging.provider.unwrap_or(Provider::Local),
        staging.final_path,
        download.content_length,
        Some(0), // TODO: compressed size
        download.mime_type,
    );
    ctx.db.complete_job(metadata, &file_job._id).await?;

    Ok(JobOutcome::Completed)
}

async fn handle_duplicate(
    ctx: &JobContext,
    staging: StagingPaths,
    existing_hash: &str,
) -> Result<JobOutcome, JobError> {
    tracing::info!("Duplicate detected (existing hash: {}), cleaning up", existing_hash);

    let path_to_delete = if ctx.storage.requires_local_staging() {
        &staging.temp
    } else {
        &staging.final_path
    };
    ctx.storage.delete(path_to_delete).await.ok();

    Ok(JobOutcome::Completed)
}

#[async_trait]
impl JobHandler for FileJobHandler {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobError> {
        let file_job = extract_file_job(&ctx.job)?;
        let (response, download) = initiate_download(&file_job.resource).await?;
        let staging = StagingPaths::resolve(&file_job.resource, &download.filename, &ctx.job_id)?;

        let hash_hex = stream_to_storage(response, ctx.storage.clone(), &staging).await?;

        match ctx.db.upsert_file_metadata(&hash_hex).await {
            Ok(UpsertResult::Inserted) => {
                handle_new_file(ctx, file_job, download, staging, hash_hex).await
            }
            Ok(UpsertResult::Duplicate(existing)) => {
                handle_duplicate(ctx, staging, &existing.file_hash).await
            }
            Err(e) => {
                tracing::error!("Error upserting file metadata: {}", e);
                Err(JobError::Retryable(format!("DB error: {}", e)))
            }
        }
    }
}

pub struct ChunkJobHandler;

#[async_trait]
impl JobHandler for ChunkJobHandler {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobError> {
        todo!()
        // 1. slice bytes for this chunk's offset range
        // 2. compress chunk
        // 3. hash chunk
        // 4. upload to storage provider
        // 5. register chunk hash in jobs:chunks:<file_hash> Redis set
        // 6. return Completed
    }
}
