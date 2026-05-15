use crate::{
    error::{JobError, JobErrorOutcome},
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
use futures_util::StreamExt;
use reqwest::Response;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};
use tokio::io::{AsyncWriteExt};
use url::Url;

type JobId = String;
type BatchId = String;

#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobErrorOutcome>;
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
    pub dry_run: bool,
}

impl JobContext {
    pub fn from_file_job(
        job: FileJob,
        db: Arc<MongoService>,
        redis: Arc<RedisService>,
        config: Arc<TomlConfig>,
        dry_run: bool,
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
                dry_run,
            };
        }

        Self {
            job_id: job._id.to_string(),
            storage: Provider::Local.into_storage(),
            db,
            redis,
            config,
            job: JobEnvelope::File(job),
            dry_run,
        }
    }

    pub fn from_chunk_job(
        job: ChunkJob,
        db: Arc<MongoService>,
        redis: Arc<RedisService>,
        config: Arc<TomlConfig>,
        dry_run: bool,
    ) -> Self {
        Self {
            job_id: job._id.clone(),
            storage: Provider::Local.into_storage(), // chunk storage resolved from manifest
            db,
            redis,
            config,
            job: JobEnvelope::Chunk(job),
            dry_run,
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

async fn uploading_resource(
    response: Response,
    storage_provider: Arc<dyn StorageProvider>,
    storage_path: String,
) -> Result<String, JobError> {
    let mut stream = response.bytes_stream();
    let mut hasher = Sha256::new();

    let (mut writer, mut upload_reader) = tokio::io::duplex(64 * 1024);

    let handle = tokio::spawn(async move {
        let _ = storage_provider
            .upload(&storage_path, &mut upload_reader)
            .await;
    });
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;

        hasher.update(&chunk); // hash branch
        writer.write_all(&chunk).await?; // upload branch
    }

    drop(writer);

    let _ = handle.await.map_err(|e| JobError::from(e))?;

    Ok(hex::encode(hasher.finalize()))
}

fn extract_file_job(job: &JobEnvelope) -> Result<&FileJob, JobError> {
    match job {
        JobEnvelope::File(file_job) => Ok(file_job),
        _ => Err(JobError::OtherFatal(
            "FileJobHandler received non-file job".into(),
        )),
    }
}

struct DownloadInfo {
    filename: String,
    content_length: u64,
    mime_type: String,
}

async fn initiate_download(resource: &Resource) -> Result<(Response, DownloadInfo), JobError> {
    let mut request = reqwest::Client::new().get(resource.url.as_str());

    if let Some(headers) = &resource.config.as_ref().and_then(|c| c.headers.as_ref()) {
        if let Some(auth) = &headers.authorization {
            request = request.header(reqwest::header::AUTHORIZATION, auth);
        }
    }

    let response = request.send().await?;

    if !response.status().is_success() {
        return Err(JobError::OtherFatal(format!(
            "Failed to download file: HTTP {}",
            response.status()
        )));
    }

    let content_length = response.content_length().unwrap_or(0);

    // First try Content-Type header
    let mut mime_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .and_then(|ct| ct.split(';').next())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    // If MIME is generic, try to detect from URL extension
    if mime_type == "application/octet-stream" || mime_type == "text/plain" {
        if let Some(filename) = filename_from_url(&resource.url) {
            if let Some(mime) = mime_guess::from_path(&filename).first_raw() {
                mime_type = mime.to_string();
            }
        }
    }

    let filename =
        filename_from_url(&resource.url).unwrap_or_else(|| "downloaded_file".to_string());

    tracing::info!("Downloading from URL: {}", resource.url);

    Ok((
        response,
        DownloadInfo {
            filename,
            content_length,
            mime_type,
        },
    ))
}

#[derive(Debug)]
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
            .ok_or(JobError::OtherFatal("Missing destination path".into()))?;

        let provider = resource
            .dest
            .as_ref()
            .and_then(|d| d.provider.as_ref())
            .cloned();

        let temp = expand_path(dest, &format!("{}.tmp_{}", filename, job_id))
            .to_string_lossy()
            .to_string();
        let final_path = expand_path(dest, filename).to_string_lossy().to_string();

        Ok(Self {
            temp,
            final_path,
            provider,
        })
    }
}

async fn stream_to_storage(
    response: Response,
    storage: Arc<dyn StorageProvider>,
    staging: &StagingPaths,
) -> Result<String, JobErrorOutcome> {
    let upload_path = if storage.requires_local_staging() {
        &staging.temp
    } else {
        &staging.final_path
    };
    match uploading_resource(response, storage, upload_path.clone()).await {
        Ok(hash) => Ok(hash),
        Err(e) => {
            tracing::error!("Error during streaming upload: {}", e);
            Err(JobErrorOutcome::from(e))
        }
    }
}

async fn handle_new_file(
    ctx: &JobContext,
    file_job: &FileJob,
    download: DownloadInfo,
    staging: StagingPaths,
    hash_hex: String,
) -> Result<JobOutcome, JobErrorOutcome> {
    tracing::info!("New file metadata inserted with hash: {}", hash_hex);

    let threshold_bytes = ctx.config.compression.threshold_mb * 1024 * 1024;
    if download.content_length > threshold_bytes {
        tracing::info!(
            "Large file ({} bytes), spawning chunks",
            download.content_length
        );
        return Ok(JobOutcome::SpawnedChunks(vec![])); // TODO: build chunk plan
    }

    ctx.storage
        .commit_temp(&staging.temp, &staging.final_path)
        .await?;

    // Try to compress if it's an image
    let (final_path, compressed_size, final_mime) = if download.mime_type.starts_with("image/") {
        match compress_image(&staging.final_path, &download.mime_type).await {
            Ok((path, size, mime)) => {
                tracing::info!(
                    "Image compressed: {} -> {} bytes",
                    download.content_length,
                    size
                );
                (path, Some(size), mime)
            }
            Err(e) => {
                tracing::warn!("Image compression failed: {}, keeping original", e);
                (staging.final_path.clone(), Some(0), download.mime_type)
            }
        }
    } else {
        (staging.final_path.clone(), Some(0), download.mime_type)
    };

    let metadata = Metadata::new(
        hash_hex,
        file_job.resource.url.clone(),
        staging.provider.unwrap_or(Provider::Local),
        final_path,
        download.content_length,
        compressed_size,
        final_mime,
    );
    ctx.db.complete_job(metadata, &file_job._id).await?;

    Ok(JobOutcome::Completed)
}

async fn handle_duplicate(
    ctx: &JobContext,
    staging: StagingPaths,
    existing_hash: &str,
) -> Result<JobOutcome, JobErrorOutcome> {
    tracing::info!(
        "Duplicate detected (existing hash: {}), cleaning up",
        existing_hash
    );

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
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobErrorOutcome> {
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
                Err(JobErrorOutcome::from(e))
            }
        }
    }
}

async fn compress_image(path: &str, mime_type: &str) -> Result<(String, u64, String), JobError> {
    use image::{ImageFormat, ImageReader};
    use std::fs::File;
    use std::io::BufWriter;

    let format = match mime_type {
        "image/jpeg" | "image/jpg" => Some(ImageFormat::Jpeg),
        "image/png" => Some(ImageFormat::Png),
        "image/webp" => Some(ImageFormat::WebP),
        "image/gif" => Some(ImageFormat::Gif),
        _ => None,
    };

    if let Some(_) = format {
        tracing::info!("Compressing image: {}", path);
        // Open and decode the image
        let img = ImageReader::open(path)?.decode()?;

        // Save as WebP for compression
        let output_path = format!("{}.webp", path);
        let file = File::create(&output_path)?;
        let writer = BufWriter::new(file);

        img.write_to(writer, ImageFormat::WebP)?;

        let metadata = std::fs::metadata(&output_path)?;
        return Ok((output_path, metadata.len(), "image/webp".to_string()));
    }

    Err(JobError::OtherFatal(
        "Unsupported image format for compression".into(),
    ))
}

pub struct ChunkJobHandler;

#[async_trait]
impl JobHandler for ChunkJobHandler {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobErrorOutcome> {
        let chunk_job = ctx.chunk_job();

        tracing::info!(job_id = %ctx.job_id, "Processing chunk {}", chunk_job.chunk_index);

        // TODO: Implement actual chunk processing:
        // 1. Download the chunk from storage (or read from local file)
        // 2. Compress if needed based on file type
        // 3. Calculate hash
        // 4. Upload to storage
        // 5. Register in Redis
        // 6. Return Completed

        // For now, just mark as completed
        ctx.redis
            .register_chunk(&chunk_job.file_hash, &ctx.job_id)
            .await?;

        tracing::info!(job_id = %ctx.job_id, "Chunk {} completed", chunk_job.chunk_index);
        Ok(JobOutcome::Completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Destination;
    use url::Url;

    #[test]
    fn test_filename_from_url_standard() {
        let url = Url::parse("https://example.com/images/photo.png").unwrap();
        assert_eq!(filename_from_url(&url), Some("photo.png".into()));
    }

    #[test]
    fn test_filename_from_url_no_path() {
        let url = Url::parse("https://example.com").unwrap();
        assert_eq!(filename_from_url(&url), None);
    }

    #[test]
    fn test_filename_from_url_root() {
        let url = Url::parse("https://example.com/").unwrap();
        assert_eq!(filename_from_url(&url), None);
    }

    #[test]
    fn test_filename_from_url_deep_path() {
        let url = Url::parse("https://cdn.example.com/a/b/c/d/file.txt?query=1").unwrap();
        assert_eq!(filename_from_url(&url), Some("file.txt".into()));
    }

    #[test]
    fn test_filename_from_url_trailing_slash() {
        let url = Url::parse("https://example.com/dir/").unwrap();
        assert_eq!(filename_from_url(&url), Some("dir".into()));
    }

    #[test]
    fn test_expand_path_simple() {
        let result = expand_path("/base", "file.txt");
        assert_eq!(result, PathBuf::from("/base/file.txt"));
    }

    #[test]
    fn test_expand_path_nested() {
        let result = expand_path("/base/dir", "sub/file.txt");
        assert_eq!(result, PathBuf::from("/base/dir/sub/file.txt"));
    }

    #[test]
    fn test_expand_path_tilde_root() {
        let home = dirs::home_dir().expect("home dir should exist in test env");
        let result = expand_path("~/downloads", "file.txt");
        assert_eq!(result, home.join("downloads/file.txt"));
    }

    #[test]
    fn test_expand_path_tilde_only() {
        let home = dirs::home_dir().expect("home dir should exist in test env");
        let result = expand_path("~", "file.txt");
        assert_eq!(result, home.join("file.txt"));
    }

    #[test]
    fn test_staging_paths_resolve_basic() {
        let url = Url::parse("https://example.com/file.png").unwrap();
        let resource = Resource {
            id: "test-id".into(),
            url,
            name: None,
            priority: None,
            dest: Some(Destination {
                provider: None,
                path: Some("/tmp".into()),
            }),
            config: None,
        };
        let paths = StagingPaths::resolve(&resource, "file.png", "job-1").unwrap();
        assert_eq!(paths.final_path, "/tmp/file.png");
        assert!(paths.temp.contains("/tmp/file.png.tmp_job-1"));
        assert!(paths.provider.is_none());
    }

    #[test]
    fn test_staging_paths_resolve_missing_dest() {
        let url = Url::parse("https://example.com/f.png").unwrap();
        let resource = Resource {
            id: "test-id".into(),
            url,
            name: None,
            priority: None,
            dest: None,
            config: None,
        };
        let result = StagingPaths::resolve(&resource, "f.png", "job-2");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing destination path")
        );
    }

    #[test]
    fn test_staging_paths_resolve_with_provider() {
        let url = Url::parse("https://example.com/f.png").unwrap();
        let resource = Resource {
            id: "test-id".into(),
            url,
            name: None,
            priority: None,
            dest: Some(Destination {
                provider: Some(Provider::S3),
                path: Some("/bucket".into()),
            }),
            config: None,
        };
        let paths = StagingPaths::resolve(&resource, "f.png", "job-3").unwrap();
        assert_eq!(paths.provider, Some(Provider::S3));
    }

    #[test]
    fn test_extract_file_job_file() {
        let file_job = FileJob {
            _id: "j1".into(),
            batch_id: "b1".into(),
            resource: Resource {
                id: "r1".into(),
                url: Url::parse("https://example.com/f.png").unwrap(),
                name: None,
                priority: None,
                dest: None,
                config: None,
            },
            priority: 0,
            status: JobStatus::Pending,
            retry_count: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            file_hash: None,
            error: None,
        };
        let envelope = JobEnvelope::File(file_job);
        let result = extract_file_job(&envelope);
        assert!(result.is_ok());
        assert_eq!(result.unwrap()._id, "j1");
    }

    #[test]
    fn test_extract_file_job_chunk_fails() {
        let chunk_job = ChunkJob {
            _id: "c1".into(),
            parent_job_id: "j1".into(),
            file_hash: "abc".into(),
            chunk_index: 0,
            offset_start: 0,
            offset_end: 99,
            priority: 0,
            status: JobStatus::Pending,
            retry_count: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            chunk_hash: None,
            error: None,
        };
        let envelope = JobEnvelope::Chunk(chunk_job);
        let result = extract_file_job(&envelope);
        assert!(result.is_err());
    }
}
