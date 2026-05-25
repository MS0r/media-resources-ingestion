use crate::{
    error::{JobError, JobErrorOutcome},
    models::{
        AppConfig, CompressionOverride, GenericCompressionStrategy, ImageCompressionStrategy,
        Metadata, Resource, UniversalCompressionStrategy, VideoCompressionStrategy,
    },
    services::{
        mongo::{MongoService, UpsertResult},
        redis::RedisService,
    },
    storage::{Provider, StorageProvider},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt::Debug, path::PathBuf, sync::Arc};
use tokio::io::AsyncWriteExt;
use url::Url;
use wreq::Response;

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
    pub config: Arc<AppConfig>,
    pub job: JobEnvelope,
}

impl JobContext {
    pub fn from_file_job(
        job: FileJob,
        db: Arc<MongoService>,
        redis: Arc<RedisService>,
        config: Arc<AppConfig>,
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
        config: Arc<AppConfig>,
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

fn mime_to_extension(mime: &str) -> Option<&'static str> {
    match mime {
        "image/webp" => Some("webp"),
        "image/avif" => Some("avif"),
        _ => None,
    }
}

fn filename_from_url(url: &Url) -> (Option<String>, Option<String>) {
    let path = url
        .path_segments()
        .and_then(|segments| segments.filter(|s| !s.is_empty()).last())
        .map(|s| s.to_string());

    match path {
        None => (None, None),
        Some(p) => match p.rfind('.') {
            None => (Some(p), None),
            Some(dot_pos) => {
                let name = p[..dot_pos].to_string();
                let ext = p[dot_pos + 1..].to_string();
                (Some(name), Some(ext))
            }
        },
    }
}

async fn download_to_temp(
    response: Response,
    temp_dir: &str,
    filename: &str,
    job_id: &str,
) -> Result<(String, String), JobError> {
    let temp_path = expand_path(temp_dir, &format!("{}.tmp_{}", filename, job_id))
        .to_string_lossy()
        .to_string();
    let mut file = tokio::fs::File::create(&temp_path).await?;
    let mut stream = response.bytes_stream();
    let mut hasher = Sha256::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
    }

    tracing::info!(job_id = %job_id, "Download complete, temp file at {}", temp_path);
    let hash = hex::encode(hasher.finalize());
    Ok((temp_path, hash))
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
    extension: String,
    content_length: u64,
    mime_type: String,
}

async fn initiate_download(resource: &Resource) -> Result<(Response, DownloadInfo), JobError> {
    let mut default_headers = wreq::header::HeaderMap::new();
    default_headers.insert(
        wreq::header::ACCEPT,
        wreq::header::HeaderValue::from_static("image/*, */*"),
    );

    let client = wreq::Client::builder()
        .emulation(wreq_util::Emulation::Chrome124)
        .default_headers(default_headers)
        .build()?;

    let mut request = client.get(resource.url.as_str());

    let origin = resource.url.origin();

    if let Ok(val) = wreq::header::HeaderValue::from_str(&origin.ascii_serialization()) {
        request = request.header(wreq::header::REFERER, val);
    }

    if let Some(headers) = &resource.config.as_ref().and_then(|c| c.headers.as_ref()) {
        if let Some(auth) = &headers.authorization {
            request = request.header(wreq::header::AUTHORIZATION, auth);
        }
        if let Some(cookie) = &headers.cookie {
            request = request.header(wreq::header::COOKIE, cookie);
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
        .get(wreq::header::CONTENT_TYPE)
        .and_then(|ct| ct.to_str().ok())
        .and_then(|ct| ct.split(';').next())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string());

    // If MIME is generic, try to detect from URL extension
    // If MIME is generic, try to detect from URL extension
    // If MIME is generic, try to detect from URL extension
    if mime_type == "application/octet-stream" || mime_type == "text/plain" {
        if let (Some(name), Some(ext)) = filename_from_url(&resource.url) {
            let detection_name = format!("{}.{}", name, ext);
            if let Some(mime) = mime_guess::from_path(&detection_name).first_raw() {
                mime_type = mime.to_string();
            }
        }
    }

    // Set filename: resource.name (+ ext from URL) first, fallback to name.ext from URL
    let (filename, extension) = match filename_from_url(&resource.url) {
        (Some(name), Some(ext)) => {
            let fname = resource.name.clone().unwrap_or(name);
            (Some(fname), Some(ext))
        }
        (Some(name), None) => {
            let fname = resource.name.clone().unwrap_or(name);
            (Some(fname), Some("bin".to_string()))
        }
        (None, Some(ext)) => {
            let fname = resource
                .name
                .clone()
                .unwrap_or_else(|| "downloaded_file".to_string());
            (Some(fname), Some(ext))
        }
        (None, None) => (resource.name.clone(), None),
    };

    tracing::info!("Downloaded from URL: {}", resource.url);
    tracing::debug!(
        "Using filename: {}, extension: {}",
        filename.as_deref().unwrap_or("<none>"),
        extension.as_deref().unwrap_or("<none>")
    );

    Ok((
        response,
        DownloadInfo {
            filename: filename.unwrap_or_else(|| "downloaded_file".to_string()),
            extension: extension.unwrap_or_else(|| "bin".to_string()),
            content_length,
            mime_type,
        },
    ))
}

async fn handle_new_file(
    ctx: &JobContext,
    file_job: &FileJob,
    download: DownloadInfo,
    temp_path: String,
    hash_hex: String,
) -> Result<JobOutcome, JobErrorOutcome> {
    tracing::info!("New file metadata inserted with hash: {}", hash_hex);

    let resource = &file_job.resource;

    let dest_path = resource
        .dest
        .as_ref()
        .and_then(|d| d.path.as_ref())
        .ok_or(JobErrorOutcome::Fatal("Missing destination path".into()))?;

    let provider = resource
        .dest
        .as_ref()
        .and_then(|d| d.provider.as_ref())
        .ok_or(JobErrorOutcome::Fatal(
            "Missing destination provider".into(),
        ))?;

    let original_mime = download.mime_type.clone();

    let mut final_path = expand_path(
        &dest_path,
        &format!("{}.{}", download.filename, download.extension),
    )
    .to_string_lossy()
    .to_string();

    let override_strategy = resource
        .config
        .as_ref()
        .and_then(|c| c.compression_override.as_ref());

    let (local_file, compressed_size, final_mime) = match override_strategy {
        Some(strategy) => match strategy {
            CompressionOverride::Image(image_s) => {
                match compress_image_local(
                    &download.filename,
                    &temp_path,
                    &download.mime_type,
                    download.content_length,
                    ctx.config.compression_quality,
                    image_s,
                )
                .await
                {
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
                        (temp_path.clone(), Some(0), download.mime_type)
                    }
                }
            }
            CompressionOverride::Video(video_s) => (temp_path.clone(), Some(0), download.mime_type),
            CompressionOverride::Generic(generic_s) => {
                (temp_path.clone(), Some(0), download.mime_type)
            }
            CompressionOverride::Universal(universal_s) => {
                (temp_path.clone(), Some(0), download.mime_type)
            }
        },
        None => (temp_path.clone(), Some(0), download.mime_type),
    };

    if final_mime != original_mime {
        if let Some(new_ext) = mime_to_extension(&final_mime) {
            final_path = expand_path(
                &dest_path,
                &format!("{}.{}", download.filename, new_ext),
            )
            .to_string_lossy()
            .to_string();
        }
    }

    let mut file = tokio::fs::File::open(&local_file)
        .await
        .map_err(|e| JobErrorOutcome::from(JobError::from(e)))?;
    ctx.storage.upload(&final_path, &mut file).await?;

    if final_path != local_file {
        tokio::fs::remove_file(&local_file).await.ok();
    }

    let metadata = Metadata::new(
        hash_hex,
        resource.url.clone(),
        provider.clone(),
        final_path,
        download.content_length,
        compressed_size,
        final_mime,
    );
    ctx.db.complete_job(metadata, &file_job._id).await?;

    Ok(JobOutcome::Completed)
}

async fn handle_duplicate(
    temp_path: &str,
    existing_hash: &str,
) -> Result<JobOutcome, JobErrorOutcome> {
    tracing::info!(
        "Duplicate detected (existing hash: {}), cleaning up",
        existing_hash
    );
    tokio::fs::remove_file(temp_path)
        .await
        .map_err(|e| JobErrorOutcome::Retryable(e.to_string()))?;
    Ok(JobOutcome::Completed)
}

#[async_trait]
impl JobHandler for FileJobHandler {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobErrorOutcome> {
        let file_job = extract_file_job(&ctx.job)?;
        let (response, download) = initiate_download(&file_job.resource).await?;

        let threshold_bytes = ctx.config.compression_threshold_mb * 1024 * 1024;
        if download.content_length > threshold_bytes {
            tracing::info!(
                "Large file ({} bytes), spawning chunks",
                download.content_length
            );
            return Ok(JobOutcome::SpawnedChunks(vec![]));
        }

        let temp_dir = &ctx.config.temp_dir;
        let (temp_path, hash_hex) =
            download_to_temp(response, temp_dir, &download.filename, &ctx.job_id).await?;

        match ctx.db.upsert_file_metadata(&hash_hex).await {
            Ok(UpsertResult::Inserted) => {
                handle_new_file(ctx, file_job, download, temp_path, hash_hex).await
            }
            Ok(UpsertResult::Duplicate(existing)) => {
                handle_duplicate(&temp_path, &existing.file_hash).await
            }
            Err(e) => {
                tokio::fs::remove_file(&temp_path).await.ok();
                Err(JobErrorOutcome::from(e))
            }
        }
    }
}

async fn compress_image_local(
    original_name: &str,
    temp_path: &str,
    mime_type: &str,
    original_size: u64,
    _quality: u8,
    strategy: &ImageCompressionStrategy,
) -> Result<(String, u64, String), JobError> {
    use image::{
        ExtendedColorType, ImageEncoder, ImageFormat, ImageReader,
        codecs::{avif::AvifEncoder, webp::WebPEncoder},
    };
    use std::fs::File;
    use std::io::BufWriter;
    use std::path::Path;

    if mime_type == "image/webp" {
        let meta = std::fs::metadata(temp_path)?;
        return Ok((temp_path.to_string(), meta.len(), mime_type.to_string()));
    }

    let format = match mime_type {
        "image/jpeg" | "image/jpg" => ImageFormat::Jpeg,
        "image/png" => ImageFormat::Png,
        "image/gif" => ImageFormat::Gif,
        _ => {
            return Err(JobError::OtherFatal(
                "Unsupported image format for compression".into(),
            ));
        }
    };

    tracing::info!("Compressing image: {}", original_name);
    let mut reader = ImageReader::open(temp_path)?;
    reader.set_format(format);
    let img = reader.decode()?;
    let rgba = img.to_rgba8();
    let (width, height) = rgba.dimensions();

    let ext = match strategy {
        ImageCompressionStrategy::Avif => "avif",
        _ => "webp",
    };
    
    let parent = Path::new(temp_path).parent().unwrap_or(Path::new("."));
    let output_path = parent.join(original_name).with_extension(ext);
    let output_path_str = output_path.to_string_lossy().to_string();

    tracing::debug!("Decoding image for compression with strategy: {:?}, on temp_path: {}", strategy, temp_path);
    match strategy {
        ImageCompressionStrategy::Avif => {
            let file = File::create(&output_path)?;
            AvifEncoder::new(BufWriter::new(file)).write_image(
                rgba.as_raw(),
                width,
                height,
                ExtendedColorType::Rgba8,
            )?;
        }
        ImageCompressionStrategy::LosslessWebp | ImageCompressionStrategy::Webp => {
            let file = File::create(&output_path)?;
            WebPEncoder::new_lossless(BufWriter::new(file)).write_image(
                rgba.as_raw(),
                width,
                height,
                ExtendedColorType::Rgba8,
            )?;
        }
    }

    let compressed_size = std::fs::metadata(&output_path)?.len();

    if compressed_size >= original_size {
        std::fs::remove_file(&output_path)?;
        let meta = std::fs::metadata(temp_path)?;
        return Ok((temp_path.to_string(), meta.len(), mime_type.to_string()));
    }

    std::fs::remove_file(temp_path)?;
    let final_mime = match strategy {
        ImageCompressionStrategy::Avif => "image/avif",
        _ => "image/webp",
    };
    Ok((output_path_str, compressed_size, final_mime.to_string()))
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
    use url::Url;

    #[test]
    fn test_filename_from_url_standard() {
        let url = Url::parse("https://example.com/images/photo.png").unwrap();
        assert_eq!(
            filename_from_url(&url),
            (Some("photo".into()), Some("png".into()))
        );
    }

    #[test]
    fn test_filename_from_url_no_path() {
        let url = Url::parse("https://example.com").unwrap();
        assert_eq!(filename_from_url(&url), (None, None));
    }

    #[test]
    fn test_filename_from_url_root() {
        let url = Url::parse("https://example.com/").unwrap();
        assert_eq!(filename_from_url(&url), (None, None));
    }

    #[test]
    fn test_filename_from_url_deep_path() {
        let url = Url::parse("https://cdn.example.com/a/b/c/d/file.txt?query=1").unwrap();
        assert_eq!(
            filename_from_url(&url),
            (Some("file".into()), Some("txt".into()))
        );
    }

    #[test]
    fn test_filename_from_url_trailing_slash() {
        let url = Url::parse("https://example.com/dir/").unwrap();
        assert_eq!(filename_from_url(&url), (Some("dir".into()), None));
    }
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
