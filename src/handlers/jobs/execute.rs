use async_trait::async_trait;
use chrono::Utc;

use crate::{
    AppConfig,
    error::JobErrorOutcome,
    models::{ChunkRef, CompressionOverride, GenericCompressionStrategy},
    services::mongo::UpsertResult,
};

use super::{
    ChunkJob, FileJob, JobContext, JobOutcome, JobStatus,
    compression::{compress_generic_local, generic_compression_extension},
    download::{
        StreamResult, download_range_chunk, download_to_temp, extract_file_job, initiate_download,
        initiate_head, initiate_range_download,
    },
    handle::{handle_duplicate, handle_new_file},
};
use crate::handlers::jobs::download::filename_from_url;

pub struct FileJobHandler;
pub struct ChunkJobHandler;

/// Parse a human-readable size like "128MB" or "16MB" into bytes.
fn parse_chunk_size(s: &str) -> u64 {
    let s = s.trim().to_uppercase();
    if let Some(n) = s.strip_suffix("GB") {
        n.trim().parse::<u64>().unwrap_or(1) * 1024 * 1024 * 1024
    } else if let Some(n) = s.strip_suffix("MB") {
        n.trim().parse::<u64>().unwrap_or(128) * 1024 * 1024
    } else if let Some(n) = s.strip_suffix("KB") {
        n.trim().parse::<u64>().unwrap_or(128) * 1024
    } else {
        s.parse::<u64>().unwrap_or(128 * 1024 * 1024)
    }
}

async fn spawn_chunk_jobs(
    file_job: &FileJob,
    total_size: u64,
    config: &AppConfig,
) -> Result<Vec<ChunkJob>, JobErrorOutcome> {
    let chunk_size = parse_chunk_size(&config.chunk_size);
    let total_chunks = ((total_size + chunk_size - 1) / chunk_size) as u32;
    let resource = &file_job.resource;

    let auth = resource
        .config
        .as_ref()
        .and_then(|c| c.headers.as_ref())
        .and_then(|h| h.authorization.clone());
    let cookie = resource
        .config
        .as_ref()
        .and_then(|c| c.headers.as_ref())
        .and_then(|h| h.cookie.clone());

    let compression_strategy = resource
        .config
        .as_ref()
        .and_then(|c| c.compression_override.as_ref())
        .and_then(|co| match co {
            CompressionOverride::Generic(s) => Some(s.clone()),
            _ => None,
        });

    let resource_name = resource.name.clone().unwrap_or_else(|| {
        filename_from_url(&resource.url)
            .0
            .unwrap_or_else(|| "file".to_string())
    });

    let dest_folder = resource.dest.as_ref().and_then(|d| d.path.as_ref()).map_or(
        format!(
            "{}/{}",
            config.default_path.trim_end_matches('/'),
            resource_name
        ),
        |p| format!("{}/{}", p.trim_end_matches('/'), resource_name),
    );

    let mut chunks = Vec::with_capacity(total_chunks as usize);

    for i in 0..total_chunks {
        let offset_start = (i as u64) * chunk_size;
        let offset_end = ((i as u64 + 1) * chunk_size - 1).min(total_size.saturating_sub(1));

        chunks.push(ChunkJob {
            _id: uuid::Uuid::new_v4().to_string(),
            parent_job_id: file_job._id.clone(),
            file_hash: None, // use parent job ID as grouping key
            chunk_index: i,
            offset_start,
            offset_end,
            priority: file_job.priority as i64,
            status: JobStatus::Pending,
            retry_count: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            chunk_hash: None,
            error: None,
            url: resource.url.clone(),
            authorization: auth.clone(),
            cookie: cookie.clone(),
            dest_path: format!("{}/chunk_{:05}.bin", dest_folder, i),
            total_chunks,
            total_file_size: total_size,
            compression_strategy: compression_strategy.clone(),
        });
    }

    tracing::info!(
        file_job_id = %file_job._id,
        total_size, chunk_size, total_chunks,
        "Spawning {} chunk jobs",
        total_chunks
    );

    Ok(chunks)
}

#[async_trait]
impl super::JobHandler for FileJobHandler {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobErrorOutcome> {
        let file_job = extract_file_job(&ctx.job)?;
        let resource = &file_job.resource;
        let threshold_bytes = ctx.config.compression_threshold_mb * 1024 * 1024;

        // ── Phase 1: HEAD preflight ──────────────────────────────────────
        let (head_size, head_ranges) = match initiate_head(resource).await {
            Ok(info) => (info.content_length, info.accept_ranges),
            Err(e) => {
                tracing::warn!(error = %e, "HEAD preflight failed, falling back to GET");
                (None, false)
            }
        };

        if let Some(size) = head_size {
            if size > threshold_bytes {
                if head_ranges {
                    let chunks = spawn_chunk_jobs(file_job, size, &ctx.config).await?;
                    return Ok(JobOutcome::SpawnedChunks(chunks));
                }
                tracing::warn!(
                    "File {} bytes exceeds threshold but server doesn't support Range — downloading full",
                    size
                );
            }
        }

        // ── Phase 2: GET + stream ────────────────────────────────────────
        let (response, mut download) = initiate_download(resource).await?;

        let resp_content_length = response.content_length();
        let resp_accept_ranges = response
            .headers()
            .get(wreq::header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map_or(false, |v| v.contains("bytes"));

        // Re-check with response headers (may differ from HEAD)
        if let Some(resp_size) = resp_content_length {
            if resp_size > threshold_bytes && resp_accept_ranges {
                drop(response);
                let chunks = spawn_chunk_jobs(file_job, resp_size, &ctx.config).await?;
                return Ok(JobOutcome::SpawnedChunks(chunks));
            }
        }

        // Decide whether to allow abort-at-threshold (only when size is unknown)
        let known_size = head_size.or(resp_content_length);
        let max_bytes = if known_size.is_none() {
            Some(threshold_bytes)
        } else {
            None
        };

        match download_to_temp(
            response,
            &ctx.config.temp_dir,
            &download.filename,
            &file_job._id,
            max_bytes,
        )
        .await?
        {
            StreamResult::Completed {
                temp_path,
                hash,
                bytes_mime,
                byte_count,
            } => {
                download.content_length = byte_count;
                if bytes_mime != "application/octet-stream" {
                    download.mime_type = bytes_mime;
                }

                match ctx.db.upsert_file_metadata(&hash).await {
                    Ok(UpsertResult::Inserted) => {
                        handle_new_file(ctx, file_job, download, temp_path, hash).await
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
            StreamResult::ThresholdExceeded { byte_count } => {
                tracing::info!(
                    "Stream exceeded threshold ({} > {}), switching to Range",
                    byte_count,
                    threshold_bytes
                );

                if resp_accept_ranges {
                    let chunks = spawn_chunk_jobs(file_job, byte_count, &ctx.config).await?;
                    return Ok(JobOutcome::SpawnedChunks(chunks));
                }

                // Probe: send a Range: 0-0 to check server support
                let auth = resource
                    .config
                    .as_ref()
                    .and_then(|c| c.headers.as_ref())
                    .and_then(|h| h.authorization.as_deref());
                let cookie = resource
                    .config
                    .as_ref()
                    .and_then(|c| c.headers.as_ref())
                    .and_then(|h| h.cookie.as_deref());

                match initiate_range_download(&resource.url, 0, 0, auth, cookie).await {
                    Ok(probe) if probe.status() == 206 => {
                        drop(probe);
                        let chunks = spawn_chunk_jobs(file_job, byte_count, &ctx.config).await?;
                        Ok(JobOutcome::SpawnedChunks(chunks))
                    }
                    _ => Err(JobErrorOutcome::Fatal(format!(
                        "File exceeds {} MB threshold (streamed {} bytes) but server does not \
                         support Range requests — cannot chunk",
                        ctx.config.compression_threshold_mb, byte_count
                    ))),
                }
            }
        }
    }
}

#[async_trait]
impl super::JobHandler for ChunkJobHandler {
    async fn execute(&self, ctx: &JobContext) -> Result<JobOutcome, JobErrorOutcome> {
        let chunk_job = ctx.chunk_job();

        tracing::info!(
            job_id = %chunk_job._id,
            "Processing chunk {} (bytes {}-{})",
            chunk_job.chunk_index,
            chunk_job.offset_start,
            chunk_job.offset_end,
        );

        let auth = chunk_job.authorization.as_deref();
        let cookie = chunk_job.cookie.as_deref();

        let response = initiate_range_download(
            &chunk_job.url,
            chunk_job.offset_start,
            chunk_job.offset_end,
            auth,
            cookie,
        )
        .await?;

        let chunk_label = format!("chunk_{:05}", chunk_job.chunk_index);
        let (temp_path, chunk_hash, byte_count) =
            download_range_chunk(response, &ctx.config.temp_dir, &chunk_label, &chunk_job._id)
                .await?;

        // Compress chunk (default gzip if no strategy specified)
        let strategy = chunk_job
            .compression_strategy
            .clone()
            .unwrap_or(GenericCompressionStrategy::Gzip);

        let (compressed_path, compressed_size) = compress_generic_local(
            &temp_path,
            &chunk_label,
            &strategy,
            ctx.config.compression_quality,
        )
        .await?;

        let ext = generic_compression_extension(&strategy);
        let storage_path = if ext.is_empty() {
            chunk_job.dest_path.clone()
        } else {
            format!("{}.{}", chunk_job.dest_path, ext)
        };

        // Upload compressed chunk
        {
            let mut file = tokio::fs::File::open(&compressed_path)
                .await
                .map_err(|e| JobErrorOutcome::Retryable(e.to_string()))?;
            ctx.storage.upload(&storage_path, &mut file).await?;
        }
        tokio::fs::remove_file(&compressed_path).await.ok();

        // Also clean up original temp if different from compressed path
        if compressed_path != temp_path {
            tokio::fs::remove_file(&temp_path).await.ok();
        }

        // Register completion in Redis
        ctx.redis
            .register_chunk(&chunk_job.parent_job_id, &chunk_job._id)
            .await?;

        // Store chunk result for later manifest assembly
        let chunk_ref = ChunkRef {
            hash: chunk_hash.clone(),
            size_original: byte_count,
            size_compressed: Some(compressed_size),
            storage_path,
            offset_start: chunk_job.offset_start,
            offset_end: chunk_job.offset_end,
        };

        tracing::info!(job_id = %chunk_job._id, "Chunk {} completed", chunk_job.chunk_index);
        Ok(JobOutcome::ChunkCompleted(chunk_ref))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        JobEnvelope,
        download::extract_file_job,
        download::filename_from_url,
        expand_path,
        types::{ChunkJob, FileJob, JobStatus},
    };
    use chrono::Utc;
    use std::path::PathBuf;
    use url::Url;

    use crate::models::Resource;

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
            file_hash: Some("abc".into()),
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
            url: Url::parse("https://example.com/f.png").unwrap(),
            authorization: None,
            cookie: None,
            dest_path: "/tmp/chunk_00000.bin".into(),
            total_chunks: 1,
            total_file_size: 100,
            compression_strategy: None,
        };
        let envelope = JobEnvelope::Chunk(chunk_job);
        let result = extract_file_job(&envelope);
        assert!(result.is_err());
    }

    use super::parse_chunk_size;

    #[test]
    fn test_parse_chunk_size_mb() {
        assert_eq!(parse_chunk_size("128MB"), 128 * 1024 * 1024);
    }

    #[test]
    fn test_parse_chunk_size_gb() {
        assert_eq!(parse_chunk_size("2GB"), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_chunk_size_default() {
        assert_eq!(parse_chunk_size(""), 128 * 1024 * 1024);
    }

    #[test]
    fn test_parse_chunk_size_raw_bytes() {
        assert_eq!(parse_chunk_size("1024"), 1024);
    }
}
