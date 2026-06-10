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

    // Extract auth headers in a single pass through the config chain
    let (auth, cookie) = resource
        .config
        .as_ref()
        .and_then(|c| c.headers.as_ref())
        .map(|h| (h.authorization.clone(), h.cookie.clone()))
        .unwrap_or_default();

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

    let base_path = resource
        .dest
        .as_ref()
        .and_then(|d| d.path.as_ref())
        .map(|p| p.trim_end_matches('/'))
        .unwrap_or_else(|| config.default_path.trim_end_matches('/'));
    let dest_folder = format!("{}/{}", base_path, resource_name);

    let build_chunk = |i: u32| {
        let offset_start = (i as u64) * chunk_size;
        let offset_end = ((i as u64 + 1) * chunk_size - 1).min(total_size.saturating_sub(1));

        ChunkJob {
            _id: uuid::Uuid::new_v4().to_string(),
            parent_job_id: file_job._id.clone(),
            file_hash: None,
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
        }
    };

    let chunks: Vec<ChunkJob> = (0..total_chunks).map(build_chunk).collect();

    tracing::info!(
        file_job_id = %file_job._id,
        total_size,
        chunk_size,
        total_chunks,
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

        // ── Phase 3: stream download ─────────────────────────────────────
        let stream_result = download_to_temp(
            response,
            &ctx.config.temp_dir,
            &download.filename,
            &file_job._id,
            None,
        )
        .await?;

        let (temp_path, hash_hex, mime_type, byte_count) = match stream_result {
            StreamResult::Completed {
                temp_path,
                hash,
                bytes_mime,
                byte_count,
            } => {
                // Use byte_count to overwrite content_length (which might be 0 if no Content-Length header)
                download.content_length = byte_count;
                if bytes_mime != "application/octet-stream" {
                    download.mime_type = bytes_mime;
                }
                (temp_path, hash, download.mime_type.clone(), byte_count)
            }
            StreamResult::ThresholdExceeded { .. } => {
                unreachable!("Threshold not set for non-chunked download")
            }
        };

        download.content_length = byte_count;

        // ── Phase 4: dedup check ─────────────────────────────────────────
        match ctx.db.upsert_file_metadata(&hash_hex).await? {
            UpsertResult::Duplicate(existing) => {
                let outcome = handle_duplicate(&temp_path, &existing.file_hash).await?;
                return Ok(outcome);
            }
            UpsertResult::Inserted => {}
        }

        // ── Phase 5: handle (compress + upload + metadata) ───────────────
        handle_new_file(ctx, file_job, download, temp_path, hash_hex).await
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

        // Cleanup temp files
        tokio::fs::remove_file(&compressed_path).await.ok();
        if compressed_path != temp_path {
            tokio::fs::remove_file(&temp_path).await.ok();
        }

        let chunk_ref = ChunkRef {
            hash: chunk_hash,
            size_original: byte_count,
            size_compressed: Some(compressed_size),
            storage_path,
            offset_start: chunk_job.offset_start,
            offset_end: chunk_job.offset_end,
        };

        Ok(JobOutcome::ChunkCompleted(chunk_ref))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_chunk_size_mb() {
        assert_eq!(parse_chunk_size("128MB"), 128 * 1024 * 1024);
        assert_eq!(parse_chunk_size(" 64MB "), 64 * 1024 * 1024);
    }

    #[test]
    fn test_parse_chunk_size_gb() {
        assert_eq!(parse_chunk_size("1GB"), 1024 * 1024 * 1024);
        assert_eq!(parse_chunk_size("2GB"), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_chunk_size_kb() {
        assert_eq!(parse_chunk_size("512KB"), 512 * 1024);
        assert_eq!(parse_chunk_size("1024KB"), 1024 * 1024);
    }

    #[test]
    fn test_parse_chunk_size_raw_bytes() {
        assert_eq!(parse_chunk_size("1048576"), 1048576);
    }

    #[test]
    fn test_parse_chunk_size_empty() {
        assert_eq!(parse_chunk_size(""), 128 * 1024 * 1024);
    }

    #[test]
    fn test_parse_chunk_size_garbage() {
        assert_eq!(parse_chunk_size("xyz"), 128 * 1024 * 1024);
    }
}
