use async_trait::async_trait;
use chrono::Utc;

use crate::{
    AppConfig,
    auth::AuthProviderRegistry,
    error::JobErrorOutcome,
    models::{ChunkRef, CompressionOverride, GenericCompressionStrategy, Resource},
    services::mongo::UpsertResult,
    storage::Provider,
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

/// Resolve the source auth token for a resource.
///
/// Returns `Some(token)` if a dynamic OAuth token should be used,
/// `None` if static headers or no auth are configured.
///
/// For S3 sources, this generates a pre-signed URL and stores it in the
/// returned data so the caller can replace the resource URL.
async fn resolve_source_auth(
    resource: &Resource,
    auth_registry: Option<&AuthProviderRegistry>,
) -> Result<Option<String>, JobErrorOutcome> {
    let config = resource.config.as_ref();
    let source_auth = config
        .and_then(|c| c.source_auth.as_deref())
        .unwrap_or("auto");

    let provider = if source_auth == "auto" {
        AuthProviderRegistry::detect_from_url(resource.url.as_str())
    } else if source_auth == "headers" || source_auth == "none" || source_auth == "auto" {
        None
    } else {
        Some(source_auth)
    };

    match provider {
        Some(name @ ("gdrive" | "dropbox")) => {
            let registry = auth_registry.ok_or_else(|| {
                JobErrorOutcome::Fatal(format!(
                    "Source auth requires {} but no auth registry configured",
                    name
                ))
            })?;
            let tp = registry.get(name).ok_or_else(|| {
                JobErrorOutcome::Fatal(format!("Source auth provider '{}' not registered", name))
            })?;
            let token = tp.access_token().await.map_err(|e| {
                JobErrorOutcome::Fatal(format!("Token refresh failed for {name}: {e}"))
            })?;
            Ok(Some(token))
        }
        Some("s3") => {
            // S3 source: generate a pre-signed URL for the full object.
            // Detection from URL would require parsing bucket/key which is complex.
            // For now, require explicit source_auth: s3 and use env vars.
            tracing::info!("S3 source auth: generating pre-signed URL");
            match generate_s3_presigned_url(resource.url.as_str()).await {
                Ok(presigned_url) => {
                    // We can't modify the resource in place, so we return the URL
                    // and let the caller use it. For simplicity, return None for auth token
                    // and let the caller handle URL replacement.
                    tracing::info!("Generated pre-signed S3 URL");
                    // Convert pre-signed URL to a header: we pass the pre-signed URL
                    // as if it were an auth token, but the caller knows to check for S3.
                    Ok(Some(format!("__S3_PRESIGNED__{}", presigned_url)))
                }
                Err(e) => Err(JobErrorOutcome::Fatal(format!(
                    "Failed to generate S3 presigned URL: {e}"
                ))),
            }
        }
        None | Some(_) => Ok(None),
    }
}

/// Attempt to generate a pre-signed URL for an S3 object.
async fn generate_s3_presigned_url(url: &str) -> Result<String, String> {
    use aws_sdk_s3::presigning::PresigningConfig;
    use std::time::Duration;

    let bucket =
        std::env::var("AWS_BUCKET").map_err(|_| "AWS_BUCKET env var not set".to_string())?;

    // Extract key from URL path
    let parsed = url::Url::parse(url).map_err(|e| format!("Invalid URL: {e}"))?;
    let key = parsed
        .path_segments()
        .map(|s| s.collect::<Vec<_>>().join("/"))
        .unwrap_or_default();

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&config);

    let presigned_config = PresigningConfig::expires_in(Duration::from_secs(3600))
        .map_err(|e| format!("Invalid presigning config: {e}"))?;

    let presigned_request = client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .presigned(presigned_config)
        .await
        .map_err(|e| format!("S3 presigning failed: {e}"))?;

    Ok(presigned_request.uri().to_string())
}

/// Check if a resolved auth value is an S3 pre-signed URL marker.
fn is_s3_presigned(auth: &str) -> bool {
    auth.starts_with("__S3_PRESIGNED__")
}

/// Extract the actual URL from a pre-signed marker string.
fn extract_s3_url(auth: &str) -> &str {
    auth.strip_prefix("__S3_PRESIGNED__").unwrap_or(auth)
}

/// Extract auth headers from resource config, falling back to resolved token.
fn resolve_auth_for_chunks(
    resource: &Resource,
    resolved_token: Option<&str>,
) -> (Option<String>, Option<String>) {
    // If we have a dynamically resolved token, use it
    if let Some(token) = resolved_token {
        if is_s3_presigned(token) {
            // S3 pre-signed URL — auth is in the URL itself, no header needed
            (None, None)
        } else {
            (Some(format!("Bearer {}", token)), None)
        }
    } else {
        // Fall back to static headers
        resource
            .config
            .as_ref()
            .and_then(|c| c.headers.as_ref())
            .map(|h| (h.authorization.clone(), h.cookie.clone()))
            .unwrap_or_default()
    }
}

async fn spawn_chunk_jobs(
    file_job: &FileJob,
    total_size: u64,
    config: &AppConfig,
    resolved_auth: Option<&str>,
) -> Result<Vec<ChunkJob>, JobErrorOutcome> {
    let chunk_size = match &file_job.chunk_size {
        Some(s) => parse_chunk_size(s),
        None => parse_chunk_size(&config.chunk_size),
    };
    let total_chunks = ((total_size + chunk_size - 1) / chunk_size) as u32;
    let resource = &file_job.resource;

    let (auth, cookie) = resolve_auth_for_chunks(resource, resolved_auth);

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

    let dest_provider = resource
        .dest
        .as_ref()
        .and_then(|d| d.provider.clone())
        .unwrap_or(Provider::Local);

    let build_chunk = |i: u32| {
        let offset_start = (i as u64) * chunk_size;
        let offset_end = ((i as u64 + 1) * chunk_size - 1).min(total_size.saturating_sub(1));

        let chunk_url = if let Some(token) = resolved_auth
            && is_s3_presigned(token)
        {
            // For S3 sources, generate a separate pre-signed URL per chunk
            // (includes the Range in the signature or uses same URL + Range header)
            url::Url::parse(extract_s3_url(token)).unwrap_or_else(|_| resource.url.clone())
        } else {
            resource.url.clone()
        };

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
            url: chunk_url,
            authorization: auth.clone(),
            cookie: cookie.clone(),
            dest_path: format!("{}/chunk_{:05}.bin", dest_folder, i),
            storage: dest_provider.clone(),
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
        let pr = ctx.progress.as_ref();

        // ── Phase 0: Resolve source auth ─────────────────────────────────
        let auth_token = resolve_source_auth(resource, ctx.auth_registry.as_deref()).await?;

        // ── Phase 1: HEAD preflight ──────────────────────────────────────
        if let Some(pr) = pr {
            pr.report("preflight", 1, Some(7), None).await;
        }
        let (head_size, head_ranges) =
            match initiate_head(resource, &ctx.http_client, auth_token.as_deref()).await {
                Ok(info) => (info.content_length, info.accept_ranges),
                Err(e) => {
                    tracing::warn!(error = %e, "HEAD preflight failed, falling back to GET");
                    (None, false)
                }
            };

        if let Some(size) = head_size {
            if size > threshold_bytes {
                if head_ranges {
                    if let Some(pr) = pr {
                        pr.report("splitting", 2, Some(7), Some("Spawning chunk jobs"))
                            .await;
                    }
                    let chunks =
                        spawn_chunk_jobs(file_job, size, &ctx.config, auth_token.as_deref())
                            .await?;
                    return Ok(JobOutcome::SpawnedChunks(chunks));
                }
                tracing::warn!(
                    "File {} bytes exceeds threshold but server doesn't support Range — downloading full",
                    size
                );
            }
        }

        // ── Phase 2: GET + stream ────────────────────────────────────────
        if let Some(pr) = pr {
            pr.report("download", 2, Some(7), None).await;
        }
        let (response, mut download) =
            initiate_download(resource, &ctx.http_client, auth_token.as_deref()).await?;

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
                if let Some(pr) = pr {
                    pr.report("splitting", 2, Some(7), Some("Spawning chunk jobs"))
                        .await;
                }
                let chunks =
                    spawn_chunk_jobs(file_job, resp_size, &ctx.config, auth_token.as_deref())
                        .await?;
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

        let (temp_path, hash_hex, _mime_type, byte_count) = match stream_result {
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
                (temp_path, hash, download.mime_type.clone(), byte_count)
            }
            StreamResult::ThresholdExceeded { byte_count } => {
                tracing::info!(
                    "Stream exceeded threshold ({} > {}), switching to Range",
                    byte_count,
                    threshold_bytes
                );

                if resp_accept_ranges {
                    let chunks =
                        spawn_chunk_jobs(file_job, byte_count, &ctx.config, auth_token.as_deref())
                            .await?;
                    return Ok(JobOutcome::SpawnedChunks(chunks));
                }

                // Probe: send a Range: 0-0 to check server support
                let (auth, cookie) = resolve_auth_for_chunks(resource, auth_token.as_deref());
                let auth_ref = auth.as_deref();
                let cookie_ref = cookie.as_deref();

                match initiate_range_download(
                    &resource.url,
                    0,
                    0,
                    auth_ref,
                    cookie_ref,
                    &ctx.http_client,
                )
                .await
                {
                    Ok((probe, _)) if probe.status() == 206 => {
                        drop(probe);
                        let chunks = spawn_chunk_jobs(
                            file_job,
                            byte_count,
                            &ctx.config,
                            auth_token.as_deref(),
                        )
                        .await?;
                        return Ok(JobOutcome::SpawnedChunks(chunks));
                    }
                    _ => {
                        return Err(JobErrorOutcome::Fatal(format!(
                            "File exceeds {} MB threshold (streamed {} bytes) but server does not \
                         support Range requests — cannot chunk",
                            ctx.config.compression_threshold_mb, byte_count
                        )));
                    }
                }
            }
        };

        download.content_length = byte_count;

        // ── Phase 4: dedup check ─────────────────────────────────────────
        if let Some(pr) = pr {
            pr.report("dedup", 3, Some(7), None).await;
        }
        match ctx.db.upsert_file_metadata(&hash_hex).await? {
            UpsertResult::Duplicate(existing) => {
                let outcome = handle_duplicate(&temp_path, &existing.file_hash).await?;
                return Ok(outcome);
            }
            UpsertResult::Inserted => {}
        }

        // ── Phase 5: handle (compress + upload + metadata) ───────────────
        if let Some(pr) = pr {
            pr.report("compressing", 5, Some(7), None).await;
        }
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

        let (response, mime) = initiate_range_download(
            &chunk_job.url,
            chunk_job.offset_start,
            chunk_job.offset_end,
            auth,
            cookie,
            &ctx.http_client,
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

        Ok(JobOutcome::ChunkCompleted(chunk_ref, mime))
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

    #[test]
    fn test_is_s3_presigned() {
        assert!(is_s3_presigned(
            "__S3_PRESIGNED__https://s3.amazonaws.com/file"
        ));
        assert!(!is_s3_presigned("Bearer token123"));
        assert!(!is_s3_presigned(""));
    }

    #[test]
    fn test_extract_s3_url() {
        assert_eq!(
            extract_s3_url("__S3_PRESIGNED__https://example.com/file"),
            "https://example.com/file"
        );
        assert_eq!(extract_s3_url("no-prefix"), "no-prefix");
    }
}
