use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::{
    error::{JobError, JobErrorOutcome},
    models::{CompressionOverride, Metadata},
};

use super::expand_path;
use crate::handlers::jobs::compression::{
    compress_image_local, compress_video_local, mime_to_extension,
};
use crate::handlers::jobs::types::DownloadInfo;
use crate::handlers::jobs::{FileJob, JobContext, JobOutcome};

pub(crate) async fn handle_new_file(
    ctx: &JobContext,
    file_job: &FileJob,
    download: DownloadInfo,
    temp_path: String,
    hash_hex: String,
) -> Result<JobOutcome, JobErrorOutcome> {
    let resource = &file_job.resource;

    let (dest_path, provider) = match &resource.dest {
        Some(dest) => {
            if let Some(path) = &dest.path
                && let Some(provider) = &dest.provider
            {
                (path, provider)
            } else {
                return Err(JobErrorOutcome::Fatal(
                    "Missing destination path or provider".to_string(),
                ));
            }
        }
        None => {
            return Err(JobErrorOutcome::Fatal(
                "Missing destination path or provider".to_string(),
            ));
        }
    };

    let original_mime = download.mime_type.clone();

    let mut final_path = expand_path(
        dest_path,
        &format!("{}.{}", download.filename, download.extension),
    )
    .to_string_lossy()
    .to_string();

    let override_strategy = resource
        .config
        .as_ref()
        .and_then(|c| c.compression_override.as_ref());

    let compression_timeout = Duration::from_secs(ctx.config.compression_timeout_secs);
    let (local_file, compressed_size, final_mime) = match override_strategy {
        Some(strategy) => match strategy {
            CompressionOverride::Image(image_s) => {
                let is_image = original_mime.starts_with("image/");
                if !is_image {
                    tracing::warn!(
                        "Image compression requested but bytes indicate MIME is {} — skipping",
                        download.mime_type
                    );
                    (temp_path.clone(), Some(0), download.mime_type)
                } else {
                    match tokio::time::timeout(
                        compression_timeout,
                        compress_image_local(
                            &download.filename,
                            &download.mime_type,
                            download.content_length,
                            ctx.config.compression_quality,
                            &temp_path,
                            image_s,
                        ),
                    )
                    .await
                    {
                        Ok(Ok((path, size, mime))) => {
                            tracing::info!(
                                "Image compressed: {} -> {} bytes",
                                download.content_length,
                                size
                            );
                            (path, Some(size), mime)
                        }
                        Ok(Err(e)) => {
                            tracing::warn!("Image compression failed: {}, keeping original", e);
                            (temp_path.clone(), Some(0), download.mime_type)
                        }
                        Err(_elapsed) => {
                            tracing::warn!(
                                "Image compression timed out after {}s, keeping original",
                                ctx.config.compression_timeout_secs
                            );
                            (temp_path.clone(), Some(0), download.mime_type)
                        }
                    }
                }
            }
            CompressionOverride::Video(video_s) => {
                let is_video = original_mime.starts_with("video/");
                if !is_video {
                    tracing::warn!(
                        "Video compression requested but bytes indicate MIME is {} — skipping",
                        download.mime_type
                    );
                    (temp_path.clone(), Some(0), download.mime_type)
                } else {
                    let cancel_video = Arc::new(AtomicBool::new(false));
                    let cancel_video_clone = cancel_video.clone();
                    match tokio::time::timeout(
                        compression_timeout,
                        compress_video_local(
                            &download.filename,
                            &download.mime_type,
                            download.content_length,
                            ctx.config.compression_quality,
                            &temp_path,
                            video_s,
                            cancel_video_clone,
                        ),
                    )
                    .await
                    {
                        Ok(Ok((path, size, mime))) => {
                            tracing::info!(
                                "Video compressed: {} -> {} bytes",
                                download.content_length,
                                size
                            );
                            (path, Some(size), mime)
                        }
                        Ok(Err(e)) => {
                            tracing::warn!("Video compression failed: {}, keeping original", e);
                            (temp_path.clone(), Some(0), download.mime_type)
                        }
                        Err(_elapsed) => {
                            tracing::warn!(
                                "Video compression timed out after {}s, keeping original",
                                ctx.config.compression_timeout_secs
                            );
                            cancel_video.store(true, Ordering::Relaxed);
                            (temp_path.clone(), Some(0), download.mime_type)
                        }
                    }
                }
            }
            CompressionOverride::Generic(_) => (temp_path.clone(), Some(0), download.mime_type),
            CompressionOverride::Universal(_) => (temp_path.clone(), Some(0), download.mime_type),
        },
        None => (temp_path.clone(), Some(0), download.mime_type),
    };

    if final_mime != original_mime
        && let Some(new_ext) = mime_to_extension(&final_mime)
    {
        final_path = expand_path(dest_path, &format!("{}.{}", download.filename, new_ext))
            .to_string_lossy()
            .to_string();
    }

    // Verify storage provider is healthy before attempting upload
    ctx.storage.health_check().await.map_err(|e| {
        tracing::error!(error = %e, "Storage provider health check failed before upload");
        JobErrorOutcome::Retryable(format!("Storage health check failed: {e}"))
    })?;

    let mut file = tokio::fs::File::open(&local_file)
        .await
        .map_err(|e| JobErrorOutcome::from(JobError::from(e)))?;
    ctx.storage.upload(&final_path, &mut file).await?;

    if final_path != local_file {
        tokio::fs::remove_file(&local_file).await.ok();
    }

    let metadata = Metadata::new(
        hash_hex.clone(),
        resource.url.clone(),
        provider.clone(),
        final_path,
        download.content_length,
        compressed_size,
        final_mime,
    );
    ctx.db.complete_job(metadata, &file_job._id).await?;
    tracing::info!("New file metadata inserted with hash: {}", hash_hex);

    Ok(JobOutcome::Completed)
}

pub(crate) async fn handle_duplicate(
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
