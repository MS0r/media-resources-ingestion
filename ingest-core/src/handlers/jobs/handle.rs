use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::{
    error::{JobError, JobErrorOutcome},
    handlers::jobs::{
        compression::{
            compress_generic_local, compress_image_local, compress_video_local,
            generic_compression_mime, mime_to_extension,
        },
        {ChunkJob, FileJob, JobContext, JobOutcome, types::DownloadInfo},
    },
    models::{ChunkRef, CompressionOverride, GenericCompressionStrategy, Manifest, Metadata},
    storage::Provider,
};
use sha2::{Digest, Sha256};
use tokio::time::timeout;

use super::expand_path;

pub(crate) async fn handle_new_file(
    ctx: &JobContext,
    file_job: &FileJob,
    download: DownloadInfo,
    temp_path: String,
    hash_hex: String,
) -> Result<JobOutcome, JobErrorOutcome> {
    let resource = &file_job.resource;
    let pr = ctx.progress.as_ref();

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
                if !original_mime.starts_with("image/") {
                    tracing::warn!(
                        "Image compression requested but bytes indicate MIME is {} — skipping",
                        download.mime_type
                    );
                    (temp_path.clone(), Some(0), download.mime_type)
                } else {
                    match timeout(
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
                if !original_mime.starts_with("video/") {
                    tracing::warn!(
                        "Video compression requested but bytes indicate MIME is {} — skipping",
                        download.mime_type
                    );
                    (temp_path.clone(), Some(0), download.mime_type)
                } else {
                    let cancel_video = Arc::new(AtomicBool::new(false));
                    let cancel_video_clone = cancel_video.clone();
                    match timeout(
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
            CompressionOverride::Generic(strategy) => {
                match compress_generic_local(
                    &temp_path,
                    &download.filename,
                    strategy,
                    ctx.config.compression_quality,
                )
                .await
                {
                    Ok((path, size)) => {
                        let mime = if path == temp_path {
                            download.mime_type.clone()
                        } else {
                            generic_compression_mime(strategy).to_string()
                        };
                        tracing::info!(
                            "Generic compressed: {} -> {} bytes",
                            download.content_length,
                            size
                        );
                        (path, Some(size), mime)
                    }
                    Err(e) => {
                        tracing::warn!("Generic compression failed: {e}, keeping original");
                        (temp_path.clone(), Some(0), download.mime_type)
                    }
                }
            }
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
    let _ = ctx.storage.health_check().await.map_err(|e| {
        tracing::error!("Storage provider health check failed before upload");
        JobErrorOutcome::from(e)
    });

    if let Some(pr) = pr {
        pr.report("uploading", 6, Some(7), None).await;
    }
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

    Ok(JobOutcome::Completed(metadata))
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
    Ok(JobOutcome::Duplicated)
}
