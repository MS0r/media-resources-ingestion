use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::{
    compression,
    error::JobErrorOutcome,
    handlers::jobs::{
        FileJob, JobContext, JobOutcome, compression::mime_to_extension, download::DownloadInfo,
    },
    job::JobEffect,
    models::Metadata,
};

use super::expand_path;

pub(crate) async fn handle_new_file(
    ctx: &JobContext,
    file_job: &FileJob,
    download: DownloadInfo,
    temp_path: String,
    hash_hex: String,
) -> Result<JobOutcome, JobErrorOutcome> {
    let resource = &file_job.spec;
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

    let quality = resource
        .config
        .as_ref()
        .and_then(|c| c.quality)
        .unwrap_or(ctx.config.compression_quality);

    // Decide compression plan — single decision point
    let plan = compression::plan::decide(override_strategy, &download.mime_type);

    // Apply the plan
    let cancel = Arc::new(AtomicBool::new(false));
    let applied = compression::plan::apply(
        &plan,
        std::path::Path::new(&temp_path),
        &download.filename,
        &download.mime_type,
        download.content_length,
        quality,
        ctx.config.compression_timeout_secs,
        cancel,
    )
    .await?;

    let local_file = applied.output_path.to_string_lossy().to_string();
    let compressed_size = Some(applied.size);
    let final_mime = applied.mime;

    if final_mime != original_mime
        && let Some(new_ext) = mime_to_extension(&final_mime)
    {
        final_path = expand_path(dest_path, &format!("{}.{}", download.filename, new_ext))
            .to_string_lossy()
            .to_string();
    }

    // Verify storage provider is healthy before attempting upload
    let _ = ctx.storage.health_check().await?;

    if let Some(pr) = pr {
        pr.report("uploading", 6, Some(7), None).await;
    }
    let mut file = tokio::fs::File::open(&local_file).await?;
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

    Ok(JobOutcome::Done(JobEffect::FileStored { metadata }))
}

pub(crate) async fn handle_duplicate(
    temp_path: &str,
    existing_hash: &str,
) -> Result<(), JobErrorOutcome> {
    tracing::info!(
        "Duplicate detected (existing hash: {}), cleaning up",
        existing_hash
    );
    tokio::fs::remove_file(temp_path).await?;
    Ok(())
}
