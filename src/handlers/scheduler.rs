use crate::{
    context::ContextFactory,
    handlers::jobs::{ChunkJobHandler, FileJobHandler, JobError, JobHandler, JobKind, JobOutcome},
};
use reqwest::header::{self, HeaderMap};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

fn get_mime_from_filename(filename: &str) -> Option<String> {
    mime_guess::from_path(filename)
        .first_raw()
        .map(|s| s.to_string())
}

fn get_mime_type(headers: &HeaderMap) -> Option<String> {
    if let Some(ct) = headers.get(header::CONTENT_TYPE)
        && let Ok(ct_str) = ct.to_str()
    {
        // remove charset if present
        let mime = ct_str.split(';').next()?.trim();
        if !mime.is_empty() {
            return Some(mime.to_string());
        }
    }

    if let Some(cd) = headers.get(header::CONTENT_DISPOSITION) {
        if let Ok(cd_str) = cd.to_str()
            && let Some(filename_part) = cd_str
                .split(';')
                .find(|part| part.trim().starts_with("filename="))
        {
            let filename = filename_part
                .trim()
                .trim_start_matches("filename=")
                .trim_matches('"');

            if let Some(mime) = mime_guess::from_path(filename).first_raw() {
                return Some(mime.to_string());
            }
        }
    }
    Some("No Mime type found".to_string())
}

pub async fn scheduler_loop(
    file_handler: Arc<FileJobHandler>,
    chunk_handler: Arc<ChunkJobHandler>,
    ctx_factory: Arc<ContextFactory>,
    max_file_workers: usize,
    max_chunk_workers: usize,
) {
    let redis = ctx_factory.redis_service();
    let file_semaphore = Arc::new(Semaphore::new(max_file_workers));
    let chunk_semaphore = Arc::new(Semaphore::new(max_chunk_workers));

    loop {
        // ZPOPMAX jobs:pending — highest priority first
        if let Ok(Some((kind, job_id))) = redis.dequeue_job().await {
            match kind {
                JobKind::File => {
                    let permit = file_semaphore.clone().acquire_owned().await.unwrap();

                    let ctx = match ctx_factory.build_file_context(&job_id).await {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            tracing::error!(job_id = %job_id, error = %e, "Failed to build job context");
                            continue;
                        }
                    };

                    let handler = file_handler.clone();
                    tokio::spawn(async move {
                        let _permit = permit;

                        match handler.execute(&ctx).await {
                            Ok(JobOutcome::SpawnedChunks(chunks)) => {}
                            Ok(JobOutcome::Completed) => {}
                            Err(JobError::Retryable(e)) => {
                                if let Err(err) = ctx.redis.retry_job(&job_id, kind).await {
                                    tracing::error!(?err, "failed to requeue retryable job");
                                }
                            }

                            Err(JobError::Fatal(e)) => {
                                tracing::error!(?e, "fatal job error");
                            }
                        }
                    });
                }
                JobKind::Chunk => {
                    let permit = chunk_semaphore.clone().acquire_owned().await.unwrap();
                    // same pattern
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
