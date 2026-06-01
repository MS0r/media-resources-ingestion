use crate::{
    context::ContextFactory,
    error::JobErrorOutcome,
    handlers::jobs::{ChunkJobHandler, FileJobHandler, JobHandler, JobKind, JobOutcome},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

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
        let worker = max_file_workers - file_semaphore.available_permits() + 1;
        if let Ok(Some((kind, job_id))) = redis.dequeue_job(worker).await {
            match kind {
                JobKind::File => {
                    let permit = file_semaphore.clone().acquire_owned().await.unwrap();
                    let job_ctx = match ctx_factory.build_file_context(&job_id).await {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            tracing::error!(job_id = %job_id, error = %e, "Failed to build job context");
                            continue;
                        }
                    };

                    let handler = file_handler.clone();
                    let redis_clone = redis.clone();
                    tokio::spawn(async move {
                        let ctx = job_ctx;
                        let _permit = permit;
                        match handler.execute(&ctx).await {
                            Ok(JobOutcome::SpawnedChunks(chunks)) => {
                                for chunk in chunks {
                                    let _ = redis_clone.enqueue_chunk_job(&chunk).await;
                                }
                            }
                            Ok(JobOutcome::Completed) => {
                                ctx.redis.complete_job(&job_id).await.ok();
                                tracing::info!(job_id = %job_id, "File job completed");
                            }
                            Err(JobErrorOutcome::Retryable(e)) => {
                                tracing::error!(?job_id, "retrying job due to error: ({e})");
                                if let Err(err) = ctx.redis.retry_job(&job_id, JobKind::File).await
                                {
                                    tracing::error!(?err, "failed to reenqueue retryable job");
                                }
                            }
                            Err(JobErrorOutcome::Fatal(e)) => {
                                tracing::error!(?e, "fatal job error");
                                ctx.redis.fail_job(&job_id, e.as_str()).await.ok();
                                ctx.db.fail_job(&job_id, e.as_str()).await.ok();
                            }
                        }
                    });
                }
                JobKind::Chunk => {
                    let permit = chunk_semaphore.clone().acquire_owned().await.unwrap();
                    let ctx = match ctx_factory.build_chunk_context(&job_id).await {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            tracing::error!(job_id = %job_id, error = %e, "Failed to build chunk job context");
                            continue;
                        }
                    };

                    let handler = chunk_handler.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        match handler.execute(&ctx).await {
                            Ok(JobOutcome::Completed) => {
                                tracing::info!(job_id = %job_id, "Chunk job completed");
                            }
                            Ok(JobOutcome::SpawnedChunks(_)) => {
                                tracing::warn!(job_id = %job_id, "Unexpected SpawnedChunks from chunk job");
                            }
                            Err(JobErrorOutcome::Retryable(e)) => {
                                tracing::error!(?job_id, "retrying chunk job due to error: ({e})");
                                if let Err(err) = ctx.redis.retry_job(&job_id, JobKind::Chunk).await
                                {
                                    tracing::error!(
                                        ?err,
                                        "failed to reenqueue retryable chunk job"
                                    );
                                }
                            }
                            Err(JobErrorOutcome::Fatal(e)) => {
                                tracing::error!(?e, "fatal chunk job error");
                            }
                        }
                    });
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
