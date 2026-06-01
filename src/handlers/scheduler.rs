use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::{
    context::ContextFactory,
    error::JobErrorOutcome,
    handlers::jobs::{ChunkJobHandler, FileJobHandler, JobHandler, JobKind, JobOutcome},
};

pub async fn scheduler_loop(
    file_handler: Arc<FileJobHandler>,
    chunk_handler: Arc<ChunkJobHandler>,
    ctx_factory: Arc<ContextFactory>,
    max_file_workers: usize,
    max_chunk_workers: usize,
    shutdown: Arc<AtomicBool>,
) {
    let redis = ctx_factory.redis_service();
    let config = ctx_factory.config().clone();
    let file_semaphore = Arc::new(Semaphore::new(max_file_workers));
    let chunk_semaphore = Arc::new(Semaphore::new(max_chunk_workers));
    let timeout_duration = Duration::from_secs(config.job_timeout_secs);
    let timeout_secs = config.job_timeout_secs;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            tracing::warn!("Shutdown signal received, stopping scheduler loop");
            break;
        }

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
                    let shutdown_flag = shutdown.clone();

                    tokio::spawn(async move {
                        let ctx = job_ctx;
                        let _permit = permit;
                        let job_id_str = job_id.clone();

                        // Heartbeat: refresh the running lease every 30s while this job runs
                        let hb_redis = redis_clone.clone();
                        let hb_job_id = job_id.clone();
                        let hb_done = Arc::new(AtomicBool::new(false));
                        let hb_done_clone = hb_done.clone();
                        let hb_shutdown = shutdown_flag.clone();
                        tokio::spawn(async move {
                            let mut interval = tokio::time::interval(Duration::from_secs(30));
                            loop {
                                interval.tick().await;
                                if hb_done_clone.load(Ordering::Relaxed)
                                    || hb_shutdown.load(Ordering::Relaxed)
                                {
                                    break;
                                }
                                let _ = hb_redis.renew_lease(&hb_job_id).await;
                            }
                        });

                        let result =
                            tokio::time::timeout(timeout_duration, handler.execute(&ctx)).await;

                        hb_done.store(true, Ordering::Relaxed);

                        match result {
                            Ok(Ok(JobOutcome::SpawnedChunks(chunks))) => {
                                for chunk in chunks {
                                    let _ = redis_clone.enqueue_chunk_job(&chunk).await;
                                }
                            }
                            Ok(Ok(JobOutcome::Completed)) => {
                                ctx.redis.complete_job(&job_id_str).await.ok();
                                tracing::info!(job_id = %job_id_str, "File job completed");
                            }
                            Ok(Err(JobErrorOutcome::Retryable(e))) => {
                                tracing::error!(?job_id_str, "retrying job due to error: ({e})");
                                if let Err(err) =
                                    ctx.redis.retry_job(&job_id_str, JobKind::File).await
                                {
                                    tracing::error!(?err, "failed to reenqueue retryable job");
                                }
                            }
                            Ok(Err(JobErrorOutcome::Fatal(e))) => {
                                tracing::error!(?e, "fatal job error");
                                ctx.redis.fail_job(&job_id_str, e.as_str()).await.ok();
                                ctx.db.fail_job(&job_id_str, e.as_str()).await.ok();
                            }
                            Err(_elapsed) => {
                                let msg = format!("Job timed out after {}s", timeout_secs);
                                tracing::error!(job_id = %job_id_str, "{}", msg);
                                ctx.redis.fail_job(&job_id_str, &msg).await.ok();
                                ctx.db.fail_job(&job_id_str, &msg).await.ok();
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
                    let redis_clone = redis.clone();
                    let shutdown_flag = shutdown.clone();

                    tokio::spawn(async move {
                        let _permit = permit;
                        let job_id_str = job_id.clone();

                        let hb_redis = redis_clone.clone();
                        let hb_job_id = job_id.clone();
                        let hb_done = Arc::new(AtomicBool::new(false));
                        let hb_done_clone = hb_done.clone();
                        let hb_shutdown = shutdown_flag.clone();
                        tokio::spawn(async move {
                            let mut interval = tokio::time::interval(Duration::from_secs(30));
                            loop {
                                interval.tick().await;
                                if hb_done_clone.load(Ordering::Relaxed)
                                    || hb_shutdown.load(Ordering::Relaxed)
                                {
                                    break;
                                }
                                let _ = hb_redis.renew_lease(&hb_job_id).await;
                            }
                        });

                        let result = handler.execute(&ctx).await;

                        hb_done.store(true, Ordering::Relaxed);

                        match result {
                            Ok(JobOutcome::Completed) => {
                                tracing::info!(job_id = %job_id_str, "Chunk job completed");
                            }
                            Ok(JobOutcome::SpawnedChunks(_)) => {
                                tracing::warn!(job_id = %job_id_str, "Unexpected SpawnedChunks from chunk job");
                            }
                            Err(JobErrorOutcome::Retryable(e)) => {
                                tracing::error!(
                                    ?job_id_str,
                                    "retrying chunk job due to error: ({e})"
                                );
                                if let Err(err) =
                                    ctx.redis.retry_job(&job_id_str, JobKind::Chunk).await
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
