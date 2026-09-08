use crate::{
    context::ContextFactory,
    error::{JobErrorOutcome, ToolError},
    handlers::jobs::{
        ChunkJob, ChunkJobHandler, FileJobHandler, JobContext, JobHandler, JobKind, JobOutcome,
    },
    models::{
        ChunkRef, GenericCompressionStrategy, Manifest, Metadata, ProgressEvent, ProgressJobType,
        ProgressStatus,
    },
    services::{mongo::MongoService, redis::RedisService},
    storage::Provider,
};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::{error::Elapsed, timeout},
};

pub async fn scheduler_loop(
    file_handler: Arc<FileJobHandler>,
    chunk_handler: Arc<ChunkJobHandler>,
    ctx_factory: Arc<ContextFactory>,
    file_semaphore: Arc<Semaphore>,
    chunk_semaphore: Arc<Semaphore>,
    shutdown: Arc<AtomicBool>,
    worker_id: u32,
) -> Result<(), ToolError> {
    let redis = ctx_factory.redis_service();
    let config = ctx_factory.config();
    let timeout_duration = Duration::from_secs(config.job_timeout_secs);
    let shard_count = config.shard_count;
    let shutdown_grace = Duration::from_secs(config.shutdown_grace_secs);
    let mut tasks: JoinSet<()> = JoinSet::new();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            tracing::warn!(
                "Shutdown signal received, draining spawned tasks (grace={:?})",
                shutdown_grace
            );
            break;
        }

        // Promote any delayed jobs whose retry time has elapsed.
        if let Err(e) = redis.promote_due_delayed(shard_count).await {
            tracing::warn!(error = %e, "Failed to promote delayed jobs");
        }

        if let Ok(Some((kind, job_id))) = redis.dequeue_job(worker_id, shard_count).await {
            match kind {
                JobKind::File => {
                    let permit = file_semaphore.clone().acquire_owned().await?;
                    let ctx = match ctx_factory.build_file_context(&job_id).await {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            tracing::error!(job_id = %job_id, error = %e, "Failed to build file job context");
                            let err_msg = format!("Context build failed: {e}");
                            redis.fail_job(&job_id, &err_msg).await.ok();
                            let event = ProgressEvent {
                                job_id: job_id.clone(),
                                job_type: ProgressJobType::FileJob,
                                stage: "failed".to_string(),
                                current: 0,
                                total: None,
                                status: ProgressStatus::Failed,
                                message: Some(err_msg),
                            };
                            redis.publish_progress(&job_id, &event).await.ok();
                            continue;
                        }
                    };

                    let handler = file_handler.clone();

                    tasks.spawn(async move {
                        let result =
                            execute(&ctx, handler, &job_id, permit, timeout_duration).await;

                        match result {
                            Ok(Ok(JobOutcome::SpawnedChunks(chunks))) => {
                                if let Err(e) = enqueue_chunks(
                                    &ctx.redis,
                                    &ctx.db,
                                    &job_id,
                                    chunks,
                                    ctx.config.shard_count,
                                )
                                .await
                                {
                                    tracing::error!(job_id = %job_id, error = %e, "Failed to enqueue chunks, failing parent job");
                                    let _ = fail_job(
                                        &ctx.redis,
                                        &ctx.db,
                                        &job_id,
                                        format!("Chunk enqueue failed: {e}"),
                                    )
                                    .await;
                                }
                            }
                            Ok(Ok(JobOutcome::Completed(metadata))) => {
                                let completion =
                                    complete_job(&ctx.redis, &ctx.db, &job_id, metadata).await;
                                if let Err(e) = &completion {
                                    tracing::error!(job_id = %job_id, error = %e, "Post-execution completion failed, marking job as failed");
                                    let _ = fail_job(
                                        &ctx.redis,
                                        &ctx.db,
                                        &job_id,
                                        format!("Post-execution completion failed: {e}"),
                                    )
                                    .await;
                                }
                                publish_done_event(
                                    &ctx.redis,
                                    &job_id,
                                    if completion.is_ok() {
                                        None
                                    } else {
                                        Some("Failed after completion")
                                    },
                                )
                                .await;
                            }
                            Ok(Ok(JobOutcome::Duplicated)) => {
                                let completion =
                                    complete_job_no_metadata(&ctx.redis, &ctx.db, &job_id).await;
                                if let Err(e) = &completion {
                                    tracing::error!(job_id = %job_id, error = %e, "Post-execution completion failed for duplicate, marking job as failed");
                                    let _ = fail_job(
                                        &ctx.redis,
                                        &ctx.db,
                                        &job_id,
                                        format!(
                                            "Post-execution completion failed for duplicate: {e}"
                                        ),
                                    )
                                    .await;
                                }
                                publish_done_event(
                                    &ctx.redis,
                                    &job_id,
                                    if completion.is_ok() {
                                        Some("Duplicate, skipped")
                                    } else {
                                        Some("Failed after completion")
                                    },
                                )
                                .await;
                            }
                            Ok(Ok(JobOutcome::ChunkCompleted(_, _))) => {
                                tracing::warn!(job_id = %job_id, "Unexpected ChunkCompleted from file job");
                            }
                            Ok(Err(JobErrorOutcome::Retryable(e))) => {
                                retry_job(&ctx.redis, &ctx.db, &job_id, e, ctx.config.shard_count)
                                    .await;
                            }
                            Ok(Err(JobErrorOutcome::Fatal(e))) => {
                                let _ = fail_job(&ctx.redis, &ctx.db, &job_id, e).await;
                            }
                            Err(_elapsed) => {
                                let _ = fail_job(
                                    &ctx.redis,
                                    &ctx.db,
                                    &job_id,
                                    format!("Job timed out after {}s", ctx.config.job_timeout_secs),
                                )
                                .await;
                            }
                        }
                    });
                }
                JobKind::Chunk => {
                    let permit = chunk_semaphore.clone().acquire_owned().await?;
                    let ctx = match ctx_factory.build_chunk_context(&job_id).await {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            tracing::error!(job_id = %job_id, error = %e, "Failed to build chunk job context");
                            redis
                                .fail_job(&job_id, &format!("Context build failed: {e}"))
                                .await
                                .ok();
                            continue;
                        }
                    };

                    let handler = chunk_handler.clone();

                    tasks.spawn(async move {
                        let result =
                            execute(&ctx, handler, &job_id, permit, timeout_duration).await;

                        match result {
                            Ok(Ok(JobOutcome::ChunkCompleted(chunk, mime))) => {
                                if let Err(e) = complete_chunk(
                                    &ctx.redis,
                                    &ctx.db,
                                    &job_id,
                                    ctx.chunk_job(),
                                    chunk,
                                    mime,
                                )
                                .await
                                {
                                    tracing::error!(job_id = %job_id, error = %e, "Failed to complete chunk job — chunk data stored but TTL will handle retry");
                                } else {
                                    tracing::info!(job_id = %job_id, "Chunk job completed");
                                }
                            }
                            Ok(Ok(JobOutcome::SpawnedChunks(_))) => {
                                tracing::warn!(job_id = %job_id, "Unexpected SpawnedChunks from chunk job");
                            }
                            Ok(Ok(JobOutcome::Completed(_))) => {
                                tracing::warn!(job_id = %job_id, "Unexpected Completed(Metadata) from chunk job");
                            }
                            Ok(Ok(JobOutcome::Duplicated)) => {
                                tracing::warn!(job_id = %job_id, "Unexpected Duplicated from chunk job");
                            }
                            Ok(Err(JobErrorOutcome::Retryable(e))) => {
                                tracing::error!(
                                    job_id = %job_id,
                                    error = %e,
                                    "Retrying chunk job"
                                );
                                if let Err(err) = ctx
                                    .redis
                                    .retry_job(
                                        &job_id,
                                        JobKind::Chunk,
                                        ctx.config.shard_count,
                                        Some(&ctx.chunk_job().parent_job_id),
                                    )
                                    .await
                                {
                                    // retry_job returns Err only when max retries exceeded
                                    // and fail_job is called internally
                                    tracing::error!(
                                        job_id = %job_id,
                                        error = %err,
                                        "Failed to reenqueue retryable chunk job"
                                    );
                                    let parent_id = ctx.chunk_job().parent_job_id.clone();
                                    let live = ctx
                                        .redis
                                        .live_chunk_count(&parent_id)
                                        .await
                                        .unwrap_or(u32::MAX);
                                    if live == 0 {
                                        let err_msg = format!(
                                            "All chunks failed permanently (last chunk: {})",
                                            ctx.chunk_job().chunk_index
                                        );
                                        fail_job(&ctx.redis, &ctx.db, &parent_id, err_msg)
                                            .await
                                            .ok();
                                        ctx.redis
                                            .cleanup_chunk_results(&parent_id)
                                            .await
                                            .ok();
                                    }
                                }
                                let parent_id = ctx.chunk_job().parent_job_id.clone();
                                let event = ProgressEvent {
                                    job_id: parent_id.clone(),
                                    job_type: ProgressJobType::FileJob,
                                    stage: "retrying".to_string(),
                                    current: 0,
                                    total: None,
                                    status: ProgressStatus::Retrying,
                                    message: Some(format!(
                                        "Chunk {} retrying: {e}",
                                        ctx.chunk_job().chunk_index
                                    )),
                                };
                                ctx.redis.publish_progress(&parent_id, &event).await.ok();
                            }
                            Ok(Err(JobErrorOutcome::Fatal(e))) => {
                                tracing::error!(job_id = %job_id, error = %e, "Fatal chunk job error");
                                fail_job(&ctx.redis, &ctx.db, &job_id, e).await.ok();
                                let parent_id = ctx.chunk_job().parent_job_id.clone();
                                // Check if parent should be failed too
                                let live = ctx
                                    .redis
                                    .live_chunk_count(&parent_id)
                                    .await
                                    .unwrap_or(u32::MAX);
                                if live == 0 {
                                    let err_msg = format!(
                                        "All chunks failed permanently (last chunk: {})",
                                        ctx.chunk_job().chunk_index
                                    );
                                    fail_job(&ctx.redis, &ctx.db, &parent_id, err_msg)
                                        .await
                                        .ok();
                                    ctx.redis
                                        .cleanup_chunk_results(&parent_id)
                                        .await
                                        .ok();
                                }
                                let event = ProgressEvent {
                                    job_id: parent_id.clone(),
                                    job_type: ProgressJobType::FileJob,
                                    stage: "failed".to_string(),
                                    current: 0,
                                    total: None,
                                    status: ProgressStatus::Failed,
                                    message: Some(format!(
                                        "Chunk {} failed: fatal error",
                                        ctx.chunk_job().chunk_index
                                    )),
                                };
                                ctx.redis.publish_progress(&parent_id, &event).await.ok();
                            }
                            Err(_elapsed) => {
                                tracing::error!(job_id = %job_id, "Chunk job timed out");
                                fail_job(
                                    &ctx.redis,
                                    &ctx.db,
                                    &job_id,
                                    "Chunk job timed out".to_string(),
                                )
                                .await
                                .ok();
                                let parent_id = ctx.chunk_job().parent_job_id.clone();
                                // Check if parent should be failed too
                                let live = ctx
                                    .redis
                                    .live_chunk_count(&parent_id)
                                    .await
                                    .unwrap_or(u32::MAX);
                                if live == 0 {
                                    let err_msg = format!(
                                        "All chunks timed out (last chunk: {})",
                                        ctx.chunk_job().chunk_index
                                    );
                                    fail_job(&ctx.redis, &ctx.db, &parent_id, err_msg)
                                        .await
                                        .ok();
                                    ctx.redis
                                        .cleanup_chunk_results(&parent_id)
                                        .await
                                        .ok();
                                }
                                let event = ProgressEvent {
                                    job_id: parent_id.clone(),
                                    job_type: ProgressJobType::FileJob,
                                    stage: "failed".to_string(),
                                    current: 0,
                                    total: None,
                                    status: ProgressStatus::Failed,
                                    message: Some(format!(
                                        "Chunk {} timed out",
                                        ctx.chunk_job().chunk_index
                                    )),
                                };
                                ctx.redis.publish_progress(&parent_id, &event).await.ok();
                            }
                        }
                    });
                }
            }
        }
    }

    // Drain phase: wait up to shutdown_grace for spawned tasks to finish
    let drain_start = std::time::Instant::now();
    while !tasks.is_empty() {
        if drain_start.elapsed() >= shutdown_grace {
            tracing::warn!(
                remaining = tasks.len(),
                "Grace period exceeded, abandoning {} tasks",
                tasks.len()
            );
            tasks.abort_all();
            break;
        }
        let remaining = shutdown_grace - drain_start.elapsed();
        match timeout(remaining, tasks.join_next()).await {
            Ok(Some(Ok(()))) => {} // task completed normally
            Ok(Some(Err(e))) => tracing::warn!(error = ?e, "Spawned task panicked"),
            Ok(None) => break, // join_next returned None (empty set)
            Err(_) => {
                tracing::warn!("Drain timeout, aborting remaining tasks");
                tasks.abort_all();
                break;
            }
        }
    }

    Ok(())
}

async fn execute(
    ctx: &JobContext,
    handler: Arc<dyn JobHandler>,
    job_id: &str,
    _permit: OwnedSemaphorePermit,
    timeout_duration: Duration,
) -> Result<Result<JobOutcome, JobErrorOutcome>, Elapsed> {
    let hb_done = Arc::new(AtomicBool::new(false));
    ctx.heartbeat
        .register(job_id.to_string(), ctx.redis.clone(), hb_done.clone());

    let result = timeout(timeout_duration, handler.execute(&ctx)).await;
    hb_done.store(true, Ordering::Relaxed);
    result
}

async fn enqueue_chunks(
    redis: &RedisService,
    db: &MongoService,
    parent_id: &str,
    chunks: Vec<ChunkJob>,
    shard_count: u32,
) -> Result<(), JobErrorOutcome> {
    redis.create_counter(parent_id).await?;
    let chunks_len = chunks.len();
    for chunk in chunks {
        redis.enqueue_chunk_job(&chunk, shard_count).await?;
        db.save_chunk_job(chunk).await?;
    }
    tracing::info!(count = chunks_len, "Chunks enqueued");
    Ok(())
}

async fn complete_job(
    redis: &RedisService,
    db: &MongoService,
    job_id: &str,
    metadata: Metadata,
) -> Result<(), JobErrorOutcome> {
    tracing::info!(
        "New file metadata inserted with hash: {}",
        metadata.file_hash
    );
    redis.complete_job(job_id).await?;
    db.complete_job(metadata, job_id).await?;
    tracing::info!(job_id = %job_id, "File job completed");
    Ok(())
}

async fn retry_job(
    redis: &RedisService,
    _db: &MongoService,
    job_id: &str,
    error: String,
    shard_count: u32,
) {
    tracing::error!(job_id = %job_id, error = %error, "Retrying job");
    if let Err(err) = redis
        .retry_job(job_id, JobKind::File, shard_count, None)
        .await
    {
        tracing::error!(job_id = %job_id, error = %err, "Failed to reenqueue retryable job");
    } else {
        let event = ProgressEvent {
            job_id: job_id.to_string(),
            job_type: ProgressJobType::FileJob,
            stage: "retrying".to_string(),
            current: 0,
            total: None,
            status: ProgressStatus::Retrying,
            message: Some(error),
        };
        let _ = redis.publish_progress(job_id, &event).await;
    }
}

async fn fail_job(
    redis: &RedisService,
    db: &MongoService,
    job_id: &str,
    error: String,
) -> Result<(), JobErrorOutcome> {
    tracing::error!(?error);
    redis.fail_job(job_id, error.as_str()).await?;
    db.fail_job(job_id, error.as_str()).await?;
    // Publish Failed sentinel so any CLI subscriber gets the terminal event
    let event = ProgressEvent {
        job_id: job_id.to_string(),
        job_type: ProgressJobType::FileJob,
        stage: "failed".to_string(),
        current: 0,
        total: None,
        status: ProgressStatus::Failed,
        message: Some(error.clone()),
    };
    let _ = redis.publish_progress(job_id, &event).await;
    Ok(())
}

/// Mark a chunk job as completed in Redis.
/// Parent-job finalization is handled inside ChunkJobHandler::execute.
async fn complete_chunk(
    redis: &RedisService,
    db: &MongoService,
    job_id: &str,
    chunk_job: &ChunkJob,
    chunk: ChunkRef,
    mime: String,
) -> Result<(), JobErrorOutcome> {
    let total_chunks = chunk_job.total_chunks;
    let chunk_index = chunk_job.chunk_index;
    let parent_id = chunk_job.parent_job_id.as_str();

    let count = redis
        .complete_chunk(job_id, chunk, chunk_index, parent_id)
        .await?;

    let _: () = redis.complete_job(job_id).await?;
    let _: () = db.complete_chunk_job(job_id).await?;

    // Publish chunk progress for the parent file job
    let event = ProgressEvent {
        job_id: parent_id.to_string(),
        job_type: ProgressJobType::FileJob,
        stage: "chunks".to_string(),
        current: count,
        total: Some(total_chunks),
        status: if count == total_chunks {
            ProgressStatus::Running
        } else {
            ProgressStatus::Running
        },
        message: Some(format!("{}/{} chunks", count, total_chunks)),
    };
    let _ = redis.publish_progress(parent_id, &event).await;

    if count == total_chunks {
        finalize_chunked_file(redis, db, chunk_job, mime).await?;
        tracing::info!(job_id = %job_id, "All chunks completed for parent job");
    }

    Ok(())
}

/// Publish a Done sentinel progress event after `complete_job` has written state.
async fn publish_done_event(redis: &RedisService, job_id: &str, message: Option<&str>) {
    let event = ProgressEvent {
        job_id: job_id.to_string(),
        job_type: ProgressJobType::FileJob,
        stage: "done".to_string(),
        current: 1,
        total: Some(1),
        status: ProgressStatus::Done,
        message: message.map(|s| s.to_string()),
    };
    let _ = redis.publish_progress(job_id, &event).await;
}

/// Mark a file job as completed without inserting metadata (duplicate case).
async fn complete_job_no_metadata(
    redis: &RedisService,
    db: &MongoService,
    job_id: &str,
) -> Result<(), JobErrorOutcome> {
    redis.complete_job(job_id).await?;
    db.mark_job_completed(job_id).await?;
    tracing::info!(job_id = %job_id, "Duplicate file job completed");
    Ok(())
}

/// Called by the last `ChunkJob` when all chunks for a file are done.
/// Builds the `Metadata` with a `Manifest`, saves it, and marks the parent
/// `FileJob` as completed.
async fn finalize_chunked_file(
    redis: &RedisService,
    db: &MongoService,
    chunk_job: &ChunkJob,
    mime: String,
) -> Result<(), JobErrorOutcome> {
    let results = redis
        .get_all_chunk_results(&chunk_job.parent_job_id)
        .await?;

    let mut sorted: Vec<_> = results.into_iter().collect();
    sorted.sort_by_key(|(idx, _)| *idx);

    let chunk_refs: Vec<ChunkRef> = sorted.into_iter().map(|(_, cr)| cr).collect();

    if chunk_refs.is_empty() {
        return Err(JobErrorOutcome::Fatal(
            "No chunk results found for finalization".into(),
        ));
    }

    // Merkle root: SHA-256 of concatenated chunk hashes
    let mut full_hasher = Sha256::new();
    let mut total_compressed = 0u64;
    for cr in &chunk_refs {
        full_hasher.update(cr.hash.as_bytes());
        total_compressed += cr.size_compressed.unwrap_or(cr.size_original);
    }
    let file_hash = hex::encode(full_hasher.finalize());

    let parent_job = db
        .get_file_job(&chunk_job.parent_job_id)
        .await
        .map_err(|e| JobErrorOutcome::Retryable(e.to_string()))?
        .ok_or_else(|| JobErrorOutcome::Fatal("Parent file job not found".to_string()))?;

    let resource = &parent_job.resource;
    let provider = resource
        .dest
        .as_ref()
        .and_then(|d| d.provider.clone())
        .unwrap_or(Provider::Local);

    let storage_path = std::path::Path::new(&chunk_refs[0].storage_path)
        .parent()
        .map_or(chunk_job.dest_path.clone(), |p| {
            p.to_string_lossy().to_string()
        });

    let compression_name = chunk_job.compression_strategy.as_ref().map(|s| {
        match s {
            GenericCompressionStrategy::Gzip => "gzip",
            GenericCompressionStrategy::Zstd => "zstd",
            GenericCompressionStrategy::Zip => "zip",
            GenericCompressionStrategy::SevenZ => "7z",
            GenericCompressionStrategy::OriginalFormat | GenericCompressionStrategy::None => "",
        }
        .to_string()
    });

    let manifest = Manifest {
        chunks: chunk_refs,
        compression: compression_name,
        original_size: chunk_job.total_file_size,
        compressed_size: total_compressed,
    };

    let mut metadata = Metadata::new(
        file_hash.clone(),
        resource.url.clone(),
        provider,
        storage_path,
        chunk_job.total_file_size,
        Some(total_compressed),
        mime,
    );
    metadata.chunk_manifest = Some(manifest);

    match db.complete_job(metadata, &chunk_job.parent_job_id).await {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, "Chunk finalization insert conflict (race), parent already completed");
        }
    }

    redis.complete_job(&chunk_job.parent_job_id).await?;
    redis
        .cleanup_chunk_results(&chunk_job.parent_job_id)
        .await?;

    // Publish final done event for the parent job
    let done_event = ProgressEvent {
        job_id: chunk_job.parent_job_id.clone(),
        job_type: ProgressJobType::FileJob,
        stage: "done".to_string(),
        current: chunk_job.total_chunks,
        total: Some(chunk_job.total_chunks),
        status: ProgressStatus::Done,
        message: Some(format!(
            "All {} chunks completed and finalized",
            chunk_job.total_chunks
        )),
    };
    let _ = redis
        .publish_progress(&chunk_job.parent_job_id, &done_event)
        .await;

    tracing::info!(
        file_hash = %file_hash,
        parent_job_id = %chunk_job.parent_job_id,
        total_chunks = %chunk_job.total_chunks,
        "Chunked file finalized"
    );

    Ok(())
}
