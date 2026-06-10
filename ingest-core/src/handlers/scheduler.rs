use crate::{
    context::ContextFactory,
    error::JobErrorOutcome,
    handlers::jobs::{
        ChunkJob, ChunkJobHandler, FileJobHandler, JobContext, JobHandler, JobKind, JobOutcome,
    },
    models::{ChunkRef, GenericCompressionStrategy, Manifest, Metadata},
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
    time::{error::Elapsed, timeout},
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
    let config = ctx_factory.config();
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
                    let ctx = match ctx_factory.build_file_context(&job_id).await {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            tracing::error!(job_id = %job_id, error = %e, "Failed to build job context");
                            continue;
                        }
                    };

                    let handler = file_handler.clone();
                    let shutdown_flag = shutdown.clone();

                    let job_id_copy = job_id.clone();
                    tokio::spawn(async move {
                        let result = execute(
                            handler,
                            &ctx,
                            job_id,
                            permit,
                            shutdown_flag,
                            timeout_duration,
                        )
                        .await;

                        match result {
                            Ok(Ok(JobOutcome::SpawnedChunks(chunks))) => {
                                if let Err(e) =
                                    enqueue_chunks(&ctx.redis, &ctx.db, &job_id_copy, chunks).await
                                {
                                    tracing::error!(job_id = %job_id_copy, error = %e, "Failed to enqueue chunks, failing parent job");
                                    let _ = fail_job(
                                        &ctx.redis,
                                        &ctx.db,
                                        &job_id_copy,
                                        format!("Chunk enqueue failed: {e}"),
                                    )
                                    .await;
                                }
                            }
                            Ok(Ok(JobOutcome::Completed(metadata))) => {
                                if let Err(e) =
                                    complete_job(&ctx.redis, &ctx.db, &job_id_copy, metadata).await
                                {
                                    tracing::error!(job_id = %job_id_copy, error = %e, "Post-execution completion failed, marking job as failed");
                                    let _ = fail_job(
                                        &ctx.redis,
                                        &ctx.db,
                                        &job_id_copy,
                                        format!("Post-execution completion failed: {e}"),
                                    )
                                    .await;
                                }
                            }
                            Ok(Ok(JobOutcome::Duplicated)) => {
                                if let Err(e) =
                                    complete_job_no_metadata(&ctx.redis, &ctx.db, &job_id_copy)
                                        .await
                                {
                                    tracing::error!(job_id = %job_id_copy, error = %e, "Post-execution completion failed for duplicate, marking job as failed");
                                    let _ = fail_job(
                                        &ctx.redis,
                                        &ctx.db,
                                        &job_id_copy,
                                        format!(
                                            "Post-execution completion failed for duplicate: {e}"
                                        ),
                                    )
                                    .await;
                                }
                            }
                            Ok(Ok(JobOutcome::ChunkCompleted(_))) => {
                                tracing::warn!(job_id = %job_id_copy, "Unexpected ChunkCompleted from file job");
                            }
                            Ok(Err(JobErrorOutcome::Retryable(e))) => {
                                retry_job(&ctx.redis, &ctx.db, &job_id_copy, e).await;
                            }
                            Ok(Err(JobErrorOutcome::Fatal(e))) => {
                                let _ = fail_job(&ctx.redis, &ctx.db, &job_id_copy, e).await;
                            }
                            Err(_elapsed) => {
                                let _ = fail_job(
                                    &ctx.redis,
                                    &ctx.db,
                                    &job_id_copy,
                                    format!("Job timed out after {}s", timeout_secs),
                                )
                                .await;
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
                    let shutdown_flag = shutdown.clone();

                    let job_id_copy = job_id.clone();
                    tokio::spawn(async move {
                        let result = execute(
                            handler,
                            &ctx,
                            job_id,
                            permit,
                            shutdown_flag,
                            timeout_duration,
                        )
                        .await;

                        match result {
                            Ok(Ok(JobOutcome::ChunkCompleted(chunk))) => {
                                if let Err(e) =
                                    complete_chunk(&ctx.redis, &ctx.db, &job_id_copy, chunk, &ctx)
                                        .await
                                {
                                    tracing::error!(job_id = %job_id_copy, error = %e, "Failed to complete chunk job — chunk data stored but TTL will handle retry");
                                } else {
                                    tracing::info!(job_id = %job_id_copy, "Chunk job completed");
                                }
                            }
                            Ok(Ok(JobOutcome::SpawnedChunks(_))) => {
                                tracing::warn!(job_id = %job_id_copy, "Unexpected SpawnedChunks from chunk job");
                            }
                            Ok(Ok(JobOutcome::Completed(_))) => {
                                tracing::warn!(job_id = %job_id_copy, "Unexpected Completed(Metadata) from chunk job");
                            }
                            Ok(Ok(JobOutcome::Duplicated)) => {
                                tracing::warn!(job_id = %job_id_copy, "Unexpected Duplicated from chunk job");
                            }
                            Ok(Err(JobErrorOutcome::Retryable(e))) => {
                                tracing::error!(
                                    ?job_id_copy,
                                    "retrying chunk job due to error: ({e})"
                                );
                                if let Err(err) =
                                    ctx.redis.retry_job(&job_id_copy, JobKind::Chunk).await
                                {
                                    tracing::error!(
                                        ?err,
                                        "failed to reenqueue retryable chunk job"
                                    );
                                }
                            }
                            Ok(Err(JobErrorOutcome::Fatal(e))) => {
                                tracing::error!(?e, "fatal chunk job error");
                            }
                            Err(_elapsed) => {
                                tracing::error!(job_id = %job_id_copy, "Chunk job timed out");
                            }
                        }
                    });
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn execute(
    handler: Arc<dyn JobHandler>,
    ctx: &JobContext,
    job_id: String,
    _permit: OwnedSemaphorePermit,
    hb_shutdown: Arc<AtomicBool>,
    timeout_duration: Duration,
) -> Result<Result<JobOutcome, JobErrorOutcome>, Elapsed> {
    let hb_redis = ctx.redis.clone();
    let hb_done = Arc::new(AtomicBool::new(false));
    let hb_done_clone = hb_done.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            if hb_done_clone.load(Ordering::Relaxed) || hb_shutdown.load(Ordering::Relaxed) {
                break;
            }
            let _ = hb_redis.renew_lease(&job_id).await;
        }
    });

    let result = timeout(timeout_duration, handler.execute(&ctx)).await;
    hb_done.store(true, Ordering::Relaxed);
    result
}

async fn enqueue_chunks(
    redis: &RedisService,
    db: &MongoService,
    parent_id: &str,
    chunks: Vec<ChunkJob>,
) -> Result<(), JobErrorOutcome> {
    redis.create_counter(parent_id).await?;
    let chunks_len = chunks.len();
    for chunk in chunks {
        redis.enqueue_chunk_job(&chunk).await?;
        db.save_chunk_job(chunk).await?;
    }
    tracing::info!("Chunks enqueued {}", chunks_len);
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

async fn retry_job(redis: &RedisService, _db: &MongoService, job_id: &str, error: String) {
    tracing::error!(?job_id, "retrying job due to error: ({error})");
    if let Err(err) = redis.retry_job(job_id, JobKind::File).await {
        tracing::error!(?err, "failed to reenqueue retryable job");
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
    Ok(())
}

/// Mark a chunk job as completed in Redis.
/// Parent-job finalization is handled inside ChunkJobHandler::execute.
async fn complete_chunk(
    redis: &RedisService,
    _db: &MongoService,
    job_id: &str,
    chunk: ChunkRef,
    ctx: &JobContext,
) -> Result<(), JobErrorOutcome> {
    let chunk_job = ctx.chunk_job();
    let total_chunks = chunk_job.total_chunks;
    let chunk_index = chunk_job.chunk_index;
    let parent_id = chunk_job.parent_job_id.as_str();

    let count = redis
        .complete_chunk(job_id, chunk, chunk_index, parent_id)
        .await?;

    let _: () = redis.complete_job(job_id).await?;

    if count == total_chunks {
        finalize_chunked_file(ctx, chunk_job).await?;
        tracing::info!(job_id = %job_id, "All chunks completed for parent job");
    }

    Ok(())
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
    ctx: &JobContext,
    chunk_job: &ChunkJob,
) -> Result<(), JobErrorOutcome> {
    let results = ctx
        .redis
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

    let parent_job = ctx
        .db
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
        "application/octet-stream".to_string(),
    );
    metadata.chunk_manifest = Some(manifest);

    match ctx
        .db
        .complete_job(metadata, &chunk_job.parent_job_id)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(error = %e, "Chunk finalization insert conflict (race), parent already completed");
        }
    }

    ctx.redis.complete_job(&chunk_job.parent_job_id).await?;
    ctx.redis
        .cleanup_chunk_results(&chunk_job.parent_job_id)
        .await?;

    tracing::info!(
        file_hash = %file_hash,
        parent_job_id = %chunk_job.parent_job_id,
        total_chunks = %chunk_job.total_chunks,
        "Chunked file finalized"
    );

    Ok(())
}
