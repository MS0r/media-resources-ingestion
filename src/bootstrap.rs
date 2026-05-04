use crate::{
    cli::{CancelScope, FilesScope, RetryScope, StatusScope},
    context::ContextFactory,
    error::BoxedError,
    handlers::{
        jobs::{Batch, ChunkJobHandler, FileJob, FileJobHandler, JobStatus},
        scheduler::scheduler_loop,
    },
    models::MainConfig,
    services::{mongo::MongoService, redis::RedisService}, 
    settings::merge_configs_yaml,
};
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

pub async fn run(config: MainConfig) -> Result<(), BoxedError> {
    tracing::info!(
        resources = config.yaml_config.resources.len(),
        "Config loaded"
    );

    // -- Services ----------------------------------------------------------
    let redis_service = RedisService::new(&config.redis_uri)?;
    tracing::info!(url = %config.redis_uri, "Redis connected");

    let mongo_service = MongoService::new(&config.mongo_uri).await?;
    tracing::info!(url = %config.mongo_uri, "MongoDB connected");

    // -- Initial batch ------------------------------------------------------
    let mut batch = Batch {
        _id: Uuid::new_v4().to_string(),
        created_at: Utc::now(),
        yaml_path: config.yaml_path.clone(),
        status: JobStatus::Pending,
        job_ids: vec![],
    };

    for resource in &config.yaml_config.resources {
        let file_job = FileJob {
            _id: resource.id.clone(),
            batch_id: batch._id.clone(),
            resource: resource.clone(),
            priority: resource
                .priority
                .or(config.yaml_config.priority)
                .unwrap_or(0),
            status: JobStatus::Pending,
            retry_count: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            file_hash: None,
            error: None,
        };
        batch.job_ids.push(file_job._id.clone());
        redis_service.enqueue_file_job(&file_job).await?;
        mongo_service.save_file_job(file_job).await?;
    }

    tracing::info!(batch_id = %batch._id, "Batch created");
    redis_service.enqueue_batch(&batch).await?;
    mongo_service.save_batch(batch).await?;

    // -- Scheduler ---------------------------------------------------------
    let file_handler = Arc::new(FileJobHandler);
    let chunk_handler = Arc::new(ChunkJobHandler);

    // -- Context factory ---------------------------------------------------
    let max_file_workers = config.toml_config.scheduler.file_workers;
    let max_chunk_workers = config.toml_config.scheduler.chunk_workers;

    let ctx_factory = Arc::new(ContextFactory::new(
        mongo_service,
        redis_service,
        config.toml_config,
    ));

    scheduler_loop(
        file_handler,
        chunk_handler,
        ctx_factory,
        max_file_workers,
        max_chunk_workers,
    )
    .await;

    Ok(())
}

pub async fn status(scope: StatusScope) -> Result<(), BoxedError> {
    match scope {
        StatusScope::Batch { batch_id } => {
            tracing::info!("Checking status of batch {}", batch_id)
        }
        StatusScope::Job { job_id } => tracing::info!("Checking status of file {}", job_id),
        StatusScope::Jobs(args) => tracing::info!("Checking status of all jobs"),
    }
    Ok(())
}

pub async fn cancel(scope: CancelScope) -> Result<(), BoxedError> {
    match scope {
        CancelScope::Batch { batch_id } => tracing::info!("Cancelling batch {}", batch_id),
        CancelScope::Job { job_id } => tracing::info!("Cancelling file {}", job_id),
    }
    Ok(())
}

pub async fn retry(scope: RetryScope) -> Result<(), BoxedError> {
    match scope {
        RetryScope::Job { job_id } => tracing::info!("Retrying file {}", job_id),
    }
    Ok(())
}

pub async fn files(scope: FilesScope) -> Result<(), BoxedError> {
    match scope {
        FilesScope::Delete { hash, yes } => tracing::info!("Listing all stored files"),
        FilesScope::Download { hash, dest } => {
            if let Some(des) = dest {
                tracing::info!("Downloading file with hash {} to {}", hash, des.display())
            } else {
                tracing::info!("Downloading file with hash {} to stdout", hash)
            }
        }
        FilesScope::Get { hash } => tracing::info!("Listing all stored files"),
        FilesScope::List(args) => tracing::info!("Listing all stored files"),
    }
    Ok(())
}
