use std::sync::Arc;
use uuid::Uuid;
use chrono::Utc;
use crate::{
    cli::Config,
    context::ContextFactory,
    handlers::{
        jobs::{Batch, ChunkJobHandler, FileJob, FileJobHandler, JobStatus},
        scheduler::scheduler_loop,
    },
    // services::mongo::MongoService,
    services::{mongo::MongoService, redis::RedisService},
};

pub async fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!(
        resources = config.yaml_config.resources.len(),
        "Config loaded"
    );

    // -- Services ----------------------------------------------------------
    let redis_service = RedisService::new(&config.redis_uri)?;
    tracing::info!(url = %config.redis_uri, "Redis connected");

    let mongo_service = MongoService::new(&config.mongo_uri).await?;
    tracing::info!(url = %config.mongo_uri, "MongoDB connected");

    // MongoService::new(...) goes here when ready

    // -- Initial batch ------------------------------------------------------
    let mut batch = Batch {
        id: Uuid::new_v4().to_string(),
        created_at: Utc::now(),
        yaml_path: config.yaml_path.clone(),
        status: JobStatus::Pending,
        job_ids: vec![],
    };

    for resource in &config.yaml_config.resources {
        tracing::info!(url = %resource.url, "Scheduling file job for resource");
        let file_job = FileJob {
            id: resource.id.clone(),
            batch_id: batch.id.clone(),
            priority: 0,
            status: JobStatus::Pending,
            retry_count: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            file_hash: None,
            error: None,
        };
        batch.job_ids.push(file_job.id.clone());
        redis_service.enqueue_file_job(&file_job).await?;
    }
    
    redis_service.enqueue_batch(&batch).await?;

    tracing::info!(batch_id = %batch.id, "Batch created");

    // -- Scheduler ---------------------------------------------------------
    let file_handler = Arc::new(FileJobHandler);
    let chunk_handler = Arc::new(ChunkJobHandler);

    // -- Context factory ---------------------------------------------------
    let max_f_workers = config.toml_config.scheduler.file_workers.clone();
    let max_c_workers = config.toml_config.scheduler.chunk_workers.clone();

    let ctx_factory = Arc::new(ContextFactory::new(
        mongo_service,
        redis_service,
        config.toml_config,
    ));

    scheduler_loop(
        file_handler,
        chunk_handler,
        ctx_factory,
        max_f_workers,
        max_c_workers,
    )
    .await;

    Ok(())
}