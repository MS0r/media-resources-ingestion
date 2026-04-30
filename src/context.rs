use tokio::fs::File;

use crate::{
    cli::Config,
    error::BoxedError,
    handlers::jobs::{ChunkJob, FileJob, JobContext, JobKind},
    models::Resource,
    services::{mongo::MongoService, redis::RedisService},
    settings::TomlConfig,
};
use std::{fmt::Error, sync::Arc};

pub struct ContextFactory {
    mongo: Arc<MongoService>,
    redis: Arc<RedisService>,
    config: Arc<TomlConfig>,
}

impl ContextFactory {
    pub fn new(mongo: MongoService, redis: RedisService, config: TomlConfig) -> Self {
        Self {
            mongo: Arc::new(mongo),
            redis: Arc::new(redis),
            config: Arc::new(config),
        }
    }

    pub fn redis_service(&self) -> Arc<RedisService> {
        self.redis.clone()
    }

    pub fn mongo_service(&self) -> Arc<MongoService> {
        self.mongo.clone()
    }

    pub async fn build_file_context(&self, job_id: &str) -> Result<JobContext, BoxedError> {
        if let Some(file_job) = self.mongo.get_file_job(job_id).await? {
            tracing::info!(job_id = %job_id, url = %file_job.resource.url, "Building file job context from Mongo");
            return Ok(JobContext::from_file_job(
                file_job,
                self.mongo.clone(),
                self.redis.clone(),
                self.config.clone(),
            ));
        } else {
            Err(format!("File job {job_id} not found in Mongo").into())
        }
    }
}
