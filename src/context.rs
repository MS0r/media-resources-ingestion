use std::{fmt::Error, sync::Arc};
use crate::{cli::Config, handlers::jobs::JobContext, models::Resource, services::{mongo::MongoService, redis::RedisService}, settings::TomlConfig};

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

    // Called once per job execution by the scheduler
    pub async fn build(&self, job_id : String) -> Result<JobContext, Box<dyn std::error::Error>> {
        if let Some(res) = self.mongo.get_resource_by_id(&job_id).await? {
            tracing::info!(url = %res.url, "Resource found for job");
            Ok(JobContext {
                job_id: job_id,
                db: self.mongo.clone(),
                redis: self.redis.clone(),
                resource: Arc::new(resource),
                config: self.config.clone(),
            })
        } else {
            tracing::error!(job_id = %job_id, "No resource found for job");
            Err(())
        }

    }
}