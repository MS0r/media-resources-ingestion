use crate::{
    error::ToolError,
    handlers::jobs::JobContext,
    models::AppConfig,
    services::{mongo::MongoService, redis::RedisService},
};
use std::sync::Arc;
use wreq::{
    Client,
    header::{ACCEPT, HeaderMap, HeaderValue},
};

pub struct ContextFactory {
    mongo: Arc<MongoService>,
    redis: Arc<RedisService>,
    config: Arc<AppConfig>,
    http_client: Arc<Client>,
}

impl ContextFactory {
    pub fn new(mongo: MongoService, redis: RedisService, config: AppConfig) -> Self {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(
            ACCEPT,
            HeaderValue::from_static(
                "video/webm,video/mp4,application/octet-stream,image/*,*/*;q=0.8",
            ),
        );

        let http_client = Arc::new(
            Client::builder()
                .emulation(wreq_util::Emulation::Chrome124)
                .default_headers(default_headers)
                .build()
                .expect("Failed to build wreq HTTP client"),
        );

        Self {
            mongo: Arc::new(mongo),
            redis: Arc::new(redis),
            config: Arc::new(config),
            http_client,
        }
    }

    pub fn redis_service(&self) -> Arc<RedisService> {
        self.redis.clone()
    }

    pub fn mongo_service(&self) -> Arc<MongoService> {
        self.mongo.clone()
    }

    pub fn config(&self) -> Arc<AppConfig> {
        self.config.clone()
    }

    pub fn http_client(&self) -> Arc<wreq::Client> {
        self.http_client.clone()
    }

    pub async fn build_file_context(&self, job_id: &str) -> Result<JobContext, ToolError> {
        if let Some(file_job) = self.mongo.get_file_job(job_id).await? {
            tracing::info!(job_id = %job_id, "Building file job context from Mongo");
            Ok(JobContext::from_file_job(
                file_job,
                self.mongo.clone(),
                self.redis.clone(),
                self.config.clone(),
                self.http_client.clone(),
            ))
        } else {
            Err(format!("File job {job_id} not found in Mongo").into())
        }
    }

    pub async fn build_chunk_context(&self, job_id: &str) -> Result<JobContext, ToolError> {
        if let Some(chunk_job) = self.mongo.get_chunk_job(job_id).await? {
            tracing::info!(job_id = %job_id, "Building chunk job context from Mongo");
            Ok(JobContext::from_chunk_job(
                chunk_job,
                self.mongo.clone(),
                self.redis.clone(),
                self.config.clone(),
                self.http_client.clone(),
            ))
        } else {
            Err(format!("Chunk job {job_id} not found in Mongo").into())
        }
    }
}
