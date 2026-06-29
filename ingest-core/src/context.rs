use std::sync::Arc;

use crate::{
    auth::AuthProviderRegistry,
    error::ToolError,
    handlers::jobs::{resolve_source_auth, JobContext},
    models::AppConfig,
    services::{mongo::MongoService, redis::RedisService},
    storage::ProviderCache,
};
use wreq::{
    Client,
    header::{ACCEPT, HeaderMap, HeaderValue},
};

pub struct ContextFactory {
    mongo: Arc<MongoService>,
    redis: Arc<RedisService>,
    config: Arc<AppConfig>,
    http_client: Arc<Client>,
    auth_registry: Option<Arc<AuthProviderRegistry>>,
    provider_cache: Arc<ProviderCache>,
}

impl ContextFactory {
    pub fn new(
        mongo: MongoService,
        redis: RedisService,
        config: AppConfig,
        auth_registry: Option<Arc<AuthProviderRegistry>>,
    ) -> Result<Self, ToolError> {
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
                .map_err(|e| ToolError::Message(format!("Failed to build HTTP client: {e}")))?,
        );

        Ok(Self {
            mongo: Arc::new(mongo),
            redis: Arc::new(redis),
            config: Arc::new(config),
            http_client,
            auth_registry,
            provider_cache: Arc::new(ProviderCache::new()),
        })
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

    pub fn provider_cache(&self) -> Arc<ProviderCache> {
        self.provider_cache.clone()
    }

    pub async fn build_file_context(&self, job_id: &str) -> Result<JobContext, ToolError> {
        if let Some(file_job) = self.mongo.get_file_job(job_id).await? {
            tracing::info!(job_id = %job_id, "Building file job context from Mongo");
            let auth_token = resolve_source_auth(&file_job.resource, self.auth_registry.as_deref())
                .await?;
            Ok(JobContext::from_file_job(
                file_job,
                self.mongo.clone(),
                self.redis.clone(),
                self.config.clone(),
                self.http_client.clone(),
                auth_token,
                &self.provider_cache,
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
                &self.provider_cache,
            ))
        } else {
            Err(format!("Chunk job {job_id} not found in Mongo").into())
        }
    }
}
