use std::sync::Arc;
use std::time::Duration;

use crate::{
    auth::AuthProviderRegistry,
    error::ToolError,
    handlers::jobs::{JobContext, resolve_source_auth},
    models::AppConfig,
    services::{heartbeat::HeartbeatSupervisor, mongo::MongoService, redis::RedisService},
    storage::ProviderCache,
};
use wreq::{
    Client,
    header::{ACCEPT, HeaderMap, HeaderValue},
};

pub struct ContextFactory {
    db: Arc<MongoService>,
    redis: Arc<RedisService>,
    config: Arc<AppConfig>,
    http_client: Arc<Client>,
    provider_cache: Arc<ProviderCache>,
    heartbeat: Arc<HeartbeatSupervisor>,
}

impl ContextFactory {
    pub fn new(
        db: MongoService,
        redis: RedisService,
        config: AppConfig,
        auth_registry: AuthProviderRegistry,
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

        let redis = Arc::new(redis);
        let heartbeat = Arc::new(HeartbeatSupervisor::new());

        Ok(Self {
            db: Arc::new(db),
            redis,
            config: Arc::new(config),
            http_client,
            provider_cache: Arc::new(ProviderCache::new(auth_registry)),
            heartbeat,
        })
    }

    pub fn redis_service(&self) -> Arc<RedisService> {
        self.redis.clone()
    }

    pub fn db_service(&self) -> Arc<MongoService> {
        self.db.clone()
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

    pub fn heartbeat(&self) -> Arc<HeartbeatSupervisor> {
        self.heartbeat.clone()
    }

    pub async fn build_file_context(&self, job_id: &str) -> Result<JobContext, ToolError> {
        if let Some(file_job) = self.db.get_file_job(job_id).await? {
            tracing::info!(job_id = %job_id, "Building file job context from Mongo");
            let auth_token = resolve_source_auth(&file_job.resource, &self.provider_cache).await?;
            Ok(JobContext::from_file_job(
                file_job,
                self.db.clone(),
                self.redis.clone(),
                self.config.clone(),
                self.http_client.clone(),
                auth_token,
                &self.provider_cache,
                self.heartbeat.clone(),
            ))
        } else {
            Err(format!("File job {job_id} not found in Mongo").into())
        }
    }

    pub async fn build_chunk_context(&self, job_id: &str) -> Result<JobContext, ToolError> {
        // Retry on None — the chunk may not have propagated to Mongo yet if
        // the producer saved to Mongo and then pushed to Redis (the correct
        // order), but a replica set or pooled connection is lagging. 5 × 50 ms
        // = 250 ms total budget; this is defense-in-depth (Step 1 in
        // enqueue_chunks eliminates the race at the source).
        const RETRIES: u32 = 5;
        const BACKOFF_MS: u64 = 50;

        for attempt in 0..=RETRIES {
            match self.db.get_chunk_job(job_id).await? {
                Some(chunk_job) => {
                    if attempt > 0 {
                        tracing::info!(
                            job_id = %job_id, attempt,
                            "Chunk context built after retry"
                        );
                    }
                    tracing::info!(job_id = %job_id, "Building chunk job context from Mongo");
                    return Ok(JobContext::from_chunk_job(
                        chunk_job,
                        self.db.clone(),
                        self.redis.clone(),
                        self.config.clone(),
                        self.http_client.clone(),
                        &self.provider_cache,
                        self.heartbeat.clone(),
                    ));
                }
                None if attempt < RETRIES => {
                    tracing::warn!(
                        job_id = %job_id, attempt,
                        "Chunk not yet in Mongo, retrying context build"
                    );
                    tokio::time::sleep(Duration::from_millis(BACKOFF_MS)).await;
                }
                None => {
                    return Err(ToolError::Message(format!(
                        "Chunk job {job_id} not found in Mongo after {} attempts",
                        RETRIES + 1
                    )));
                }
            }
        }
        unreachable!()
    }
}
