//! Single `Services` handle — replaces `ContextFactory` + duplicate `ProviderCache`.
//!
//! Built once in `serve()` and shared via `Arc` between the gRPC server and the worker.

use std::sync::Arc;

use crate::{
    auth::AuthProviderRegistry,
    error::ToolError,
    models::AppConfig,
    services::{heartbeat::HeartbeatSupervisor, mongo::MongoService, redis::RedisService},
    storage::ProviderCache,
};
use wreq::{
    Client,
    header::{ACCEPT, HeaderMap, HeaderValue},
};

/// Shared services handle — built once, cloned cheaply via `Arc`.
pub struct Services {
    pub db: Arc<MongoService>,
    pub redis: Arc<RedisService>,
    pub storage: Arc<ProviderCache>,
    pub http: Arc<Client>,
    pub heartbeat: Arc<HeartbeatSupervisor>,
    pub config: Arc<AppConfig>,
}

impl Services {
    /// Build all services from an `AppConfig`. Called once in `serve()`.
    pub async fn build(config: AppConfig) -> Result<Arc<Self>, ToolError> {
        let mongo = MongoService::new(
            &config.mongo_uri,
            config.mongo_pool_min,
            config.mongo_pool_max,
        )
        .await?;
        let redis = RedisService::new(
            &config.redis_uri,
            config.running_job_ttl_secs,
            config.max_retries,
            config.backoff_secs.clone(),
        )?;

        let auth_registry = Self::build_auth_registry();
        let storage = Arc::new(ProviderCache::new(auth_registry));

        let mut default_headers = HeaderMap::new();
        default_headers.insert(
            ACCEPT,
            HeaderValue::from_static(
                "video/webm,video/mp4,application/octet-stream,image/*,*/*;q=0.8",
            ),
        );

        let http = Arc::new(
            Client::builder()
                .emulation(wreq_util::Emulation::Chrome124)
                .default_headers(default_headers)
                .build()
                .map_err(|e| ToolError::Message(format!("Failed to build HTTP client: {e}")))?,
        );

        let heartbeat = Arc::new(HeartbeatSupervisor::new());

        Ok(Arc::new(Self {
            db: Arc::new(mongo),
            redis: Arc::new(redis),
            storage,
            http,
            heartbeat,
            config: Arc::new(config),
        }))
    }

    /// Build auth registry from environment variables.
    /// Replaces `bootstrap::init_auth_registry()`.
    fn build_auth_registry() -> AuthProviderRegistry {
        use crate::auth::OAuthTokenProvider;

        let mut registry = AuthProviderRegistry::new();

        // Google Drive — OAuth refresh-token (from stored config file or env vars)
        match OAuthTokenProvider::from_env_or_file(
            "GDRIVE",
            "https://oauth2.googleapis.com/token",
            "gdrive",
        ) {
            Ok(p) => {
                tracing::info!("GDrive OAuth token provider registered");
                registry.register("gdrive", Arc::new(p));
            }
            Err(e) => {
                tracing::debug!("GDrive OAuth not configured: {e}");
            }
        }

        // Dropbox OAuth — try config file first, then env vars
        match OAuthTokenProvider::from_env_or_file(
            "DROPBOX",
            "https://api.dropbox.com/oauth2/token",
            "dropbox",
        ) {
            Ok(p) => {
                tracing::info!("Dropbox OAuth token provider registered");
                registry.register("dropbox", Arc::new(p));
            }
            Err(e) => {
                tracing::debug!("Dropbox OAuth not configured: {e}");
            }
        }

        registry
    }

    /// Build a `JobContext` for a dequeued file job.
    pub async fn build_file_context(
        &self,
        job_id: &str,
    ) -> Result<crate::handlers::jobs::JobContext, ToolError> {
        if let Some(file_job) = self.db.get_file_job(job_id).await? {
            tracing::info!(job_id = %job_id, "Building file job context from Mongo");
            let auth_token =
                crate::handlers::jobs::resolve_source_auth(&file_job.spec, &self.storage).await?;
            Ok(crate::handlers::jobs::JobContext::from_file_job(
                file_job,
                self.db.clone(),
                self.redis.clone(),
                self.config.clone(),
                self.http.clone(),
                auth_token,
                &self.storage,
                self.heartbeat.clone(),
            ))
        } else {
            Err(format!("File job {job_id} not found in Mongo").into())
        }
    }

    /// Build a `JobContext` for a dequeued chunk job.
    /// Retries on Mongo write-visibility lag.
    pub async fn build_chunk_context(
        &self,
        job_id: &str,
    ) -> Result<crate::handlers::jobs::JobContext, ToolError> {
        use std::time::Duration;

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
                    return Ok(crate::handlers::jobs::JobContext::from_chunk_job(
                        chunk_job,
                        self.db.clone(),
                        self.redis.clone(),
                        self.config.clone(),
                        self.http.clone(),
                        &self.storage,
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
