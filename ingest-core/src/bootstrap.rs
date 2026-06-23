use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;
use url::Url;
use uuid::Uuid;

use crate::{
    auth::{AuthProviderRegistry, OAuthTokenProvider},
    context::ContextFactory,
    error::ToolError,
    handlers::{
        jobs::{Batch, ChunkJobHandler, FileJob, FileJobHandler, JobStatus},
        scheduler::scheduler_loop,
    },
    models::{AppConfig, Destination, Resource, ResourceLevelConfig},
    services::{mongo::MongoService, redis::RedisService},
    storage::Provider,
};

/// Initialize the auth provider registry from environment variables.
/// This is called once at worker startup.
fn init_auth_registry() -> AuthProviderRegistry {
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

fn parent_values(mut res: Resource, config: &AppConfig) -> (Resource, i32) {
    let resource_priority = res.priority.unwrap_or(config.priority);

    let cfg = res.config.get_or_insert_with(ResourceLevelConfig::default);

    if cfg.compression_override.is_none() {
        cfg.compression_override = config.compression_override.clone();
    }
    if cfg.headers.is_none() {
        cfg.headers = config.headers.clone();
    }
    if cfg.quality.is_none() {
        cfg.quality = config.quality;
    }
    if cfg.source_auth.is_none() {
        cfg.source_auth = config.source_auth.clone();
    }

    let dest = res.dest.get_or_insert_with(|| Destination {
        provider: Some(Provider::from(config.default_provider.clone())),
        path: Some(config.default_path.clone()),
    });
    if dest.provider.is_none() {
        dest.provider = Some(Provider::from(config.default_provider.clone()));
    }
    if dest.path.is_none() {
        dest.path = Some(config.default_path.clone());
    }

    (res, resource_priority)
}

pub async fn enqueue(config: &AppConfig, resources: &[Resource]) -> Result<String, ToolError> {
    tracing::info!(resources = resources.len(), "Config loaded");

    if resources.is_empty() {
        tracing::warn!("Empty YAML file - no jobs created");
        return Ok(String::new());
    }

    let mut urls = std::collections::HashSet::new();
    for resource in resources {
        if !urls.insert(resource.url.as_str()) {
            tracing::error!("Duplicate URL found in YAML: {}", resource.url);
            return Err(ToolError::ValidationError(
                "Duplicate URL found in YAML".to_string(),
            ));
        }
    }

    if config.dry_run {
        validate_dry_run(resources).await?;
        return Ok(String::new());
    }

    let redis_service = match RedisService::new(
        &config.redis_uri,
        config.running_job_ttl_secs,
        config.max_retries,
        config.backoff_secs.clone(),
    ) {
        Ok(svc) => {
            tracing::info!(url = %config.redis_uri, "Redis connected");
            svc
        }
        Err(e) => {
            tracing::error!("Failed to connect to Redis: {}", e);
            return Err(ToolError::RedisError(e));
        }
    };

    let mongo_service = MongoService::new(&config.mongo_uri).await?;

    let temp_dir = &config.temp_dir;
    tokio::fs::create_dir_all(temp_dir).await?;

    let batch_id = Uuid::new_v4().to_string();
    let mut batch = Batch {
        _id: batch_id.clone(),
        created_at: chrono::Utc::now(),
        yaml_path: config.yaml_path.clone(),
        status: JobStatus::Pending,
        job_ids: vec![],
    };

    for resource in resources {
        let (res, resource_priority) = parent_values(resource.clone(), config);

        let file_job = FileJob {
            _id: resource.id.clone(),
            batch_id: batch_id.clone(),
            resource: res,
            priority: resource_priority,
            status: JobStatus::Pending,
            retry_count: 0,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            file_hash: None,
            error: None,
            chunk_size: Some(config.chunk_size.clone()),
        };
        batch.job_ids.push(file_job._id.clone());
        tracing::debug!(job_id=%file_job._id,"Inserted file job from the batch id: {}", batch._id);
        mongo_service.save_file_job(&file_job).await?;
        redis_service.enqueue_file_job(&file_job).await?;
    }

    tracing::info!(batch_id = %batch._id, "Batch created");
    mongo_service.save_batch(&batch).await?;
    redis_service.enqueue_batch(&batch).await?;

    Ok(batch_id)
}

/// Worker body that uses pre-created services and a shared shutdown flag.
/// Used by both the server (auto-started worker) and standalone worker.
pub async fn worker_with_services(
    mongo_service: MongoService,
    redis_service: RedisService,
    config: AppConfig,
    shutdown: Arc<AtomicBool>,
) -> Result<(), ToolError> {
    match redis_service.recover_orphaned_jobs().await {
        Ok(n) => {
            if n > 0 {
                tracing::warn!(count = n, "Recovered orphaned jobs at worker startup");
            }
        }
        Err(e) => tracing::warn!(error = %e, "Failed to recover orphaned jobs"),
    }

    match redis_service.cleanup_orphaned_chunks(&mongo_service).await {
        Ok(n) => {
            if n > 0 {
                tracing::warn!(count = n, "Cleaned up orphaned chunk tracking keys");
            }
        }
        Err(e) => tracing::warn!(error = %e, "Failed to clean up orphaned chunk keys"),
    }

    let temp_dir = &config.temp_dir;
    tokio::fs::create_dir_all(temp_dir).await?;

    let file_handler = Arc::new(FileJobHandler);
    let chunk_handler = Arc::new(ChunkJobHandler);

    // Initialize auth providers
    let auth_registry = Arc::new(init_auth_registry());

    tracing::info!("Starting worker mode");
    tracing::info!(
        file_workers = config.file_workers,
        chunk_workers = config.chunk_workers,
        "Worker pool sizes"
    );

    let file_semaphore = Arc::new(Semaphore::new(config.file_workers));
    let chunk_semaphore = Arc::new(Semaphore::new(config.chunk_workers));

    let ctx_factory = Arc::new(ContextFactory::new(
        mongo_service,
        redis_service,
        config,
        Some(auth_registry),
    )?);

    scheduler_loop(
        file_handler,
        chunk_handler,
        ctx_factory,
        file_semaphore,
        chunk_semaphore,
        shutdown,
    )
    .await?;

    Ok(())
}

pub async fn worker(config: AppConfig) -> Result<(), ToolError> {
    let redis_service = match RedisService::new(
        &config.redis_uri,
        config.running_job_ttl_secs,
        config.max_retries,
        config.backoff_secs.clone(),
    ) {
        Ok(svc) => {
            tracing::info!(url = %config.redis_uri, "Redis connected");
            svc
        }
        Err(e) => {
            tracing::error!("Failed to connect to Redis: {}", e);
            return Err(e.into());
        }
    };

    let mongo_service = MongoService::new(&config.mongo_uri).await?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::warn!(
            "SIGINT received, initiating graceful shutdown (press Ctrl+C again to force)"
        );
        shutdown_clone.store(true, Ordering::Relaxed);
    });

    worker_with_services(mongo_service, redis_service, config, shutdown).await
}

/// Perform a preflight check on a URL (HEAD request for HTTP/HTTPS)
pub async fn preflight_url(url: &Url) -> Result<String, String> {
    if url.scheme() == "http" || url.scheme() == "https" {
        let response = wreq::Client::new()
            .head(url.as_str())
            .send()
            .await
            .map_err(|e| format!("Request failed: {e}"))?;

        if response.status().is_success() {
            let content_type = response
                .headers()
                .get(wreq::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown");
            Ok(format!("content-type: {content_type}"))
        } else {
            Err(format!("HTTP {}", response.status()))
        }
    } else if url.scheme() == "ftp" {
        Ok("FTP URL (basic validation)".to_string())
    } else if url.scheme() == "file" {
        if let Ok(path) = url.to_file_path() {
            if path.exists() {
                Ok("local file exists".to_string())
            } else {
                Err("local file not found".to_string())
            }
        } else {
            Err("invalid file URL".to_string())
        }
    } else {
        Err(format!("Unsupported scheme: {}", url.scheme()))
    }
}

/// Validate YAML configuration and preflight URLs without downloading
async fn validate_dry_run(resources: &[Resource]) -> Result<(), ToolError> {
    println!("Dry-run mode: validating configuration...");
    println!("Found {} resources to validate\n", resources.len());

    let mut all_valid = true;

    for resource in resources {
        print!("  Checking {} ... ", resource.url);
        use std::io::Write;
        std::io::stdout().flush().ok();

        match preflight_url(&resource.url).await {
            Ok(info) => {
                println!("OK ({info})");
            }
            Err(e) => {
                println!("FAILED: {e}");
                all_valid = false;
            }
        }
    }

    if all_valid {
        println!("\nAll resources validated successfully.");
        Ok(())
    } else {
        eprintln!("\nSome resources failed validation.");
        Err(ToolError::ValidationError(
            "Some resources failed validation".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Headers;
    use crate::storage::Provider;
    use url::Url;

    fn resource_with_dest(provider: Option<Provider>, path: Option<String>) -> Resource {
        Resource {
            id: uuid::Uuid::new_v4().to_string(),
            url: Url::parse("https://example.com/f.png").unwrap(),
            name: None,
            priority: None,
            dest: Some(Destination { provider, path }),
            config: None,
        }
    }

    fn resource_no_dest() -> Resource {
        Resource {
            id: uuid::Uuid::new_v4().to_string(),
            url: Url::parse("https://example.com/f.png").unwrap(),
            name: None,
            priority: None,
            dest: None,
            config: None,
        }
    }

    fn test_config() -> AppConfig {
        AppConfig {
            redis_uri: "redis://localhost".into(),
            mongo_uri: "mongodb://localhost".into(),
            file_workers: 0,
            chunk_workers: 0,
            max_pending_jobs: 0,
            max_per_host: 0,
            job_timeout_secs: 0,
            compression_threshold_mb: 0,
            compression_quality: 0,
            compression_timeout_secs: 0,
            default_provider: "local".into(),
            default_path: "/default/path".into(),
            chunk_size: "".into(),
            temp_dir: "".into(),
            running_job_ttl_secs: 0,
            max_retries: 0,
            backoff_secs: vec![],
            compression_override: None,
            headers: None,
            quality: None,
            source_auth: None,
            yaml_path: std::path::PathBuf::new(),
            priority: 0,
            dry_run: false,
            follow: false,
            output: crate::models::OutputFormat::Table,
        }
    }

    fn resource_with_config(headers: Option<Headers>) -> Resource {
        Resource {
            id: uuid::Uuid::new_v4().to_string(),
            url: Url::parse("https://example.com/f.png").unwrap(),
            name: None,
            priority: None,
            dest: None,
            config: Some(ResourceLevelConfig {
                compression_override: None,
                quality: None,
                headers,
                source_auth: None,
            }),
        }
    }

    fn resource_no_config() -> Resource {
        Resource {
            id: uuid::Uuid::new_v4().to_string(),
            url: Url::parse("https://example.com/f.png").unwrap(),
            name: None,
            priority: None,
            dest: None,
            config: None,
        }
    }

    fn resource_with_quality(quality: Option<u8>) -> Resource {
        Resource {
            id: uuid::Uuid::new_v4().to_string(),
            url: Url::parse("https://example.com/f.png").unwrap(),
            name: None,
            priority: None,
            dest: None,
            config: Some(ResourceLevelConfig {
                compression_override: None,
                quality,
                headers: None,
                source_auth: None,
            }),
        }
    }

    #[test]
    fn inherits_full_dest_when_resource_has_none() {
        let res = resource_no_dest();
        let (updated, _) = parent_values(res, &test_config());
        let d = updated.dest.unwrap();
        assert_eq!(d.provider.unwrap().to_string(), "local");
        assert_eq!(d.path.unwrap(), "/default/path");
    }

    #[test]
    fn keeps_resource_dest_when_fully_specified() {
        let res = resource_with_dest(Some(Provider::S3), Some("/custom".to_string()));
        let (updated, _) = parent_values(res, &test_config());
        let d = updated.dest.unwrap();
        assert_eq!(d.provider.unwrap().to_string(), "s3");
        assert_eq!(d.path.unwrap(), "/custom");
    }

    #[test]
    fn fills_missing_provider_from_parent() {
        let res = resource_with_dest(None, Some("/custom".to_string()));
        let (updated, _) = parent_values(res, &test_config());
        let d = updated.dest.unwrap();
        assert_eq!(d.provider.unwrap().to_string(), "local");
        assert_eq!(d.path.unwrap(), "/custom");
    }

    #[test]
    fn fills_missing_path_from_parent() {
        let res = resource_with_dest(Some(Provider::S3), None);
        let (updated, _) = parent_values(res, &test_config());
        let d = updated.dest.unwrap();
        assert_eq!(d.provider.unwrap().to_string(), "s3");
        assert_eq!(d.path.unwrap(), "/default/path");
    }

    #[test]
    fn inherits_appconfig_default_dest_when_resource_has_no_dest() {
        let res = resource_no_dest();
        let (updated, _) = parent_values(res, &test_config());
        let d = updated.dest.unwrap();
        assert_eq!(d.provider.unwrap().to_string(), "local");
        assert_eq!(d.path.unwrap(), "/default/path");
    }

    #[test]
    fn inherits_headers_when_resource_has_no_config() {
        let mut config = test_config();
        config.headers = Some(Headers {
            authorization: Some("Bearer token".to_string()),
            cookie: None,
        });
        let res = resource_no_config();
        let (updated, _) = parent_values(res, &config);
        assert_eq!(
            updated
                .config
                .unwrap()
                .headers
                .unwrap()
                .authorization
                .unwrap(),
            "Bearer token"
        );
    }

    #[test]
    fn keeps_resource_headers_when_specified() {
        let mut config = test_config();
        config.headers = Some(Headers {
            authorization: Some("Bearer token".to_string()),
            cookie: None,
        });
        let res = resource_with_config(Some(Headers {
            authorization: Some("Custom".to_string()),
            cookie: None,
        }));
        let (updated, _) = parent_values(res, &config);
        assert_eq!(
            updated
                .config
                .unwrap()
                .headers
                .unwrap()
                .authorization
                .unwrap(),
            "Custom"
        );
    }

    #[test]
    fn inherits_headers_into_existing_config() {
        let mut config = test_config();
        config.headers = Some(Headers {
            authorization: Some("Bearer token".to_string()),
            cookie: None,
        });
        let res = resource_with_config(None);
        let (updated, _) = parent_values(res, &config);
        assert_eq!(
            updated
                .config
                .unwrap()
                .headers
                .unwrap()
                .authorization
                .unwrap(),
            "Bearer token"
        );
    }

    #[test]
    fn inherits_quality_when_resource_has_no_config() {
        let mut config = test_config();
        config.quality = Some(85);
        let res = resource_no_config();
        let (updated, _) = parent_values(res, &config);
        assert_eq!(updated.config.unwrap().quality.unwrap(), 85);
    }

    #[test]
    fn inherits_quality_into_existing_config() {
        let mut config = test_config();
        config.quality = Some(85);
        let res = resource_with_quality(None);
        let (updated, _) = parent_values(res, &config);
        assert_eq!(updated.config.unwrap().quality.unwrap(), 85);
    }

    #[test]
    fn keeps_resource_quality_when_specified() {
        let mut config = test_config();
        config.quality = Some(85);
        let res = resource_with_quality(Some(90));
        let (updated, _) = parent_values(res, &config);
        assert_eq!(updated.config.unwrap().quality.unwrap(), 90);
    }
}
