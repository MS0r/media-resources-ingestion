use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::{
    AppConfig, JobStatusFilter, MongoService, ToolError, bootstrap,
    models::{self, ProgressEvent, ProgressJobType, ProgressStatus},
    services::redis::RedisService,
    settings::load_toml,
};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// Helper to convert an arbitrary Display error into `Status::internal`.
/// Used instead of orphan-rule-violating `From` impls for foreign error types.
fn internal_err<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(e.to_string())
}

pub mod proto {
    tonic::include_proto!("ingest");

    pub use super::proto::ingest_service_server::IngestService;
    pub use super::proto::ingest_service_server::IngestServiceServer;
}

use proto::ingest_service_server::IngestService;
use proto::*;

#[derive(Clone)]
pub struct IngestServer {
    mongo: MongoService,
    redis: RedisService,
    toml_config: crate::TomlRawConfig,
}

impl IngestServer {
    pub async fn new(toml_path: &Path) -> Result<Self, ToolError> {
        let redis_uri = std::env::var("REDIS_URI")?;
        let mongo_uri = std::env::var("MONGODB_URI")?;
        let toml_config = load_toml(&toml_path.to_path_buf())?;

        let mongo = MongoService::new(&mongo_uri).await?;
        let redis = RedisService::new(&redis_uri, 3600, 3, vec![5, 30, 120])?;

        Ok(Self {
            mongo,
            redis,
            toml_config,
        })
    }

    fn build_app_config(
        &self,
        yaml_content: &str,
        priority: i32,
        file_workers: i32,
        dry_run: bool,
    ) -> Result<(AppConfig, Vec<models::Resource>), ToolError> {
        let redis_uri = std::env::var("REDIS_URI")?;
        let mongo_uri = std::env::var("MONGODB_URI")?;
        let yaml = load_yaml_from_str(yaml_content)?;
        let priority = if priority > 0 { Some(priority) } else { None };
        let workers = if file_workers > 0 {
            Some(file_workers as usize)
        } else {
            None
        };

        let run_config = crate::RunConfig {
            yaml_path: std::path::PathBuf::new(),
            dry_run,
            priority,
            workers,
            follow: false,
            no_follow: true,
            output: crate::OutputFormat::Json,
        };

        let config = AppConfig::from_sources(
            &yaml,
            self.toml_config.clone(),
            run_config,
            redis_uri,
            mongo_uri,
        );

        Ok((config, yaml.resources))
    }
}

fn load_yaml_from_str(yaml_content: &str) -> Result<models::IngestionConfig, ToolError> {
    let request: models::IngestionConfig = serde_yaml::from_str(yaml_content)?;
    Ok(request)
}

#[tonic::async_trait]
impl IngestService for IngestServer {
    async fn enqueue(
        &self,
        request: Request<EnqueueRequest>,
    ) -> Result<Response<EnqueueResponse>, Status> {
        let req = request.into_inner();
        let (config, resources) = self.build_app_config(
            &req.yaml_content,
            req.priority,
            req.file_workers,
            req.dry_run,
        )?;

        let job_count = resources.len() as i32;
        let batch_id = bootstrap::enqueue(&config, &resources).await?;

        Ok(Response::new(EnqueueResponse {
            batch_id,
            job_count,
        }))
    }

    async fn get_batch_status(
        &self,
        request: Request<GetBatchStatusRequest>,
    ) -> Result<Response<BatchStatus>, Status> {
        let batch_id = request.into_inner().batch_id;
        let batch = self
            .mongo
            .get_batch(&batch_id)
            .await
            .map_err(internal_err)?
            .ok_or_else(|| Status::not_found(format!("batch {batch_id} not found")))?;

        Ok(Response::new(BatchStatus {
            batch_id: batch._id,
            status: format!("{:?}", batch.status),
            created_at: batch.created_at.to_rfc3339(),
            total_jobs: batch.job_ids.len() as i32,
            job_ids: batch.job_ids,
        }))
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<JobDetail>, Status> {
        let job_id = request.into_inner().job_id;
        let job = self
            .mongo
            .get_file_job(&job_id)
            .await
            .map_err(internal_err)?
            .ok_or_else(|| Status::not_found(format!("job {job_id} not found")))?;

        Ok(Response::new(JobDetail {
            job_id: job._id,
            batch_id: job.batch_id,
            status: format!("{:?}", job.status),
            url: job.resource.url.to_string(),
            error: job.error.unwrap_or_default(),
            retry_count: job.retry_count as i32,
            created_at: job.created_at.to_rfc3339(),
        }))
    }

    async fn list_jobs(
        &self,
        request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let req = request.into_inner();
        let filter = if req.filter.is_empty() {
            None
        } else {
            Some(match req.filter.to_lowercase().as_str() {
                "pending" => JobStatusFilter::Pending,
                "running" => JobStatusFilter::Running,
                "completed" => JobStatusFilter::Completed,
                "failed" => JobStatusFilter::Failed,
                "retrying" => JobStatusFilter::Retrying,
                "cancelled" => JobStatusFilter::Cancelled,
                _ => {
                    return Err(Status::invalid_argument(format!(
                        "unknown filter: {}",
                        req.filter
                    )));
                }
            })
        };
        let limit = if req.limit > 0 {
            req.limit as usize
        } else {
            50
        };

        let jobs = self
            .mongo
            .list_jobs(filter, limit)
            .await
            .map_err(internal_err)?;

        let job_details = jobs
            .into_iter()
            .map(|job| JobDetail {
                job_id: job._id,
                batch_id: job.batch_id,
                status: format!("{:?}", job.status),
                url: job.resource.url.to_string(),
                error: job.error.unwrap_or_default(),
                retry_count: job.retry_count as i32,
                created_at: job.created_at.to_rfc3339(),
            })
            .collect();

        Ok(Response::new(ListJobsResponse { jobs: job_details }))
    }

    async fn cancel_job(
        &self,
        request: Request<CancelJobRequest>,
    ) -> Result<Response<ActionResponse>, Status> {
        let job_id = request.into_inner().job_id;
        let cancelled = self.mongo.cancel_job(&job_id).await.map_err(internal_err)?;

        if cancelled {
            self.redis.cancel_job(&job_id).await.map_err(internal_err)?;
            publish_cancelled(&self.redis, &job_id, ProgressJobType::FileJob).await;
        }

        Ok(Response::new(ActionResponse {
            success: cancelled,
            message: if cancelled {
                format!("job {job_id} cancelled")
            } else {
                format!("job {job_id} not found or not pending")
            },
        }))
    }

    async fn cancel_batch(
        &self,
        request: Request<CancelBatchRequest>,
    ) -> Result<Response<ActionResponse>, Status> {
        let batch_id = request.into_inner().batch_id;

        let count = self
            .mongo
            .cancel_batch_jobs(&batch_id)
            .await
            .map_err(internal_err)?;

        if let Some(batch) = self
            .mongo
            .get_batch(&batch_id)
            .await
            .map_err(internal_err)?
        {
            self.redis
                .cancel_jobs(&batch.job_ids)
                .await
                .map_err(internal_err)?;
            for job_id in &batch.job_ids {
                publish_cancelled(&self.redis, job_id, ProgressJobType::FileJob).await;
            }
        }

        Ok(Response::new(ActionResponse {
            success: count > 0,
            message: format!("cancelled {count} pending jobs in batch {batch_id}"),
        }))
    }

    async fn retry_job(
        &self,
        request: Request<RetryJobRequest>,
    ) -> Result<Response<ActionResponse>, Status> {
        let job_id = request.into_inner().job_id;
        let retried = self
            .mongo
            .retry_failed_job(&job_id)
            .await
            .map_err(internal_err)?;

        if retried {
            let _ = self
                .redis
                .retry_job(&job_id, crate::handlers::jobs::JobKind::File)
                .await;
            let event = ProgressEvent {
                job_id: job_id.clone(),
                job_type: ProgressJobType::FileJob,
                stage: "retrying".to_string(),
                current: 0,
                total: None,
                status: ProgressStatus::Retrying,
                message: Some("Manually retried via API".to_string()),
            };
            let _ = self.redis.publish_progress(&job_id, &event).await;
        }

        Ok(Response::new(ActionResponse {
            success: retried,
            message: if retried {
                format!("job {job_id} re-enqueued for retry")
            } else {
                format!("job {job_id} not found or not in failed state")
            },
        }))
    }

    async fn list_files(
        &self,
        request: Request<ListFilesRequest>,
    ) -> Result<Response<ListFilesResponse>, Status> {
        let req = request.into_inner();
        let mime = if req.mime_type.is_empty() {
            None
        } else {
            Some(req.mime_type.as_str())
        };
        let provider = if req.provider.is_empty() {
            None
        } else {
            Some(req.provider.as_str())
        };
        let limit = if req.limit > 0 {
            req.limit as usize
        } else {
            100
        };

        let files = self
            .mongo
            .list_files(mime, provider, limit)
            .await
            .map_err(internal_err)?;

        let file_metadatas: Vec<FileMetadata> = files
            .into_iter()
            .map(|f| FileMetadata {
                file_hash: f.file_hash,
                original_url: f.original_url.to_string(),
                storage_provider: f.storage_provider.to_string(),
                storage_path: f.storage_path,
                original_file_size: f.original_file_size,
                compressed_file_size: f.compressed_file_size.unwrap_or(0),
                compression_ratio: f.compression_ratio.unwrap_or(0.0) as f64,
                mime_type: f.mime_type,
                upload_date: chrono::Utc::now().to_rfc3339(),
            })
            .collect();

        Ok(Response::new(ListFilesResponse {
            files: file_metadatas,
        }))
    }

    async fn get_file(
        &self,
        request: Request<GetFileRequest>,
    ) -> Result<Response<FileMetadata>, Status> {
        let hash = request.into_inner().hash;
        let metadata = self
            .mongo
            .get_file_metadata(&hash)
            .await
            .map_err(internal_err)?
            .ok_or_else(|| Status::not_found(format!("file with hash {hash} not found")))?;

        Ok(Response::new(FileMetadata {
            file_hash: metadata.file_hash,
            original_url: metadata.original_url.to_string(),
            storage_provider: metadata.storage_provider.to_string(),
            storage_path: metadata.storage_path,
            original_file_size: metadata.original_file_size,
            compressed_file_size: metadata.compressed_file_size.unwrap_or(0),
            compression_ratio: metadata.compression_ratio.unwrap_or(0.0) as f64,
            mime_type: metadata.mime_type,
            upload_date: {
                let ts = metadata.upload_date.timestamp_millis();
                let secs = ts / 1000;
                let nsecs = ((ts % 1000) * 1_000_000) as u32;
                chrono::DateTime::from_timestamp(secs, nsecs)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_default()
            },
        }))
    }
}

impl From<ToolError> for Status {
    fn from(e: ToolError) -> Self {
        match e {
            ToolError::RedisError(_)
            | ToolError::MongoError(_)
            | ToolError::MongoPoolError(_)
            | ToolError::MongoConnectionError(_)
            | ToolError::BsonError(_)
            | ToolError::IoError(_)
            | ToolError::JsonError(_)
            | ToolError::WreqError(_)
            | ToolError::JobExecutionError(_)
            | ToolError::Message(_)
            | ToolError::ServerError(_) => Status::internal(e.to_string()),

            ToolError::ConfigError(_)
            | ToolError::ConfigParseError(_)
            | ToolError::YamlError(_)
            | ToolError::ValidationError(_)
            | ToolError::EnvError(_)
            | ToolError::UrlParseError(_) => Status::invalid_argument(e.to_string()),

            ToolError::AuthError(_) | ToolError::AuthResolution(_) => Status::unauthenticated(e.to_string()),

            ToolError::SemaphoreError(_) => Status::unavailable(e.to_string()),

            ToolError::Interrupted => Status::cancelled("operation interrupted"),
        }
    }
}

/// Start the gRPC server with an auto-started worker in background.
pub async fn serve(addr: SocketAddr, toml_path: &Path) -> Result<(), ToolError> {
    ffmpeg_next::init().ok();
    let shutdown = Arc::new(AtomicBool::new(false));

    let toml_config = load_toml(&toml_path.to_path_buf())?;
    let redis_uri = std::env::var("REDIS_URI")?;
    let mongo_uri = std::env::var("MONGODB_URI")?;
    let mongo = MongoService::new(&mongo_uri).await?;
    let redis = RedisService::new(&redis_uri, 3600, 3, vec![5, 30, 120])?;

    tracing::info!("Ingest gRPC server listening on {addr}");

    let worker_config =
        AppConfig::from_worker_args(toml_config.clone(), redis_uri, mongo_uri, None);

    let worker_shutdown = shutdown.clone();
    let worker_mongo = mongo.clone();
    let worker_redis = redis.clone();

    tokio::spawn(async move {
        tracing::info!("Worker auto-started with gRPC server");
        if let Err(e) = bootstrap::worker_with_services(
            worker_mongo,
            worker_redis,
            worker_config,
            worker_shutdown,
        )
        .await
        {
            tracing::error!(error = %e, "Worker exited with error");
        } else {
            tracing::info!("Worker stopped cleanly");
        }
    });

    let ingest_server = IngestServer {
        mongo,
        redis,
        toml_config,
    };

    let server_shutdown = shutdown.clone();
    let (signal_tx, signal_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::warn!("SIGINT received, shutting down server and worker...");
        server_shutdown.store(true, Ordering::Relaxed);
        let _ = signal_tx.send(());
    });

    Server::builder()
        .add_service(IngestServiceServer::new(ingest_server))
        .serve_with_shutdown(addr, async {
            signal_rx.await.ok();
        })
        .await?;

    Ok(())
}

/// Publish a Cancelled/Failed progress event so CLI subscribers unblock.
async fn publish_cancelled(redis: &RedisService, job_id: &str, job_type: ProgressJobType) {
    let event = ProgressEvent {
        job_id: job_id.to_string(),
        job_type,
        stage: "cancelled".to_string(),
        current: 0,
        total: None,
        status: ProgressStatus::Failed,
        message: Some("Job was cancelled".to_string()),
    };
    let _ = redis.publish_progress(job_id, &event).await;
}
