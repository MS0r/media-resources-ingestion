use crate::{
    cli::{CancelScope, FilesScope, RetryScope, RunArgs, StatusScope},
    context::ContextFactory,
    error::ToolError,
    handlers::{
        jobs::{Batch, ChunkJobHandler, FileJob, FileJobHandler, JobStatus},
        scheduler::scheduler_loop,
    },
    models::MainConfig,
    services::{mongo::MongoService, redis::RedisService},
};
use chrono::Utc;
use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use uuid::Uuid;

pub async fn run(config: MainConfig, args: RunArgs) -> Result<(), ToolError> {
    tracing::info!(
        resources = config.yaml_config.resources.len(),
        "Config loaded"
    );

    if config.yaml_config.resources.is_empty() {
        tracing::warn!("Empty YAML file - no jobs created");
        return Ok(());
    }

    // Validate YAML - check for duplicate URLs
    let mut urls = std::collections::HashSet::new();
    for resource in &config.yaml_config.resources {
        if !urls.insert(resource.url.as_str()) {
            tracing::error!("Duplicate URL found in YAML: {}", resource.url);
            return Err(ToolError::ValidationError("Duplicate URL found in YAML".to_string()));
        }
    }

    // --dry-run: validate YAML and preflight URLs without downloading
    if args.dry_run {
        return validate_dry_run(&config.yaml_config).await;
    }  

    // -- Services ----------------------------------------------------------
    let redis_service = match RedisService::new(&config.redis_uri) {
        Ok(svc) => {
            tracing::info!(url = %config.redis_uri, "Redis connected");
            svc
        }
        Err(e) => {
            tracing::error!("Failed to connect to Redis: {}", e);
            return Err(ToolError::RedisError(e));
        }
    };

    let mongo_service = match MongoService::new(&config.mongo_uri).await {
        Ok(svc) => {
            tracing::info!(url = %config.mongo_uri, "MongoDB connected");
            svc
        }
        Err(e) => {
            tracing::error!("Failed to connect to MongoDB: {}", e);
            return Err(ToolError::MongoConnectionError(e));
        }
    };

    let priority = args.priority.or(config.yaml_config.priority).unwrap_or(0);

    // -- Initial batch ------------------------------------------------------
    let batch_id = Uuid::new_v4().to_string();
    let mut batch = Batch {
        _id: batch_id.clone(),
        created_at: Utc::now(),
        yaml_path: config.yaml_path,
        status: JobStatus::Pending,
        job_ids: vec![],
    };

    for resource in &config.yaml_config.resources {

        let resource_priority = match resource.priority {
            Some(p) => p,
            None => priority,
        };

        let file_job = FileJob {
            _id: resource.id.clone(),
            batch_id: batch_id.clone(),
            resource: resource.clone(),
            priority: resource_priority,
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

    // Check if follow mode is enabled (default: true)
    let follow = args.follow || !args.no_follow;

    let mut had_failures = false;

    if follow && atty::is(atty::Stream::Stdout) {
        // Show progress bar
        let pb = ProgressBar::new(config.yaml_config.resources.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} jobs ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        tracing::info!("Starting ingestion with follow mode enabled");
        scheduler_loop(
            file_handler,
            chunk_handler,
            ctx_factory,
            max_file_workers,
            max_chunk_workers,
        )
        .await;

        pb.finish_with_message("Ingestion complete");
    } else {
        // No-follow mode or pipe: return immediately with batch ID
        if !atty::is(atty::Stream::Stdout) {
            tracing::info!("Detected pipe, disabling follow mode");
        }
        println!("Batch ID: {}", batch_id);
        return Ok(());
    }

    // Check if any jobs failed
    let mongo = MongoService::new(&config.mongo_uri).await?;
    let failed_jobs = mongo.list_jobs(Some(crate::cli::JobStatus::Failed), 1000).await?;

    if !failed_jobs.is_empty() {
        had_failures = true;
        eprintln!("{} jobs failed during ingestion", failed_jobs.len());
    }

    if had_failures {
        Err(ToolError::JobExecutionError("Some jobs failed".to_string()))
    } else {
        Ok(())
    }
}

pub async fn status(scope: StatusScope, mongo_uri : String) -> Result<(), ToolError> {
    let mongo = MongoService::new(&mongo_uri).await?;

    match scope {
        StatusScope::Batch { batch_id } => {
            tracing::info!("Checking status of batch {}", batch_id);
            if let Some(batch) = mongo.get_batch(&batch_id).await? {
                println!("Batch ID: {}", batch._id);
                println!("Created: {}", batch.created_at);
                println!("Status: {:?}", batch.status);
                println!("Jobs: {}", batch.job_ids.len());
            } else {
                eprintln!("Batch {} not found", batch_id);
            }
        }
        StatusScope::Job { job_id } => {
            tracing::info!("Checking status of job {}", job_id);
            if let Some(job) = mongo.get_file_job(&job_id).await? {
                println!("Job ID: {}", job._id);
                println!("Status: {:?}", job.status);
                println!("URL: {}", job.resource.url);
                if let Some(error) = &job.error {
                    println!("Error: {}", error);
                }
            } else {
                eprintln!("Job {} not found", job_id);
            }
        }
        StatusScope::Jobs(args) => {
            let filter = args.filter;
            let limit = args.limit;
            let output = args.output;
            tracing::info!("Checking status of all jobs with filter {:?}", filter);
            let jobs = mongo.list_jobs(filter, limit).await?;
            
            match output {
                crate::cli::OutputFormat::Table => {
                    for job in jobs {
                        println!("Job: {} - Status: {:?} - URL: {}", job._id, job.status, job.resource.url);
                    }
                }
                crate::cli::OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&jobs)?);
                }
            }
        }
    }
    Ok(())
}

pub async fn cancel(scope: CancelScope, mongo_uri : String, redis_uri: String) -> Result<(), ToolError> {
    let mongo = MongoService::new(&mongo_uri).await?;

    let redis = RedisService::new(&redis_uri)?;

    match scope {
        CancelScope::Batch { batch_id } => {
            tracing::info!("Cancelling batch {}", batch_id);
            let count = mongo.cancel_batch_jobs(&batch_id).await?;
            println!("Cancelled {} pending jobs in batch {}", count, batch_id);
            // Also remove from Redis queue
            redis.cancel_batch_jobs(&batch_id).await?;
        }
        CancelScope::Job { job_id } => {
            tracing::info!("Cancelling job {}", job_id);
            let cancelled = mongo.cancel_job(&job_id).await?;
            if cancelled {
                println!("Job {} cancelled", job_id);
                redis.cancel_job(&job_id).await?;
            } else {
                eprintln!("Job {} not found or not pending", job_id);
            }
        }
    }
    Ok(())
}

pub async fn retry(scope: RetryScope, mongo_uri : String) -> Result<(), ToolError> {
    let mongo = MongoService::new(&mongo_uri).await?;

    match scope {
        RetryScope::Job { job_id } => {
            tracing::info!("Retrying job {}", job_id);
            let retried = mongo.retry_failed_job(&job_id).await?;
            if retried {
                println!("Job {} re-enqueued for retry", job_id);
            } else {
                eprintln!("Job {} not found or not in failed state", job_id);
            }
        }
    }
    Ok(())
}

pub async fn files(scope: FilesScope, mongo_uri : String) -> Result<(), ToolError> {
    let mongo = MongoService::new(&mongo_uri).await?;

    match scope {
        FilesScope::List(args) => {
            tracing::info!("Listing stored files with filters");
            let files = mongo.list_files(args.mime.as_deref(), args.provider.as_deref(), args.limit).await?;
            
            match args.output {
                crate::cli::OutputFormat::Table => {
                    for file in files {
                        println!("Hash: {} - MIME: {} - Size: {} bytes", 
                            file.file_hash, file.mime_type, file.original_file_size);
                    }
                }
                crate::cli::OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&files)?);
                }
            }
        }
        FilesScope::Get { hash } => {
            tracing::info!("Getting metadata for file {}", hash);
            if let Some(metadata) = mongo.get_file_metadata(&hash).await? {
                println!("File Hash: {}", metadata.file_hash);
                println!("Original URL: {}", metadata.original_url);
                println!("Storage Provider: {:?}", metadata.storage_provider);
                println!("MIME Type: {}", metadata.mime_type);
                println!("Original Size: {} bytes", metadata.original_file_size);
                if let Some(compressed) = metadata.compressed_file_size {
                    println!("Compressed Size: {} bytes", compressed);
                }
                println!("Upload Date: {}", metadata.upload_date);
            } else {
                eprintln!("File with hash {} not found", hash);
            }
        }
        FilesScope::Download { hash, dest } => {
            if let Some(ref des) = dest {
                tracing::info!("Downloading file with hash {} to {}", hash, des.display());
            } else {
                tracing::info!("Downloading file with hash {} to stdout", hash);
            }

            if let Some(metadata) = mongo.get_file_metadata(&hash).await? {
                // Create storage provider once
                let storage = metadata.storage_provider.into_storage();

                // Check if file is chunked
                if let Some(manifest) = &metadata.chunk_manifest {
                    // Chunked file - reconstruct from chunks
                    tracing::info!("Reconstructing chunked file with {} chunks", manifest.chunks.len());
                    eprintln!("{}", "Downloading chunked file (reconstructing from chunks)...".yellow());

                    if let Some(ref dest_path) = dest {
                        // Write to file
                        let mut output_file = tokio::fs::File::create(dest_path).await?;
                        for chunk_ref in &manifest.chunks {
                            match storage.download(&chunk_ref.storage_path).await {
                                Ok(mut reader) => {
                                    tokio::io::copy(&mut reader, &mut output_file).await?;
                                }
                                Err(e) => {
                                    eprintln!("Failed to download chunk {}: {}", chunk_ref.hash, e);
                                    return Err(ToolError::JobExecutionError(format!("Failed to download chunk {}: {}", chunk_ref.hash, e)));
                                }
                            }
                        }
                        println!("File reconstructed and saved to {}", dest_path.display());
                    } else {
                        // Write to stdout
                        let mut stdout = tokio::io::stdout();
                        for chunk_ref in &manifest.chunks {
                            match storage.download(&chunk_ref.storage_path).await {
                                Ok(mut reader) => {
                                    tokio::io::copy(&mut reader, &mut stdout).await?;
                                }
                                Err(e) => {
                                    eprintln!("Failed to download chunk {}: {}", chunk_ref.hash, e);
                                    return Err(ToolError::JobExecutionError(format!("Failed to download chunk {}: {}", chunk_ref.hash, e)));
                                }
                            }
                        }
                    }
                } else {
                    // Non-chunked file - direct download
                    match storage.download(&metadata.storage_path).await {
                        Ok(mut reader) => {
                            if let Some(ref dest_path) = dest {
                                let mut output_file = tokio::fs::File::create(dest_path).await?;
                                tokio::io::copy(&mut reader, &mut output_file).await?;
                                println!("File saved to {}", dest_path.display());
                            } else {
                                let mut stdout = tokio::io::stdout();
                                tokio::io::copy(&mut reader, &mut stdout).await?;
                            }
                        }
                        Err(e) => {
                            eprintln!("Download failed: {}", e);
                            return Err(ToolError::JobExecutionError(format!("Download failed: {}", e)));
                        }
                    }
                }
            } else {
                eprintln!("File with hash {} not found", hash);
                return Err(ToolError::ConfigError(format!("File with hash {} not found", hash)));
            }
        }
        FilesScope::Delete { hash, yes } => {
            if !yes {
                println!("{}", "Are you sure you want to delete this file? (y/N)".yellow());
                let mut input = String::new();
                std::io::stdin().read_line(&mut input)?;
                if !input.trim().eq_ignore_ascii_case("y") {
                    println!("{}", "Deletion cancelled".yellow());
                    return Ok(());
                }
            }
            tracing::info!("Deleting file with hash {}", hash);
            // TODO: Decrement counter or delete chunks+manifest+metadata
            eprintln!("Delete not yet implemented");
        }
    }
    Ok(())
}

/// Validate YAML configuration and preflight URLs without downloading
async fn validate_dry_run(yaml_config: &crate::models::IngestionConfig) -> Result<(), ToolError> {
    use colored::*;

    println!("{}", "Dry-run mode: validating configuration...".bold());
    println!("Found {} resources to validate\n", yaml_config.resources.len());

    let mut all_valid = true;

    for resource in &yaml_config.resources {
        print!("  Checking {} ... ", resource.url);
        // Flush to ensure output appears in order
        use std::io::Write;
        std::io::stdout().flush().ok();

        match preflight_url(&resource.url).await {
            Ok(info) => {
                println!("{} ({})", "OK".green(), info);
            }
            Err(e) => {
                println!("{}: {}", "FAILED".red(), e);
                all_valid = false;
            }
        }
    }

    if all_valid {
        println!("\n{}", "✓ All resources validated successfully.".green().bold());
        Ok(())
    } else {
        eprintln!("\n{}", "✗ Some resources failed validation.".red().bold());
        Err(ToolError::ValidationError("Some resources failed validation".to_string()))
    }
}

/// Perform a preflight check on a URL (HEAD request for HTTP/HTTPS)
async fn preflight_url(url: &url::Url) -> Result<String, String> {
    if url.scheme() == "http" || url.scheme() == "https" {
        let response = reqwest::Client::new()
            .head(url.as_str())
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        if response.status().is_success() {
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown");
            Ok(format!("content-type: {}", content_type))
        } else {
            Err(format!("HTTP {}", response.status()))
        }
    } else if url.scheme() == "ftp" {
        Ok("FTP URL (basic validation)".to_string())
    } else if url.scheme() == "file" {
        // Check if local file exists
        if let Some(path) = url.to_file_path().ok() {
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
