use std::path::Path;

use colored::*;
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use ingest_core::models::{ProgressEvent, ProgressStatus};
use ingest_core::server::proto::ingest_service_client::IngestServiceClient;
use ingest_core::server::proto::*;
use tonic::transport::Endpoint;

use crate::cli;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn connect(server_addr: &str) -> Result<IngestServiceClient<tonic::transport::Channel>> {
    let endpoint = Endpoint::from_shared(format!("http://{server_addr}"))?;
    Ok(IngestServiceClient::connect(endpoint).await?)
}

pub async fn handle_run(args: cli::RunArgs, server_addr: &str) -> Result<()> {
    let yaml_content = std::fs::read_to_string(&args.yaml_path)
        .map_err(|e| format!("Failed to read YAML file: {e}"))?;

    let mut client = connect(server_addr).await?;

    let response = client
        .enqueue(EnqueueRequest {
            yaml_content,
            priority: args.priority.unwrap_or(0),
            file_workers: args.workers.unwrap_or(0),
            dry_run: args.dry_run,
        })
        .await?;

    let resp = response.into_inner();

    if args.dry_run || resp.job_count == 0 {
        return Ok(());
    }

    println!("{} {}", "Batch ID:".cyan(), resp.batch_id);

    let follow = args.follow || !args.no_follow;
    if !follow {
        println!(
            "{}",
            "Use `ingest status batch <id>` to track progress".yellow()
        );
        return Ok(());
    }

    follow_batch(&resp.batch_id, server_addr).await
}

async fn follow_batch(batch_id: &str, server_addr: &str) -> Result<()> {
    let redis_uri = std::env::var("REDIS_URI").map_err(|_| {
        "REDIS_URI not set (required for follow mode)"
            .red()
            .to_string()
    })?;
    let redis_client = redis::Client::open(redis_uri.as_str())
        .map_err(|e| format!("Redis connection failed: {e}"))?;

    // Fetch job IDs from the batch
    let mut client = connect(server_addr).await?;
    let batch = client
        .get_batch_status(GetBatchStatusRequest {
            batch_id: batch_id.to_string(),
        })
        .await?
        .into_inner();

    let total_jobs = batch.job_ids.len();
    if total_jobs == 0 {
        println!("{}", "No jobs in batch".yellow());
        return Ok(());
    }

    let mp = MultiProgress::new();
    let mut handles = Vec::with_capacity(total_jobs);

    for (i, job_id) in batch.job_ids.iter().enumerate() {
        let pb = mp.add(ProgressBar::new_spinner());
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed}] {msg}")?
                .tick_chars("⠁⠁⠉⠙⠚⠒⠂⠂⠒⠲⠴⠤⠄⠄⠤⠴⠲⠒⠂⠂⠒⠚⠙⠉⠈⠈"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(80));
        pb.set_message(format!("[{}/{}] Queued...", i + 1, total_jobs));

        let rc = redis_client.clone();
        let jid = job_id.clone();
        handles.push(tokio::spawn(async move {
            follow_job(&pb, &rc, &jid, i + 1, total_jobs).await
        }));
    }

    let mut failed = false;
    for h in handles {
        if h.await.unwrap().is_err() {
            failed = true;
        }
    }
    mp.clear().ok();

    if failed {
        eprintln!("{}", "One or more jobs failed".red());
        std::process::exit(1);
    }
    Ok(())
}

/// Subscribe to progress events for a single job and drive its indicatif bar.
async fn follow_job(
    pb: &ProgressBar,
    redis_client: &redis::Client,
    job_id: &str,
    idx: usize,
    total: usize,
) -> Result<()> {
    // Fallback poll: check if job already finished before we subscribed
    let mut state_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| format!("Redis state check failed: {e}"))?;
    let state_key = format!("jobs:state:{job_id}");
    let status: Option<String> = redis::Cmd::hget(&state_key, "status")
        .query_async(&mut state_conn)
        .await?;
    if let Some(ref s) = status {
        match s.as_str() {
            "completed" => {
                pb.finish_with_message(format!("[{}/{}] ✓ Completed", idx, total));
                return Ok(());
            }
            "failed" => {
                let err: Option<String> = redis::Cmd::hget(&state_key, "error")
                    .query_async(&mut state_conn)
                    .await?;
                pb.finish_with_message(format!(
                    "[{}/{}] ✗ Failed: {}",
                    idx,
                    total,
                    err.unwrap_or_default()
                ));
                return Err("Job failed".into());
            }
            _ => {}
        }
    }
    drop(state_conn);

    // Subscribe to progress channel
    let mut pubsub = redis_client
        .get_async_pubsub()
        .await
        .map_err(|e| format!("Redis subscribe failed: {e}"))?;
    let channel = format!("jobs:progress:{job_id}");
    pubsub.subscribe(&channel).await?;

    let mut stream = pubsub.on_message();
    let label = format!("[{}/{}]", idx, total);

    while let Some(msg) = stream.next().await {
        let payload: String = msg.get_payload().unwrap_or_else(|_| String::new());
        let Ok(event) = serde_json::from_str::<ProgressEvent>(&payload) else {
            continue;
        };

        match event.status {
            ProgressStatus::Done => {
                pb.finish_with_message(format!(
                    "{} ✓ {}",
                    label,
                    event.message.unwrap_or_else(|| "Done".into())
                ));
                return Ok(());
            }
            ProgressStatus::Failed => {
                pb.finish_with_message(format!(
                    "{} ✗ {}",
                    label,
                    event.message.unwrap_or_else(|| "Failed".into())
                ));
                return Err("Job failed".into());
            }
            ProgressStatus::Retrying => {
                pb.set_message(format!(
                    "{} ↻ {}",
                    label,
                    event.message.unwrap_or(event.stage)
                ));
            }
            ProgressStatus::Running => {
                let msg = event.message.clone().unwrap_or_else(|| event.stage.clone());
                pb.set_message(format!("{} {}", label, msg));
            }
        }
    }

    pb.finish_with_message(format!("{} Disconnected", label));
    Ok(())
}

pub async fn handle_server(args: cli::ServerArgs) -> Result<()> {
    let addr = args.addr.parse()?;
    let toml_path = args
        .config
        .unwrap_or_else(|| Path::new(".ingest/config.toml").to_path_buf());

    ffmpeg_next::init().ok();
    ingest_core::server::serve(addr, &toml_path).await?;
    Ok(())
}

pub async fn handle_status(scope: cli::StatusScope, server_addr: &str) -> Result<()> {
    let mut client = connect(server_addr).await?;

    match scope {
        cli::StatusScope::Batch { batch_id } => {
            let response = client
                .get_batch_status(GetBatchStatusRequest {
                    batch_id: batch_id.clone(),
                })
                .await?;
            let batch = response.into_inner();
            println!("Batch ID: {}", batch.batch_id);
            println!("Status: {}", batch.status);
            println!("Created: {}", batch.created_at);
            println!("Jobs: {}", batch.total_jobs);
        }
        cli::StatusScope::Job { job_id } => {
            let response = client
                .get_job_status(GetJobStatusRequest {
                    job_id: job_id.clone(),
                })
                .await?;
            let job = response.into_inner();
            println!("Job ID: {}", job.job_id);
            println!("Batch ID: {}", job.batch_id);
            println!("Status: {}", job.status);
            println!("URL: {}", job.url);
            if !job.error.is_empty() {
                println!("Error: {}", job.error);
            }
            println!("Retries: {}", job.retry_count);
            println!("Created: {}", job.created_at);
        }
        cli::StatusScope::Jobs(args) => {
            let filter = args.filter.map(|f| match f {
                cli::JobStatus::Pending => "pending",
                cli::JobStatus::Running => "running",
                cli::JobStatus::Completed => "completed",
                cli::JobStatus::Failed => "failed",
                cli::JobStatus::Retrying => "retrying",
                cli::JobStatus::Cancelled => "cancelled",
            });
            let response = client
                .list_jobs(ListJobsRequest {
                    filter: filter.unwrap_or_default().to_string(),
                    limit: args.limit as i32,
                })
                .await?;
            let jobs = response.into_inner().jobs;
            if jobs.is_empty() {
                println!("No jobs found");
            }
            for job in &jobs {
                println!(
                    "Job: {} - Status: {} - URL: {}",
                    job.job_id, job.status, job.url
                );
            }
        }
    }
    Ok(())
}

pub async fn handle_cancel(scope: cli::CancelScope, server_addr: &str) -> Result<()> {
    let mut client = connect(server_addr).await?;

    match scope {
        cli::CancelScope::Batch { batch_id } => {
            let response = client
                .cancel_batch(CancelBatchRequest {
                    batch_id: batch_id.clone(),
                })
                .await?;
            let resp = response.into_inner();
            println!("{}", resp.message);
        }
        cli::CancelScope::Job { job_id } => {
            let response = client
                .cancel_job(CancelJobRequest {
                    job_id: job_id.clone(),
                })
                .await?;
            let resp = response.into_inner();
            if resp.success {
                println!("{}", resp.message);
            } else {
                eprintln!("{}", resp.message);
            }
        }
    }
    Ok(())
}

pub async fn handle_retry(scope: cli::RetryScope, server_addr: &str) -> Result<()> {
    let mut client = connect(server_addr).await?;

    match scope {
        cli::RetryScope::Job { job_id } => {
            let response = client
                .retry_job(RetryJobRequest {
                    job_id: job_id.clone(),
                })
                .await?;
            let resp = response.into_inner();
            if resp.success {
                println!("{}", resp.message);
            } else {
                eprintln!("{}", resp.message);
            }
        }
    }
    Ok(())
}

pub async fn handle_files(scope: cli::FilesScope, server_addr: &str) -> Result<()> {
    let mut client = connect(server_addr).await?;

    match scope {
        cli::FilesScope::List(args) => {
            let response = client
                .list_files(ListFilesRequest {
                    mime_type: args.mime.unwrap_or_default(),
                    provider: args.provider.unwrap_or_default(),
                    limit: args.limit as i32,
                })
                .await?;
            let files = response.into_inner().files;
            if files.is_empty() {
                println!("No files found");
            }
            for f in &files {
                println!(
                    "Hash: {} - MIME: {} - Size: {} bytes",
                    f.file_hash, f.mime_type, f.original_file_size
                );
            }
        }
        cli::FilesScope::Get { hash } => {
            let response = client
                .get_file(GetFileRequest { hash: hash.clone() })
                .await?;
            let f = response.into_inner();
            println!("File Metadata:");
            println!("  Hash: {}", f.file_hash);
            println!("  URL: {}", f.original_url);
            println!("  Provider: {}", f.storage_provider);
            println!("  Path: {}", f.storage_path);
            println!("  Size: {} bytes", f.original_file_size);
            println!("  Compressed: {} bytes", f.compressed_file_size);
            println!("  Ratio: {:.2}", f.compression_ratio);
            println!("  MIME: {}", f.mime_type);
        }
        cli::FilesScope::Delete { hash: _, yes } => {
            if !yes {
                print!(
                    "{}",
                    "Are you sure you want to delete this file? (y/N) ".yellow()
                );
                std::io::Write::flush(&mut std::io::stdout()).ok();
                let mut input = String::new();
                std::io::stdin().read_line(&mut input)?;
                if !input.trim().eq_ignore_ascii_case("y") {
                    println!("{}", "Deletion cancelled".yellow());
                    return Ok(());
                }
            }
            eprintln!("Delete not yet implemented via gRPC");
        }
    }
    Ok(())
}
