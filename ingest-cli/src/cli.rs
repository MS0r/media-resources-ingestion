use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "ingest")]
#[command(version, about = "Media resources ingestion CLI tool", long_about = None)]
pub struct Cli {
    #[command(flatten)]
    pub global: Global,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Args, Clone)]
pub struct Global {
    #[arg(long, value_name = "ADDR", default_value = "[::1]:50051", global = true)]
    pub server: String,
    /// Log format
    #[arg(long, value_enum, default_value_t = LogFormat::Pretty)]
    pub log_format: LogFormat,
    /// Increase log verbosity (-v info, -vv debug, -vvv trace)
    #[arg(short, long, action = ArgAction::Count)]
    pub verbose: u8,
    /// Suppress all output except errors
    #[arg(short, long)]
    pub quiet: bool,
    /// Disable ANSI colour output
    #[arg(long)]
    pub no_color: bool,
    /// Path to TOML config file (for server/local mode)
    #[arg(short, long, value_name = "FILE", default_value = ".ingest/config.toml")]
    pub config: PathBuf,
}

#[derive(Subcommand, Clone)]
pub enum Commands {
    /// Submit a YAML config for ingestion
    Run(RunArgs),
    /// Start a local gRPC server with auto-started worker
    Server(ServerArgs),
    /// Inspect jobs and batches
    Status {
        #[command(subcommand)]
        scope: StatusScope,
    },
    /// Cancel pending jobs
    Cancel {
        #[command(subcommand)]
        scope: CancelScope,
    },
    /// Manually retry a failed job
    Retry {
        #[command(subcommand)]
        scope: RetryScope,
    },
    /// Browse and manage stored files
    Files {
        #[command(subcommand)]
        scope: FilesScope,
    },
}

#[derive(Args, Clone)]
pub struct RunArgs {
    pub yaml_path: PathBuf,
    #[arg(long)]
    pub dry_run: bool,
    #[arg(long)]
    pub priority: Option<i32>,
    #[arg(long)]
    pub workers: Option<i32>,
    #[arg(long, overrides_with = "no_follow")]
    pub follow: bool,
    #[arg(long, overrides_with = "follow")]
    pub no_follow: bool,
}

#[derive(Args, Clone)]
pub struct ServerArgs {
    #[arg(long, value_name = "ADDR", default_value = "[::1]:50051")]
    pub addr: String,
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,
}

#[derive(Subcommand, Clone)]
pub enum StatusScope {
    Batch { batch_id: String },
    Job { job_id: String },
    Jobs(StatusJobsArgs),
}

#[derive(Args, Clone)]
pub struct StatusJobsArgs {
    #[arg(long, value_name = "status", value_enum)]
    pub filter: Option<JobStatus>,
    #[arg(long, default_value_t = 50)]
    pub limit: usize,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Retrying,
    Cancelled,
}

#[derive(Subcommand, Clone)]
pub enum CancelScope {
    Batch { batch_id: String },
    Job { job_id: String },
}

#[derive(Subcommand, Clone)]
pub enum RetryScope {
    Job { job_id: String },
}

#[derive(Subcommand, Clone)]
pub enum FilesScope {
    List(ListFilesArgs),
    Get {
        hash: String,
    },
    Delete {
        hash: String,
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Args, Clone)]
pub struct ListFilesArgs {
    #[arg(long, value_name = "type")]
    pub mime: Option<String>,
    #[arg(long, value_name = "name")]
    pub provider: Option<String>,
    #[arg(long, value_name = "N", default_value_t = 100)]
    pub limit: usize,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum LogFormat {
    Pretty,
    Json,
}
