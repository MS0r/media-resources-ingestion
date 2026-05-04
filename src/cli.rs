use crate::{
    models::{IngestionConfig},
    settings::{TomlConfig, load_config as load_toml_config},
    error::BoxedError,
};
use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "media-resources-ingestion")]
#[command(version, about = "Media resources ingestion CLI tool", long_about = None)]
pub struct Cli {
    /// Path to TOML config file
    #[arg(short, long, value_name = "FILE", default_value = ".ingest/config.toml")]
    pub config: PathBuf,
    #[command(flatten)]
    pub global: Global,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Args)]
pub struct Global {
    /// Log format
    #[arg(long, value_enum, default_value_t = LogFormat::Pretty)]
    log_format: LogFormat,
    /// Increase log verbosity (-v info, -vv debug, -vvv trace)
    #[arg(short, long, action = ArgAction::Count)]
    verbose: u8,
    /// Suppress all output except errors
    #[arg(short, long)]
    quiet: bool,
    /// Disable ANSI colour output
    #[arg(long)]
    no_color: bool,
}

#[derive(Subcommand)]

pub enum Commands {
    ///Submit a YAML config and process all resources
    Run(RunArgs),
    ///Inspect jobs and batches
    Status {
        #[command(subcommand)]
        scope: StatusScope,
    },
    ///Cancel pending jobs
    Cancel {
        #[command(subcommand)]
        scope: CancelScope,
    },
    ///Manually retry a failed job
    Retry {
        #[command(subcommand)]
        scope: RetryScope,
    },
    ///Browse and manage stored files
    Files {
        #[command(subcommand)]
        scope: FilesScope,
    },
}

#[derive(Args)]
pub struct RunArgs {
    pub yaml_path: PathBuf,
    /// Validate YAML and preflight URLs without downloading
    #[arg(long)]
    dry_run: bool,
    /// Override top-level priority for this batch
    #[arg(long)]
    priority: Option<i32>,
    /// Override SCHEDULER_FILE_WORKERS for this run
    #[arg(long)]
    workers: Option<usize>,
    /// Stream live progress to the terminal
    #[arg(long, default_value_t = true, overrides_with = "no_follow")]
    follow: bool,
    /// Return immediately after enqueuing; print batch ID
    #[arg(long, overrides_with = "follow")]
    no_follow: bool,
    /// Output format for final summary
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    output: OutputFormat,
}

#[derive(Subcommand)]
pub enum StatusScope {
    ///All jobs in a batch
    Batch {
        batch_id: String,
    },
    ///All jobs for a specific resource
    Job {
        job_id: String,
    },
    ///List all jobs
    Jobs(StatusJobsArgs)
}

#[derive(Args)]
pub struct StatusJobsArgs {
    #[arg(long, value_name = "status", value_enum, help = "Filter jobs by status")]
    filter: Option<JobStatus>,

    #[arg(long, help = "Max results", default_value_t = 50)]
    limit: usize,

    #[arg(long, value_name = "fmt", value_enum, help = "Output format for final summary", default_value_t = OutputFormat::Table)]
    output: OutputFormat,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Retrying
}

#[derive(Subcommand)]
pub enum CancelScope {
    ///Cancel all pending jobs in batch
    Batch {
        batch_id: String,
    },
    ///Cancel a single pending job
    Job {
        job_id: String,
    },
}

#[derive(Subcommand)]
pub enum RetryScope {
    ///Retry a single failed job
    Job {
        job_id: String,
    },
}

#[derive(Subcommand)]
pub enum FilesScope {
    ///List stored files
    List(ListFilesArgs),
    ///Print metadata for a file
    Get {
        ///File content hash
        hash: String,
    },
    /// Reconstruct and stream a file to DEST (stdout if omitted)
    Download {
        /// File content hash
        hash: String,
        /// Destination path (omit for stdout)
        dest: Option<PathBuf>,
    },

    /// Delete a file (prompts for confirmation unless --yes)
    Delete {
        /// File content hash
        hash: String,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
    },

}

#[derive(Args)]
pub struct ListFilesArgs {
    #[arg(long, value_name = "type", help = "Filter by MIME type (e.g. image/webp)")]
    mime: Option<String>,

    #[arg(long, value_name = "name", help = "Filter by storage provider")]
    provider: Option<String>,

    #[arg(long, value_name = "DATE", help = "ISO 8601 date lower bound")]
    from: Option<String>,

    #[arg(long, value_name = "DATE", help = "ISO 8601 date upper bound")]
    to: Option<String>,

    #[arg(long, value_name = "N", default_value_t = 100)]
    limit: usize,

    #[arg(long, value_name = "fmt", value_enum, help = "Output format for final summary", default_value_t = OutputFormat::Table)]
    output: OutputFormat,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Pretty,
    Json
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum OutputFormat {
    Table,
    Json
}

pub fn load_config(path: &PathBuf) -> Result<IngestionConfig, BoxedError> {
    let content = std::fs::read_to_string(path)?;
    let request: IngestionConfig = serde_yaml::from_str(&content)?;
    Ok(request)
}

pub struct CliConfig {
    pub toml_config : TomlConfig,
    pub redis_uri: String,
    pub mongo_uri: String,
    pub cli : Cli,
}

pub fn get_config() -> Result<CliConfig, BoxedError> {
    let cli = Cli::parse();
    let toml_config = load_toml_config(&cli.config)?;
    let redis_uri = std::env::var("REDIS_URI").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let mongo_uri = std::env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017/ingestion".to_string());

    Ok(CliConfig {
        toml_config,
        redis_uri,
        mongo_uri,
        cli
    })
}
