use crate::{
    error::ToolError,
    models::{Destination, IngestionConfig},
    settings::{TomlConfig, load_config as load_toml_config},
    storage::Provider,
};
use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "ingest")]
#[command(version, about = "Media resources ingestion CLI tool", long_about = None)]
pub struct Cli {
    /// Path to TOML config file
    #[arg(
        short,
        long,
        value_name = "FILE",
        default_value = ".ingest/config.toml"
    )]
    pub config: PathBuf,
    #[command(flatten)]
    pub global: Global,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Args, Clone)]
pub struct Global {
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
}

#[derive(Subcommand, Clone)]
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

#[derive(Args, Clone)]
pub struct RunArgs {
    pub yaml_path: PathBuf,
    /// Validate YAML and preflight URLs without downloading
    #[arg(long)]
    pub dry_run: bool,
    /// Override top-level priority for this batch
    #[arg(long)]
    pub priority: Option<i32>,
    /// Override SCHEDULER_FILE_WORKERS for this run
    #[arg(long)]
    pub workers: Option<usize>,
    /// Stream live progress to the terminal
    #[arg(long, overrides_with = "no_follow")]
    pub follow: bool,
    /// Return immediately after enqueuing; print batch ID
    #[arg(long, overrides_with = "follow")]
    pub no_follow: bool,
    /// Output format for final summary
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
}

#[derive(Subcommand, Clone)]
pub enum StatusScope {
    ///All jobs in a batch
    Batch { batch_id: String },
    ///All jobs for a specific resource
    Job { job_id: String },
    ///List all jobs
    Jobs(StatusJobsArgs),
}

#[derive(Args, Clone)]
pub struct StatusJobsArgs {
    #[arg(
        long,
        value_name = "status",
        value_enum,
        help = "Filter jobs by status"
    )]
    pub filter: Option<JobStatus>,

    #[arg(long, help = "Max results", default_value_t = 50)]
    pub limit: usize,

    #[arg(long, value_name = "fmt", value_enum, help = "Output format", default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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
    ///Cancel all pending jobs in batch
    Batch { batch_id: String },
    ///Cancel a single pending job
    Job { job_id: String },
}

#[derive(Subcommand, Clone)]
pub enum RetryScope {
    ///Retry a single failed job
    Job { job_id: String },
}

#[derive(Subcommand, Clone)]
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

#[derive(Args, Clone)]
pub struct ListFilesArgs {
    #[arg(
        long,
        value_name = "type",
        help = "Filter by MIME type (e.g. image/webp)"
    )]
    pub mime: Option<String>,

    #[arg(long, value_name = "name", help = "Filter by storage provider")]
    pub provider: Option<String>,

    #[arg(long, value_name = "DATE", help = "ISO 8601 date lower bound")]
    pub from: Option<String>,

    #[arg(long, value_name = "DATE", help = "ISO 8601 date upper bound")]
    pub to: Option<String>,

    #[arg(long, value_name = "N", default_value_t = 100)]
    pub limit: usize,

    #[arg(long, value_name = "fmt", value_enum, help = "Output format", default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Pretty,
    Json,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
}

pub fn load_config(path: &PathBuf) -> Result<IngestionConfig, ToolError> {
    let content = std::fs::read_to_string(path)?;
    let mut request: IngestionConfig = serde_yaml::from_str(&content).map_err(|e| {
        tracing::error!("YAML parse error: {}", e);
        ToolError::ConfigError(format!("YAML parse error: {}", e))
    })?;

    let dest = request.default_dest.get_or_insert_with(|| Destination {
        provider: None,
        path: None,
    });
    if dest.provider.is_none() {
        dest.provider = Some(Provider::Local);
    }
    if dest.path.is_none() {
        let cwd = std::env::current_dir().map_err(|e| {
            ToolError::ConfigError(format!("Failed to get current directory: {}", e))
        })?;
        dest.path = Some(cwd.to_string_lossy().to_string());
    }

    Ok(request)
}

pub struct CliConfig {
    pub toml_config: TomlConfig,
    pub redis_uri: String,
    pub mongo_uri: String,
    pub cli: Cli,
}

pub fn get_config() -> Result<CliConfig, ToolError> {
    let cli = Cli::parse();
    let toml_config = load_toml_config(&cli.config)?;

    let redis_uri = std::env::var("REDIS_URI").map_err(|_| {
        tracing::error!("REDIS_URI environment variable is required");
        ToolError::ConfigError("REDIS_URI not set".to_string())
    })?;

    let mongo_uri = std::env::var("MONGODB_URI").map_err(|_| {
        tracing::error!("MONGODB_URI environment variable is required");
        ToolError::ConfigError("MONGODB_URI not set".to_string())
    })?;

    // Validate secret environment variables based on providers used
    validate_secrets()?;

    Ok(CliConfig {
        toml_config,
        redis_uri,
        mongo_uri,
        cli,
    })
}

fn validate_secrets() -> Result<(), ToolError> {
    // Check AWS credentials if S3 provider is needed
    if std::env::var("AWS_S3_BUCKET").is_ok() {
        if std::env::var("AWS_ACCESS_KEY_ID").is_err()
            || std::env::var("AWS_SECRET_ACCESS_KEY").is_err()
        {
            tracing::error!(
                "AWS credentials required: set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_S3_BUCKET"
            );
            return Err(ToolError::AuthError("AWS credentials required: set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_S3_BUCKET".to_string()));
        }
    }

    // Check Google Drive credentials if needed
    if std::env::var("GDRIVE_CLIENT_ID").is_ok() {
        if std::env::var("GDRIVE_CLIENT_SECRET").is_err()
            || std::env::var("GDRIVE_REFRESH_TOKEN").is_err()
        {
            tracing::error!("Google Drive credentials incomplete");
            return Err(ToolError::AuthError(
                "Google Drive credentials incomplete".to_string(),
            ));
        }
    }

    // Check Dropbox credentials if needed
    if std::env::var("DROPBOX_APP_KEY").is_ok() {
        if std::env::var("DROPBOX_APP_SECRET").is_err()
            || std::env::var("DROPBOX_REFRESH_TOKEN").is_err()
        {
            tracing::error!("Dropbox credentials incomplete");
            return Err(ToolError::AuthError(
                "Dropbox credentials incomplete".to_string(),
            ));
        }
    }

    Ok(())
}
