use crate::{error::ToolError, models::IngestionConfig};
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
    ///Parse YAML and enqueue jobs to Redis; no download (same as run --no-follow)
    Enqueue(EnqueueArgs),
    ///Start a standalone worker that picks up pending jobs from Redis
    Worker(WorkerArgs),
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

#[derive(Args, Clone)]
pub struct EnqueueArgs {
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
    /// Output format for final summary
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
}

#[derive(Args, Clone)]
pub struct WorkerArgs {
    /// Override SCHEDULER_FILE_WORKERS for this worker
    #[arg(long)]
    pub workers: Option<usize>,
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
    let request: IngestionConfig = serde_yaml::from_str(&content).map_err(|e| {
        tracing::error!("YAML parse error: {}", e);
        ToolError::ConfigError(format!("YAML parse error: {}", e))
    })?;

    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::io::Write;

    #[test]
    fn test_cli_run_basic() {
        let cli = Cli::try_parse_from(["ingest", "run", "test.yaml"]).unwrap();
        assert!(
            matches!(cli.command, Commands::Run(ref args) if args.yaml_path.to_string_lossy() == "test.yaml")
        );
    }

    #[test]
    fn test_cli_run_all_flags() {
        let cli = Cli::try_parse_from([
            "ingest",
            "run",
            "test.yaml",
            "--dry-run",
            "--priority",
            "42",
            "--workers",
            "10",
            "--no-follow",
            "--output",
            "json",
        ])
        .unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert!(args.dry_run);
                assert_eq!(args.priority, Some(42));
                assert_eq!(args.workers, Some(10));
                assert!(args.no_follow);
                assert!(matches!(args.output, OutputFormat::Json));
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_defaults() {
        let cli = Cli::try_parse_from(["ingest", "run", "test.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert!(!args.dry_run);
                assert_eq!(args.priority, None);
                assert_eq!(args.workers, None);
                assert!(!args.follow);
                assert!(!args.no_follow);
                assert!(matches!(args.output, OutputFormat::Table));
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_follow_overrides_no_follow() {
        let cli =
            Cli::try_parse_from(["ingest", "run", "test.yaml", "--no-follow", "--follow"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert!(args.follow);
                assert!(!args.no_follow);
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_run_no_follow_overrides_follow() {
        let cli =
            Cli::try_parse_from(["ingest", "run", "test.yaml", "--follow", "--no-follow"]).unwrap();
        match cli.command {
            Commands::Run(args) => {
                assert!(!args.follow);
                assert!(args.no_follow);
            }
            _ => panic!("expected Run command"),
        }
    }

    #[test]
    fn test_cli_status_batch() {
        let cli = Cli::try_parse_from(["ingest", "status", "batch", "b_abc123"]).unwrap();
        match cli.command {
            Commands::Status { scope } => match scope {
                StatusScope::Batch { batch_id } => assert_eq!(batch_id, "b_abc123"),
                _ => panic!("expected Batch scope"),
            },
            _ => panic!("expected Status command"),
        }
    }

    #[test]
    fn test_cli_status_job() {
        let cli = Cli::try_parse_from(["ingest", "status", "job", "j_xyz"]).unwrap();
        match cli.command {
            Commands::Status { scope } => match scope {
                StatusScope::Job { job_id } => assert_eq!(job_id, "j_xyz"),
                _ => panic!("expected Job scope"),
            },
            _ => panic!("expected Status command"),
        }
    }

    #[test]
    fn test_cli_status_jobs_defaults() {
        let cli = Cli::try_parse_from(["ingest", "status", "jobs"]).unwrap();
        match cli.command {
            Commands::Status { scope } => match scope {
                StatusScope::Jobs(args) => {
                    assert!(args.filter.is_none());
                    assert_eq!(args.limit, 50);
                    assert!(matches!(args.output, OutputFormat::Table));
                }
                _ => panic!("expected Jobs scope"),
            },
            _ => panic!("expected Status command"),
        }
    }

    #[test]
    fn test_cli_status_jobs_all_flags() {
        let cli = Cli::try_parse_from([
            "ingest", "status", "jobs", "--filter", "failed", "--limit", "100", "--output", "json",
        ])
        .unwrap();
        match cli.command {
            Commands::Status { scope } => match scope {
                StatusScope::Jobs(args) => {
                    assert_eq!(args.filter, Some(JobStatus::Failed));
                    assert_eq!(args.limit, 100);
                    assert!(matches!(args.output, OutputFormat::Json));
                }
                _ => panic!("expected Jobs scope"),
            },
            _ => panic!("expected Status command"),
        }
    }

    #[test]
    fn test_cli_cancel_batch() {
        let cli = Cli::try_parse_from(["ingest", "cancel", "batch", "b_abc"]).unwrap();
        match cli.command {
            Commands::Cancel { scope } => match scope {
                CancelScope::Batch { batch_id } => assert_eq!(batch_id, "b_abc"),
                _ => panic!("expected Cancel batch"),
            },
            _ => panic!("expected Cancel command"),
        }
    }

    #[test]
    fn test_cli_cancel_job() {
        let cli = Cli::try_parse_from(["ingest", "cancel", "job", "j_def"]).unwrap();
        match cli.command {
            Commands::Cancel { scope } => match scope {
                CancelScope::Job { job_id } => assert_eq!(job_id, "j_def"),
                _ => panic!("expected Cancel job"),
            },
            _ => panic!("expected Cancel command"),
        }
    }

    #[test]
    fn test_cli_retry_job() {
        let cli = Cli::try_parse_from(["ingest", "retry", "job", "j_ghi"]).unwrap();
        match cli.command {
            Commands::Retry { scope } => match scope {
                RetryScope::Job { job_id } => assert_eq!(job_id, "j_ghi"),
            },
            _ => panic!("expected Retry command"),
        }
    }

    #[test]
    fn test_cli_files_list_defaults() {
        let cli = Cli::try_parse_from(["ingest", "files", "list"]).unwrap();
        match cli.command {
            Commands::Files { scope } => match scope {
                FilesScope::List(args) => {
                    assert!(args.mime.is_none());
                    assert!(args.provider.is_none());
                    assert!(args.from.is_none());
                    assert!(args.to.is_none());
                    assert_eq!(args.limit, 100);
                    assert!(matches!(args.output, OutputFormat::Table));
                }
                _ => panic!("expected Files List"),
            },
            _ => panic!("expected Files command"),
        }
    }

    #[test]
    fn test_cli_files_list_all_filters() {
        let cli = Cli::try_parse_from([
            "ingest",
            "files",
            "list",
            "--mime",
            "image/webp",
            "--provider",
            "local",
            "--from",
            "2024-01-01",
            "--to",
            "2024-12-31",
            "--limit",
            "50",
            "--output",
            "json",
        ])
        .unwrap();
        match cli.command {
            Commands::Files { scope } => match scope {
                FilesScope::List(args) => {
                    assert_eq!(args.mime.unwrap(), "image/webp");
                    assert_eq!(args.provider.unwrap(), "local");
                    assert_eq!(args.from.unwrap(), "2024-01-01");
                    assert_eq!(args.to.unwrap(), "2024-12-31");
                    assert_eq!(args.limit, 50);
                    assert!(matches!(args.output, OutputFormat::Json));
                }
                _ => panic!("expected Files List"),
            },
            _ => panic!("expected Files command"),
        }
    }

    #[test]
    fn test_cli_files_get() {
        let cli = Cli::try_parse_from(["ingest", "files", "get", "abc123hash"]).unwrap();
        match cli.command {
            Commands::Files { scope } => match scope {
                FilesScope::Get { hash } => assert_eq!(hash, "abc123hash"),
                _ => panic!("expected Files Get"),
            },
            _ => panic!("expected Files command"),
        }
    }

    #[test]
    fn test_cli_files_download_with_dest() {
        let cli =
            Cli::try_parse_from(["ingest", "files", "download", "hash123", "/tmp/output.bin"])
                .unwrap();
        match cli.command {
            Commands::Files { scope } => match scope {
                FilesScope::Download { hash, dest } => {
                    assert_eq!(hash, "hash123");
                    assert_eq!(dest.unwrap().to_string_lossy(), "/tmp/output.bin");
                }
                _ => panic!("expected Files Download"),
            },
            _ => panic!("expected Files command"),
        }
    }

    #[test]
    fn test_cli_files_download_stdout() {
        let cli = Cli::try_parse_from(["ingest", "files", "download", "hash123"]).unwrap();
        match cli.command {
            Commands::Files { scope } => match scope {
                FilesScope::Download { hash, dest } => {
                    assert_eq!(hash, "hash123");
                    assert!(dest.is_none());
                }
                _ => panic!("expected Files Download"),
            },
            _ => panic!("expected Files command"),
        }
    }

    #[test]
    fn test_cli_files_delete_with_yes() {
        let cli = Cli::try_parse_from(["ingest", "files", "delete", "hash123", "--yes"]).unwrap();
        match cli.command {
            Commands::Files { scope } => match scope {
                FilesScope::Delete { hash, yes } => {
                    assert_eq!(hash, "hash123");
                    assert!(yes);
                }
                _ => panic!("expected Files Delete"),
            },
            _ => panic!("expected Files command"),
        }
    }

    #[test]
    fn test_cli_files_delete_no_flag() {
        let cli = Cli::try_parse_from(["ingest", "files", "delete", "hash123"]).unwrap();
        match cli.command {
            Commands::Files { scope } => match scope {
                FilesScope::Delete { hash, yes } => {
                    assert_eq!(hash, "hash123");
                    assert!(!yes);
                }
                _ => panic!("expected Files Delete"),
            },
            _ => panic!("expected Files command"),
        }
    }

    #[test]
    fn test_cli_global_flags() {
        let cli = Cli::try_parse_from([
            "ingest",
            "--config",
            "/custom/config.toml",
            "--log-format",
            "json",
            "-v",
            "-q",
            "--no-color",
            "run",
            "test.yaml",
        ])
        .unwrap();
        assert_eq!(cli.config.to_string_lossy(), "/custom/config.toml");
        assert!(matches!(cli.global.log_format, LogFormat::Json));
        assert_eq!(cli.global.verbose, 1);
        assert!(cli.global.quiet);
        assert!(cli.global.no_color);
    }

    #[test]
    fn test_cli_verbose_counts() {
        let cli = Cli::try_parse_from(["ingest", "-v", "run", "test.yaml"]).unwrap();
        assert_eq!(cli.global.verbose, 1);

        let cli = Cli::try_parse_from(["ingest", "-vv", "run", "test.yaml"]).unwrap();
        assert_eq!(cli.global.verbose, 2);

        let cli = Cli::try_parse_from(["ingest", "-vvv", "run", "test.yaml"]).unwrap();
        assert_eq!(cli.global.verbose, 3);
    }

    #[test]
    fn test_cli_invalid_subcommand() {
        let result = Cli::try_parse_from(["ingest", "unknown", "test.yaml"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_missing_required_arg() {
        let result = Cli::try_parse_from(["ingest", "run"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_job_status_serde() {
        let status = JobStatus::Pending;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"pending\"");
        let back: JobStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);
    }

    #[test]
    fn test_job_status_serde_all_variants() {
        for (input, expected) in [
            ("pending", JobStatus::Pending),
            ("running", JobStatus::Running),
            ("completed", JobStatus::Completed),
            ("failed", JobStatus::Failed),
            ("retrying", JobStatus::Retrying),
            ("cancelled", JobStatus::Cancelled),
        ] {
            let json = format!("\"{}\"", input);
            let deser: JobStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(deser, expected, "failed for {}", input);
        }
    }

    #[test]
    fn test_output_format_default_table() {
        let cli = Cli::try_parse_from(["ingest", "run", "test.yaml"]).unwrap();
        match cli.command {
            Commands::Run(args) => assert!(matches!(args.output, OutputFormat::Table)),
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn test_cli_help_does_not_panic() {
        let result = Cli::try_parse_from(["ingest", "--help"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_config_yields_none_default_dest_when_omitted() {
        let dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let yaml_path = dir.join("test.yaml");
        let mut f = std::fs::File::create(&yaml_path).unwrap();
        f.write_all(b"resources:\n  - url: https://example.com/f.png\n")
            .unwrap();
        f.flush().unwrap();

        let config = load_config(&yaml_path).unwrap();
        let dest = config.default_dest.expect("default_dest should be Some");
        assert!(dest.provider.is_none());
        assert!(dest.path.is_none());

        std::fs::remove_dir_all(&dir).ok();
    }
}

pub fn load_env_uris() -> Result<(String, String), ToolError> {
    let redis_uri = std::env::var("REDIS_URI")?;
    let mongo_uri = std::env::var("MONGODB_URI")?;

    validate_secrets()?;

    Ok((redis_uri, mongo_uri))
}

fn validate_secrets() -> Result<(), ToolError> {
    // Check AWS credentials if S3 provider is needed
    if std::env::var("AWS_S3_BUCKET").is_ok()
        && (std::env::var("AWS_ACCESS_KEY_ID").is_err()
            || std::env::var("AWS_SECRET_ACCESS_KEY").is_err())
    {
        tracing::warn!(
            "AWS credentials not given: set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_S3_BUCKET"
        );
    }

    // Check Google Drive credentials if needed
    if std::env::var("GDRIVE_CLIENT_ID").is_ok()
        && (std::env::var("GDRIVE_CLIENT_SECRET").is_err()
            || std::env::var("GDRIVE_REFRESH_TOKEN").is_err())
    {
        tracing::warn!("Google Drive credentials incomplete");
    }

    // Check Dropbox credentials if needed
    if std::env::var("DROPBOX_APP_KEY").is_ok()
        && (std::env::var("DROPBOX_APP_SECRET").is_err()
            || std::env::var("DROPBOX_REFRESH_TOKEN").is_err())
    {
        tracing::warn!("Dropbox credentials incomplete");
    }

    Ok(())
}
