use std::path::PathBuf;

use crate::models::OutputFormat;

/// Non-CLI configuration for the `run` command.
/// Populated by ingest-cli from Clap args, or by ingest-server from gRPC request fields.
#[derive(Debug, Clone)]
pub struct RunConfig {
    pub yaml_path: PathBuf,
    pub dry_run: bool,
    pub priority: Option<i32>,
    pub workers: Option<usize>,
    pub follow: bool,
    pub no_follow: bool,
    pub output: OutputFormat,
}

/// Non-CLI configuration for the `enqueue` command.
#[derive(Debug, Clone)]
pub struct EnqueueConfig {
    pub yaml_path: PathBuf,
    pub dry_run: bool,
    pub priority: Option<i32>,
    pub workers: Option<usize>,
    pub output: OutputFormat,
}
