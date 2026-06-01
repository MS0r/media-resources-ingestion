use crate::error::ToolError;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerConfig {
    pub file_workers: usize,
    pub chunk_workers: usize,
    pub max_pending_jobs: usize,
    pub max_per_host: usize,
    #[serde(default = "default_job_timeout")]
    pub job_timeout_secs: u64,
}

const fn default_job_timeout() -> u64 {
    7200
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompressionConfig {
    pub threshold_mb: u64,
    pub quality: u8,
    #[serde(default = "default_compression_timeout")]
    pub max_compression_seconds: u64,
}

const fn default_compression_timeout() -> u64 {
    300
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub default_provider: String,
    pub default_path: String,
    pub chunk_size: String,
    pub temp_dir: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_running_job_ttl")]
    pub running_job_ttl_secs: u64,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u8,
    #[serde(default = "default_backoff_secs")]
    pub backoff_secs: Vec<u64>,
}

const fn default_running_job_ttl() -> u64 {
    3600
}

const fn default_max_attempts() -> u8 {
    3
}

fn default_backoff_secs() -> Vec<u64> {
    vec![5, 30, 120]
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            running_job_ttl_secs: default_running_job_ttl(),
            max_attempts: default_max_attempts(),
            backoff_secs: default_backoff_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TomlRawConfig {
    pub scheduler: SchedulerConfig,
    pub compression: CompressionConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub retry: RetryConfig,
}

pub fn load_toml(path: &PathBuf) -> Result<TomlRawConfig, ToolError> {
    let toml_fs = std::fs::read_to_string(path)?;
    let config: TomlRawConfig = toml::from_str(&toml_fs)?;
    Ok(config)
}
