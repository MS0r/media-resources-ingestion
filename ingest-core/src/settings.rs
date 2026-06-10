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

#[cfg(test)]
mod tests {
    use super::*;

    const TOML_FULL: &str = r#"
[scheduler]
file_workers = 5
chunk_workers = 20
max_pending_jobs = 10000
max_per_host = 2

[compression]
threshold_mb = 512
quality = 95

[storage]
default_provider = "local"
default_path = "~/downloads"
chunk_size = "128MB"
temp_dir = "/tmp/ingest"
"#;

    const TOML_MINIMAL: &str = r#"
[scheduler]
file_workers = 1
chunk_workers = 1
max_pending_jobs = 0
max_per_host = 1

[compression]
threshold_mb = 0
quality = 0

[storage]
default_provider = "s3"
default_path = "~"
chunk_size = "64MB"
temp_dir = "/tmp/ingest"
"#;

    const TOML_LARGE: &str = r#"
[scheduler]
file_workers = 100
chunk_workers = 200
max_pending_jobs = 1000000
max_per_host = 10

[compression]
threshold_mb = 1024
quality = 100

[storage]
default_provider = "s3"
default_path = "/mnt/storage"
chunk_size = "512MB"
temp_dir = "/tmp/ingest"
"#;

    #[test]
    fn test_toml_config_full_deserialization() {
        let config: TomlRawConfig = toml::from_str(TOML_FULL).expect("Failed to parse TOML");

        assert_eq!(config.scheduler.file_workers, 5);
        assert_eq!(config.scheduler.chunk_workers, 20);
        assert_eq!(config.scheduler.max_pending_jobs, 10000);
        assert_eq!(config.scheduler.max_per_host, 2);
        assert_eq!(config.compression.threshold_mb, 512);
        assert_eq!(config.compression.quality, 95);
        assert_eq!(config.storage.default_provider, "local");
        assert_eq!(config.storage.default_path, "~/downloads");
        assert_eq!(config.storage.chunk_size, "128MB");
    }

    #[test]
    fn test_toml_config_minimal_values() {
        let config: TomlRawConfig = toml::from_str(TOML_MINIMAL).expect("Failed to parse TOML");

        assert_eq!(config.scheduler.file_workers, 1);
        assert_eq!(config.scheduler.max_per_host, 1);
        assert_eq!(config.compression.quality, 0);
    }

    #[test]
    fn test_toml_config_large_values() {
        let config: TomlRawConfig = toml::from_str(TOML_LARGE).expect("Failed to parse TOML");

        assert_eq!(config.scheduler.file_workers, 100);
        assert_eq!(config.scheduler.chunk_workers, 200);
        assert_eq!(config.scheduler.max_pending_jobs, 1_000_000);
        assert_eq!(config.compression.threshold_mb, 1024);
        assert_eq!(config.compression.quality, 100);
    }
}
