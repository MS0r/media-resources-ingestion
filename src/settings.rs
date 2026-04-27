use crate::cli::LogFormat;
use std::path::PathBuf;
use serde::{Deserialize};

#[derive(Debug, Clone, Deserialize)]
pub struct TomlConfig {
    pub cli : CliConfig,
    pub scheduler : SchedulerConfig,
    pub compression : CompressionConfig,
    pub storage : StorageConfig,
    pub retry : RetryConfig
}

#[derive(Debug, Clone, Deserialize)]
pub struct CliConfig {
    pub log_format: LogFormat,
    pub no_color: bool
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerConfig {
    pub file_workers: usize,
    pub chunk_workers : usize,
    pub max_pending_jobs: usize,
    pub max_per_host : usize
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompressionConfig {
    pub threshold_mb : usize,
    pub quality : u8
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub default_provider: String,
    pub default_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
    pub attempt_1_secs: u16,
    pub attempt_2_secs: u16,
    pub attempt_3_secs: u16,
}

pub fn load_config(path : PathBuf) -> Result<TomlConfig, Box<dyn std::error::Error>> {
    let toml_fs = std::fs::read_to_string(path)?;
    let config: TomlConfig = toml::from_str(&toml_fs)?;
    Ok(config)
}