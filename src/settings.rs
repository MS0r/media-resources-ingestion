use crate::error::ToolError;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerConfig {
    pub file_workers: usize,
    pub chunk_workers: usize,
    pub max_pending_jobs: usize,
    pub max_per_host: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompressionConfig {
    pub threshold_mb: u64,
    pub quality: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub default_provider: String,
    pub default_path: String,
    pub chunk_size: String,
    pub temp_dir: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TomlRawConfig {
    pub scheduler: SchedulerConfig,
    pub compression: CompressionConfig,
    pub storage: StorageConfig,
}

pub fn load_toml(path: &PathBuf) -> Result<TomlRawConfig, ToolError> {
    let toml_fs = std::fs::read_to_string(path)?;
    let config: TomlRawConfig = toml::from_str(&toml_fs)?;
    Ok(config)
}
