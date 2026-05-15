use crate::{error::ToolError, models::IngestionConfig};
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
    pub log_format: String,
    pub no_color: bool,
    pub custom_flags: CustomFlags,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CustomFlags {
    pub dry_run: bool,
    pub follow: bool,
    pub output: String,
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
    pub threshold_mb : u64,
    pub quality : u8
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub default_provider: String,
    pub default_path: String,
    pub chunk_size: String
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
    pub attempt_1_secs: u16,
    pub attempt_2_secs: u16,
    pub attempt_3_secs: u16,
}

pub fn load_config(path : &PathBuf) -> Result<TomlConfig, ToolError> {
    let toml_fs = std::fs::read_to_string(path)?;
    let config: TomlConfig = toml::from_str(&toml_fs)?;
    Ok(config)
}

pub fn merge_configs_yaml(yaml: &IngestionConfig, toml: TomlConfig) -> Result<TomlConfig, ToolError> {
    let mut merged = toml.clone();

    if let Some(ref dest) = yaml.default_dest {
        if let Some(ref provider) = dest.provider {
            merged.storage.default_provider = provider.to_string();
        }
        if let Some(ref path) = dest.path {
            merged.storage.default_path = path.clone();
        }
    }

    if let Some(ref chunk_size) = yaml.chunk_size {
        merged.storage.chunk_size = chunk_size.clone();
    }
    
    Ok(merged)
}

