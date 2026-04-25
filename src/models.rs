use serde::{Deserialize, Serialize};
use crate::cli::LogFormat;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Headers {
    pub authorization: Option<String>,
    pub cookie: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ImageCompressionStrategy {
    #[default]
    Avif,
    Webp,
    LosslessWebp,
    OriginalFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum VideoCompressionStrategy {
    #[default]
    H265,
    Av1,
    OriginalFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum GenericCompressionStrategy {
    #[default]
    Zstd,
    Zip,
    SevenZ,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum UniversalCompressionStrategy {
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CompressionOverride {
    Image(ImageCompressionStrategy),
    Video(VideoCompressionStrategy),
    Generic(GenericCompressionStrategy),
    Universal(UniversalCompressionStrategy),
}

impl Default for CompressionOverride {
    fn default() -> Self {
        CompressionOverride::Universal(UniversalCompressionStrategy::None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceLevelConfig {
    pub force_compress: Option<bool>,
    #[serde(default)]
    pub compression_override: Option<CompressionOverride>,
    pub quality: Option<u8>,
    #[serde(default)]
    pub headers: Option<Headers>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Destination {
    #[serde(default)]
    pub provider: Option<Provider>,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    pub url: Url,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub dest: Option<Destination>,
    #[serde(default)]
    pub config: Option<ResourceLevelConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    #[default]
    Local,
    Gdrive,
    Dropbox,
    S3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionRequest {
    #[serde(default = "default_provider")]
    pub provider: Provider,
    pub path: String,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub chunk_size: Option<String>,
    #[serde(default)]
    pub compression_override: Option<CompressionOverride>,
    #[serde(default)]
    pub headers: Option<Headers>,
    pub resources: Vec<Resource>,
}

fn default_provider() -> Provider {
    Provider::Local
}

#[derive(Debug, Clone, Deserialize)]
pub struct TomlConfig {
    cli : CliConfig,
    scheduler : SchedulerConfig,
    compression : CompressionConfig,
    storage : StorageConfig,
    retry : RetryConfig
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