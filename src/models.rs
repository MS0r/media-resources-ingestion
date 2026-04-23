use serde::{Deserialize, Serialize};

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
    pub url: String,
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