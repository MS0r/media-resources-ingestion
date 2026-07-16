use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncRead;

use crate::{
    auth::{AuthProviderRegistry, StaticTokenProvider, TokenProvider},
    providers::{DropboxProvider, GDriveProvider, S3Provider},
};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    #[default]
    Local,
    Gdrive,
    Dropbox,
    S3,
}

impl std::fmt::Display for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Provider::Local => write!(f, "local"),
            Provider::Gdrive => write!(f, "gdrive"),
            Provider::Dropbox => write!(f, "dropbox"),
            Provider::S3 => write!(f, "s3"),
        }
    }
}

impl From<String> for Provider {
    fn from(s: String) -> Self {
        match s.to_lowercase().as_str() {
            "gdrive" => Provider::Gdrive,
            "dropbox" => Provider::Dropbox,
            "s3" => Provider::S3,
            _ => Provider::Local,
        }
    }
}

pub type DynError = Box<dyn Error + Send + Sync>;

#[async_trait]
pub trait StorageProvider: Send + Sync {
    async fn upload(&self, path: &str, file: &mut File) -> Result<(), DynError>;
    async fn download(&self, path: &str) -> Result<Box<dyn AsyncRead + Unpin + Send>, DynError>;
    async fn health_check(&self) -> Result<(), DynError>;
}

// ── LocalProvider ────────────────────────────────────────────────────────────

pub struct LocalProvider;

#[async_trait]
impl StorageProvider for LocalProvider {
    async fn upload(&self, path: &str, file: &mut File) -> Result<(), DynError> {
        let parent = std::path::Path::new(path)
            .parent()
            .ok_or("Invalid path: no parent")?;
        tokio::fs::create_dir_all(parent).await?;

        let mut out = tokio::fs::File::create(path).await?;
        tokio::io::copy(file, &mut out).await?;
        tracing::info!("Local upload: {} -> {}", path, path);
        Ok(())
    }

    async fn download(&self, path: &str) -> Result<Box<dyn AsyncRead + Unpin + Send>, DynError> {
        let file = tokio::fs::File::open(path).await?;
        Ok(Box::new(file))
    }

    async fn health_check(&self) -> Result<(), DynError> {
        Ok(())
    }
}

// ── ProviderCache (singleton per provider type) ─────────────────────────────

/// Pre-created cache of all storage providers, created once and shared via `Arc`.
///
/// Each provider is created at construction time with its default configuration.
/// `get()` returns an `Arc::clone` — a cheap ref-count bump.
///
/// Also owns the `AuthProviderRegistry` so that source-auth resolution
/// for downloads can reuse the same token providers used for storage.
pub struct ProviderCache {
    local: Arc<dyn StorageProvider>,
    gdrive: Arc<dyn StorageProvider>,
    dropbox: Arc<dyn StorageProvider>,
    s3: Arc<dyn StorageProvider>,
    auth_registry: AuthProviderRegistry,
}

impl ProviderCache {
    pub fn new(auth_registry: AuthProviderRegistry) -> Self {
        Self {
            local: Arc::new(LocalProvider),
            gdrive: Arc::new({
                let gdrive_root =
                    std::env::var("GDRIVE_PARENT_FOLDER_ID").unwrap_or_else(|_| "root".into());
                let token = auth_registry.get("gdrive").unwrap_or_else(|| {
                    Arc::new(StaticTokenProvider::new(
                        String::new(),
                        "gdrive-unconfigured",
                    ))
                });
                GDriveProvider::new(gdrive_root, token)
            }),
            dropbox: Arc::new({
                let token = auth_registry.get("dropbox").unwrap_or_else(|| {
                    Arc::new(StaticTokenProvider::new(
                        String::new(),
                        "dropbox-unconfigured",
                    ))
                });
                DropboxProvider::new(token)
            }),
            s3: Arc::new({
                let bucket = std::env::var("AWS_BUCKET").unwrap_or_else(|_| "default".into());
                S3Provider::new(bucket)
            }),
            auth_registry,
        }
    }

    pub fn get(&self, provider: &Provider) -> Arc<dyn StorageProvider> {
        match provider {
            Provider::Local => self.local.clone(),
            Provider::Gdrive => self.gdrive.clone(),
            Provider::Dropbox => self.dropbox.clone(),
            Provider::S3 => self.s3.clone(),
        }
    }

    pub fn get_token_provider(&self, name: &str) -> Option<Arc<dyn TokenProvider>> {
        self.auth_registry.get(name)
    }

    /// Attempt to detect the source provider from a URL hostname.
    pub fn detect_from_url(url: &str) -> Option<&'static str> {
        AuthProviderRegistry::detect_from_url(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[test]
    fn test_provider_default_is_local() {
        assert_eq!(Provider::default(), Provider::Local);
    }

    #[test]
    fn test_provider_display() {
        assert_eq!(Provider::Local.to_string(), "local");
        assert_eq!(Provider::Gdrive.to_string(), "gdrive");
        assert_eq!(Provider::Dropbox.to_string(), "dropbox");
        assert_eq!(Provider::S3.to_string(), "s3");
    }

    #[test]
    fn test_provider_from_string() {
        assert_eq!(Provider::from("local".to_string()), Provider::Local);
        assert_eq!(Provider::from("LOCAL".to_string()), Provider::Local);
        assert_eq!(Provider::from("gdrive".to_string()), Provider::Gdrive);
        assert_eq!(Provider::from("GDRIVE".to_string()), Provider::Gdrive);
        assert_eq!(Provider::from("dropbox".to_string()), Provider::Dropbox);
        assert_eq!(Provider::from("s3".to_string()), Provider::S3);
        assert_eq!(Provider::from("unknown".to_string()), Provider::Local);
        assert_eq!(Provider::from("".to_string()), Provider::Local);
    }

    #[test]
    fn test_provider_serde_roundtrip() {
        for p in [
            Provider::Local,
            Provider::Gdrive,
            Provider::Dropbox,
            Provider::S3,
        ] {
            let json = serde_json::to_string(&p).unwrap();
            let deserialized: Provider = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, p);
        }
    }

    #[tokio::test]
    async fn test_local_provider_health_check() {
        let provider = LocalProvider;
        let result = provider.health_check();
        assert!(result.await.is_ok());
    }

    #[tokio::test]
    async fn test_local_provider_upload_download_roundtrip() {
        let tmp = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let dest = tmp.join("test_file.bin");
        let dest_str = dest.to_string_lossy().to_string();

        // Create source data
        let src = tmp.join("source.bin");
        let content = b"hello world this is a test file for local provider";
        tokio::fs::write(&src, content).await.unwrap();

        let provider = LocalProvider;
        let mut src_file = tokio::fs::File::open(&src).await.unwrap();
        provider.upload(&dest_str, &mut src_file).await.unwrap();

        // Verify destination file exists and has correct content
        assert!(dest.exists());
        let mut downloaded = provider.download(&dest_str).await.unwrap();
        let mut buf = Vec::new();
        downloaded.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, content);

        tokio::fs::remove_dir_all(&tmp).await.ok();
    }

    #[tokio::test]
    async fn test_provider_cache_get_local() {
        let cache = ProviderCache::new(AuthProviderRegistry::new());
        let local = cache.get(&Provider::Local);
        // verify it returns a valid provider (health_check passes)
        assert!(local.health_check().await.is_ok());
    }
}
