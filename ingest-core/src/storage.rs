use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncRead;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;

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

pub struct LocalProvider;
pub struct GDriveProvider;

async fn start_drive_session(token: &str, filename: &str) -> Result<String, DynError> {
    let client = wreq::Client::new();

    let res = client
        .post("https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable")
        .bearer_auth(token)
        .header("X-Upload-Content-Type", "application/octet-stream")
        .header("Content-Length", "0")
        .header(
            "X-Upload-Content-Length",
            &std::fs::metadata(filename)?.len().to_string(),
        )
        .send()
        .await?;

    Ok(res
        .headers()
        .get("Location")
        .ok_or("Missing Location header")?
        .to_str()?
        .to_string())
}

#[async_trait]
pub trait StorageProvider: Send + Sync {
    async fn upload(&self, path: &str, file: &mut File)
    -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn download(
        &self,
        path: &str,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send>, Box<dyn Error + Send + Sync>>;
    async fn health_check(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub type DynError = Box<dyn Error + Send + Sync>;

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

#[async_trait]
impl StorageProvider for GDriveProvider {
    async fn upload(&self, _path: &str, _file: &mut File) -> Result<(), DynError> {
        todo!()
    }

    async fn download(&self, _path: &str) -> Result<Box<dyn AsyncRead + Unpin + Send>, DynError> {
        todo!()
    }

    async fn health_check(&self) -> Result<(), DynError> {
        Err("Google Drive not implemented".into())
    }
}

pub struct DropboxProvider;

#[async_trait]
impl StorageProvider for DropboxProvider {
    async fn upload(&self, _path: &str, _file: &mut File) -> Result<(), DynError> {
        todo!()
    }

    async fn download(&self, _path: &str) -> Result<Box<dyn AsyncRead + Unpin + Send>, DynError> {
        todo!()
    }

    async fn health_check(&self) -> Result<(), DynError> {
        Err("Dropbox not implemented".into())
    }
}

pub struct S3Provider;

#[async_trait]
impl StorageProvider for S3Provider {
    async fn upload(&self, _path: &str, _file: &mut File) -> Result<(), DynError> {
        Err("S3Provider is a stub, not implemented".into())
    }

    async fn download(&self, _path: &str) -> Result<Box<dyn AsyncRead + Unpin + Send>, DynError> {
        Err("S3Provider is a stub, not implemented".into())
    }

    async fn health_check(&self) -> Result<(), DynError> {
        Err("S3 not implemented".into())
    }
}

impl Provider {
    pub fn into_storage(&self) -> Arc<dyn StorageProvider> {
        match self {
            Provider::Local => Arc::new(LocalProvider),
            Provider::Gdrive => Arc::new(GDriveProvider),
            Provider::Dropbox => Arc::new(DropboxProvider),
            Provider::S3 => Arc::new(S3Provider),
        }
    }
}
