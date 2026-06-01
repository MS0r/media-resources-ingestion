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
        .json(&serde_json::json!({ "name": filename }))
        .send()
        .await?;

    Ok(res.headers()["Location"].to_str()?.to_string())
}

pub struct DropboxProvider;
pub struct S3Provider;

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

pub type DynError = Box<dyn Error + Send + Sync>;

#[async_trait]
pub trait StorageProvider: Send + Sync {
    async fn upload(
        &self,
        key: &str,
        data: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(), DynError>;

    async fn download(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, DynError>;

    async fn delete(&self, key: &str) -> Result<(), DynError>;

    /// Whether this provider needs a local temp file before the hash is known.
    /// Remote providers stream directly; local needs temp-then-rename to avoid
    /// overwriting an existing file before we know if it's a duplicate.
    fn requires_local_staging(&self) -> bool {
        false // remote providers override to false (default)
    }

    async fn commit_temp(&self, _temp_path: &str, _final_path: &str) -> Result<(), DynError> {
        // Default implementation does nothing; local provider will override to rename
        Ok(())
    }

    /// Verify the storage backend is reachable and operational.
    /// Called before upload to fail fast when the provider is down.
    /// Default implementation returns Ok — override to add real checks.
    async fn health_check(&self) -> Result<(), DynError> {
        Ok(())
    }
}
#[async_trait]
impl StorageProvider for LocalProvider {
    async fn upload(
        &self,
        key: &str,
        data: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(), DynError> {
        tracing::info!("Uploading locally: {}", key);
        // Ensure parent directory exists
        if let Some(parent) = std::path::Path::new(key).parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = File::create(key).await?;
        tokio::io::copy(data, &mut file).await?;
        Ok(())
    }

    async fn download(&self, key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, DynError> {
        println!("Downloading locally: {}", key);
        let file = File::open(key).await?;
        Ok(Box::new(file) as Box<dyn AsyncRead + Send + Unpin>)
    }

    async fn delete(&self, key: &str) -> Result<(), DynError> {
        println!("Deleting locally: {}", key);
        tokio::fs::remove_file(key).await?;
        Ok(())
    }

    fn requires_local_staging(&self) -> bool {
        true
    }

    async fn commit_temp(&self, temp_path: &str, final_path: &str) -> Result<(), DynError> {
        tokio::fs::rename(temp_path, final_path).await?;
        Ok(())
    }

    /// Verifies the local filesystem is writable by checking /tmp.
    async fn health_check(&self) -> Result<(), DynError> {
        let probe = std::path::Path::new("/tmp/.ingest_health_check");
        tokio::fs::write(probe, b"").await?;
        tokio::fs::remove_file(probe).await?;
        Ok(())
    }
}

#[async_trait]
impl StorageProvider for GDriveProvider {
    async fn upload(
        &self,
        key: &str,
        data: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(), DynError> {
        let token = std::env::var("GDRIVE_TOKEN")?;
        let upload_url = start_drive_session(&token, key).await?;
        let client = wreq::Client::new();

        let mut stream = ReaderStream::new(data);

        let mut offset: u64 = 0;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if chunk.is_empty() {
                continue;
            }
            let len = chunk.len() as u64;
            let end = offset + len - 1;

            let _ = client
                .put(&upload_url)
                .bearer_auth(&token)
                .header("Content-Length", len)
                .header("Content-Range", format!("bytes {}-{}/{}", offset, end, "*"))
                .body(chunk)
                .send()
                .await?
                .error_for_status();

            offset += len;
        }

        Ok(())
    }

    async fn download(&self, _key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, DynError> {
        Err("Not implemented".into())
    }

    async fn delete(&self, key: &str) -> Result<(), DynError> {
        println!("Deleting locally: {}", key);
        Ok(())
    }

    async fn health_check(&self) -> Result<(), DynError> {
        let token =
            std::env::var("GDRIVE_TOKEN").map_err(|_| "GDRIVE_TOKEN not set".to_string())?;
        let client = wreq::Client::new();
        let resp = client
            .get("https://www.googleapis.com/drive/v3/about?fields=kind")
            .bearer_auth(&token)
            .send()
            .await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(format!(
                "Google Drive API health check failed: HTTP {}",
                resp.status()
            )
            .into())
        }
    }
}
#[async_trait]
impl StorageProvider for DropboxProvider {
    async fn upload(
        &self,
        _key: &str,
        _data: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(), DynError> {
        println!("Uploading locally: {}", _key);
        Ok(())
    }

    async fn download(&self, _key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, DynError> {
        Err("Not implemented".into())
    }

    async fn delete(&self, key: &str) -> Result<(), DynError> {
        println!("Deleting locally: {}", key);
        Ok(())
    }

    async fn health_check(&self) -> Result<(), DynError> {
        let _ =
            std::env::var("DROPBOX_APP_KEY").map_err(|_| "DROPBOX_APP_KEY not set".to_string())?;
        let _ = std::env::var("DROPBOX_APP_SECRET")
            .map_err(|_| "DROPBOX_APP_SECRET not set".to_string())?;
        Ok(())
    }
}
#[async_trait]
impl StorageProvider for S3Provider {
    async fn upload(
        &self,
        _key: &str,
        _data: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(), DynError> {
        todo!();
        // let config = aws_config::load_from_env().await;
        // let client = Client::new(&config);

        // // AsyncRead → Stream<Bytes>
        // let stream = ReaderStream::new(data);

        // // Stream<Bytes> → S3 ByteStream
        // let byte_stream = ByteStream::new(
        //     stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        // );

        // client
        //     .put_object()
        //     .bucket("my-bucket")
        //     .key(key)
        //     .body(byte_stream)
        //     .send()
        //     .await?;

        // Ok(())
    }

    async fn download(&self, _key: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, DynError> {
        Err("Not implemented".into())
    }

    async fn delete(&self, key: &str) -> Result<(), DynError> {
        println!("Deleting locally: {}", key);
        Ok(())
    }

    async fn health_check(&self) -> Result<(), DynError> {
        std::env::var("AWS_ACCESS_KEY_ID").map_err(|_| "AWS_ACCESS_KEY_ID not set".to_string())?;
        std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| "AWS_SECRET_ACCESS_KEY not set".to_string())?;
        std::env::var("AWS_S3_BUCKET").map_err(|_| "AWS_S3_BUCKET not set".to_string())?;
        Ok(())
    }
}
