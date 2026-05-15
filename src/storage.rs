use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio_stream::StreamExt;
use async_trait::async_trait;

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
    let client = reqwest::Client::new();

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

    async fn download(
        &self,
        key: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, DynError>;

    async fn delete(&self, key: &str) -> Result<(), DynError>;

    /// Whether this provider needs a local temp file before the hash is known.
    /// Remote providers stream directly; local needs temp-then-rename to avoid
    /// overwriting an existing file before we know if it's a duplicate.
    fn requires_local_staging(&self) -> bool {
        false // remote providers override to false (default)
    }

    async fn commit_temp(&self, temp_path: &str, final_path: &str) -> Result<(), DynError> {
        // Default implementation does nothing; local provider will override to rename
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
        let mut file = File::create(key).await?;
        tokio::io::copy(data, &mut file).await?;
        Ok(())
    }

    async fn download(
        &self,
        key: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, DynError> {
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
        let client = reqwest::Client::new();

        let mut stream = ReaderStream::new(data);

        let mut offset: u64 = 0;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if chunk.is_empty() { continue; }
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

    async fn download(
        &self,
        key: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, DynError> {
        Err("Not implemented".into())
    }

    async fn delete(&self, key: &str) -> Result<(), DynError> {
        println!("Deleting locally: {}", key);
        Ok(())
    }
}
#[async_trait]
impl StorageProvider for DropboxProvider {
    async fn upload(
        &self,
        key: &str,
        data: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(), DynError> {
        println!("Uploading locally: {}", key);
        Ok(())
    }

    async fn download(
        &self,
        key: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, DynError> {
        Err("Not implemented".into())
    }

    async fn delete(&self, key: &str) -> Result<(), DynError> {
        println!("Deleting locally: {}", key);
        Ok(())
    }
}
#[async_trait]
impl StorageProvider for S3Provider {
    async fn upload(
        &self,
        key: &str,
        data: &mut (dyn AsyncRead + Send + Unpin),
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

    async fn download(
        &self,
        key: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, DynError> {
        Err("Not implemented".into())
    }

    async fn delete(&self, key: &str) -> Result<(), DynError> {
        println!("Deleting locally: {}", key);
        Ok(())
    }
}