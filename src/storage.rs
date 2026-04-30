use serde::{Deserialize, Serialize};
use tokio::fs::File;
use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncRead;
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

pub struct LocalProvider;
pub struct GDriveProvider;
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

type DynError = Box<dyn Error + Send + Sync>;

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
        println!("Uploading locally: {}", key);
        let mut file = File::create(key).await?;
        tokio::io::copy(data, &mut file).await?;
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