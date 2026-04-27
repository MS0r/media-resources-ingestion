use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    #[default]
    Local,
    Gdrive,
    Dropbox,
    S3,
}

pub trait StorageProvider: Send + Sync {
    async fn upload(&self, key:&str, data: impl AsyncRead) -> Result<(), Box<dyn std::error::Error>>;
    async fn download(&self, key: &str) -> Result<impl AsyncRead, Box<dyn std::error::Error>>;
    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error>>;
}

impl StorageProvider for Provider {
    async fn upload(&self, key: &str, data: impl AsyncRead) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Provider::Local => {
                // Implement local file system upload logic here
                Ok(())
            },
            Provider::Gdrive => {
                // Implement Google Drive upload logic here
                Ok(())
            },
            Provider::Dropbox => {
                // Implement Dropbox upload logic here
                Ok(())
            },
            Provider::S3 => {
                // Implement S3 upload logic here
                Ok(())
            },
        }
    }

    async fn download(&self, key: &str) -> Result<impl AsyncRead, Box<dyn std::error::Error>> {
        match self {
            Provider::Local => {
                // Implement local file system download logic here
                Err("Not implemented".into())
            },
            Provider::Gdrive => {
                // Implement Google Drive download logic here
                Err("Not implemented".into())
            },
            Provider::Dropbox => {
                // Implement Dropbox download logic here
                Err("Not implemented".into())
            },
            Provider::S3 => {
                // Implement S3 download logic here
                Err("Not implemented".into())
            },
        }
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Provider::Local => {
                // Implement local file system delete logic here
                Ok(())
            },
            Provider::Gdrive => {
                // Implement Google Drive delete logic here
                Ok(())
            },
            Provider::Dropbox => {
                // Implement Dropbox delete logic here
                Ok(())
            },
            Provider::S3 => {
                // Implement S3 delete logic here
                Ok(())
            },
        }
    }
}