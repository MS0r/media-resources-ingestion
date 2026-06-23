use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use tokio::sync::OnceCell;

use crate::error::ToolError;
use crate::storage::{DynError, StorageProvider};

pub struct S3Provider {
    bucket: String,
    client: OnceCell<aws_sdk_s3::Client>,
}

impl S3Provider {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            client: OnceCell::new(),
        }
    }

    /// Create from environment: reads `AWS_BUCKET`, `AWS_REGION` via SDK default chain.
    pub async fn from_env() -> Result<Self, ToolError> {
        let bucket = std::env::var("AWS_BUCKET")
            .map_err(|_| ToolError::AuthError("AWS_BUCKET env var not set".into()))?;
        Ok(Self::new(bucket))
    }

    async fn client(&self) -> &aws_sdk_s3::Client {
        self.client
            .get_or_init(|| async {
                let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .load()
                    .await;
                aws_sdk_s3::Client::new(&config)
            })
            .await
    }

    pub async fn bucket(&self) -> &str {
        &self.bucket
    }
}

#[async_trait]
impl StorageProvider for S3Provider {
    async fn upload(&self, path: &str, file: &mut tokio::fs::File) -> Result<(), DynError> {
        use tokio::io::AsyncReadExt;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        let body = ByteStream::from(data);
        self.client()
            .await
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(body)
            .send()
            .await?;
        tracing::info!("S3 upload: {} -> s3://{}/{}", path, self.bucket, path);
        Ok(())
    }

    async fn download(
        &self,
        path: &str,
    ) -> Result<Box<dyn tokio::io::AsyncRead + Unpin + Send>, DynError> {
        let output = self
            .client()
            .await
            .get_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await?;
        let data = output.body.collect().await?;
        let bytes = data.into_bytes();
        Ok(Box::new(std::io::Cursor::new(bytes.to_vec())))
    }

    async fn health_check(&self) -> Result<(), DynError> {
        self.client()
            .await
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await?;
        Ok(())
    }
}
