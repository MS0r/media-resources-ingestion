use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::OnceCell;

use crate::auth::{OAuthTokenProvider, TokenProvider};
use crate::error::ToolError;
use crate::storage::{DynError, StorageProvider};

pub struct DropboxProvider {
    token_provider: OnceCell<Arc<dyn TokenProvider>>,
}

impl DropboxProvider {
    pub fn new() -> Self {
        Self {
            token_provider: OnceCell::new(),
        }
    }

    /// Create from environment variables (`DROPBOX_APP_KEY`, `_SECRET`, `_REFRESH_TOKEN`).
    pub fn from_env() -> Result<Self, ToolError> {
        let _ = OAuthTokenProvider::from_env(
            "DROPBOX",
            "https://api.dropbox.com/oauth2/token",
            "dropbox",
        )?;
        Ok(Self::new())
    }

    async fn token_provider(&self) -> &Arc<dyn TokenProvider> {
        self.token_provider
            .get_or_init(|| async {
                match OAuthTokenProvider::from_env_or_file(
                    "DROPBOX",
                    "https://api.dropbox.com/oauth2/token",
                    "dropbox",
                ) {
                    Ok(p) => Arc::new(p) as Arc<dyn TokenProvider>,
                    Err(e) => {
                        tracing::error!("Dropbox auth not configured: {e}");
                        Arc::new(crate::auth::StaticTokenProvider::new(
                            String::new(),
                            "dropbox-unconfigured",
                        ))
                    }
                }
            })
            .await
    }

    async fn token_string(&self) -> Result<String, DynError> {
        Ok(self.token_provider().await.access_token().await?)
    }

    /// Read a failed response body and return a description that includes both
    /// the HTTP status and the structured Dropbox error (if available).
    async fn describe_error(resp: wreq::Response) -> String {
        let status = resp.status();
        let status_int: u16 = status.as_u16();
        match resp.text().await {
            Ok(body) => Self::format_error(status_int, &body),
            Err(_) => format!("HTTP {status_int}"),
        }
    }

    /// Ensure a path starts with `/` as required by the Dropbox API.
    fn dropbox_path(path: &str) -> String {
        if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        }
    }

    /// Build an error string from a Dropbox API status code and response body.
    /// Extracts `error_summary` first, falls back to `error..tag`, then the raw body.
    fn format_error(status: u16, body: &str) -> String {
        if body.is_empty() {
            return format!("HTTP {status}");
        }
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(body) {
            let summary = val["error_summary"].as_str().unwrap_or("");
            if !summary.is_empty() {
                return format!("HTTP {status} ({summary})");
            }
            let tag = val["error"]
                .as_object()
                .and_then(|o| o.get(".tag"))
                .and_then(|t| t.as_str())
                .unwrap_or("");
            if !tag.is_empty() {
                return format!("HTTP {status} ({tag})");
            }
        }
        format!("HTTP {status} - {body}")
    }
}

#[async_trait]
impl StorageProvider for DropboxProvider {
    async fn upload(&self, path: &str, file: &mut tokio::fs::File) -> Result<(), DynError> {
        let token = self.token_string().await?;

        use tokio::io::AsyncReadExt;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;

        let api_path = Self::dropbox_path(path);
        let dropbox_arg = serde_json::json!({
            "path": api_path,
            "mode": "add",
            "autorename": false,
            "mute": false,
            "strict_conflict": false,
        });

        let client = wreq::Client::new();
        let resp = client
            .post("https://content.dropboxapi.com/2/files/upload")
            .bearer_auth(&token)
            .header("Dropbox-API-Arg", dropbox_arg.to_string())
            .header("Content-Type", "application/octet-stream")
            .body(data)
            .send()
            .await?;

        if !resp.status().is_success() {
            let detail = Self::describe_error(resp).await;
            return Err(format!("Dropbox upload failed: {detail}").into());
        }

        if let Ok(val) = resp.json::<serde_json::Value>().await {
            let file_id = val["id"].as_str().unwrap_or("?");
            let display_path = val["path_display"].as_str().unwrap_or(path);
            tracing::info!("Dropbox upload: {path} -> {display_path} (id: {file_id})");
        } else {
            tracing::info!("Dropbox upload: {path} -> Dropbox");
        }
        Ok(())
    }

    async fn download(
        &self,
        path: &str,
    ) -> Result<Box<dyn tokio::io::AsyncRead + Unpin + Send>, DynError> {
        let token = self.token_string().await?;

        let dropbox_arg = serde_json::json!({ "path": Self::dropbox_path(path) });

        let client = wreq::Client::new();
        let response = client
            .post("https://content.dropboxapi.com/2/files/download")
            .bearer_auth(&token)
            .header("Dropbox-API-Arg", dropbox_arg.to_string())
            .send()
            .await?;

        if !response.status().is_success() {
            let detail = Self::describe_error(response).await;
            return Err(format!("Dropbox download failed: {detail}").into());
        }

        let data = response.bytes().await?;
        Ok(Box::new(std::io::Cursor::new(data.to_vec())))
    }

    async fn health_check(&self) -> Result<(), DynError> {
        let token = self.token_string().await?;
        let client = wreq::Client::new();
        let resp = client
            .post("https://api.dropboxapi.com/2/users/get_current_account")
            .bearer_auth(&token)
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let detail = Self::describe_error(resp).await;
            Err(format!("Dropbox health check failed: {detail}").into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropbox_path_adds_leading_slash() {
        assert_eq!(
            DropboxProvider::dropbox_path("games/file.zip"),
            "/games/file.zip"
        );
    }

    #[test]
    fn dropbox_path_preserves_existing_slash() {
        assert_eq!(
            DropboxProvider::dropbox_path("/games/file.zip"),
            "/games/file.zip"
        );
    }

    #[test]
    fn dropbox_path_empty_string() {
        assert_eq!(DropboxProvider::dropbox_path(""), "/");
    }

    #[test]
    fn format_error_uses_summary() {
        let body = r#"{"error":{".tag":"path"},"error_summary":"path/not_found/..."}"#;
        let desc = DropboxProvider::format_error(409, body);
        assert!(desc.contains("path/not_found"), "{desc}");
    }

    #[test]
    fn format_error_falls_back_to_tag() {
        let body = r#"{"error":{".tag":"payload_too_large"}}"#;
        let desc = DropboxProvider::format_error(400, body);
        assert!(desc.contains("payload_too_large"), "{desc}");
    }

    #[test]
    fn format_error_empty_body() {
        let desc = DropboxProvider::format_error(500, "");
        assert_eq!(desc, "HTTP 500");
    }

    #[test]
    fn format_error_non_json_body() {
        let desc = DropboxProvider::format_error(400, "not json");
        assert_eq!(desc, "HTTP 400 - not json");
    }
}
