use async_trait::async_trait;
use std::{
    collections::HashMap,
    path::{Component, Path},
    sync::Arc,
};
use tokio::{
    fs::File,
    sync::{Mutex, OnceCell},
};

use crate::{
    auth::{OAuthTokenProvider, StaticTokenProvider, TokenProvider},
    error::ToolError,
    storage::{DynError, StorageProvider},
};

pub struct GDriveProvider {
    parent_folder_id: String,
    token_provider: OnceCell<Arc<dyn TokenProvider>>,
    folder_cache: Mutex<HashMap<String, String>>,
}

impl GDriveProvider {
    pub fn new(parent_folder_id: String) -> Self {
        Self {
            parent_folder_id,
            token_provider: OnceCell::new(),
            folder_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Create from environment variables (`GDRIVE_CLIENT_ID` + `_SECRET` + `_REFRESH_TOKEN`).
    pub fn from_env(parent_folder_id: String) -> Result<Self, ToolError> {
        let _ = OAuthTokenProvider::from_env_or_file(
            "GDRIVE",
            "https://oauth2.googleapis.com/token",
            "gdrive",
        )?;
        Ok(Self::new(parent_folder_id))
    }

    async fn token_provider(&self) -> &Arc<dyn TokenProvider> {
        self.token_provider
            .get_or_init(|| async {
                match OAuthTokenProvider::from_env_or_file(
                    "GDRIVE",
                    "https://oauth2.googleapis.com/token",
                    "gdrive",
                ) {
                    Ok(p) => Arc::new(p) as Arc<dyn TokenProvider>,
                    Err(e) => {
                        tracing::error!("GDrive auth not configured: {e}");
                        Arc::new(StaticTokenProvider::new(
                            String::new(),
                            "gdrive-unconfigured",
                        ))
                    }
                }
            })
            .await
    }

    async fn token_string(&self) -> Result<String, DynError> {
        Ok(self.token_provider().await.access_token().await?)
    }

    /// Walk the directory path components under `self.parent_folder_id`,
    /// creating any missing folders. Returns the Drive folder ID of the
    /// deepest component (or `self.parent_folder_id` if no directory).
    /// Results are cached so repeated calls with the same path are cheap.
    async fn ensure_folders(&self, dir_path: &Path, token: &str) -> Result<String, DynError> {
        let dir_str = dir_path.to_string_lossy();
        if dir_str.is_empty() {
            return Ok(self.parent_folder_id.clone());
        }

        // Fast path: already cached
        {
            let cache = self.folder_cache.lock().await;
            if let Some(id) = cache.get(dir_str.as_ref()) {
                return Ok(id.clone());
            }
        }

        let mut current_parent = self.parent_folder_id.clone();
        let mut accumulated = String::new();
        let mut create_cache = HashMap::new();

        for component in dir_path.components() {
            if let Component::Normal(name) = component {
                let name_str = name.to_str().ok_or("Invalid UTF-8 in path component")?;

                if !accumulated.is_empty() {
                    accumulated.push('/');
                }
                accumulated.push_str(name_str);

                // Check cache again for this sub-path
                {
                    let cache = self.folder_cache.lock().await;
                    if let Some(id) = cache.get(&accumulated) {
                        current_parent = id.clone();
                        continue;
                    }
                }

                // Build query: find folder with this name under current parent
                let q = format!(
                    "name='{}' and '{}' in parents and mimeType='application/vnd.google-apps.folder'",
                    name_str.replace('\'', "\\'"),
                    current_parent
                );
                let url = format!(
                    "https://www.googleapis.com/drive/v3/files?q={}&fields=files(id)&pageSize=1",
                    urlencoding(&q)
                );

                let client = wreq::Client::new();
                let resp = client.get(&url).bearer_auth(token).send().await?;

                let folder_id = if resp.status().is_success() {
                    let body: serde_json::Value = resp.json().await?;
                    let files = body["files"].as_array().cloned().unwrap_or_default();
                    if let Some(file) = files.into_iter().next() {
                        file["id"].as_str().unwrap().to_string()
                    } else {
                        // Folder does not exist – create it
                        let meta = serde_json::json!({
                            "name": name_str,
                            "parents": [current_parent],
                            "mimeType": "application/vnd.google-apps.folder"
                        });
                        let body_bytes = serde_json::to_vec(&meta)?;
                        let create_resp = client
                            .post("https://www.googleapis.com/drive/v3/files")
                            .bearer_auth(token)
                            .header("Content-Type", "application/json; charset=UTF-8")
                            .body(body_bytes)
                            .send()
                            .await?;

                        let create_status = create_resp.status();
                        if !create_status.is_success() {
                            let text = create_resp.text().await.unwrap_or_default();
                            return Err(format!(
                                "GDrive folder creation failed: HTTP {} - {}",
                                create_status, text
                            )
                            .into());
                        }
                        let created: serde_json::Value = create_resp.json().await?;
                        created["id"]
                            .as_str()
                            .ok_or("GDrive folder creation returned no id")?
                            .to_string()
                    }
                } else {
                    let query_status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    return Err(format!(
                        "GDrive folder query failed: HTTP {} - {}",
                        query_status, text
                    )
                    .into());
                };

                create_cache.insert(accumulated.clone(), folder_id.clone());
                current_parent = folder_id;
            }
        }

        // Persist all newly-resolved entries into the shared cache
        let mut cache = self.folder_cache.lock().await;
        for (k, v) in create_cache {
            cache.entry(k).or_insert(v);
        }
        drop(cache);

        Ok(current_parent)
    }
}

#[async_trait]
impl StorageProvider for GDriveProvider {
    async fn upload(&self, path: &str, file: &mut File) -> Result<(), DynError> {
        let token = self.token_string().await?;

        let file_path = Path::new(path);
        let file_size = file.metadata().await?.len();
        let file_name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file");

        // Resolve (and create if needed) the parent folder hierarchy
        let parent_id = self
            .ensure_folders(file_path.parent().unwrap_or_else(|| Path::new("")), &token)
            .await?;

        // Start resumable upload session
        let upload_url = start_drive_session(&token, file_size, file_name, &parent_id).await?;

        // Read file and upload
        use tokio::io::AsyncReadExt;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;

        let client = wreq::Client::new();
        let resp = client
            .put(&upload_url)
            .header("Content-Length", data.len().to_string())
            .body(data)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(format!("GDrive upload failed: HTTP {}", resp.status()).into());
        }

        tracing::info!("GDrive upload: {} -> Drive", path);
        Ok(())
    }

    async fn download(
        &self,
        path: &str,
    ) -> Result<Box<dyn tokio::io::AsyncRead + Unpin + Send>, DynError> {
        let token = self.token_string().await?;
        let file_id = path.trim_start_matches('/');
        let url = format!(
            "https://www.googleapis.com/drive/v3/files/{}?alt=media",
            urlencoding(file_id)
        );

        let client = wreq::Client::new();
        let response = client.get(&url).bearer_auth(&token).send().await?;

        if !response.status().is_success() {
            return Err(format!("GDrive download failed: HTTP {}", response.status()).into());
        }

        let data = response.bytes().await?;
        Ok(Box::new(std::io::Cursor::new(data.to_vec())))
    }

    async fn health_check(&self) -> Result<(), DynError> {
        let token = self.token_string().await?;
        let client = wreq::Client::new();
        let resp = client
            .get("https://www.googleapis.com/drive/v3/files?pageSize=1")
            .bearer_auth(&token)
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(format!("GDrive health check failed: HTTP {}", resp.status()).into())
        }
    }
}

async fn start_drive_session(
    token: &str,
    file_size: u64,
    file_name: &str,
    parent_folder_id: &str,
) -> Result<String, DynError> {
    let client = wreq::Client::new();

    let mut metadata = serde_json::json!({ "name": file_name });
    if !parent_folder_id.is_empty() && parent_folder_id != "root" {
        metadata["parents"] = serde_json::json!([parent_folder_id]);
    }
    let body = serde_json::to_vec(&metadata)?;

    let res = client
        .post("https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable")
        .bearer_auth(token)
        .header("Content-Type", "application/json; charset=UTF-8")
        .header("X-Upload-Content-Type", "application/octet-stream")
        .header("Content-Length", body.len().to_string())
        .header("X-Upload-Content-Length", file_size.to_string())
        .body(body)
        .send()
        .await?;

    let status = res.status();
    if !status.is_success() {
        let body_text = res.text().await.unwrap_or_default();
        return Err(format!(
            "GDrive resumable session initiation failed: HTTP {} - {}",
            status, body_text
        )
        .into());
    }

    Ok(res
        .headers()
        .get("Location")
        .ok_or_else(|| {
            format!(
                "GDrive returned {} but no Location header in response",
                status
            )
        })?
        .to_str()?
        .to_string())
}

fn urlencoding(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            _ => {
                result.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_urlencoding_plain_text() {
        assert_eq!(urlencoding("file.txt"), "file.txt");
    }

    #[test]
    fn test_urlencoding_spaces() {
        assert_eq!(urlencoding("my file.txt"), "my%20file.txt");
    }

    #[test]
    fn test_urlencoding_special_chars() {
        assert_eq!(urlencoding("a/b?c#d"), "a%2Fb%3Fc%23d");
    }

    #[test]
    fn test_urlencoding_already_encoded() {
        assert_eq!(urlencoding("%20"), "%2520");
    }

    #[test]
    fn test_urlencoding_unicode() {
        assert_eq!(urlencoding("résumé"), "r%C3%A9sum%C3%A9");
    }

    #[test]
    fn test_urlencoding_empty() {
        assert_eq!(urlencoding(""), "");
    }

    #[test]
    fn test_urlencoding_dashes_and_dots() {
        assert_eq!(urlencoding("file-name.v2.tar.gz"), "file-name.v2.tar.gz");
    }
}
