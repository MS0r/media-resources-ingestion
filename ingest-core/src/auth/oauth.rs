use async_trait::async_trait;
use serde::Deserialize;
use std::{env::var as env_var, time::Instant};
use tokio::sync::Mutex;

use crate::error::ToolError;

use super::TokenProvider;

/// OAuth2 token provider that uses a refresh token to obtain access tokens.
///
/// Supports both Google Drive and Dropbox refresh-token flows.
/// Environment variables expected:
///   - `{PREFIX}_CLIENT_ID`
///   - `{PREFIX}_CLIENT_SECRET`
///   - `{PREFIX}_REFRESH_TOKEN`
pub struct OAuthTokenProvider {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    token_url: String,
    name: &'static str,
    cached_token: Mutex<Option<CachedToken>>,
}

struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default = "default_expires_in")]
    expires_in: u64,
}

fn default_expires_in() -> u64 {
    3600
}

impl OAuthTokenProvider {
    pub fn new(
        client_id: String,
        client_secret: String,
        refresh_token: String,
        token_url: String,
        name: &'static str,
    ) -> Self {
        Self {
            client_id,
            client_secret,
            refresh_token,
            token_url,
            name,
            cached_token: Mutex::new(None),
        }
    }

    /// Build the provider from environment variables.
    ///
    /// `prefix` is the env-var prefix (e.g. `GDRIVE` → `GDRIVE_CLIENT_ID`).
    /// `token_url` is the OAuth token endpoint.
    /// `name` is a human-readable label.
    pub fn from_env(prefix: &str, token_url: &str, name: &'static str) -> Result<Self, ToolError> {
        let client_id = env_var(format!("{}_CLIENT_ID", prefix))?;
        let client_secret = env_var(format!("{}_CLIENT_SECRET", prefix))?;
        let refresh_token = env_var(format!("{}_REFRESH_TOKEN", prefix))?;
        Ok(Self::new(
            client_id,
            client_secret,
            refresh_token,
            token_url.to_string(),
            name,
        ))
    }

    /// Build the provider from the stored auth config file, falling back to env vars.
    ///
    /// Looks up `prefix` (lowercased) in `~/.ingest/auth.toml` first;
    /// if not found, falls back to `from_env()`.
    pub fn from_env_or_file(
        prefix: &str,
        token_url: &str,
        name: &'static str,
    ) -> Result<Self, ToolError> {
        // Try config file first
        let prefix_lower = prefix.to_lowercase();
        if let Ok(config) = super::load_auth_config() {
            match prefix_lower.as_str() {
                "dropbox" => {
                    if let Some(ref dropbox) = config.dropbox {
                        return Ok(Self::new(
                            dropbox.client_id.clone(),
                            dropbox.client_secret.clone(),
                            dropbox.refresh_token.clone(),
                            token_url.to_string(),
                            name,
                        ));
                    }
                }
                "gdrive" => {
                    if let Some(ref gdrive) = config.gdrive {
                        return Ok(Self::new(
                            gdrive.client_id.clone(),
                            gdrive.client_secret.clone(),
                            gdrive.refresh_token.clone(),
                            token_url.to_string(),
                            name,
                        ));
                    }
                }
                _ => {}
            }
        }

        // Fall back to env vars
        Self::from_env(prefix, token_url, name)
    }

    async fn refresh(&self) -> Result<String, ToolError> {
        let client = wreq::Client::new();
        let params = [
            ("client_id", self.client_id.as_str()),
            ("client_secret", self.client_secret.as_str()),
            ("refresh_token", self.refresh_token.as_str()),
            ("grant_type", "refresh_token"),
        ];
        let resp: TokenResponse = client
            .post(&self.token_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&params)
            .send()
            .await?
            .json()
            .await?;

        let expiry_secs = resp.expires_in.saturating_sub(300);
        let mut cache = self.cached_token.lock().await;
        *cache = Some(CachedToken {
            access_token: resp.access_token.clone(),
            expires_at: Instant::now() + std::time::Duration::from_secs(expiry_secs),
        });
        Ok(resp.access_token)
    }
}

#[async_trait]
impl TokenProvider for OAuthTokenProvider {
    async fn access_token(&self) -> Result<String, ToolError> {
        let cache = self.cached_token.lock().await;
        if let Some(cached) = &*cache {
            if cached.expires_at > Instant::now() {
                return Ok(cached.access_token.clone());
            }
        }
        drop(cache);
        self.refresh().await
    }

    fn name(&self) -> &'static str {
        self.name
    }
}
