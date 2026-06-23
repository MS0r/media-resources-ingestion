mod device_auth;
mod oauth;
mod static_provider;

use async_trait::async_trait;
use std::sync::Arc;

use crate::error::ToolError;

/// A provider of access tokens for authenticating with external services.
#[async_trait]
pub trait TokenProvider: Send + Sync {
    /// Returns a current access token (without "Bearer " prefix).
    async fn access_token(&self) -> Result<String, ToolError>;
    /// Human-readable name for logging/errors.
    fn name(&self) -> &'static str;
}

pub use device_auth::{
    DropboxAuth, GdriveAuth, StoredAuthConfig, dropbox_auth_flow, gdrive_auth_flow,
    load_auth_config, save_auth_config,
};
pub use oauth::OAuthTokenProvider;
pub use static_provider::StaticTokenProvider;

/// Registry of named token providers, initialized once at startup.
#[derive(Default)]
pub struct AuthProviderRegistry {
    providers: Vec<(String, Arc<dyn TokenProvider>)>,
}

impl AuthProviderRegistry {
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    pub fn register(&mut self, name: &str, provider: Arc<dyn TokenProvider>) {
        self.providers.push((name.to_string(), provider));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn TokenProvider>> {
        self.providers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, p)| p.clone())
    }

    /// Attempt to detect the source provider from a URL hostname.
    pub fn detect_from_url(url: &str) -> Option<&'static str> {
        let host = url::Url::parse(url).ok()?.host_str()?.to_lowercase();
        if host.contains("drive.google.com") || host.contains("docs.google.com") {
            Some("gdrive")
        } else if host.contains("dropbox.com") || host.contains("dropboxusercontent.com") {
            Some("dropbox")
        } else if host.ends_with("amazonaws.com") || host.contains("s3.") {
            Some("s3")
        } else {
            None
        }
    }
}
