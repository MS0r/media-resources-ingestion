//! Source authentication — strongly-typed enum replacing the stringly-typed
//! `source_auth: Option<String>` field.

use serde::{Deserialize, Serialize};
use url::Url;

use crate::auth::AuthProviderRegistry;

/// Source authentication strategy for downloading a resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceAuth {
    /// Detect provider from URL hostname (default).
    #[serde(alias = "auto")]
    Auto,
    /// No source auth — use static headers from YAML only.
    #[serde(alias = "none")]
    None,
    /// Use static YAML headers only, no token resolution.
    #[serde(alias = "headers")]
    Headers,
    /// Force GDrive token resolution.
    Gdrive,
    /// Force Dropbox token resolution.
    Dropbox,
    /// Force S3 presigning.
    #[serde(alias = "s3")]
    S3Presigned,
}

impl Default for SourceAuth {
    fn default() -> Self {
        Self::Auto
    }
}

impl SourceAuth {
    /// Resolve `Auto` to a concrete variant based on the URL hostname.
    pub fn resolve_for_url(self, url: &Url) -> Self {
        match self {
            Self::Auto => Self::detect(url),
            other => other,
        }
    }

    /// Detect the provider from a URL hostname.
    fn detect(url: &Url) -> Self {
        match AuthProviderRegistry::detect_from_url(url.as_str()) {
            Some("gdrive") => Self::Gdrive,
            Some("dropbox") => Self::Dropbox,
            Some("s3") => Self::S3Presigned,
            _ => Self::None,
        }
    }

    /// Returns `true` if this variant requires dynamic token resolution.
    pub fn needs_token(self) -> bool {
        matches!(self, Self::Gdrive | Self::Dropbox | Self::S3Presigned)
    }
}

impl std::fmt::Display for SourceAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::None => write!(f, "none"),
            Self::Headers => write!(f, "headers"),
            Self::Gdrive => write!(f, "gdrive"),
            Self::Dropbox => write!(f, "dropbox"),
            Self::S3Presigned => write!(f, "s3"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_auth_default_is_auto() {
        assert_eq!(SourceAuth::default(), SourceAuth::Auto);
    }

    #[test]
    fn test_source_auth_serde_roundtrip() {
        for (input, expected) in [
            ("auto", SourceAuth::Auto),
            ("none", SourceAuth::None),
            ("headers", SourceAuth::Headers),
            ("gdrive", SourceAuth::Gdrive),
            ("dropbox", SourceAuth::Dropbox),
            ("s3", SourceAuth::S3Presigned),
        ] {
            let json = format!("\"{}\"", input);
            let deser: SourceAuth = serde_json::from_str(&json).unwrap();
            assert_eq!(deser, expected, "failed for {}", input);
        }
    }

    #[test]
    fn test_source_auth_display() {
        assert_eq!(SourceAuth::Auto.to_string(), "auto");
        assert_eq!(SourceAuth::None.to_string(), "none");
        assert_eq!(SourceAuth::Headers.to_string(), "headers");
        assert_eq!(SourceAuth::Gdrive.to_string(), "gdrive");
        assert_eq!(SourceAuth::Dropbox.to_string(), "dropbox");
        assert_eq!(SourceAuth::S3Presigned.to_string(), "s3");
    }

    #[test]
    fn test_source_auth_needs_token() {
        assert!(!SourceAuth::Auto.needs_token());
        assert!(!SourceAuth::None.needs_token());
        assert!(!SourceAuth::Headers.needs_token());
        assert!(SourceAuth::Gdrive.needs_token());
        assert!(SourceAuth::Dropbox.needs_token());
        assert!(SourceAuth::S3Presigned.needs_token());
    }

    #[test]
    fn test_source_auth_resolve_for_url_gdrive() {
        let url = Url::parse("https://drive.google.com/file/d/123").unwrap();
        assert_eq!(SourceAuth::Auto.resolve_for_url(&url), SourceAuth::Gdrive);
    }

    #[test]
    fn test_source_auth_resolve_for_url_dropbox() {
        let url = Url::parse("https://dropbox.com/scl/fi/abc/file.txt").unwrap();
        assert_eq!(SourceAuth::Auto.resolve_for_url(&url), SourceAuth::Dropbox);
    }

    #[test]
    fn test_source_auth_resolve_for_url_s3() {
        let url = Url::parse("https://mybucket.s3.amazonaws.com/file.txt").unwrap();
        assert_eq!(
            SourceAuth::Auto.resolve_for_url(&url),
            SourceAuth::S3Presigned
        );
    }

    #[test]
    fn test_source_auth_resolve_for_url_none() {
        let url = Url::parse("https://example.com/file.txt").unwrap();
        assert_eq!(SourceAuth::Auto.resolve_for_url(&url), SourceAuth::None);
    }

    #[test]
    fn test_source_auth_resolve_for_url_explicit() {
        let url = Url::parse("https://example.com/file.txt").unwrap();
        // Explicit variants are not overridden by URL detection
        assert_eq!(SourceAuth::Gdrive.resolve_for_url(&url), SourceAuth::Gdrive);
        assert_eq!(
            SourceAuth::Headers.resolve_for_url(&url),
            SourceAuth::Headers
        );
    }
}
