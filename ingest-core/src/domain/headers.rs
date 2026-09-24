//! HTTP headers — a tiny typed wrapper around the two headers the system
//! actually cares about (`Authorization`, `Cookie`).
//!
//! Lifted out of `models.rs` so that `domain/` can be the home for pure data
//! types. `domain::SourceAuth`, `domain::JobStatus`, and `Headers` together
//! form the set of types with zero IO or service dependencies.

use serde::{Deserialize, Serialize};
use wreq::{
    RequestBuilder,
    header::{AUTHORIZATION, COOKIE},
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub struct Headers {
    pub authorization: Option<String>,
    pub cookie: Option<String>,
}

impl Headers {
    /// Create a bearer token header.
    pub fn bearer(token: &str) -> Self {
        Self {
            authorization: Some(format!("Bearer {token}")),
            cookie: None,
        }
    }

    /// Merge another `Headers` into this one. Per-field `Some` wins.
    pub fn merge(self, other: Self) -> Self {
        Self {
            authorization: self.authorization.or(other.authorization),
            cookie: self.cookie.or(other.cookie),
        }
    }

    /// Apply these headers to a `RequestBuilder`.
    pub fn apply(&self, mut request: RequestBuilder) -> RequestBuilder {
        if let Some(auth) = &self.authorization {
            request = request.header(AUTHORIZATION, auth.as_str());
        }
        if let Some(cookie) = &self.cookie {
            request = request.header(COOKIE, cookie.as_str());
        }
        request
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headers_bearer() {
        let h = Headers::bearer("xyz");
        assert_eq!(h.authorization.as_deref(), Some("Bearer xyz"));
        assert!(h.cookie.is_none());
    }

    #[test]
    fn test_headers_merge_per_field_some_wins() {
        let a = Headers {
            authorization: Some("A".into()),
            cookie: None,
        };
        let b = Headers {
            authorization: Some("B".into()),
            cookie: Some("c".into()),
        };
        let merged = a.merge(b);
        assert_eq!(merged.authorization.as_deref(), Some("A"));
        assert_eq!(merged.cookie.as_deref(), Some("c"));
    }

    #[test]
    fn test_headers_serde_roundtrip() {
        let h = Headers {
            authorization: Some("Bearer xyz".into()),
            cookie: Some("k=v".into()),
        };
        let json = serde_json::to_string(&h).unwrap();
        let back: Headers = serde_json::from_str(&json).unwrap();
        assert_eq!(back.authorization, h.authorization);
        assert_eq!(back.cookie, h.cookie);
    }
}
