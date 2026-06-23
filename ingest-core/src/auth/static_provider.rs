use async_trait::async_trait;

use crate::error::ToolError;

use super::TokenProvider;

/// A static (fixed) token provider for simple API-key-style auth.
pub struct StaticTokenProvider {
    token: String,
    name: &'static str,
}

impl StaticTokenProvider {
    pub fn new(token: String, name: &'static str) -> Self {
        Self { token, name }
    }
}

#[async_trait]
impl TokenProvider for StaticTokenProvider {
    async fn access_token(&self) -> Result<String, ToolError> {
        Ok(self.token.clone())
    }

    fn name(&self) -> &'static str {
        self.name
    }
}
