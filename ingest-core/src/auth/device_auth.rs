use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use url::Url;

use crate::error::ToolError;

/// Configuration saved to `~/.ingest/auth.toml` after device auth.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StoredAuthConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dropbox: Option<DropboxAuth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gdrive: Option<GdriveAuth>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropboxAuth {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GdriveAuth {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
}

/// Bind a TCP listener on localhost and return the redirect_uri, CSRF state, and listener.
async fn bind_oauth_listener() -> Result<(String, String, TcpListener), ToolError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let redirect_uri = format!("http://localhost:{port}");
    let state = uuid::Uuid::new_v4().to_string();
    Ok((redirect_uri, state, listener))
}

/// Accept a single HTTP connection on the listener, parse the OAuth callback,
/// verify the CSRF state, extract the authorization code, and send the response.
async fn await_oauth_callback(
    listener: TcpListener,
    expected_state: &str,
) -> Result<String, ToolError> {
    let (mut stream, _) = listener.accept().await?;

    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;

    let request = String::from_utf8_lossy(&buf[..n]);
    let request_line = request.lines().next().unwrap_or("");
    let path = request_line.split_whitespace().nth(1).unwrap_or("");

    let parsed = Url::parse(&format!("http://localhost{path}"))?;

    let returned_state = parsed
        .query_pairs()
        .find(|(k, _)| k == "state")
        .map(|(_, v)| v.to_string());

    if returned_state.as_deref() != Some(expected_state) {
        return Err(ToolError::AuthError(
            "CSRF state mismatch — authorization may have been intercepted".into(),
        ));
    }

    let code = parsed
        .query_pairs()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.to_string())
        .ok_or_else(|| ToolError::AuthError("No authorization code in callback".into()))?;

    let response_body = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Content-Type: text/html; charset=utf-8\r\n",
        "Content-Length: 140\r\n",
        "Connection: close\r\n",
        "\r\n",
        "<!DOCTYPE html><html><body>",
        "<h1>Authorization complete!</h1>",
        "<p>You can close this window and return to the terminal.</p>",
        "</body></html>"
    );
    stream.write_all(response_body.as_bytes()).await?;

    Ok(code)
}

/// Perform the Dropbox OAuth authorization code flow.
///
/// 1. Starts a local HTTP server on a random port.
/// 2. Prints the Dropbox authorization URL for the user to visit.
/// 3. Captures the redirect with the authorization code.
/// 4. Exchanges the code for a refresh token.
/// 5. Returns the refresh token.
pub async fn dropbox_auth_flow(client_id: &str, client_secret: &str) -> Result<String, ToolError> {
    let (redirect_uri, state, listener) = bind_oauth_listener().await?;

    let auth_url = Url::parse_with_params(
        "https://www.dropbox.com/oauth2/authorize",
        &[
            ("client_id", client_id),
            ("redirect_uri", &redirect_uri),
            ("response_type", "code"),
            ("token_access_type", "offline"),
            ("state", &state),
        ],
    )?;

    println!("\n  Visit this URL in your browser:\n");
    println!("  {}\n", auth_url);
    println!("  (The CLI will wait for you to complete authorization)\n");

    let code = await_oauth_callback(listener, &state).await?;

    let exchange_client = wreq::Client::new();
    let params = [
        ("code", code.as_str()),
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("redirect_uri", &redirect_uri),
        ("grant_type", "authorization_code"),
    ];

    #[derive(Deserialize)]
    struct TokenExchangeResponse {
        refresh_token: Option<String>,
        error: Option<String>,
        error_description: Option<String>,
    }

    let token_resp: TokenExchangeResponse = exchange_client
        .post("https://api.dropbox.com/oauth2/token")
        .form(&params)
        .send()
        .await?
        .json()
        .await?;

    if let Some(err) = token_resp.error {
        let desc = token_resp.error_description.unwrap_or_default();
        return Err(ToolError::AuthError(format!(
            "Dropbox OAuth error: {err} — {desc}"
        )));
    }

    token_resp.refresh_token.ok_or_else(|| {
        ToolError::AuthError(
            "No refresh token in response. Ensure token_access_type=offline is used.".into(),
        )
    })
}

/// Perform the Google Drive OAuth authorization code flow.
///
/// 1. Starts a local HTTP server on a random port.
/// 2. Prints the Google authorization URL for the user to visit.
/// 3. Captures the redirect with the authorization code.
/// 4. Exchanges the code for a refresh token.
/// 5. Returns (client_id, client_secret, refresh_token).
pub async fn gdrive_auth_flow(
    client_id: &str,
    client_secret: &str,
) -> Result<(String, String, String), ToolError> {
    let (redirect_uri, state, listener) = bind_oauth_listener().await?;

    let auth_url = Url::parse_with_params(
        "https://accounts.google.com/o/oauth2/v2/auth",
        &[
            ("client_id", client_id),
            ("redirect_uri", &redirect_uri),
            ("response_type", "code"),
            ("scope", "https://www.googleapis.com/auth/drive"),
            ("access_type", "offline"),
            ("state", &state),
        ],
    )?;

    println!("\n  Visit this URL in your browser:\n");
    println!("  {}\n", auth_url);
    println!("  (The CLI will wait for you to complete authorization)\n");

    let code = await_oauth_callback(listener, &state).await?;

    let exchange_client = wreq::Client::new();
    let params = [
        ("code", code.as_str()),
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("redirect_uri", &redirect_uri),
        ("grant_type", "authorization_code"),
    ];

    #[derive(Deserialize)]
    struct TokenExchangeResponse {
        refresh_token: Option<String>,
        error: Option<String>,
        error_description: Option<String>,
    }

    let token_resp: TokenExchangeResponse = exchange_client
        .post("https://oauth2.googleapis.com/token")
        .form(&params)
        .send()
        .await?
        .json()
        .await?;

    if let Some(err) = token_resp.error {
        let desc = token_resp.error_description.unwrap_or_default();
        return Err(ToolError::AuthError(format!(
            "Google OAuth error: {err} — {desc}"
        )));
    }

    let refresh_token = token_resp.refresh_token.ok_or_else(|| {
        ToolError::AuthError(
            "No refresh token in response. Ensure access_type=offline is used and the \
             consent screen allows offline access."
                .into(),
        )
    })?;

    Ok((
        client_id.to_string(),
        client_secret.to_string(),
        refresh_token,
    ))
}

/// Load the stored auth config from `~/.ingest/auth.toml`.
pub fn load_auth_config() -> Result<StoredAuthConfig, ToolError> {
    let path = auth_config_path();
    if !path.exists() {
        return Ok(StoredAuthConfig::default());
    }
    let content = std::fs::read_to_string(&path)?;
    let config: StoredAuthConfig = toml::from_str(&content)?;
    Ok(config)
}

/// Save the stored auth config to `~/.ingest/auth.toml`.
pub fn save_auth_config(config: &StoredAuthConfig) -> Result<(), ToolError> {
    let path = auth_config_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let content = toml::to_string_pretty(config)
        .map_err(|e| ToolError::AuthError(format!("Failed to serialize auth config: {e}")))?;
    std::fs::write(&path, &content)?;
    tracing::info!(path = %path.display(), "Auth config saved");
    Ok(())
}

fn auth_config_path() -> std::path::PathBuf {
    let home = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
    home.join(".ingest").join("auth.toml")
}
