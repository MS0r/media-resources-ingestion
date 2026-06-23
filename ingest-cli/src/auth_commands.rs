use ingest_core::auth::{
    DropboxAuth, GdriveAuth, dropbox_auth_flow, gdrive_auth_flow, load_auth_config,
    save_auth_config,
};

pub async fn handle_auth_dropbox() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client_id = std::env::var("DROPBOX_APP_KEY")
        .map_err(|_| "DROPBOX_APP_KEY not set. Set this env var to your Dropbox app key.")?;
    let client_secret = std::env::var("DROPBOX_APP_SECRET")
        .map_err(|_| "DROPBOX_APP_SECRET not set. Set this env var to your Dropbox app secret.")?;

    println!("Starting Dropbox device authorization flow...\n");

    let refresh_token = dropbox_auth_flow(&client_id, &client_secret).await?;

    let mut config = load_auth_config()?;
    config.dropbox = Some(DropboxAuth {
        client_id: client_id.clone(),
        client_secret: client_secret.clone(),
        refresh_token: refresh_token.clone(),
    });
    save_auth_config(&config)?;

    println!("✓ Dropbox refresh token saved to ~/.ingest/auth.toml");
    println!("  You can now use Dropbox storage without setting DROPBOX_REFRESH_TOKEN.");

    Ok(())
}

pub async fn handle_auth_gdrive() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client_id = std::env::var("GDRIVE_CLIENT_ID").map_err(|_| {
        "GDRIVE_CLIENT_ID not set. \
         Get your OAuth client ID from Google Cloud Console (Desktop app type, \
         with http://localhost as an authorized redirect URI)."
    })?;
    let client_secret = std::env::var("GDRIVE_CLIENT_SECRET").map_err(
        |_| "GDRIVE_CLIENT_SECRET not set. Set this env var to your Google OAuth client secret.",
    )?;

    println!("Starting Google Drive OAuth authorization code flow...\n");
    println!("  A browser window will open or you'll visit the URL shown below.");
    println!("  Google will redirect to a localhost server after authorization.\n");

    let (_client_id, _client_secret, refresh_token) =
        gdrive_auth_flow(&client_id, &client_secret).await?;

    let mut config = load_auth_config().unwrap_or_default();
    config.gdrive = Some(GdriveAuth {
        client_id: client_id.clone(),
        client_secret: client_secret.clone(),
        refresh_token: refresh_token.clone(),
    });
    save_auth_config(&config)?;

    println!("✓ Google Drive refresh token saved to ~/.ingest/auth.toml");
    println!("  You can now use Google Drive storage without setting GDRIVE_REFRESH_TOKEN.");

    Ok(())
}
