mod cli;
mod models;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| format!("{}=debug", env!("CARGO_PKG_NAME")).into())
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting media-resources-ingestion CLI...");

    let config_path = cli::get_config_path()?;
    tracing::info!("Loading config from: {:?}", config_path);

    let request = cli::load_config(config_path.to_str().unwrap())?;

    tracing::info!("Config loaded: {} resources to process", request.resources.len());

    println!("Config loaded successfully. {} resources found.", request.resources.len());

    Ok(())
}