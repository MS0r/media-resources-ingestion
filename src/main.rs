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

    let config = cli::get_config()?;
    tracing::info!("Loading config from: {:?}", config);

    println!("{}",config.path);
    tracing::info!("Config loaded: {} resources to process", config.resources.len());

    println!("Config loaded successfully. {} resources found.", config.resources.len());

    Ok(())
}