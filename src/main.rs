mod cli;
mod models;
mod settings;
mod state;
mod services;
mod handlers;

use url::Url;
use tokio::runtime::Runtime;
use handlers::scheduler::download_file;
use crate::{models::Resource, services::{mongo::MongoService, redis::RedisService}};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let rt = Runtime::new()?;

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| format!("{}=debug", env!("CARGO_PKG_NAME")).into())
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting media-resources-ingestion CLI...");
    
    let config = cli::get_config()?;
    tracing::info!("Config loaded: {} resources to process", config.resources.len());
    
    let resource1 = &config.resources[0];
    
    rt.block_on(download_file(&resource1))?;
    tracing::info!("File downloaded successfully to {}", resource1.dest.as_ref().unwrap().path.as_ref().unwrap());
    
    // let mongo_uri = std::env::var("MONGODB_URI").unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
    // tracing::info!("Using MongoDB URL: {}", mongo_uri);
    // let mongo_service = rt.block_on(MongoService::new(&mongo_uri))?;
    // let redis_uri = std::env::var("REDIS_URI").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    // tracing::info!("Using Redis URL: {}", redis_uri);
    // let redis_service = RedisService::new(&redis_uri)?;

    // let state = state::AppState::new(mongo_service, redis_service);
    

    Ok(())
}