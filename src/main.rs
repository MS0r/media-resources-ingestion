mod state;
mod services;
mod routes;
mod error;
mod models;

use routes::system_route;
use services::redis::RedisService;
use state::AppState;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| format!("{}=debug,tower-http=debug", env!("CARGO_PKG_NAME")).into())
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    tracing::info!("Starting up...");

    let mongodb_url = std::env::var("MONGODB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@db:5432/url_shortener".to_string());
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://redis:6379".to_string());
    
    let redis_service = RedisService::new(&redis_url)?;
    let _conn = redis_service.get_connection().await?;
    tracing::info!("Successfully connected to Redis at {}", redis_url);

    let state = AppState::new(mongodb_url, redis_service);

    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_headers(Any)
        .allow_origin(Any);


    let app = axum::Router::new()
        .nest("/api/v1", system_route())
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("Listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}