use crate::{
    error::{ErrorResponse, AppError, AppResult},
    models::HealthCheckResponse,
    state::AppState,
};

use axum::{
    Json, Router, extract::{Extension, State}, http::StatusCode, response::IntoResponse, routing::{get, post}
};

pub fn route() -> Router<AppState> {
    Router::new()
    .route("/health", get(health_check))
}

async fn health_check(
    State(_state): State<AppState>
) -> AppResult<impl IntoResponse> {

    let status = HealthCheckResponse {
        status: "ok".to_string(),
    };
    
    Ok(Json(status))
}