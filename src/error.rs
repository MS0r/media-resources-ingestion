use serde::{Deserialize, Serialize};
use axum::{
    http::StatusCode, response::IntoResponse, Json
};

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    error: String,
    message: String,
    field: Option<String>,
    job_id: Option<String>,
}

pub enum AppError {
    NotFound(ErrorResponse),
    InternalError(ErrorResponse),
    BadRequest(ErrorResponse),   
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AppError::NotFound(err) => (StatusCode::NOT_FOUND, Json(err)).into_response(),
            AppError::InternalError(err) => (StatusCode::INTERNAL_SERVER_ERROR, Json(err)).into_response(),
            AppError::BadRequest(err) => (StatusCode::BAD_REQUEST, Json(err)).into_response(),
        }
    }
}

pub type AppResult<T> = Result<T, AppError>;