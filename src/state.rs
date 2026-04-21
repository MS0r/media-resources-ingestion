use crate::services::redis::RedisService;

use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub mongodb: Arc<String>,
    pub redis_service: RedisService,
}

impl AppState {
    pub fn new(mongodb: String, redis_service: RedisService) -> Self {
        Self {
            mongodb: Arc::new(mongodb),
            redis_service,
        }
    }
}