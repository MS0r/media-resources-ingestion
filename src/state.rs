use crate::services::redis::RedisService;
use crate::services::mongo::MongoDBService;

use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub mongodb_service: MongoDBService,
    pub redis_service: RedisService,
    pub api_key : Arc<String>
}

impl AppState {
    pub fn new(mongodb_service: MongoDBService, redis_service: RedisService, api_key : String) -> Self {
        Self {
            mongodb_service,
            redis_service,
            api_key : Arc::new(api_key)
        }
    }
}