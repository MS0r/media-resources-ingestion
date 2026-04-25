use crate::services::redis::RedisService;
use crate::services::mongo::MongoService;

#[derive(Clone)]
pub struct AppState {
    pub mongo_service: MongoService,
    pub redis_service: RedisService,
}

impl AppState {
    pub fn new(mongo_service: MongoService, redis_service: RedisService) -> Self {
        Self {
            mongo_service,
            redis_service,
        }
    }
}