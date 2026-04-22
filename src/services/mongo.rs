use mongodb::Client;

#[derive(Clone)]
pub struct MongoDBService {
    client: Client,
}

impl MongoDBService {
    pub async fn new(uri: &str) -> Result<Self, mongodb::error::Error> {
        let client = Client::with_uri_str(uri).await?;
        Ok(Self { client })
    }
}

pub async fn execute_command(service : &MongoDBService, command: mongodb::bson::Document) -> Result<mongodb::bson::Document, mongodb::error::Error> {
    service.client.database("media-metadata").run_command(command).await
}   