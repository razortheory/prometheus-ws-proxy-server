use crate::cache::Cache;
use redis::{Client, Commands};
use std::error::Error;

pub struct RedisCache {
    client: Client,
}

impl RedisCache {
    pub fn init(redis_url: &str) -> RedisCache {
        let client = Client::open(redis_url);
        if client.is_err() {
            panic!("Unable to connect to redis client: {}", client.unwrap_err())
        }
        RedisCache {
            client: client.unwrap(),
        }
    }
}

impl Cache for RedisCache {
    fn get(&self, key_name: &str) -> Result<String, Box<dyn Error>> {
        let mut connection = self.client.get_connection()?;
        let result = connection.get(key_name)?;
        Ok(result)
    }
}
