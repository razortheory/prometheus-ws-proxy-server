use crate::cache::Cache;
use redis::{Client, Commands, RedisResult};
use std::error::Error;

#[derive(Clone)]
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
    fn get_safe(&self, key_name: &str) -> Option<String> {
        let connection_result = self.client.get_connection();
        if connection_result.is_err() {
            return None;
        }
        let mut connection = connection_result.unwrap();

        let result: RedisResult<String> = connection.get(key_name);
        if result.is_err() {
            return None;
        }

        return Some(String::from(result.unwrap()));
    }
    fn set(&self, key_name: &str, value: String) -> Result<(), Box<dyn Error>> {
        let mut connection = self.client.get_connection()?;
        let result = connection.set(key_name, value)?;
        Ok(result)
    }
    fn set_if_not_exists(&self, key_name: &str, value: String) -> Result<(), Box<dyn Error>> {
        let mut connection = self.client.get_connection()?;
        let result = connection.set_nx(key_name, value)?;
        Ok(result)
    }
    fn set_timeout(&self, key_name: &str, seconds: usize) -> Result<(), Box<dyn Error>> {
        let mut connection = self.client.get_connection()?;
        let result = connection.expire(key_name, seconds)?;
        Ok(result)
    }
}
