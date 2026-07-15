use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

#[derive(Deserialize, Debug)]
pub struct RedisConfig {
    pub host: String,
    pub port: u16,
    pub db: u16,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub redis: RedisConfig,
    pub url_prefix: String,
    pub host: String,
    pub port: u16,
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Config, Box<dyn Error + Send + Sync>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let c: Config = serde_json::from_reader(reader)?;
        Ok(c)
    }
}
