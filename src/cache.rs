use std::error::Error;

pub trait Cache {
    fn get(&self, key_name: &str) -> Result<String, Box<dyn Error>>;
    fn get_safe(&self, key_name: &str) -> Option<String>;
    fn set(&self, key_name: &str, value: String) -> Result<(), Box<dyn Error>>;
    fn set_if_not_exists(&self, key_name: &str, value: String) -> Result<(), Box<dyn Error>>;
    fn set_timeout(&self, key_name: &str, seconds: usize) -> Result<(), Box<dyn Error>>;
}
