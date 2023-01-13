use std::error::Error;

pub trait Cache {
    fn get(&self, key_name: &str) -> Result<String, Box<dyn Error>>;
    fn get_safe(&self, key_name: &str) -> String;
    fn set(&self, key_name: &str, value: String) -> Result<(), Box<dyn Error>>;
}
