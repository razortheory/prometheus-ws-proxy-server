use std::error::Error;

pub trait Cache {
    fn get(&self, key_name: &str) -> Result<String, Box<dyn Error>>;
}
