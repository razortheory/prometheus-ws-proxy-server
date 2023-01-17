use serde::Deserialize;
use serde_json::Value;
use serde_valid::Validate;

fn default_worker_name() -> String {
    "unknown".to_string()
}

fn default_version() -> u16 {
    1
}

// todo: optional worker and version
#[derive(Validate, Deserialize, Debug)]
pub struct WSRegisterRequest {
    #[serde(rename(deserialize = "type"))]
    pub message_type: String,
    pub instance: String,
    #[serde(default = "default_worker_name")]
    pub worker: String,
    #[serde(default = "default_version")]
    pub version: u16,
}

#[derive(Validate, Deserialize, Debug)]
pub struct WSProxyCallResponse {
    #[serde(rename(deserialize = "type"))]
    pub message_type: String,
    pub uid: String,
    pub body: String,
    pub status: u16,
}

#[derive(Validate, Deserialize, Debug)]
pub struct WSProxyReadyResponse {
    #[serde(rename(deserialize = "type"))]
    pub message_type: String,
    pub uid: String,
    pub worker: String,
}

#[derive(Validate, Deserialize, Debug)]
#[serde(untagged)]
pub enum WSRequest {
    Value(Value),
}
