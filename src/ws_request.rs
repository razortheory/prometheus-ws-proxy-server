use serde::Deserialize;
use serde_json::Value;
use serde_valid::Validate;

#[derive(Validate, Deserialize, Debug)]
pub struct WSRegisterRequest {
    #[serde(rename(deserialize = "type"))]
    pub message_type: String,
    pub instance: String,
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
#[serde(untagged)]
pub enum WSRequest {
    Value(Value),
}
