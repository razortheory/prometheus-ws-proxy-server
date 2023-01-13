use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct WSCallResourceRequest {
    #[serde(rename(serialize = "type"))]
    pub message_type: String,
    pub uid: String,
    pub resource: String,
}
