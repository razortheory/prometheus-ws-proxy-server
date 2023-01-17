use core::result::Result;
use std::collections::HashMap;
use std::sync::Arc;
use log::debug;
use warp::filters::ws::Message;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Clone)]
pub struct Client {
    pub instance_id: String,
    pub senders: HashMap<String, mpsc::UnboundedSender<Result<Message, warp::Error>>>
}

pub type Clients = Arc<RwLock<HashMap<String, Client>>>;

pub async fn remove_client(clients: &Clients, instance_id: String, sender_id: String) {
    let mut writer = clients.write().await;
    // todo: block access to writer
    let client = writer.get(instance_id.clone().as_str()).unwrap().clone();
    let mut senders = client.senders;
    senders.remove(sender_id.as_str());
    debug!("{} sockets exists", senders.len());
    writer.insert(
        instance_id.clone(),
        Client {instance_id, senders }
    );
}