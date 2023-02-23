use crate::ws_response::{WSCallResourceRequest, WSReadyRequest};
use crate::{Cache, RedisCache, WSResult};
use core::result::Result;
use futures::future::join_all;
use futures::FutureExt;
use log::{debug, error, warn};
use rand::prelude::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use warp::filters::ws::Message;
use warp::http::StatusCode;
use warp::reply::with_status;
use warp::Reply;

#[derive(Debug, Clone)]
pub struct Client {
    pub instance_id: String,
    pub senders: HashMap<String, mpsc::UnboundedSender<Result<Message, warp::Error>>>,
    pub version: u16,
}

pub type Clients = Arc<RwLock<HashMap<String, Client>>>;

pub async fn remove_client(clients: &Clients, instance_id: String, sender_id: String) {
    debug!("removing worker {} from {}", sender_id, instance_id);
    let mut writer = clients.write().await;
    debug!(
        "removing worker {} from {}. lock acquired",
        sender_id, instance_id
    );
    let client = writer.get(instance_id.clone().as_str()).unwrap().clone();
    let mut senders = client.senders;
    senders.remove(sender_id.clone().as_str());
    debug!("{} sockets exists", senders.len());
    writer.insert(
        instance_id.clone(),
        Client {
            instance_id: instance_id.clone(),
            senders,
            version: client.version,
        },
    );
    debug!(
        "removing worker {} from {}. releasing lock",
        sender_id, instance_id
    );
}

async fn ping_worker(
    instance_id: String,
    worker: String,
    sender: &mpsc::UnboundedSender<Result<Message, warp::Error>>,
) -> bool {
    return match sender.send(Ok(Message::ping(""))) {
        Ok(()) => {
            debug!("ping sent to {} ({})", instance_id, worker);
            true
        }
        Err(e) => {
            error!("Handle ping error: {:?}", e);
            false
        }
    };
}

async fn ping_client(instance_id: String, client: Client) -> Vec<(String, String)> {
    // gather futures
    let mut futures = Vec::new();
    let mut workers = Vec::new();
    for (worker, sender) in client.senders.iter() {
        futures.push(ping_worker(instance_id.clone(), worker.clone(), sender).boxed());
        workers.push(worker.clone());
    }

    let mut dead_clients = Vec::new();
    for (result, worker) in join_all(futures).await.into_iter().zip(workers) {
        if !result {
            dead_clients.push((instance_id.clone(), worker.clone()));
        }
    }

    dead_clients
}

pub async fn ping_clients(clients: &Clients) {
    loop {
        let mut clients_snapshot: Vec<(String, Client)> = Vec::new();
        {
            for (instance_id, client) in clients.read().await.iter() {
                clients_snapshot.push((instance_id.clone(), client.clone()));
            }
        }

        // gather futures
        let mut futures = Vec::new();
        for (instance_id, client) in clients_snapshot {
            futures.push(ping_client(instance_id.clone(), client.clone()).boxed());
        }

        // drop dead clients
        for result in join_all(futures).await {
            for (instance_id, worker) in result {
                remove_client(clients, instance_id, worker).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(60000)).await;
    }
}

pub async fn wait_for_worker_available(app_cache: RedisCache, uid: String) -> Option<String> {
    let mut counter = 0;
    while counter < 300 {
        let worker_ready_result = app_cache.get_safe(format!("response_{}_ready", uid).as_str());
        if worker_ready_result.is_none() {
            debug!("no workers ready yet for request {}", uid);
            tokio::time::sleep(Duration::from_millis(100)).await;
            counter += 1;
            continue;
        }
        let worker_ready = worker_ready_result.unwrap();
        debug!("found worker for {}: {}", uid, worker_ready);
        return Some(worker_ready);
    }
    return None;
}

pub async fn wait_for_response(app_cache: RedisCache, uid: String) -> Option<WSResult<impl Reply>> {
    let mut counter = 0;
    while counter < 200 {
        let status_code_result = app_cache.get_safe(format!("response_{}_status", uid).as_str());
        if status_code_result.is_none() {
            debug!("no response for request {}", uid);
            tokio::time::sleep(Duration::from_millis(150)).await;
            counter += 1;
            continue;
        }
        let status_code = status_code_result.unwrap();
        debug!("got response, status_code: {}", status_code);
        let body_result = app_cache.get_safe(format!("response_{}_body", uid).as_str());
        if body_result.is_none() {
            warn!("unable to find response body for {}", uid);
            return None;
        }
        let body = body_result.unwrap();
        let status_code_obj = StatusCode::from_u16(status_code.as_str().parse().unwrap()).unwrap();
        return Some(Ok(with_status(body, status_code_obj)));
    }
    return None;
}

pub async fn get_client(instance_id: String, clients: &Clients) -> Option<Client> {
    let client_result = clients.read().await.get(&instance_id).cloned();
    if !client_result.is_some() {
        debug!("no such client: {}", instance_id);
        return None;
    }

    Some(client_result.unwrap())
}

pub async fn ask_resource_from_worker(
    sender: &mpsc::UnboundedSender<Result<Message, warp::Error>>,
    resource: String,
    uid: String,
) -> bool {
    let message = WSCallResourceRequest {
        message_type: "request".to_string(),
        uid: uid.clone(),
        resource: resource.clone(),
    };
    let request_json = serde_json::to_string(&message).unwrap();

    return match sender.send(Ok(Message::text(request_json))) {
        Ok(()) => true,
        Err(e) => {
            error!("Handle Request: {:?}", e);
            false
        }
    };
}

pub async fn ask_resource(
    instance_id: String,
    clients: &Clients,
    resource: String,
    uid: String,
) -> bool {
    let client_result = get_client(instance_id.clone(), &clients).await;
    if client_result.is_none() {
        return false;
    }
    let client = client_result.unwrap();

    let mut senders = Vec::from_iter(client.senders.iter());
    senders.shuffle(&mut thread_rng());

    for (key, sender) in senders {
        debug!(
            "trying to call resource {} using worker {} on {}, uid: {}",
            resource, key, instance_id, uid
        );
        let result = ask_resource_from_worker(sender, resource.clone(), uid.clone()).await;
        if result == true {
            return true;
        }
    }
    return false;
}

async fn ask_worker_readiness(
    sender: &mpsc::UnboundedSender<Result<Message, warp::Error>>,
    uid: String,
) -> bool {
    let message = WSReadyRequest {
        message_type: "ready".to_string(),
        uid: uid.clone(),
    };
    let request_json = serde_json::to_string(&message).unwrap();

    return match sender.send(Ok(Message::text(request_json))) {
        Ok(()) => {
            debug!("message sent");
            true
        }
        Err(e) => {
            error!("Handle Request: {:?}", e);
            false
        }
    };
}

pub async fn ask_readiness(instance_id: String, clients: &Clients, uid: String) -> bool {
    let client_result = get_client(instance_id.clone(), &clients).await;
    if client_result.is_none() {
        return false;
    }
    let client = client_result.unwrap();

    let mut senders = Vec::from_iter(client.senders.iter());
    senders.shuffle(&mut thread_rng());
    let results = join_all(
        senders
            .iter()
            .map(|s| ask_worker_readiness(s.1, uid.clone()).boxed())
            .collect::<Vec<_>>(),
    )
    .await;
    for (result, sender) in results.iter().zip(senders.iter()) {
        if *result {
            continue;
        }
        remove_client(clients, instance_id.clone(), sender.0.clone()).await;
    }

    true
}
