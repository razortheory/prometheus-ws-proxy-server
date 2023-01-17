use std::time::Duration;
use log::{debug, error};
use crate::{Cache, Clients, RedisCache, ws, WSResult as Result};
use uuid::Uuid;
use warp::{http::StatusCode, Reply, ws::Message};
use warp::reply::with_status;
use crate::ws_response::{WSCallResourceRequest, WSReadyRequest};
use rand::thread_rng;
use rand::seq::SliceRandom;
use crate::ws_clients::remove_client;


pub async fn ws_handler(ws: warp::ws::Ws, clients: Clients, app_cache: RedisCache) -> Result<impl Reply> {
    Ok(ws.on_upgrade(move |socket| ws::client_connection(socket, clients, app_cache)))
}

pub async fn health_handler() -> Result<impl Reply> {
    Ok(StatusCode::OK)
}

// todo: make response optional
async fn wait_for_worker_available(app_cache: RedisCache, uid: String) -> String {
    let mut counter = 0;
    while counter < 100 {
        let worker_ready = app_cache.get_safe(format!("response_{}_ready", uid).as_str());
        if worker_ready == String::from("") {
            debug!("no workers ready for request {}", uid);
            tokio::time::sleep(Duration::from_millis(100)).await;
            counter += 1;
            continue;
        }
        debug!("found worker for {}: {}", uid, worker_ready);
        return worker_ready;
    }
    return String::from("");
}

async fn wait_for_response(app_cache: RedisCache, uid: String) -> Result<impl Reply> {
    let mut counter = 0;
    while counter < 60 {
        let status_code = app_cache.get_safe(format!("response_{}_status", uid).as_str());
        if status_code == String::from("") {
            debug!("no response for request {}", uid);
            tokio::time::sleep(Duration::from_millis(500)).await;
            counter += 1;
            continue;
        }
        debug!("got response, status_code: {}", status_code);
        let status_code_obj = StatusCode::from_u16(status_code.as_str().parse().unwrap()).unwrap();
        let body = app_cache.get_safe(format!("response_{}_body", uid).as_str());
        return Ok(with_status(body, status_code_obj));
    }
    // todo: return custom reject with timeout
    return Err(warp::reject::not_found());
}

pub async fn call_resource_handler(instance_id: String, resource: String, clients: Clients, app_cache: RedisCache) -> Result<impl Reply> {
    let client_result = clients.read().await.get(&instance_id).cloned();
    if !client_result.is_some() {
        debug!("no such client: {}", instance_id);
        return Err(warp::reject::not_found());
    }

    let client = client_result.unwrap();

    debug!("found client: {}", instance_id);
    let uid = Uuid::new_v4().to_string();

    let mut senders = Vec::from_iter(client.senders.iter());
    senders.shuffle(&mut thread_rng());

    for (key, sender) in senders {
        let request_json: String;
        if client.version == 1 {
            let message = WSCallResourceRequest {
                message_type: "request".to_string(),
                uid: uid.clone(),
                resource: resource.clone(),
            };
            request_json = serde_json::to_string(&message).unwrap();
        } else {
            let message = WSReadyRequest {
                message_type: "ready".to_string(),
                uid: uid.clone(),
            };
            request_json = serde_json::to_string(&message).unwrap();
        }

        match sender.send(Ok(Message::text(request_json))) {
            Ok(()) => {
                // we send request to every worker, so no break required
                // debug!("message sent");
                if client.version == 1 {
                    break;
                }
            },
            Err(e) => {
                error!("Handle Request: {:?}", e);
                let instance_id = instance_id.clone();
                remove_client(&clients, instance_id, key.clone()).await;
                // todo: close socket
            }
        }
    }

    if client.version > 1 {
        let worker_name = wait_for_worker_available(app_cache.clone(), uid.clone()).await;
        if worker_name == "" {
            error!("unable to find worker for {}", uid);
            // todo: return better error response
            return Err(warp::reject::not_found());
        }

        let sender = client.senders.get(&worker_name).unwrap();
        let message = WSCallResourceRequest {
            message_type: "request".to_string(),
            uid: uid.clone(),
            resource: resource.clone(),
        };
        let request_json = serde_json::to_string(&message).unwrap();
        match sender.send(Ok(Message::text(request_json))) {
            Ok(()) => {
                debug!("message sent");
            },
            Err(e) => {
                error!("Handle Request: {:?}", e);
                // todo: close socket
            }
        }
    }

    wait_for_response(app_cache, uid).await
}
