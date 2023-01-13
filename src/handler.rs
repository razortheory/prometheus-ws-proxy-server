use std::time::Duration;
use log::{debug, error};
use crate::{ws, Clients, WSResult as Result, RedisCache, Cache};
use uuid::Uuid;
use warp::{http::StatusCode, ws::Message, Reply};
use warp::reply::with_status;
use crate::ws_response::WSCallResourceRequest;


pub async fn ws_handler(ws: warp::ws::Ws, clients: Clients, app_cache: RedisCache) -> Result<impl Reply> {
    Ok(ws.on_upgrade(move |socket| ws::client_connection(socket, clients, app_cache)))
}

pub async fn health_handler() -> Result<impl Reply> {
    Ok(StatusCode::OK)
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
    for sender in client.sender {
        let message = WSCallResourceRequest {
            message_type: "request".to_string(),
            uid: uid.clone(),
            resource: resource.clone(),
        };
        let request_json = serde_json::to_string(&message).unwrap();
        debug!("{:?}", request_json);
        match sender.send(Ok(Message::text(request_json))) {
            Ok(()) => (),
            Err(e) => {
                // warn!("Handle Request: {:?}", e);
                // return Ok(StatusCode::OK);
                error!("Handle Request: {:?}", e);
                return Err(warp::reject::not_found());
            }
        }
    }

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
        let reply = warp::reply();
        let status_code_obj = StatusCode::from_u16(status_code.as_str().parse().unwrap()).unwrap();
        let body = app_cache.get_safe(format!("response_{}_body", uid).as_str());
        return Ok(with_status(body, status_code_obj));
    }

    Ok(with_status(String::from(""), StatusCode::OK))
}
