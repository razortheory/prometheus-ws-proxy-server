use crate::ws;
use crate::ws_clients;
use crate::Clients;
use crate::RedisCache;
use crate::WSResult;
use log::{debug, error};
use uuid::Uuid;
use warp::{http::StatusCode, Reply};

pub async fn ws_handler(
    ws: warp::ws::Ws,
    clients: Clients,
    app_cache: RedisCache,
) -> WSResult<impl Reply> {
    Ok(ws
        .max_frame_size(64 * 1024 * 1024)
        .max_message_size(64 * 1024 * 1024)
        .on_upgrade(move |socket| ws::client_connection(socket, clients, app_cache)))
}

pub async fn health_handler() -> WSResult<impl Reply> {
    Ok(StatusCode::OK)
}

pub async fn call_resource_handler(
    instance_id: String,
    resource: String,
    clients: Clients,
    app_cache: RedisCache,
) -> WSResult<impl Reply> {
    let client_result = ws_clients::get_client(instance_id.clone(), &clients).await;
    if client_result.is_none() {
        return Err(warp::reject::not_found());
    }
    let client = client_result.unwrap();

    debug!("found client: {}", instance_id);
    let uid = Uuid::new_v4().to_string();

    if client.version > 1 {
        // protocol version 2
        // ask all workers whether they are ready and then asks first for resource
        ws_clients::ask_readiness(instance_id.clone(), &clients, uid.clone()).await;

        let worker_name_result =
            ws_clients::wait_for_worker_available(app_cache.clone(), uid.clone()).await;
        if worker_name_result.is_none() {
            error!("unable to find worker for {}", uid);
            return Err(warp::reject::not_found());
        }

        let worker_name = worker_name_result.unwrap();
        let sender = client.senders.get(&worker_name).unwrap();
        let result = ws_clients::ask_resource_from_worker(sender, resource, uid.clone()).await;
        if result == false {
            error!(
                "unable to send request {} to worker {} despite it was ready",
                uid, worker_name
            );
            return Err(warp::reject::reject());
        }
    } else {
        // protocol version 1
        // shuffle workers and try to request all of them sequentially
        ws_clients::ask_resource(instance_id.clone(), &clients, resource.clone(), uid.clone())
            .await;
    }

    match ws_clients::wait_for_response(app_cache, uid).await {
        Some(response) => response,
        None => Err(warp::reject::reject()),
    }
}
