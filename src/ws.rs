use crate::ws_clients::{Client, Clients};
use crate::ws_request::{WSProxyCallResponse, WSProxyReadyResponse, WSRegisterRequest, WSRequest};
use crate::{Cache, RedisCache};
use futures::StreamExt;
use log::{debug, info, warn};
use serde_json::{from_str, Value};
use serde_valid::json::FromJsonValue;
use std::collections::HashMap;
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;
use warp::ws::WebSocket;

pub async fn client_connection(ws: WebSocket, clients: Clients, app_cache: RedisCache) {
    let (client_ws_sender, mut client_ws_rcv) = ws.split();
    let (client_sender, client_rcv) = unbounded_channel();
    let client_rcv = UnboundedReceiverStream::new(client_rcv);

    tokio::task::spawn(client_rcv.forward(client_ws_sender));

    debug!("someone connected");

    while let Some(result) = client_ws_rcv.next().await {
        let client_sender = client_sender.clone();
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                warn!("error receiving ws message: {}", e);
                break;
            }
        };

        if msg.is_text() {
            let request_str = msg.to_str().unwrap();
            debug!("received {}", request_str);
            let json_value: Value = from_str(request_str).unwrap();
            let request_enum_result = WSRequest::from_json_value(json_value);
            if request_enum_result.is_err() {
                // should never happen
                warn!(
                    "{}",
                    request_enum_result
                        .unwrap_err()
                        .as_validation_errors()
                        .unwrap()
                        .to_string()
                );
                break;
            }

            let request_enum = request_enum_result.unwrap();
            match request_enum {
                WSRequest::Value(json_value) => {
                    let message_type_value = json_value.get("type");
                    if message_type_value.is_some() {
                        let message_type = message_type_value.unwrap().as_str().unwrap();
                        match message_type {
                            "register" => {
                                let request_result = WSRegisterRequest::from_json_value(json_value);
                                if request_result.is_err() {
                                    // todo: handle missing fields
                                    warn!("{:?}", request_result.unwrap_err());
                                    // println!("{}", request_result.unwrap_err().as_validation_errors().unwrap().to_string());
                                    break;
                                }
                                let request = request_result.unwrap();
                                debug!("new register request: {:?}", request);
                                let instance_id = request.instance;

                                let worker_name: String;
                                if request.worker == "unknown" {
                                    worker_name = Uuid::new_v4().to_string();
                                } else {
                                    worker_name = request.worker;
                                }

                                info!(
                                    "registering worker {} for instance {}",
                                    worker_name, instance_id
                                );

                                {
                                    let mut writer = clients.write().await;
                                    if writer.contains_key(instance_id.clone().as_str()) {
                                        debug!("client exists, attaching new sender.");
                                        let client = writer
                                            .get(instance_id.clone().as_str())
                                            .unwrap()
                                            .clone();
                                        let mut sender = client.senders;
                                        sender.insert(worker_name, client_sender);
                                        debug!("{} sockets exists", sender.len());
                                        writer.insert(
                                            instance_id.clone(),
                                            Client {
                                                instance_id,
                                                senders: sender,
                                                version: request.version,
                                            },
                                        );
                                    } else {
                                        writer.insert(
                                            instance_id.clone(),
                                            Client {
                                                instance_id: instance_id.clone(),
                                                senders: HashMap::from([(
                                                    worker_name,
                                                    client_sender,
                                                )]),
                                                version: request.version,
                                            },
                                        );
                                        info!("client was created successfully");
                                    }
                                }
                            }
                            "ping" => {
                                debug!("ping");
                                // todo: check socket exists in client structure
                            }
                            "response" => {
                                let request_result =
                                    WSProxyCallResponse::from_json_value(json_value);
                                if request_result.is_err() {
                                    // todo: handle missing fields
                                    debug!("{:?}", request_result.unwrap_err());
                                    // println!("{}", request_result.unwrap_err().as_validation_errors().unwrap().to_string());
                                    break;
                                }
                                let request = request_result.unwrap();
                                debug!("{:?}", request);

                                let body_cache_key = format!("response_{}_body", request.uid);
                                app_cache
                                    .set(body_cache_key.as_str(), request.body)
                                    .unwrap();
                                app_cache.set_timeout(body_cache_key.as_str(), 60).unwrap();

                                let status_cache_key = format!("response_{}_status", request.uid);
                                app_cache
                                    .set(status_cache_key.as_str(), request.status.to_string())
                                    .unwrap();
                                app_cache
                                    .set_timeout(status_cache_key.as_str(), 60)
                                    .unwrap();
                            }
                            "ready" => {
                                let request_result =
                                    WSProxyReadyResponse::from_json_value(json_value);
                                if request_result.is_err() {
                                    // todo: handle missing fields
                                    debug!("{:?}", request_result.unwrap_err());
                                    // println!("{}", request_result.unwrap_err().as_validation_errors().unwrap().to_string());
                                    break;
                                }
                                let request = request_result.unwrap();
                                debug!("{} ready to process {}", request.worker, request.uid);
                                let ready_cache_key = format!("response_{}_ready", request.uid);
                                app_cache
                                    .set_if_not_exists(ready_cache_key.as_str(), request.worker)
                                    .unwrap();
                                app_cache.set_timeout(ready_cache_key.as_str(), 60).unwrap();
                            }
                            _ => {
                                debug!("unknown type");
                            }
                        }
                    }
                }
            }
        } else if msg.is_pong() {
            debug!("pong received");
        } else if msg.is_ping() {
            let ping_message = msg.into_bytes();
            debug!(
                "ping received: {}",
                String::from_utf8(ping_message).unwrap()
            );
        } else if msg.is_close() {
            debug!("close received");
        } else if msg.is_binary() {
            warn!("binary data received");
        }
    }
}
