use futures::StreamExt;
use futures::FutureExt;
use crate::{Cache, Client, Clients, RedisCache};
use serde_json::{from_str, Value};
use serde_valid::json::FromJsonValue;
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::ws::{Message, WebSocket};
use crate::ws_request::{WSProxyCallResponse, WSRegisterRequest, WSRequest};

// #[derive(Deserialize, Debug)]
// pub struct TopicsRequest {
//     topics: Vec<String>,
// }

pub async fn client_connection(ws: WebSocket, clients: Clients, app_cache: RedisCache) {
    let (client_ws_sender, mut client_ws_rcv) = ws.split();
    let (client_sender, client_rcv) = unbounded_channel();
    let client_rcv = UnboundedReceiverStream::new(client_rcv);

    tokio::task::spawn(client_rcv.forward(client_ws_sender).map(|result| {
        if let Err(e) = result {
            eprintln!("error sending websocket msg: {}", e);
        }
    }));

    println!("someone connected");

    // let mut instance_id = String::from("unknown");

    while let Some(result) = client_ws_rcv.next().await {
        // todo: respond ping

        let client_sender = client_sender.clone();
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("error receiving ws message: {}", e);
                break;
            }
        };
        let request_str = msg.to_str().unwrap();
        println!("received {}", request_str);
        let json_value: Value = from_str(request_str).unwrap();
        let request_enum_result = WSRequest::from_json_value(json_value);
        if request_enum_result.is_err() {
            // should never happen
            println!("{}", request_enum_result.unwrap_err().as_validation_errors().unwrap().to_string());
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
                                println!("{:?}", request_result.unwrap_err());
                                // println!("{}", request_result.unwrap_err().as_validation_errors().unwrap().to_string());
                                break;
                            }
                            let request = request_result.unwrap();
                            println!("{:?}", request);
                            let instance_id = request.instance;
                            println!("registering instance {}", instance_id);

                            let mut writer = clients.write().await;
                            if writer.contains_key(instance_id.clone().as_str()) {
                                println!("found client, attaching new sender.");
                                let client = writer.get(instance_id.clone().as_str()).unwrap().clone();
                                let mut sender = client.sender;
                                println!("{} sockets exists", sender.len());
                                sender.push(client_sender);
                                writer.insert(
                                    instance_id.clone(),
                                    Client {instance_id, sender}
                                );
                            } else {
                                writer.insert(
                                    instance_id.clone(),
                                    Client {
                                        instance_id: instance_id.clone(),
                                        sender: vec![client_sender.clone()],
                                    }
                                );
                                println!("client was created successfully");
                            }
                        },
                        "ping" => {
                            println!("ping");
                            // todo: check socket exists in client structure
                        },
                        "response" => {
                            let request_result = WSProxyCallResponse::from_json_value(json_value);
                            if request_result.is_err() {
                                // todo: handle missing fields
                                println!("{:?}", request_result.unwrap_err());
                                // println!("{}", request_result.unwrap_err().as_validation_errors().unwrap().to_string());
                                break;
                            }
                            let request = request_result.unwrap();
                            println!("{:?}", request);
                            app_cache.set(format!("response_{}_body", request.uid).as_str(), request.body).unwrap();
                            app_cache.set(format!("response_{}_status", request.uid).as_str(), request.status.to_string()).unwrap();
                        }
                        _ => {
                            println!("unknown type");
                        }
                    }
                }
            }
        }

        // client_msg(&id, msg, &clients).await;
    }

    // clients.write().await.remove(&id);
    // println!("{} disconnected", id);
}

// async fn client_msg(id: &str, msg: Message, clients: &Clients) {
//     println!("received message from {}: {:?}", id, msg);
//     let message = match msg.to_str() {
//         Ok(v) => v,
//         Err(_) => return,
//     };
//
//     if message == "ping" || message == "ping\n" {
//         return;
//     }
//
//     let topics_req: TopicsRequest = match from_str(&message) {
//         Ok(v) => v,
//         Err(e) => {
//             eprintln!("error while parsing message to topics request: {}", e);
//             return;
//         }
//     };
//
//     let mut locked = clients.write().await;
//     if let Some(v) = locked.get_mut(id) {
//         v.topics = topics_req.topics;
//     }
// }