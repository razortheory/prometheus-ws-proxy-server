use clap::{arg, Command};
use log::{info};

use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::sync::{Arc};
use tokio::sync::{mpsc, RwLock};
use warp::{ws::Message, Filter, Rejection};

use crate::cache::Cache;
use crate::cache_redis::RedisCache;

mod handler;
mod ws;
mod ws_request;
mod cache;
mod cache_redis;
mod ws_response;

type WSResult<T> = Result<T, Rejection>;
type Clients = Arc<RwLock<HashMap<String, Client>>>;

#[derive(Debug, Clone)]
pub struct Client {
    pub instance_id: String,
    pub sender: Vec<mpsc::UnboundedSender<Result<Message, warp::Error>>>
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let matches = Command::new("Prometheus websocket server")
        .version("2.0.0")
        .author("Roman Karpovich <fpm.th13f@gmail.com>")
        .about("Proxy prometheus requests with no additional config")
        .args(&[
            arg!([config] "path to config")
                .default_value("client_config.json"),
        ]
        )
        .get_matches();

    let config_path = matches.get_one::<String>("config").unwrap();
    info!("Using config {}", config_path);

    let url_prefix = "proxy";
    let port = 8081;
    let host = [0, 0, 0, 0];

    let clients: Clients = Arc::new(RwLock::with_max_readers(HashMap::new(), 1));
    let app_cache = RedisCache::init("redis://localhost:6379/0");

    let mut base_prefix = warp::any().boxed();
    if url_prefix != "" {
        base_prefix = base_prefix.and(warp::path(url_prefix)).boxed();
    }

    let health_route = base_prefix.clone()
        .and(warp::path!("health"))
        .and_then(handler::health_handler);

    let ws_route = base_prefix.clone()
        .and(warp::path!("ws"))
        // .and(log_body())
        // .map(warp::reply).with(log)
        .and(warp::ws())
        .and(with_clients(clients.clone()))
        .and(with_cache(app_cache.clone()))
        .and_then(handler::ws_handler);

    let call_resource = base_prefix.clone()
        .and(warp::path!("request"/String/String))
        .and(with_clients(clients.clone()))
        .and(with_cache(app_cache.clone()))
        .and_then(handler::call_resource_handler);

    let routes = health_route
        .or(call_resource)
        .or(ws_route)
        .with(warp::cors().allow_any_origin());

    warp::serve(routes).run((host, port)).await;

    Ok(())
}

fn with_clients(clients: Clients) -> impl Filter<Extract = (Clients,), Error = Infallible> + Clone {
    warp::any().map(move || clients.clone())
}

fn with_cache(cache: RedisCache) -> impl Filter<Extract = (RedisCache,), Error = Infallible> + Clone {
    warp::any().map(move || cache.clone())
}
