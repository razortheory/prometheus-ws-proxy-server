#![feature(future_join)]

use clap::{arg, value_parser, ArgAction, Command};
use log::{debug, info};

use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::future::join;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::{Filter, Rejection};

use crate::cache::Cache;
use crate::cache_redis::RedisCache;
use crate::config::Config;
use crate::ws_clients::Clients;

mod cache;
mod cache_redis;
mod config;
mod handler;
mod ws;
mod ws_clients;
mod ws_request;
mod ws_response;

type WSResult<T> = Result<T, Rejection>;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let matches = Command::new("Prometheus websocket server")
        .version(VERSION)
        .author("Roman Karpovich <fpm.th13f@gmail.com>")
        .about("Proxy prometheus requests with no network hassle")
        .args(&[
            arg!([config] "path to config").default_value("client_config.json"),
            arg!(--sentry_dsn ... "sentry DSN")
                .action(ArgAction::Set)
                .value_parser(value_parser!(String)),
        ])
        .get_matches();

    let sentry_dsn = matches.get_one::<String>("sentry_dsn");
    let _guard;
    match sentry_dsn {
        Some(sentry_dsn) => {
            debug!("got {} as sentry dsn", sentry_dsn);
            _guard = sentry::init((
                sentry_dsn.clone(),
                sentry::ClientOptions {
                    release: sentry::release_name!(),
                    attach_stacktrace: true,
                    ..Default::default()
                },
            ));
            info!("Sentry configured");
        }
        None => {
            info!("Sentry not configured");
        }
    };

    let config_path = matches.get_one::<String>("config").unwrap();
    info!("Using config {}", config_path);
    let config = Config::from_file(config_path).unwrap();

    let url_prefix = config.url_prefix;
    let port = config.port;
    let host = config.host.parse::<Ipv4Addr>().expect("Invalid host");

    let clients: Clients = Arc::new(RwLock::with_max_readers(HashMap::new(), 1));
    let app_cache = RedisCache::init(
        format!(
            "redis://{}:{}/{}",
            config.redis.host, config.redis.port, config.redis.db
        )
        .as_str(),
    );

    let mut base_prefix = warp::any().boxed();
    if url_prefix != "" {
        base_prefix = base_prefix.and(warp::path(url_prefix)).boxed();
    }

    let health_route = base_prefix
        .clone()
        .and(warp::path!("health"))
        .and_then(handler::health_handler);

    let ws_route = base_prefix
        .clone()
        .and(warp::path!("ws"))
        // .and(log_body())
        // .map(warp::reply).with(log)
        .and(warp::ws())
        .and(with_clients(clients.clone()))
        .and(with_cache(app_cache.clone()))
        .and_then(handler::ws_handler);

    let call_resource = base_prefix
        .clone()
        .and(warp::path!("request" / String / String))
        .and(with_clients(clients.clone()))
        .and(with_cache(app_cache.clone()))
        .and_then(handler::call_resource_handler);

    let routes = health_route
        .or(call_resource)
        .or(ws_route)
        .with(warp::cors().allow_any_origin());

    let ping_clients = ws_clients::ping_clients(&clients);
    let run_server = warp::serve(routes).run((host, port));

    join!(run_server, ping_clients).await;

    Ok(())
}

fn with_clients(clients: Clients) -> impl Filter<Extract = (Clients,), Error = Infallible> + Clone {
    warp::any().map(move || clients.clone())
}

fn with_cache(
    cache: RedisCache,
) -> impl Filter<Extract = (RedisCache,), Error = Infallible> + Clone {
    warp::any().map(move || cache.clone())
}
