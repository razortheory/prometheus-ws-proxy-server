pub mod config;
mod metrics;
mod server;

pub use server::{build_router, AppState};

#[cfg(test)]
mod tests {
    use super::metrics::{CloseReason, ScrapeOutcome};
    use super::server::Limits;
    use super::{build_router, config::Config, AppState};
    use axum::body::Body;
    use futures_util::{SinkExt, StreamExt};
    use http::{Request, StatusCode};
    use serde_json::{json, Value};
    use std::collections::HashSet;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{connect_async, tungstenite::Message};
    use tower::ServiceExt;

    #[test]
    fn legacy_redis_config_is_still_accepted() {
        let config: Config = serde_json::from_str(
            r#"{
                "redis": {"host": "redis", "port": 6379, "db": 2},
                "url_prefix": "proxy",
                "host": "0.0.0.0",
                "port": 8081
            }"#,
        )
        .expect("legacy config should parse");

        assert_eq!(config.url_prefix, "proxy");
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 8081);
    }

    #[tokio::test]
    async fn health_route_accepts_trailing_and_no_slash() {
        let app = build_router("proxy", AppState::new());

        for uri in ["/proxy/health", "/proxy/health/"] {
            let response = app
                .clone()
                .oneshot(Request::get(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
        }

        let response = app
            .oneshot(
                Request::post("/proxy/response/unknown")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from("status=200&body=ignored"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn test_server(
        state: AppState,
    ) -> (
        String,
        tokio_util::sync::CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = state.shutdown_token();
        let app = build_router("proxy", state);
        let server_shutdown = shutdown.clone();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(server_shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        (format!("127.0.0.1:{}", address.port()), shutdown, handle)
    }

    async fn receive_json<S>(socket: &mut S) -> Value
    where
        S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if matches!(value["type"].as_str(), Some("ping" | "pong")) {
                        continue;
                    }
                    return value;
                }
                Message::Ping(_) | Message::Pong(_) => continue,
                message => panic!("unexpected websocket message: {message:?}"),
            }
        }
    }

    #[tokio::test]
    async fn protocol_v2_round_trip_uses_websocket_response() {
        let state = AppState::for_tests(Duration::from_secs(1), Duration::from_secs(1), 1024);
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut socket, _) = connect_async(format!("ws://{address}/proxy/ws/"))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({"type":"register","instance":"demo","worker":"worker-1","version":2})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("demo", "worker-1", 0).await;

        let get = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node"
        )));
        let ready = receive_json(&mut socket).await;
        assert_eq!(ready["type"], "ready");
        socket
            .send(Message::Text(
                json!({"type":"ready","uid":ready["uid"],"worker":"worker-1"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let request = receive_json(&mut socket).await;
        assert_eq!(request["type"], "request");
        assert_eq!(request["resource"], "node");
        socket
            .send(Message::Text(
                json!({"type":"response","uid":request["uid"],"status":201,"body":"metric 42\n"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();

        let response = get.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.text().await.unwrap(), "metric 42\n");
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn protocol_v3_round_trip_accepts_form_post_response() {
        let state = AppState::for_tests(Duration::from_secs(1), Duration::from_secs(1), 1024);
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({"type":"register","instance":"demo","worker":"worker-3","version":3})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("demo", "worker-3", 0).await;

        let get = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node/"
        )));
        let ready = receive_json(&mut socket).await;
        socket
            .send(Message::Text(
                json!({"type":"ready","uid":ready["uid"],"worker":"worker-3"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let request = receive_json(&mut socket).await;

        let post = reqwest::Client::new()
            .post(format!(
                "http://{address}/proxy/response/{}/",
                request["uid"].as_str().unwrap()
            ))
            .form(&[("status", "202"), ("body", "from form")])
            .send()
            .await
            .unwrap();
        assert_eq!(post.status(), StatusCode::OK);
        let response = get.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(response.text().await.unwrap(), "from form");
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn disconnected_worker_is_removed_without_removing_replacement_generation() {
        let state =
            AppState::for_tests(Duration::from_millis(100), Duration::from_millis(100), 1024);
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut old_socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        old_socket
            .send(Message::Text(
                json!({"type":"register","instance":"demo","worker":"same","version":1})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let old_generation = state.wait_for_worker_after("demo", "same", 0).await;
        let (mut new_socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        new_socket
            .send(Message::Text(
                json!({"type":"register","instance":"demo","worker":"same","version":1})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let new_generation = state
            .wait_for_worker_after("demo", "same", old_generation)
            .await;
        old_socket.close(None).await.unwrap();

        let get = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node"
        )));
        let request = receive_json(&mut new_socket).await;
        new_socket
            .send(Message::Text(
                json!({"type":"response","uid":request["uid"],"status":200,"body":"ok"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(get.await.unwrap().unwrap().status(), StatusCode::OK);
        assert_eq!(state.debug_counts().await, (1, 0));

        new_socket.close(None).await.unwrap();
        state
            .wait_for_worker_removed("demo", "same", new_generation)
            .await;
        assert_eq!(state.debug_counts().await, (0, 0));
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn busy_worker_returns_unavailable() {
        let state = AppState::for_tests(Duration::from_millis(50), Duration::from_secs(5), 1024);
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({"type":"register","instance":"demo","worker":"one","version":1})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("demo", "one", 0).await;

        let first = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node"
        )));
        let request = receive_json(&mut socket).await;
        let second = reqwest::get(format!("http://{address}/proxy/request/demo/node"))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
        socket
            .send(Message::Text(
                json!({"type":"response","uid":request["uid"],"status":200,"body":"ok"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
        assert_eq!(state.debug_counts().await, (1, 0));

        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn response_timeout_releases_worker() {
        let state = AppState::for_tests(Duration::from_millis(50), Duration::from_secs(1), 1024);
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({"type":"register","instance":"demo","worker":"one","version":1})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("demo", "one", 0).await;

        let get = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node"
        )));
        let _request = receive_json(&mut socket).await;
        assert_eq!(
            get.await.unwrap().unwrap().status(),
            StatusCode::NOT_IMPLEMENTED
        );
        assert_eq!(state.debug_counts().await, (1, 0));

        let next = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node"
        )));
        let request = receive_json(&mut socket).await;
        socket
            .send(Message::Text(
                json!({"type":"response","uid":request["uid"],"status":200,"body":"ok"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(next.await.unwrap().unwrap().status(), StatusCode::OK);
        assert_eq!(state.debug_counts().await, (1, 0));

        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ready_timeout_returns_unavailable_and_cleans_pending_request() {
        let state = AppState::for_tests(Duration::from_millis(30), Duration::from_secs(1), 1024);
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({"type":"register","instance":"demo","worker":"two","version":2})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("demo", "two", 0).await;

        let get = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node"
        )));
        let ready = receive_json(&mut socket).await;
        assert_eq!(ready["type"], "ready");
        assert_eq!(
            get.await.unwrap().unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(state.debug_counts().await, (1, 0));

        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ready_broadcast_selects_live_worker_without_waiting_for_stale_worker() {
        let state = AppState::for_tests(Duration::from_secs(1), Duration::from_secs(1), 1024);
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut stale, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        stale
            .send(Message::Text(
                json!({"type":"register","instance":"demo","worker":"stale","version":2})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("demo", "stale", 0).await;
        let (mut live, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        live.send(Message::Text(
            json!({"type":"register","instance":"demo","worker":"live","version":2})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        state.wait_for_worker_after("demo", "live", 0).await;

        let get = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node"
        )));
        let (stale_ready, live_ready) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(receive_json(&mut stale), receive_json(&mut live))
        })
        .await
        .expect("ready was not broadcast to every idle worker");
        assert_eq!(stale_ready["uid"], live_ready["uid"]);
        live.send(Message::Text(
            json!({"type":"ready","uid":live_ready["uid"],"worker":"live"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let request = receive_json(&mut live).await;
        live.send(Message::Text(
            json!({"type":"response","uid":request["uid"],"status":200,"body":"live"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

        let response = get.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "live");
        assert_eq!(state.debug_counts().await, (2, 0));
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_requests_atomically_select_different_workers() {
        let state = AppState::for_tests(Duration::from_secs(1), Duration::from_secs(1), 1024);
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut first_worker, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        first_worker
            .send(Message::Text(
                json!({"type":"register","instance":"demo","worker":"first","version":3})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("demo", "first", 0).await;
        let (mut second_worker, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        second_worker
            .send(Message::Text(
                json!({"type":"register","instance":"demo","worker":"second","version":3})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("demo", "second", 0).await;

        let first_get = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node"
        )));
        let second_get = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/metrics"
        )));
        let (first_ready_a, first_ready_b, second_ready_a, second_ready_b) =
            tokio::time::timeout(Duration::from_secs(1), async {
                let (first_a, first_b) = tokio::join!(
                    receive_json(&mut first_worker),
                    receive_json(&mut second_worker)
                );
                let (second_a, second_b) = tokio::join!(
                    receive_json(&mut first_worker),
                    receive_json(&mut second_worker)
                );
                (first_a, second_a, first_b, second_b)
            })
            .await
            .expect("both requests were not broadcast to both workers");
        let first_uids = HashSet::from([
            first_ready_a["uid"].as_str().unwrap(),
            first_ready_b["uid"].as_str().unwrap(),
        ]);
        let second_uids = HashSet::from([
            second_ready_a["uid"].as_str().unwrap(),
            second_ready_b["uid"].as_str().unwrap(),
        ]);
        assert_eq!(first_uids, second_uids);
        assert_eq!(first_uids.len(), 2);

        let first_uid = first_ready_a["uid"].as_str().unwrap();
        let second_uid = first_uids
            .iter()
            .copied()
            .find(|uid| *uid != first_uid)
            .unwrap();
        first_worker
            .send(Message::Text(
                json!({"type":"ready","uid":first_uid,"worker":"first"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        second_worker
            .send(Message::Text(
                json!({"type":"ready","uid":second_uid,"worker":"second"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let first_request = receive_json(&mut first_worker).await;
        let second_request = receive_json(&mut second_worker).await;
        assert_ne!(first_request["uid"], second_request["uid"]);
        first_worker
            .send(Message::Text(
                json!({"type":"response","uid":first_request["uid"],"status":200,"body":"one"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        second_worker
            .send(Message::Text(
                json!({"type":"response","uid":second_request["uid"],"status":200,"body":"two"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();

        assert_eq!(first_get.await.unwrap().unwrap().status(), StatusCode::OK);
        assert_eq!(second_get.await.unwrap().unwrap().status(), StatusCode::OK);
        assert_eq!(state.debug_counts().await, (2, 0));
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn global_capacity_rejects_work_even_when_another_worker_is_idle() {
        let state = AppState::for_tests(Duration::from_secs(1), Duration::from_secs(1), 1);
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut first_socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        first_socket
            .send(Message::Text(
                json!({"type":"register","instance":"first","worker":"one","version":1})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("first", "one", 0).await;
        let (mut second_socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        second_socket
            .send(Message::Text(
                json!({"type":"register","instance":"second","worker":"two","version":1})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("second", "two", 0).await;

        let first = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/first/node"
        )));
        let request = receive_json(&mut first_socket).await;
        let overloaded = reqwest::get(format!("http://{address}/proxy/request/second/node"))
            .await
            .unwrap();
        assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
        first_socket
            .send(Message::Text(
                json!({"type":"response","uid":request["uid"],"status":200,"body":"done"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);

        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn unknown_instance_stays_not_found_when_global_capacity_is_exhausted() {
        let state = AppState::for_tests(Duration::from_secs(1), Duration::from_secs(1), 1);
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({"type":"register","instance":"known","worker":"one","version":1})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("known", "one", 0).await;

        let occupied = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/known/node"
        )));
        let request = receive_json(&mut socket).await;
        let unknown = reqwest::get(format!("http://{address}/proxy/request/missing/node"))
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

        socket
            .send(Message::Text(
                json!({"type":"response","uid":request["uid"],"status":200,"body":"done"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        assert_eq!(occupied.await.unwrap().unwrap().status(), StatusCode::OK);
        assert_eq!(state.debug_counts().await, (1, 0));
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn graceful_shutdown_closes_registered_websocket_promptly() {
        let state = AppState::new();
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({"type":"register","instance":"demo","worker":"shutdown","version":3})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        state.wait_for_worker_after("demo", "shutdown", 0).await;

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server did not stop promptly")
            .unwrap();
    }

    fn fast_limits() -> Limits {
        Limits {
            ready_timeout: Duration::from_secs(1),
            response_timeout: Duration::from_secs(1),
            ..Limits::default()
        }
    }

    async fn register<S>(socket: &mut S, instance: &str, worker: &str, version: u16)
    where
        S: SinkExt<Message> + Unpin,
        <S as futures_util::Sink<Message>>::Error: std::fmt::Debug,
    {
        socket
            .send(Message::Text(
                json!({"type":"register","instance":instance,"worker":worker,"version":version})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    }

    /// Reads until a close frame arrives and returns its reason.
    async fn receive_close_reason<S>(socket: &mut S) -> String
    where
        S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match socket.next().await {
                    Some(Ok(Message::Close(Some(frame)))) => return frame.reason.to_string(),
                    Some(Ok(Message::Close(None))) => return String::new(),
                    Some(Ok(_)) => continue,
                    other => panic!("connection ended without a close frame: {other:?}"),
                }
            }
        })
        .await
        .expect("no close frame received")
    }

    #[tokio::test]
    async fn replaced_connection_receives_close_frame_and_ends() {
        let state = AppState::for_tests_with(fast_limits());
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut old_socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        register(&mut old_socket, "demo", "same", 3).await;
        let old_generation = state.wait_for_worker_after("demo", "same", 0).await;
        let (mut new_socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        register(&mut new_socket, "demo", "same", 3).await;
        state
            .wait_for_worker_after("demo", "same", old_generation)
            .await;

        assert_eq!(
            receive_close_reason(&mut old_socket).await,
            "worker replaced"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.metrics().closes(CloseReason::Replaced) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("replaced connection loop kept running");
        assert_eq!(state.debug_counts().await, (1, 0));
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn heartbeat_timeout_sends_close_frame() {
        // The first tick already sees a timeout, so no ping precedes the
        // close frame; a ping would make the client queue a pong that it then
        // writes into the closed socket, and the resulting reset can discard
        // the close frame before the test reads it.
        let state = AppState::for_tests_with(Limits {
            heartbeat_interval: Duration::from_millis(100),
            heartbeat_timeout: Duration::from_millis(10),
            ..fast_limits()
        });
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        register(&mut socket, "demo", "silent", 3).await;
        let generation = state.wait_for_worker_after("demo", "silent", 0).await;

        // Not polling the socket suppresses automatic pongs, so the server
        // sees silence.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(receive_close_reason(&mut socket).await, "heartbeat timeout");
        state
            .wait_for_worker_removed("demo", "silent", generation)
            .await;
        assert_eq!(state.metrics().closes(CloseReason::HeartbeatTimeout), 1);
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn silent_worker_is_skipped_for_dispatch() {
        let state = AppState::for_tests_with(Limits {
            stale_after: Duration::from_millis(150),
            ..fast_limits()
        });
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut quiet, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        register(&mut quiet, "demo", "quiet", 1).await;
        state.wait_for_worker_after("demo", "quiet", 0).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let only_stale = reqwest::get(format!("http://{address}/proxy/request/demo/node"))
            .await
            .unwrap();
        assert_eq!(only_stale.status(), StatusCode::SERVICE_UNAVAILABLE);

        let (mut fresh, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        register(&mut fresh, "demo", "fresh", 1).await;
        state.wait_for_worker_after("demo", "fresh", 0).await;
        let get = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node"
        )));
        let request = receive_json(&mut fresh).await;
        fresh
            .send(Message::Text(
                json!({"type":"response","uid":request["uid"],"status":200,"body":"fresh"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let response = get.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "fresh");
        assert_eq!(state.metrics().scrapes(ScrapeOutcome::NoIdleWorker), 1);
        drop(quiet);
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn scrape_timeout_header_bounds_the_ready_wait() {
        let state = AppState::for_tests_with(Limits {
            ready_timeout: Duration::from_secs(30),
            ..fast_limits()
        });
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        register(&mut socket, "demo", "mute", 3).await;
        state.wait_for_worker_after("demo", "mute", 0).await;

        let started = std::time::Instant::now();
        let response = reqwest::Client::new()
            .get(format!("http://{address}/proxy/request/demo/node"))
            .header("X-Prometheus-Scrape-Timeout-Seconds", "1")
            .send()
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(elapsed >= Duration::from_millis(400), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
        assert_eq!(state.metrics().scrapes(ScrapeOutcome::ReadyTimeout), 1);
        assert_eq!(state.debug_counts().await, (1, 0));
        drop(socket);
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn scrape_is_retried_on_another_worker_when_the_selected_one_disconnects() {
        let state = AppState::for_tests_with(fast_limits());
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut lost, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        register(&mut lost, "demo", "lost", 3).await;
        state.wait_for_worker_after("demo", "lost", 0).await;
        let (mut survivor, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        register(&mut survivor, "demo", "survivor", 3).await;
        state.wait_for_worker_after("demo", "survivor", 0).await;

        let get = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node"
        )));
        let first_ready = receive_json(&mut lost).await;
        lost.send(Message::Text(
            json!({"type":"ready","uid":first_ready["uid"],"worker":"lost"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let request = receive_json(&mut lost).await;
        assert_eq!(request["type"], "request");
        lost.close(None).await.unwrap();

        let retry_ready = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let message = receive_json(&mut survivor).await;
                if message["type"] == "ready" && message["uid"] != first_ready["uid"] {
                    return message;
                }
            }
        })
        .await
        .expect("scrape was not retried on the surviving worker");
        survivor
            .send(Message::Text(
                json!({"type":"ready","uid":retry_ready["uid"],"worker":"survivor"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let request = receive_json(&mut survivor).await;
        survivor
            .send(Message::Text(
                json!({"type":"response","uid":request["uid"],"status":200,"body":"retried"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();

        let response = get.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "retried");
        assert_eq!(state.metrics().retries(), 1);
        assert_eq!(state.debug_counts().await, (1, 0));
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn scrape_waits_for_a_reconnecting_worker_after_losing_the_only_one() {
        let state = AppState::for_tests_with(fast_limits());
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut first, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        register(&mut first, "demo", "unknown", 1).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.debug_counts().await.0 == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        let get = tokio::spawn(reqwest::get(format!(
            "http://{address}/proxy/request/demo/node"
        )));
        let _request = receive_json(&mut first).await;
        first.close(None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (mut second, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        register(&mut second, "demo", "unknown", 1).await;
        let request = receive_json(&mut second).await;
        second
            .send(Message::Text(
                json!({"type":"response","uid":request["uid"],"status":200,"body":"again"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let response = get.await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "again");
        assert_eq!(state.metrics().retries(), 1);
        shutdown.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn metrics_route_reports_workers_and_scrape_outcomes() {
        let state = AppState::for_tests_with(fast_limits());
        let (address, shutdown, server) = test_server(state.clone()).await;
        let (mut socket, _) = connect_async(format!("ws://{address}/proxy/ws"))
            .await
            .unwrap();
        register(&mut socket, "demo", "worker", 3).await;
        state.wait_for_worker_after("demo", "worker", 0).await;
        let missing = reqwest::get(format!("http://{address}/proxy/request/missing/node"))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        let metrics = reqwest::get(format!("http://{address}/metrics"))
            .await
            .unwrap();
        assert_eq!(metrics.status(), StatusCode::OK);
        let text = metrics.text().await.unwrap();
        for line in [
            "proxy_workers{version=\"3\"} 1",
            "proxy_worker_registrations_total{version=\"3\"} 1",
            "proxy_websocket_connections 1",
            "proxy_scrapes_total{result=\"unknown_instance\"} 1",
        ] {
            assert!(
                text.lines().any(|candidate| candidate == line),
                "{line}\n{text}"
            );
        }
        drop(socket);
        shutdown.cancel();
        server.await.unwrap();
    }
}
