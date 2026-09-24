#![cfg(unix)]

use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn signal(child: &Child, name: &str) {
    let status = Command::new("kill")
        .arg(format!("-{name}"))
        .arg(child.id().to_string())
        .status()
        .expect("kill is available");
    assert!(status.success());
}

async fn healthy(port: u16) -> bool {
    matches!(
        reqwest::get(format!("http://127.0.0.1:{port}/proxy/health/")).await,
        Ok(response) if response.status().is_success()
    )
}

#[tokio::test]
async fn sighup_is_ignored_and_sigterm_still_stops_the_server() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let config = std::env::temp_dir().join(format!("proxy-server-reload-{port}.json"));
    std::fs::write(
        &config,
        format!(
            r#"{{"redis":{{"host":"localhost","port":6379,"db":0}},"url_prefix":"proxy","host":"127.0.0.1","port":{port}}}"#
        ),
    )
    .unwrap();

    let mut server = Server(
        Command::new(env!("CARGO_BIN_EXE_prometheus-proxy-server"))
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let started = tokio::time::timeout(Duration::from_secs(10), async {
        while !healthy(port).await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(started.is_ok(), "server did not start");

    signal(&server.0, "HUP");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        server.0.try_wait().unwrap().is_none(),
        "SIGHUP stopped the server"
    );
    assert!(healthy(port).await);

    signal(&server.0, "TERM");
    let stopped = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = server.0.try_wait().unwrap() {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("SIGTERM did not stop the server");
    assert!(stopped.success());
    let _ = std::fs::remove_file(config);
}
