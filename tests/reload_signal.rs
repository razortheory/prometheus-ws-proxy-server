#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn describe(status: ExitStatus) -> String {
    match status.signal() {
        Some(signal) => format!("{status} (killed by signal {signal})"),
        None => status.to_string(),
    }
}

/// Waits for a log line containing `needle`, failing if the server exits.
fn wait_for_line(server: &mut Child, lines: &mpsc::Receiver<String>, needle: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match lines.recv_timeout(Duration::from_millis(100)) {
            Ok(line) if line.contains(needle) => return,
            Ok(_) | Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                let status = server.wait().unwrap();
                panic!(
                    "server exited before logging {needle:?}: {}",
                    describe(status)
                );
            }
        }
        if let Some(status) = server.try_wait().unwrap() {
            panic!(
                "server exited before logging {needle:?}: {}",
                describe(status)
            );
        }
        assert!(Instant::now() < deadline, "server never logged {needle:?}");
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
            .env_remove("RUST_LOG")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let (lines_tx, lines) = mpsc::channel();
    let stdout = server.0.stdout.take().unwrap();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = lines_tx.send(line);
        }
    });
    // Logged only after every signal handler is registered.
    wait_for_line(&mut server.0, &lines, "signal handlers installed");
    let started = tokio::time::timeout(Duration::from_secs(10), async {
        while !healthy(port).await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(started.is_ok(), "server did not start");

    signal(&server.0, "HUP");
    wait_for_line(&mut server.0, &lines, "SIGHUP received");
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
    // A graceful shutdown returns from main; the default SIGTERM action
    // would end the process by signal instead.
    assert_eq!(stopped.code(), Some(0), "{}", describe(stopped));
    let _ = std::fs::remove_file(config);
}
