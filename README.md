# Prometheus WebSocket Proxy Server

[![CI](https://github.com/razortheory/prometheus-ws-proxy-server/actions/workflows/ci.yml/badge.svg)](https://github.com/razortheory/prometheus-ws-proxy-server/actions/workflows/ci.yml)

`prometheus-proxy-server` lets Prometheus scrape exporters behind NAT, firewalls, and private networks without opening inbound exporter ports. Proxy clients establish outbound WebSocket connections; the server maps normal Prometheus HTTP requests onto those connections and returns the exporter response.

This is product version 3. It is a resource-bounded Rust replacement for the long-running Python service while preserving its routes, configuration shape, and historical wire protocols. The matching client is [prometheus-ws-proxy-client](https://github.com/razortheory/prometheus-ws-proxy-client).

## How it works

```text
Prometheus
    |  GET /proxy/request/<instance>/<resource>/
    v
proxy server  <==== WebSocket ====  proxy client  ---- HTTP ----> exporter
    ^                                      |
    +--------- WebSocket or HTTP ----------+
```

The server selects one ready client worker, forwards the allow-listed resource name, waits for its response, and returns the exact exporter status and body to Prometheus.

## Compatibility

Product version and wire version are separate concepts. The v3 server accepts all historical wire modes:

| Wire version | Selection handshake | Response transport | Typical client |
| --- | --- | --- | --- |
| `1` | Direct request | WebSocket | Oldest Python/Rust client |
| `2` | `ready` / selected worker | WebSocket | Existing Rust v2 client |
| `3` | `ready` / selected worker | HTTP form POST | Existing Python client and Rust v3 default |

Text JSON ping/pong and RFC 6455 control ping/pong are supported. Routes accept both trailing-slash and no-trailing-slash forms.

Wire v3 client delivery is transport-level at-least-once because a successful response POST can be followed by a lost HTTP reply and a WebSocket fallback. Pending UIDs in this server are single-use, so a duplicate transport delivery cannot complete a scrape twice.

## Install a release

Releases contain one stripped, static Linux amd64 binary and its checksum. Pin a version in automation:

```bash
VERSION=v3.0.0
BASE="https://github.com/razortheory/prometheus-ws-proxy-server/releases/download/${VERSION}"
curl -fLO "${BASE}/prometheus-proxy-server-linux-amd64"
curl -fLO "${BASE}/prometheus-proxy-server-linux-amd64.sha256"
sha256sum --check prometheus-proxy-server-linux-amd64.sha256
sudo install -m 0755 prometheus-proxy-server-linux-amd64 \
  /usr/local/bin/prometheus-proxy-server
prometheus-proxy-server --version
```

Verify the canonical filename before installing it under its runtime name; the checksum file intentionally contains the release asset basename.

The binary is built with musl and rustls and has no runtime dependency on glibc or OpenSSL. Install `ca-certificates` if Sentry reporting uses HTTPS.

## Configuration

Pass the JSON configuration path as the positional argument:

```json
{
  "redis": {
    "host": "localhost",
    "port": 6379,
    "db": 0
  },
  "url_prefix": "proxy",
  "host": "127.0.0.1",
  "port": 8080
}
```

- `host` and `port` are the listen address.
- `url_prefix` is trimmed of surrounding slashes and prefixes every route. An empty value serves routes at `/`.
- `redis.host`, `redis.port`, and `redis.db` remain required in the legacy JSON shape so existing configuration can be reused. They are parsed but intentionally unused; v3 keeps all connection and request state in bounded process memory and does not connect to Redis.

The default positional filename remains `client_config.json` for CLI compatibility, but an explicit server path is recommended:

```bash
prometheus-proxy-server /etc/prometheus-proxy/server.json -v
```

`-v`/`--verbose` is repeatable. `RUST_LOG` overrides the derived log filter. The existing `--sentry_dsn <DSN>` spelling is supported, and DSN values are not logged.

## HTTP and WebSocket routes

With `url_prefix: "proxy"`:

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/proxy/health/` | Liveness check |
| `GET` upgrade | `/proxy/ws/` | Client WebSocket registration and traffic |
| `GET` | `/proxy/request/{instance}/{resource}/` | Prometheus scrape endpoint |
| `POST` form | `/proxy/response/{uid}/` | Historical wire-v3 response endpoint |

An unknown instance returns `404`. A known instance with no available worker or exhausted bounded capacity returns `503`. A client response timeout returns `501`. Successful exporter responses preserve their status and body.

Example Nginx location:

```nginx
location /proxy/ {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 60s;
}
```

The public Prometheus target syntax remains:

```text
https://prometheus.example.com/proxy/request/host-a/node/
```

## Deployment security

The server provides plain HTTP/WebSocket endpoints and does not authenticate client registration itself. Bind it to loopback or a private address, terminate TLS at a trusted reverse proxy, and enforce authentication or Cloudflare Access there for both HTTP and WebSocket routes. Do not expose the backend listen port directly to the internet.

Client JSON files can contain Cloudflare service-token credentials. Keep them owned by the client service account with mode `0600`, and never commit production values.

## systemd

```ini
[Unit]
Description=Prometheus WebSocket proxy server
After=network-online.target
Wants=network-online.target

[Service]
User=prometheus
Group=prometheus
ExecStart=/usr/local/bin/prometheus-proxy-server /etc/prometheus-proxy/server.json
Restart=always
RestartSec=2
TimeoutStopSec=20
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

SIGINT and SIGTERM trigger graceful shutdown, bounded by 15 seconds inside the process.

## Resource and failure bounds

- At most 1,024 globally admitted scrape requests.
- One in-flight scrape per worker connection.
- Worker outbound queue capacity: 8 messages.
- 64 MiB maximum HTTP form body, WebSocket frame, and WebSocket message.
- Ready-selection timeout: 30 seconds.
- Exporter response timeout: 30 seconds.
- Heartbeat every 15 seconds; stale connection timeout: 45 seconds.
- WebSocket send timeout: 5 seconds.

Disconnected generations, empty instance maps, and completed or timed-out pending UIDs are removed. Capacity exhaustion fails fast instead of growing queues.

## Single-process deployment

Client WebSockets and pending requests live only in the process that accepted them. Redis is no longer used and there is no shared connection registry. Run one active server process for a given public route; ordinary round-robin balancing across multiple server processes will send scrapes to processes that do not own the corresponding client connection.

For availability, keep the previous server ready as a controlled upstream rollback rather than running both behind the same load-balanced route.

## Build and test from source

Rust 1.96 is pinned for normal development. The declared minimum supported Rust version is 1.88.
Cargo uses `sccache` through `.cargo/config.toml`, so install sccache 0.16.0 and
keep it on `PATH` before running the commands below. Local Cargo builds share
the user's normal sccache store; GitHub Actions uses its cache backend.

```bash
cargo +1.96.0 fmt --all -- --check
cargo +1.96.0 test --locked --all-targets --all-features
cargo +1.96.0 clippy --locked --all-targets --all-features -- -D warnings
cargo +1.88.0 test --locked --all-targets --all-features
```

Build the release artifact exactly as CI does:

```bash
docker buildx build \
  --platform linux/amd64 \
  --target artifact \
  --secret id=actions_results_url,env=ACTIONS_RESULTS_URL \
  --secret id=actions_runtime_token,env=ACTIONS_RUNTIME_TOKEN \
  --output type=local,dest=dist \
  .
```

The BuildKit secrets are optional when those environment variables are unset.
Docker builds then fall back to the builder's shared local sccache mount.

## Releases and Ubuntu support

Pushing a tag such as `v3.0.0` starts the release workflow. The tag must exactly match the Cargo package version. CI builds one static `prometheus-proxy-server-linux-amd64`, verifies that it has no ELF interpreter or dynamic dependencies, creates a SHA-256 file, and runs that same artifact in Ubuntu 16.04, 18.04, 20.04, 22.04, 24.04, and 26.04 containers before publishing it.

Container smoke tests verify each Ubuntu userspace, not its historical kernel. In particular, Ubuntu 16.04 normally runs kernel 4.4 while GitHub and Docker hosts use a newer kernel. Treat Ubuntu 16 support as provisional until the artifact has also run on a real Ubuntu 16 host or VM. Building directly on the existing Ubuntu 16 server remains an additional fallback, not the primary release process.

## Safe server-first rollout

1. Start Rust v3 on a shadow port or hostname and exercise it with existing Python clients.
2. Switch the existing Nginx `/proxy` upstream from Python to Rust. Prometheus targets and Python client configurations stay unchanged.
3. Keep the Python server running on its previous rollback port, but remove it from the production upstream.
4. Replace Python clients with Rust clients in small batches.
5. Remove Python only after the observation window.

Rollback is an Nginx upstream change back to Python. New Rust clients can continue talking to the old Python server, so server and client rollback decisions remain independent.
