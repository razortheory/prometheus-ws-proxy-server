use axum::body::Bytes;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Form, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};
use uuid::Uuid;

const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const MAX_IN_FLIGHT: usize = 1024;
const MAX_READY_CANDIDATES: usize = 1024;
const WORKER_OUTBOX_SIZE: usize = 8;
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(45);
const SOCKET_SEND_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    workers: Mutex<HashMap<String, HashMap<String, Worker>>>,
    pending: Mutex<HashMap<String, Pending>>,
    capacity: Arc<Semaphore>,
    generation: AtomicU64,
    ready_timeout: Duration,
    response_timeout: Duration,
    shutdown: CancellationToken,
    #[cfg(test)]
    workers_changed: tokio::sync::Notify,
}

#[derive(Clone)]
struct Worker {
    identity: WorkerIdentity,
    version: u16,
    sender: mpsc::Sender<Message>,
    busy: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WorkerIdentity {
    instance: String,
    name: String,
    generation: u64,
}

struct ReservedWorker {
    identity: WorkerIdentity,
    version: u16,
    sender: mpsc::Sender<Message>,
    release_state: Option<AppState>,
}

impl ReservedWorker {
    fn transfer_to(&mut self, lease: &mut RequestLease) {
        lease.set_worker(self.identity.clone());
        self.release_state = None;
    }
}

impl Drop for ReservedWorker {
    fn drop(&mut self) {
        let Some(state) = self.release_state.take() else {
            return;
        };
        let identity = self.identity.clone();
        tokio::spawn(async move {
            state.release_worker(&identity).await;
        });
    }
}

#[derive(Clone)]
struct ReadyCandidate {
    identity: WorkerIdentity,
    sender: mpsc::Sender<Message>,
}

enum DispatchSelection {
    Legacy(ReservedWorker),
    Ready(Vec<ReadyCandidate>),
}

struct Pending {
    phase: PendingPhase,
}

enum PendingPhase {
    Ready {
        allowed: HashSet<WorkerIdentity>,
        selected: oneshot::Sender<ReservedWorker>,
    },
    Response {
        worker: WorkerIdentity,
        response: oneshot::Sender<ProxyResponse>,
    },
}

#[derive(Debug)]
struct ProxyResponse {
    status: StatusCode,
    body: String,
}

enum ReserveError {
    Unknown,
    Unavailable,
}

struct RequestLease {
    state: AppState,
    worker: Option<WorkerIdentity>,
    uid: String,
    permit: Option<OwnedSemaphorePermit>,
    active: bool,
}

impl RequestLease {
    fn new(state: AppState, uid: String, permit: OwnedSemaphorePermit) -> Self {
        Self {
            state,
            worker: None,
            uid,
            permit: Some(permit),
            active: true,
        }
    }

    fn set_worker(&mut self, worker: WorkerIdentity) {
        self.worker = Some(worker);
    }

    async fn finish(mut self) {
        self.state
            .finish_request(self.worker.as_ref(), &self.uid)
            .await;
        self.active = false;
        self.permit.take();
    }
}

impl Drop for RequestLease {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        let state = self.state.clone();
        let worker = self.worker.clone();
        let uid = self.uid.clone();
        tokio::spawn(async move {
            state.finish_request(worker.as_ref(), &uid).await;
        });
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    pub fn new() -> Self {
        Self::with_limits(READY_TIMEOUT, RESPONSE_TIMEOUT, MAX_IN_FLIGHT)
    }

    fn with_limits(
        ready_timeout: Duration,
        response_timeout: Duration,
        max_in_flight: usize,
    ) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Self {
            inner: Arc::new(Inner {
                workers: Mutex::new(HashMap::new()),
                pending: Mutex::new(HashMap::new()),
                capacity: Arc::new(Semaphore::new(max_in_flight)),
                generation: AtomicU64::new(1),
                ready_timeout,
                response_timeout,
                shutdown: CancellationToken::new(),
                #[cfg(test)]
                workers_changed: tokio::sync::Notify::new(),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_tests(
        ready_timeout: Duration,
        response_timeout: Duration,
        max_in_flight: usize,
    ) -> Self {
        Self::with_limits(ready_timeout, response_timeout, max_in_flight)
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown.clone()
    }

    #[cfg(test)]
    pub(crate) async fn debug_counts(&self) -> (usize, usize) {
        let workers = self
            .inner
            .workers
            .lock()
            .await
            .values()
            .map(HashMap::len)
            .sum();
        let pending = self.inner.pending.lock().await.len();
        (workers, pending)
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_worker_after(
        &self,
        instance: &str,
        name: &str,
        generation: u64,
    ) -> u64 {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(current) = self
                    .inner
                    .workers
                    .lock()
                    .await
                    .get(instance)
                    .and_then(|workers| workers.get(name))
                    .map(|worker| worker.identity.generation)
                    .filter(|current| *current > generation)
                {
                    return current;
                }
                self.inner.workers_changed.notified().await;
            }
        })
        .await
        .expect("worker registration was not processed")
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_worker_removed(
        &self,
        instance: &str,
        name: &str,
        generation: u64,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let still_registered = self
                    .inner
                    .workers
                    .lock()
                    .await
                    .get(instance)
                    .and_then(|workers| workers.get(name))
                    .is_some_and(|worker| worker.identity.generation == generation);
                if !still_registered {
                    return;
                }
                self.inner.workers_changed.notified().await;
            }
        })
        .await
        .expect("worker disconnect was not processed");
    }

    async fn register_worker(
        &self,
        instance: String,
        requested_name: String,
        version: u16,
        sender: mpsc::Sender<Message>,
    ) -> WorkerIdentity {
        let name = if requested_name.is_empty() || requested_name == "unknown" {
            Uuid::new_v4().to_string()
        } else {
            requested_name
        };
        let identity = WorkerIdentity {
            instance: instance.clone(),
            name: name.clone(),
            generation: self.inner.generation.fetch_add(1, Ordering::Relaxed),
        };

        let replaced = {
            let mut workers = self.inner.workers.lock().await;
            workers.entry(instance).or_default().insert(
                name,
                Worker {
                    identity: identity.clone(),
                    version,
                    sender,
                    busy: false,
                },
            )
        };
        if let Some(replaced) = replaced {
            let _ = replaced.sender.try_send(Message::Close(Some(CloseFrame {
                code: 1001,
                reason: "worker replaced".into(),
            })));
            self.remove_pending_for(&replaced.identity).await;
        }
        #[cfg(test)]
        self.inner.workers_changed.notify_one();
        identity
    }

    async fn unregister_worker(&self, identity: &WorkerIdentity) {
        let removed = {
            let mut all_workers = self.inner.workers.lock().await;
            let mut removed = false;
            let mut remove_instance = false;
            if let Some(workers) = all_workers.get_mut(&identity.instance) {
                if workers
                    .get(&identity.name)
                    .is_some_and(|worker| worker.identity.generation == identity.generation)
                {
                    workers.remove(&identity.name);
                    removed = true;
                }
                remove_instance = workers.is_empty();
            }
            if remove_instance {
                all_workers.remove(&identity.instance);
            }
            removed
        };

        if removed {
            self.remove_pending_for(identity).await;
            #[cfg(test)]
            self.inner.workers_changed.notify_one();
            info!(
                instance = identity.instance,
                worker = identity.name,
                "worker disconnected"
            );
        }
    }

    async fn remove_pending_for(&self, identity: &WorkerIdentity) {
        self.inner
            .pending
            .lock()
            .await
            .retain(|_, pending| match &mut pending.phase {
                PendingPhase::Ready { allowed, .. } => {
                    allowed.remove(identity);
                    !allowed.is_empty()
                }
                PendingPhase::Response { worker, .. } => worker != identity,
            });
    }

    async fn select_workers(&self, instance: &str) -> Result<DispatchSelection, ReserveError> {
        let mut all_workers = self.inner.workers.lock().await;
        let workers = all_workers.get_mut(instance).ok_or(ReserveError::Unknown)?;

        let ready_workers: Vec<_> = workers
            .values()
            .filter(|worker| !worker.busy && worker.version >= 2 && !worker.sender.is_closed())
            .take(MAX_READY_CANDIDATES)
            .map(|worker| ReadyCandidate {
                identity: worker.identity.clone(),
                sender: worker.sender.clone(),
            })
            .collect();
        if !ready_workers.is_empty() {
            return Ok(DispatchSelection::Ready(ready_workers));
        }

        let legacy = workers
            .values_mut()
            .find(|worker| !worker.busy && worker.version < 2 && !worker.sender.is_closed());
        if let Some(worker) = legacy {
            worker.busy = true;
            return Ok(DispatchSelection::Legacy(ReservedWorker {
                identity: worker.identity.clone(),
                version: worker.version,
                sender: worker.sender.clone(),
                release_state: Some(self.clone()),
            }));
        }

        Err(ReserveError::Unavailable)
    }

    async fn instance_exists(&self, instance: &str) -> bool {
        self.inner.workers.lock().await.contains_key(instance)
    }

    async fn finish_request(&self, identity: Option<&WorkerIdentity>, uid: &str) {
        self.inner.pending.lock().await.remove(uid);
        let Some(identity) = identity else {
            return;
        };
        self.release_worker(identity).await;
    }

    async fn release_worker(&self, identity: &WorkerIdentity) {
        let mut all_workers = self.inner.workers.lock().await;
        if let Some(worker) = all_workers
            .get_mut(&identity.instance)
            .and_then(|workers| workers.get_mut(&identity.name))
            .filter(|worker| worker.identity.generation == identity.generation)
        {
            worker.busy = false;
        }
    }

    async fn insert_ready(
        &self,
        uid: String,
        allowed: HashSet<WorkerIdentity>,
    ) -> oneshot::Receiver<ReservedWorker> {
        let (sender, receiver) = oneshot::channel();
        self.inner.pending.lock().await.insert(
            uid,
            Pending {
                phase: PendingPhase::Ready {
                    allowed,
                    selected: sender,
                },
            },
        );
        receiver
    }

    async fn insert_response(
        &self,
        uid: String,
        worker: WorkerIdentity,
    ) -> oneshot::Receiver<ProxyResponse> {
        let (sender, receiver) = oneshot::channel();
        self.inner.pending.lock().await.insert(
            uid,
            Pending {
                phase: PendingPhase::Response {
                    worker,
                    response: sender,
                },
            },
        );
        receiver
    }

    async fn remove_ready_candidate(&self, uid: &str, identity: &WorkerIdentity) {
        let mut pending = self.inner.pending.lock().await;
        let remove_request = pending.get_mut(uid).is_some_and(|pending| {
            if let PendingPhase::Ready { allowed, .. } = &mut pending.phase {
                allowed.remove(identity);
                allowed.is_empty()
            } else {
                false
            }
        });
        if remove_request {
            pending.remove(uid);
        }
    }

    async fn mark_ready(&self, uid: &str, worker: &WorkerIdentity) {
        let mut pending = self.inner.pending.lock().await;
        let Some(Pending {
            phase: PendingPhase::Ready { allowed, .. },
        }) = pending.get_mut(uid)
        else {
            return;
        };
        if !allowed.remove(worker) {
            return;
        }

        let reserved = {
            let mut workers = self.inner.workers.lock().await;
            workers
                .get_mut(&worker.instance)
                .and_then(|workers| workers.get_mut(&worker.name))
                .filter(|candidate| {
                    candidate.identity.generation == worker.generation && !candidate.busy
                })
                .map(|candidate| {
                    candidate.busy = true;
                    ReservedWorker {
                        identity: candidate.identity.clone(),
                        version: candidate.version,
                        sender: candidate.sender.clone(),
                        release_state: Some(self.clone()),
                    }
                })
        };

        let Some(reserved) = reserved else {
            if allowed.is_empty() {
                pending.remove(uid);
            }
            return;
        };
        let selected = pending.remove(uid).and_then(|pending| match pending.phase {
            PendingPhase::Ready { selected, .. } => Some(selected),
            PendingPhase::Response { .. } => None,
        });
        drop(pending);
        if let Some(selected) = selected {
            let _ = selected.send(reserved);
        }
    }

    async fn complete_response(
        &self,
        uid: &str,
        worker: Option<&WorkerIdentity>,
        response: ProxyResponse,
    ) -> bool {
        let pending = {
            let mut pending = self.inner.pending.lock().await;
            let matches = pending.get(uid).is_some_and(|pending| {
                matches!(
                    &pending.phase,
                    PendingPhase::Response {
                        worker: pending_worker,
                        ..
                    } if worker.is_none_or(|worker| pending_worker == worker)
                )
            });
            matches.then(|| pending.remove(uid)).flatten()
        };
        if let Some(Pending {
            phase:
                PendingPhase::Response {
                    response: response_sender,
                    ..
                },
        }) = pending
        {
            response_sender.send(response).is_ok()
        } else {
            false
        }
    }
}

pub fn build_router(url_prefix: &str, state: AppState) -> Router {
    let prefix = url_prefix.trim_matches('/');
    let base = if prefix.is_empty() {
        String::new()
    } else {
        format!("/{prefix}")
    };

    Router::new()
        .route(&format!("{base}/health"), get(health_handler))
        .route(&format!("{base}/health/"), get(health_handler))
        .route(&format!("{base}/ws"), get(websocket_handler))
        .route(&format!("{base}/ws/"), get(websocket_handler))
        .route(
            &format!("{base}/request/{{instance}}/{{resource}}"),
            get(call_resource_handler),
        )
        .route(
            &format!("{base}/request/{{instance}}/{{resource}}/"),
            get(call_resource_handler),
        )
        .route(&format!("{base}/response/{{uid}}"), post(response_handler))
        .route(&format!("{base}/response/{{uid}}/"), post(response_handler))
        .layer(DefaultBodyLimit::max(MAX_MESSAGE_SIZE))
        .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.max_frame_size(MAX_MESSAGE_SIZE)
        .max_message_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| websocket_connection(socket, state))
}

async fn call_resource_handler(
    State(state): State<AppState>,
    Path((instance, resource)): Path<(String, String)>,
) -> Response {
    if !state.instance_exists(&instance).await {
        return (StatusCode::NOT_FOUND, "no such client").into_response();
    }
    let permit = match state.inner.capacity.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return if state.instance_exists(&instance).await {
                StatusCode::SERVICE_UNAVAILABLE.into_response()
            } else {
                (StatusCode::NOT_FOUND, "no such client").into_response()
            }
        }
    };
    let selection = match state.select_workers(&instance).await {
        Ok(selection) => selection,
        Err(ReserveError::Unknown) => {
            return (StatusCode::NOT_FOUND, "no such client").into_response()
        }
        Err(ReserveError::Unavailable) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    let uid = Uuid::new_v4().to_string();
    let mut lease = RequestLease::new(state.clone(), uid.clone(), permit);

    let worker = match selection {
        DispatchSelection::Legacy(worker) => Ok(worker),
        DispatchSelection::Ready(candidates) => select_ready_worker(&state, &uid, candidates).await,
    };
    let result = match worker {
        Ok(mut worker) => {
            debug!(protocol_version = worker.version, %uid, "worker selected");
            worker.transfer_to(&mut lease);
            dispatch_request(&state, &worker, &uid, resource).await
        }
        Err(status) => Err(status),
    };
    lease.finish().await;
    match result {
        Ok(response) => (response.status, response.body).into_response(),
        Err(status) => status.into_response(),
    }
}

async fn select_ready_worker(
    state: &AppState,
    uid: &str,
    candidates: Vec<ReadyCandidate>,
) -> Result<ReservedWorker, StatusCode> {
    let allowed = candidates
        .iter()
        .map(|candidate| candidate.identity.clone())
        .collect();
    let selected = state.insert_ready(uid.to_owned(), allowed).await;
    let mut sent = 0;
    for candidate in candidates {
        if send_json(
            &candidate.sender,
            &ServerMessage::Ready {
                uid: uid.to_owned(),
            },
        )
        .is_ok()
        {
            sent += 1;
        } else {
            state.remove_ready_candidate(uid, &candidate.identity).await;
        }
    }
    if sent == 0 {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    match tokio::time::timeout(state.inner.ready_timeout, selected).await {
        Ok(Ok(worker)) => Ok(worker),
        Ok(Err(_)) | Err(_) => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
}

async fn dispatch_request(
    state: &AppState,
    worker: &ReservedWorker,
    uid: &str,
    resource: String,
) -> Result<ProxyResponse, StatusCode> {
    let response = state
        .insert_response(uid.to_owned(), worker.identity.clone())
        .await;
    send_json(
        &worker.sender,
        &ServerMessage::Request {
            uid: uid.to_owned(),
            resource,
        },
    )?;
    match tokio::time::timeout(state.inner.response_timeout, response).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(_)) => Err(StatusCode::SERVICE_UNAVAILABLE),
        Err(_) => Err(StatusCode::NOT_IMPLEMENTED),
    }
}

fn send_json(sender: &mpsc::Sender<Message>, message: &ServerMessage) -> Result<(), StatusCode> {
    let json = serde_json::to_string(message).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    sender
        .try_send(Message::Text(json.into()))
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

#[derive(Deserialize)]
struct FormResponse {
    #[serde(default = "default_error_status")]
    status: u16,
    #[serde(default)]
    body: String,
}

fn default_error_status() -> u16 {
    500
}

async fn response_handler(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    Form(response): Form<FormResponse>,
) -> StatusCode {
    let Ok(status) = StatusCode::from_u16(response.status) else {
        return StatusCode::BAD_REQUEST;
    };
    state
        .complete_response(
            &uid,
            None,
            ProxyResponse {
                status,
                body: response.body,
            },
        )
        .await;
    StatusCode::OK
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ServerMessage {
    Ready { uid: String },
    Request { uid: String, resource: String },
}

fn default_version() -> u16 {
    1
}

fn default_worker() -> String {
    "unknown".to_owned()
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ClientMessage {
    Register {
        instance: String,
        #[serde(default = "default_worker")]
        worker: String,
        #[serde(default = "default_version")]
        version: u16,
    },
    Ping,
    Pong,
    Ready {
        uid: String,
        #[serde(default)]
        worker: String,
    },
    Response {
        uid: String,
        status: u16,
        body: String,
    },
}

async fn websocket_connection(mut socket: WebSocket, state: AppState) {
    let (outbox_sender, mut outbox_receiver) = mpsc::channel(WORKER_OUTBOX_SIZE);
    let mut identity: Option<WorkerIdentity> = None;
    let mut heartbeat =
        tokio::time::interval_at(Instant::now() + HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_activity = Instant::now();
    let shutdown = state.shutdown_token();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                send_websocket(&mut socket, Message::Close(Some(CloseFrame {
                    code: 1001,
                    reason: "server shutdown".into(),
                }))).await;
                break;
            }
            _ = heartbeat.tick() => {
                if last_activity.elapsed() >= HEARTBEAT_TIMEOUT {
                    warn!("websocket heartbeat timed out");
                    break;
                }
                if !send_websocket(&mut socket, Message::Text("{\"type\":\"ping\"}".into())).await
                    || !send_websocket(&mut socket, Message::Ping(Bytes::new())).await
                {
                    break;
                }
            }
            outgoing = outbox_receiver.recv() => {
                match outgoing {
                    Some(message) => {
                        if !send_websocket(&mut socket, message).await {
                            break;
                        }
                    }
                    None => break,
                }
            }
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        last_activity = Instant::now();
                        handle_client_text(
                            text.as_str(),
                            &state,
                            &outbox_sender,
                            &mut identity,
                        ).await;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        last_activity = Instant::now();
                        if !send_websocket(&mut socket, Message::Pong(payload)).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_activity = Instant::now();
                        debug!("control pong received");
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Binary(_))) => warn!("binary websocket message ignored"),
                    Some(Err(error)) => {
                        warn!(%error, "websocket receive error");
                        break;
                    }
                }
            }
        }
    }

    if let Some(identity) = identity {
        state.unregister_worker(&identity).await;
    }
}

async fn send_websocket(socket: &mut WebSocket, message: Message) -> bool {
    matches!(
        tokio::time::timeout(SOCKET_SEND_TIMEOUT, socket.send(message)).await,
        Ok(Ok(()))
    )
}

async fn handle_client_text(
    text: &str,
    state: &AppState,
    sender: &mpsc::Sender<Message>,
    identity: &mut Option<WorkerIdentity>,
) {
    let message = match serde_json::from_str::<ClientMessage>(text) {
        Ok(message) => message,
        Err(error) => {
            warn!(%error, "invalid websocket JSON ignored");
            return;
        }
    };

    match message {
        ClientMessage::Register {
            instance,
            worker,
            version,
        } => {
            if let Some(old_identity) = identity.take() {
                state.unregister_worker(&old_identity).await;
            }
            let registered = state
                .register_worker(instance, worker, version, sender.clone())
                .await;
            info!(
                instance = registered.instance,
                worker = registered.name,
                version,
                "worker registered"
            );
            *identity = Some(registered);
        }
        ClientMessage::Ping => {
            let _ = sender.try_send(Message::Text("{\"type\":\"pong\"}".into()));
        }
        ClientMessage::Pong => {
            debug!("JSON pong received");
        }
        ClientMessage::Ready { uid, worker } => {
            debug!(%uid, %worker, "worker ready");
            if let Some(identity) = identity.as_ref() {
                state.mark_ready(&uid, identity).await;
            }
        }
        ClientMessage::Response { uid, status, body } => {
            let Ok(status) = StatusCode::from_u16(status) else {
                warn!(%uid, status, "invalid proxied status ignored");
                return;
            };
            if let Some(identity) = identity.as_ref() {
                state
                    .complete_response(&uid, Some(identity), ProxyResponse { status, body })
                    .await;
            }
        }
    }
}
