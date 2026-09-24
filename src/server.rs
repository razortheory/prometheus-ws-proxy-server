use axum::body::Bytes;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Form, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex, Notify, Semaphore};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::metrics::{CloseReason, Metrics, ScrapeOutcome, Snapshot};

const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const MAX_IN_FLIGHT: usize = 1024;
const MAX_READY_CANDIDATES: usize = 1024;
const WORKER_OUTBOX_SIZE: usize = 8;
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(45);
const SOCKET_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_SEND_TIMEOUT: Duration = Duration::from_secs(1);
/// A worker that has sent nothing for two heartbeat intervals is not given new
/// scrapes; its connection stays open until the heartbeat timeout decides.
const STALE_AFTER: Duration = Duration::from_secs(30);
/// Upper bound for waiting on a replacement worker after the selected one
/// disconnected mid-scrape. Clients reconnect after about one second.
const RETRY_WAIT: Duration = Duration::from_secs(3);
const MIN_RETRY_BUDGET: Duration = Duration::from_millis(500);
const MAX_DISPATCH_ATTEMPTS: usize = 3;
const SCRAPE_TIMEOUT_HEADER: &str = "x-prometheus-scrape-timeout-seconds";

#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub(crate) ready_timeout: Duration,
    pub(crate) response_timeout: Duration,
    pub(crate) max_in_flight: usize,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) heartbeat_timeout: Duration,
    pub(crate) stale_after: Duration,
    pub(crate) retry_wait: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            ready_timeout: READY_TIMEOUT,
            response_timeout: RESPONSE_TIMEOUT,
            max_in_flight: MAX_IN_FLIGHT,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            heartbeat_timeout: HEARTBEAT_TIMEOUT,
            stale_after: STALE_AFTER,
            retry_wait: RETRY_WAIT,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    workers: Mutex<HashMap<String, HashMap<String, Worker>>>,
    pending: Mutex<HashMap<String, Pending>>,
    capacity: Arc<Semaphore>,
    generation: AtomicU64,
    limits: Limits,
    shutdown: CancellationToken,
    metrics: Metrics,
    epoch: Instant,
    /// Woken when a worker registers or becomes idle.
    worker_available: Notify,
    #[cfg(test)]
    workers_changed: tokio::sync::Notify,
}

#[derive(Clone)]
struct Worker {
    identity: WorkerIdentity,
    version: u16,
    sender: mpsc::Sender<Message>,
    busy: bool,
    last_seen: Arc<AtomicU64>,
    evict: CancellationToken,
}

/// Per-connection handles shared with the worker registry.
struct ConnectionLink {
    sender: mpsc::Sender<Message>,
    /// Milliseconds since the state epoch at which the last frame arrived.
    last_seen: Arc<AtomicU64>,
    /// Cancelled when another connection registers the same worker name.
    evict: CancellationToken,
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

/// Why a single dispatch attempt failed.
enum AttemptError {
    /// The candidate workers disconnected or stopped accepting messages; the
    /// scrape can be retried on another worker.
    Lost(Vec<WorkerIdentity>),
    Failed(ScrapeOutcome),
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
    active: bool,
}

impl RequestLease {
    fn new(state: AppState, uid: String) -> Self {
        Self {
            state,
            worker: None,
            uid,
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
        Self::with_limits(Limits::default())
    }

    fn with_limits(limits: Limits) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Self {
            inner: Arc::new(Inner {
                workers: Mutex::new(HashMap::new()),
                pending: Mutex::new(HashMap::new()),
                capacity: Arc::new(Semaphore::new(limits.max_in_flight)),
                generation: AtomicU64::new(1),
                limits,
                shutdown: CancellationToken::new(),
                metrics: Metrics::default(),
                epoch: Instant::now(),
                worker_available: Notify::new(),
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
        Self::with_limits(Limits {
            ready_timeout,
            response_timeout,
            max_in_flight,
            ..Limits::default()
        })
    }

    #[cfg(test)]
    pub(crate) fn for_tests_with(limits: Limits) -> Self {
        Self::with_limits(limits)
    }

    #[cfg(test)]
    pub(crate) fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown.clone()
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.inner.epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    async fn metrics_snapshot(&self) -> Snapshot {
        let mut workers_by_version = [0; 3];
        for worker in self
            .inner
            .workers
            .lock()
            .await
            .values()
            .flat_map(HashMap::values)
        {
            workers_by_version[crate::metrics::version_index(worker.version)] += 1;
        }
        let pending = self.inner.pending.lock().await.len();
        let in_flight = self
            .inner
            .limits
            .max_in_flight
            .saturating_sub(self.inner.capacity.available_permits());
        Snapshot {
            workers_by_version,
            pending,
            in_flight,
        }
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
        link: &ConnectionLink,
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
                    sender: link.sender.clone(),
                    busy: false,
                    last_seen: link.last_seen.clone(),
                    evict: link.evict.clone(),
                },
            )
        };
        if let Some(replaced) = replaced {
            info!(
                instance = replaced.identity.instance,
                worker = replaced.identity.name,
                "worker replaced by a new connection"
            );
            replaced.evict.cancel();
            self.remove_pending_for(&replaced.identity).await;
        }
        self.inner.metrics.worker_registered(version);
        self.inner.worker_available.notify_waiters();
        #[cfg(test)]
        self.inner.workers_changed.notify_one();
        identity
    }

    async fn unregister_worker(&self, identity: &WorkerIdentity, reason: CloseReason) {
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
                reason = reason.label(),
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

    async fn select_workers(
        &self,
        instance: &str,
        excluded: &HashSet<WorkerIdentity>,
    ) -> Result<DispatchSelection, ReserveError> {
        let now = self.now_ms();
        let stale_after =
            u64::try_from(self.inner.limits.stale_after.as_millis()).unwrap_or(u64::MAX);
        let usable = |worker: &Worker| {
            !worker.busy
                && !worker.sender.is_closed()
                && !excluded.contains(&worker.identity)
                && now.saturating_sub(worker.last_seen.load(Ordering::Relaxed)) <= stale_after
        };
        let mut all_workers = self.inner.workers.lock().await;
        let workers = all_workers.get_mut(instance).ok_or(ReserveError::Unknown)?;

        let ready_workers: Vec<_> = workers
            .values()
            .filter(|worker| worker.version >= 2 && usable(worker))
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
            .find(|worker| worker.version < 2 && usable(worker));
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
            self.inner.worker_available.notify_waiters();
        }
    }

    /// Waits until a usable worker for `instance` appears or `wait` elapses.
    async fn wait_for_worker(
        &self,
        instance: &str,
        excluded: &HashSet<WorkerIdentity>,
        wait: Duration,
    ) -> Option<DispatchSelection> {
        let deadline = Instant::now() + wait;
        loop {
            let notified = self.inner.worker_available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Ok(selection) = self.select_workers(instance, excluded).await {
                return Some(selection);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return None;
            }
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
        .route("/metrics", get(metrics_handler))
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

async fn metrics_handler(State(state): State<AppState>) -> Response {
    let snapshot = state.metrics_snapshot().await;
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.inner.metrics.render(&snapshot),
    )
        .into_response()
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.max_frame_size(MAX_MESSAGE_SIZE)
        .max_message_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| websocket_connection(socket, state))
}

/// Prometheus announces its scrape timeout; waiting past it keeps a worker
/// reserved for a request nobody reads. The deadline is the timeout itself:
/// a response that arrives just before it still counts for Prometheus.
fn scrape_deadline(headers: &HeaderMap, now: Instant) -> Option<Instant> {
    let seconds = headers
        .get(SCRAPE_TIMEOUT_HEADER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<f64>()
        .ok()?;
    let timeout = Duration::try_from_secs_f64(seconds).ok()?;
    if timeout.is_zero() {
        return None;
    }
    now.checked_add(timeout)
}

fn bounded(limit: Duration, deadline: Option<Instant>) -> Duration {
    match deadline {
        Some(deadline) => limit.min(deadline.saturating_duration_since(Instant::now())),
        None => limit,
    }
}

async fn call_resource_handler(
    State(state): State<AppState>,
    Path((instance, resource)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let now = Instant::now();
    let limits = state.inner.limits;
    // Without the header, retries stay within the single-attempt worst case.
    let deadline = scrape_deadline(&headers, now)
        .or_else(|| now.checked_add(limits.ready_timeout + limits.response_timeout));
    let result = proxy_scrape(&state, &instance, resource, deadline).await;
    let outcome = match &result {
        Ok(_) => ScrapeOutcome::Proxied,
        Err(outcome) => *outcome,
    };
    state.inner.metrics.scrape_finished(outcome);
    match result {
        Ok(response) => (response.status, response.body).into_response(),
        Err(ScrapeOutcome::UnknownInstance) => {
            (StatusCode::NOT_FOUND, "no such client").into_response()
        }
        Err(ScrapeOutcome::ResponseTimeout) => StatusCode::NOT_IMPLEMENTED.into_response(),
        Err(outcome) => {
            debug!(%instance, result = outcome.label(), "scrape failed");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

async fn proxy_scrape(
    state: &AppState,
    instance: &str,
    resource: String,
    deadline: Option<Instant>,
) -> Result<ProxyResponse, ScrapeOutcome> {
    if !state.instance_exists(instance).await {
        return Err(ScrapeOutcome::UnknownInstance);
    }
    let _permit = match state.inner.capacity.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return Err(if state.instance_exists(instance).await {
                ScrapeOutcome::Capacity
            } else {
                ScrapeOutcome::UnknownInstance
            })
        }
    };

    let mut excluded = HashSet::new();
    let mut lost_worker = false;
    for attempt in 1..=MAX_DISPATCH_ATTEMPTS {
        let selection = match state.select_workers(instance, &excluded).await {
            Ok(selection) => selection,
            // Without a disconnect there is nothing to wait for: a busy
            // instance answers 503 immediately.
            Err(ReserveError::Unknown) if !lost_worker => {
                return Err(ScrapeOutcome::UnknownInstance)
            }
            Err(ReserveError::Unavailable) if !lost_worker => {
                return Err(ScrapeOutcome::NoIdleWorker)
            }
            Err(_) => {
                let wait = bounded(state.inner.limits.retry_wait, deadline);
                match state.wait_for_worker(instance, &excluded, wait).await {
                    Some(selection) => selection,
                    None => return Err(ScrapeOutcome::WorkerLost),
                }
            }
        };

        let uid = Uuid::new_v4().to_string();
        let mut lease = RequestLease::new(state.clone(), uid.clone());
        let worker = match selection {
            DispatchSelection::Legacy(worker) => Ok(worker),
            DispatchSelection::Ready(candidates) => {
                select_ready_worker(state, &uid, candidates, deadline).await
            }
        };
        let result = match worker {
            Ok(mut worker) => {
                debug!(protocol_version = worker.version, %uid, attempt, "worker selected");
                worker.transfer_to(&mut lease);
                dispatch_request(state, &worker, &uid, resource.clone(), deadline).await
            }
            Err(error) => Err(error),
        };
        lease.finish().await;

        match result {
            Ok(response) => return Ok(response),
            Err(AttemptError::Failed(outcome)) => return Err(outcome),
            Err(AttemptError::Lost(identities)) => {
                excluded.extend(identities);
                lost_worker = true;
                let remaining = bounded(Duration::MAX, deadline);
                if attempt == MAX_DISPATCH_ATTEMPTS || remaining < MIN_RETRY_BUDGET {
                    break;
                }
                state.inner.metrics.dispatch_retried();
                debug!(%instance, attempt, "selected worker disconnected; retrying scrape");
            }
        }
    }
    Err(ScrapeOutcome::WorkerLost)
}

async fn select_ready_worker(
    state: &AppState,
    uid: &str,
    candidates: Vec<ReadyCandidate>,
    deadline: Option<Instant>,
) -> Result<ReservedWorker, AttemptError> {
    let identities: Vec<_> = candidates
        .iter()
        .map(|candidate| candidate.identity.clone())
        .collect();
    let selected = state
        .insert_ready(uid.to_owned(), identities.iter().cloned().collect())
        .await;
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
        return Err(AttemptError::Lost(identities));
    }

    let wait = bounded(state.inner.limits.ready_timeout, deadline);
    match tokio::time::timeout(wait, selected).await {
        Ok(Ok(worker)) => Ok(worker),
        // Every candidate disconnected before answering.
        Ok(Err(_)) => Err(AttemptError::Lost(identities)),
        Err(_) => Err(AttemptError::Failed(ScrapeOutcome::ReadyTimeout)),
    }
}

async fn dispatch_request(
    state: &AppState,
    worker: &ReservedWorker,
    uid: &str,
    resource: String,
    deadline: Option<Instant>,
) -> Result<ProxyResponse, AttemptError> {
    let lost = || AttemptError::Lost(vec![worker.identity.clone()]);
    let response = state
        .insert_response(uid.to_owned(), worker.identity.clone())
        .await;
    send_json(
        &worker.sender,
        &ServerMessage::Request {
            uid: uid.to_owned(),
            resource,
        },
    )
    .map_err(|_| lost())?;
    let wait = bounded(state.inner.limits.response_timeout, deadline);
    match tokio::time::timeout(wait, response).await {
        Ok(Ok(response)) => Ok(response),
        // The worker disconnected or was replaced while the request was open.
        Ok(Err(_)) => Err(lost()),
        Err(_) => Err(AttemptError::Failed(ScrapeOutcome::ResponseTimeout)),
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
    state.inner.metrics.connection_opened();
    let (outbox_sender, mut outbox_receiver) = mpsc::channel(WORKER_OUTBOX_SIZE);
    let link = ConnectionLink {
        sender: outbox_sender,
        last_seen: Arc::new(AtomicU64::new(state.now_ms())),
        evict: CancellationToken::new(),
    };
    let limits = state.inner.limits;
    let heartbeat_timeout = u64::try_from(limits.heartbeat_timeout.as_millis()).unwrap_or(u64::MAX);
    let mut identity: Option<WorkerIdentity> = None;
    let mut heartbeat = tokio::time::interval_at(
        Instant::now() + limits.heartbeat_interval,
        limits.heartbeat_interval,
    );
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let shutdown = state.shutdown_token();

    let reason = loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                close_websocket(&mut socket, "server shutdown").await;
                break CloseReason::Shutdown;
            }
            _ = link.evict.cancelled() => {
                close_websocket(&mut socket, "worker replaced").await;
                break CloseReason::Replaced;
            }
            _ = heartbeat.tick() => {
                let silent = state.now_ms().saturating_sub(link.last_seen.load(Ordering::Relaxed));
                if silent >= heartbeat_timeout {
                    let (instance, worker) = identity_labels(identity.as_ref());
                    warn!(instance, worker, silent_ms = silent, "websocket heartbeat timed out");
                    close_websocket(&mut socket, "heartbeat timeout").await;
                    break CloseReason::HeartbeatTimeout;
                }
                if !send_websocket(&mut socket, Message::Text("{\"type\":\"ping\"}".into())).await
                    || !send_websocket(&mut socket, Message::Ping(Bytes::new())).await
                {
                    break CloseReason::SendFailed;
                }
            }
            outgoing = outbox_receiver.recv() => {
                match outgoing {
                    Some(message) => {
                        if !send_websocket(&mut socket, message).await {
                            break CloseReason::SendFailed;
                        }
                    }
                    None => break CloseReason::SendFailed,
                }
            }
            incoming = socket.next() => {
                if let Some(Ok(_)) = &incoming {
                    link.last_seen.store(state.now_ms(), Ordering::Relaxed);
                }
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        handle_client_text(
                            text.as_str(),
                            &state,
                            &link,
                            &mut identity,
                        ).await;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if !send_websocket(&mut socket, Message::Pong(payload)).await {
                            break CloseReason::SendFailed;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        debug!("control pong received");
                    }
                    Some(Ok(Message::Close(_))) => {
                        // Flushes the close reply that tungstenite queued.
                        let _ = tokio::time::timeout(
                            CLOSE_SEND_TIMEOUT,
                            futures_util::SinkExt::close(&mut socket),
                        )
                        .await;
                        break CloseReason::ClientClose;
                    }
                    None => break CloseReason::Reset,
                    Some(Ok(Message::Binary(_))) => warn!("binary websocket message ignored"),
                    Some(Err(error)) => {
                        let (instance, worker) = identity_labels(identity.as_ref());
                        warn!(instance, worker, %error, "websocket receive error");
                        break receive_error_reason(&error.to_string());
                    }
                }
            }
        }
    };

    state.inner.metrics.connection_closed(reason);
    if let Some(identity) = identity {
        state.unregister_worker(&identity, reason).await;
    }
}

fn identity_labels(identity: Option<&WorkerIdentity>) -> (&str, &str) {
    identity.map_or(("", ""), |identity| {
        (identity.instance.as_str(), identity.name.as_str())
    })
}

/// Classifies a receive error; transport-level ends are reported as resets.
fn receive_error_reason(error: &str) -> CloseReason {
    let error = error.to_ascii_lowercase();
    if error.contains("reset")
        || error.contains("closing handshake")
        || error.contains("broken pipe")
        || error.contains("connection aborted")
        || error.contains("unexpected eof")
    {
        CloseReason::Reset
    } else {
        CloseReason::ReceiveError
    }
}

async fn send_websocket(socket: &mut WebSocket, message: Message) -> bool {
    matches!(
        tokio::time::timeout(SOCKET_SEND_TIMEOUT, socket.send(message)).await,
        Ok(Ok(()))
    )
}

/// Best effort close frame so the peer logs a reason instead of a reset.
async fn close_websocket(socket: &mut WebSocket, reason: &'static str) {
    let _ = tokio::time::timeout(
        CLOSE_SEND_TIMEOUT,
        socket.send(Message::Close(Some(CloseFrame {
            code: 1001,
            reason: reason.into(),
        }))),
    )
    .await;
}

async fn handle_client_text(
    text: &str,
    state: &AppState,
    link: &ConnectionLink,
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
                state
                    .unregister_worker(&old_identity, CloseReason::ClientClose)
                    .await;
            }
            let registered = state.register_worker(instance, worker, version, link).await;
            info!(
                instance = registered.instance,
                worker = registered.name,
                version,
                "worker registered"
            );
            *identity = Some(registered);
        }
        ClientMessage::Ping => {
            let _ = link
                .sender
                .try_send(Message::Text("{\"type\":\"pong\"}".into()));
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

#[cfg(test)]
mod tests {
    use super::{receive_error_reason, scrape_deadline, CloseReason};
    use axum::http::HeaderMap;
    use std::time::Duration;
    use tokio::time::Instant;

    fn deadline_for(value: &str) -> Option<Duration> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Prometheus-Scrape-Timeout-Seconds",
            value.parse().unwrap(),
        );
        let now = Instant::now();
        scrape_deadline(&headers, now).map(|deadline| deadline - now)
    }

    #[test]
    fn scrape_deadline_is_the_prometheus_timeout() {
        assert_eq!(deadline_for("10"), Some(Duration::from_secs(10)));
        assert_eq!(deadline_for("19.5"), Some(Duration::from_millis(19500)));
        assert_eq!(deadline_for(" 0.3 "), Some(Duration::from_millis(300)));
        for invalid in ["0", "-1", "nan", "inf", "soon", ""] {
            assert_eq!(deadline_for(invalid), None, "{invalid}");
        }
        assert_eq!(scrape_deadline(&HeaderMap::new(), Instant::now()), None);
    }

    #[test]
    fn transport_errors_are_classified_as_resets() {
        for error in [
            "WebSocket protocol error: Connection reset without closing handshake",
            "IO error: Connection reset by peer (os error 104)",
            "IO error: Broken pipe (os error 32)",
        ] {
            assert_eq!(receive_error_reason(error), CloseReason::Reset, "{error}");
        }
        assert_eq!(
            receive_error_reason("Space limit exceeded: Message too long"),
            CloseReason::ReceiveError
        );
    }
}
