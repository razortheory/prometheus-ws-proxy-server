use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Why a WebSocket connection loop ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CloseReason {
    /// The peer sent a close frame.
    ClientClose,
    /// The transport ended without a close frame (EOF, ECONNRESET, broken pipe).
    Reset,
    /// Any other receive error, such as a protocol violation.
    ReceiveError,
    /// Nothing was received within the heartbeat timeout.
    HeartbeatTimeout,
    /// A send did not complete within the send timeout or failed.
    SendFailed,
    /// Another connection registered the same worker name.
    Replaced,
    /// The server is shutting down.
    Shutdown,
}

impl CloseReason {
    const ALL: [Self; 7] = [
        Self::ClientClose,
        Self::Reset,
        Self::ReceiveError,
        Self::HeartbeatTimeout,
        Self::SendFailed,
        Self::Replaced,
        Self::Shutdown,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::ClientClose => "client_close",
            Self::Reset => "reset",
            Self::ReceiveError => "receive_error",
            Self::HeartbeatTimeout => "heartbeat_timeout",
            Self::SendFailed => "send_failed",
            Self::Replaced => "replaced",
            Self::Shutdown => "shutdown",
        }
    }

    fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|reason| *reason == self)
            .expect("every close reason is listed")
    }
}

/// Final outcome of one Prometheus scrape request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScrapeOutcome {
    /// A worker returned the exporter status and body.
    Proxied,
    /// No worker is registered for the instance (404).
    UnknownInstance,
    /// The global in-flight limit is exhausted (503).
    Capacity,
    /// Every registered worker is busy, stale, or closing (503).
    NoIdleWorker,
    /// No worker answered the ready broadcast in time (503).
    ReadyTimeout,
    /// Selected workers disconnected and no retry succeeded (503).
    WorkerLost,
    /// The selected worker did not return a response in time (501).
    ResponseTimeout,
}

impl ScrapeOutcome {
    const ALL: [Self; 7] = [
        Self::Proxied,
        Self::UnknownInstance,
        Self::Capacity,
        Self::NoIdleWorker,
        Self::ReadyTimeout,
        Self::WorkerLost,
        Self::ResponseTimeout,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Proxied => "proxied",
            Self::UnknownInstance => "unknown_instance",
            Self::Capacity => "capacity",
            Self::NoIdleWorker => "no_idle_worker",
            Self::ReadyTimeout => "ready_timeout",
            Self::WorkerLost => "worker_lost",
            Self::ResponseTimeout => "response_timeout",
        }
    }

    fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|outcome| *outcome == self)
            .expect("every scrape outcome is listed")
    }
}

/// Wire versions are reported as 1, 2, and 3; newer versions count as 3.
pub(crate) fn version_index(version: u16) -> usize {
    match version {
        0 | 1 => 0,
        2 => 1,
        _ => 2,
    }
}

const VERSION_LABELS: [&str; 3] = ["1", "2", "3"];

#[derive(Default)]
pub(crate) struct Metrics {
    open_connections: AtomicI64,
    closes: [AtomicU64; 7],
    registrations: [AtomicU64; 3],
    scrapes: [AtomicU64; 7],
    retries: AtomicU64,
}

pub(crate) struct Snapshot {
    pub(crate) workers_by_version: [u64; 3],
    pub(crate) pending: usize,
    pub(crate) in_flight: usize,
}

impl Metrics {
    pub(crate) fn connection_opened(&self) {
        self.open_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn connection_closed(&self, reason: CloseReason) {
        self.open_connections.fetch_sub(1, Ordering::Relaxed);
        self.closes[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn worker_registered(&self, version: u16) {
        self.registrations[version_index(version)].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn scrape_finished(&self, outcome: ScrapeOutcome) {
        self.scrapes[outcome.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn dispatch_retried(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn closes(&self, reason: CloseReason) -> u64 {
        self.closes[reason.index()].load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn scrapes(&self, outcome: ScrapeOutcome) -> u64 {
        self.scrapes[outcome.index()].load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn retries(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }

    /// Renders the Prometheus text exposition format.
    pub(crate) fn render(&self, snapshot: &Snapshot) -> String {
        let mut out = String::with_capacity(4096);
        header(
            &mut out,
            "proxy_build_info",
            "gauge",
            "Server build version.",
        );
        let _ = writeln!(
            out,
            "proxy_build_info{{version=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION")
        );

        header(
            &mut out,
            "proxy_websocket_connections",
            "gauge",
            "Open WebSocket connections, registered or not.",
        );
        let _ = writeln!(
            out,
            "proxy_websocket_connections {}",
            self.open_connections.load(Ordering::Relaxed).max(0)
        );

        header(
            &mut out,
            "proxy_workers",
            "gauge",
            "Registered worker connections by wire version.",
        );
        for (label, count) in VERSION_LABELS.iter().zip(snapshot.workers_by_version) {
            let _ = writeln!(out, "proxy_workers{{version=\"{label}\"}} {count}");
        }

        header(
            &mut out,
            "proxy_worker_registrations_total",
            "counter",
            "Worker registrations by wire version.",
        );
        for (label, count) in VERSION_LABELS.iter().zip(&self.registrations) {
            let _ = writeln!(
                out,
                "proxy_worker_registrations_total{{version=\"{label}\"}} {}",
                count.load(Ordering::Relaxed)
            );
        }

        header(
            &mut out,
            "proxy_websocket_closes_total",
            "counter",
            "Ended WebSocket connections by reason.",
        );
        for reason in CloseReason::ALL {
            let _ = writeln!(
                out,
                "proxy_websocket_closes_total{{reason=\"{}\"}} {}",
                reason.label(),
                self.closes[reason.index()].load(Ordering::Relaxed)
            );
        }

        header(
            &mut out,
            "proxy_scrapes_total",
            "counter",
            "Finished scrape requests by outcome.",
        );
        for outcome in ScrapeOutcome::ALL {
            let _ = writeln!(
                out,
                "proxy_scrapes_total{{result=\"{}\"}} {}",
                outcome.label(),
                self.scrapes[outcome.index()].load(Ordering::Relaxed)
            );
        }

        header(
            &mut out,
            "proxy_scrape_retries_total",
            "counter",
            "Scrape dispatches retried after the selected worker disconnected.",
        );
        let _ = writeln!(
            out,
            "proxy_scrape_retries_total {}",
            self.retries.load(Ordering::Relaxed)
        );

        header(
            &mut out,
            "proxy_pending_requests",
            "gauge",
            "Scrape requests waiting for a ready answer or a response.",
        );
        let _ = writeln!(out, "proxy_pending_requests {}", snapshot.pending);

        header(
            &mut out,
            "proxy_in_flight_requests",
            "gauge",
            "Admitted scrape requests holding global capacity.",
        );
        let _ = writeln!(out, "proxy_in_flight_requests {}", snapshot.in_flight);
        out
    }
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

#[cfg(test)]
mod tests {
    use super::{CloseReason, Metrics, ScrapeOutcome, Snapshot};

    #[test]
    fn renders_every_series_with_labels() {
        let metrics = Metrics::default();
        metrics.connection_opened();
        metrics.connection_opened();
        metrics.connection_closed(CloseReason::HeartbeatTimeout);
        metrics.worker_registered(3);
        metrics.worker_registered(7);
        metrics.worker_registered(1);
        metrics.scrape_finished(ScrapeOutcome::WorkerLost);
        metrics.dispatch_retried();

        let text = metrics.render(&Snapshot {
            workers_by_version: [1, 0, 2],
            pending: 4,
            in_flight: 5,
        });

        for line in [
            "proxy_websocket_connections 1",
            "proxy_workers{version=\"1\"} 1",
            "proxy_workers{version=\"3\"} 2",
            "proxy_worker_registrations_total{version=\"3\"} 2",
            "proxy_worker_registrations_total{version=\"1\"} 1",
            "proxy_websocket_closes_total{reason=\"heartbeat_timeout\"} 1",
            "proxy_websocket_closes_total{reason=\"reset\"} 0",
            "proxy_scrapes_total{result=\"worker_lost\"} 1",
            "proxy_scrape_retries_total 1",
            "proxy_pending_requests 4",
            "proxy_in_flight_requests 5",
            "# TYPE proxy_scrapes_total counter",
        ] {
            assert!(text.lines().any(|candidate| candidate == line), "{line}");
        }
    }
}
