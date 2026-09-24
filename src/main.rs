use clap::{ArgAction, Parser};
use prometheus_proxy_server::{build_router, config::Config, AppState};
use std::error::Error;
use std::future::IntoFuture;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(
    name = "Prometheus websocket server",
    version = VERSION,
    author = "Roman Karpovich <fpm.th13f@gmail.com>",
    about = "Proxy prometheus requests with no network hassle"
)]
struct Cli {
    #[arg(default_value = "client_config.json", help = "path to config")]
    config: PathBuf,

    #[arg(long = "sentry_dsn", help = "sentry DSN")]
    sentry_dsn: Option<String>,

    #[arg(
        short = 'v',
        long = "verbose",
        action = ArgAction::Count,
        help = "increases log verbosity for each occurrence"
    )]
    verbose: u8,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let cli = Cli::parse();
    let shutdown_signals = ShutdownSignals::install()?;
    #[cfg(unix)]
    let _reload_task = tokio::spawn(ignore_reload_signal(tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::hangup(),
    )?));
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(default_log_filter(cli.verbose))),
        )
        .try_init()?;

    let _sentry_guard = cli.sentry_dsn.map(|sentry_dsn| {
        sentry::init((
            sentry_dsn,
            sentry::ClientOptions {
                release: sentry::release_name!(),
                attach_stacktrace: true,
                ..Default::default()
            },
        ))
    });

    info!("signal handlers installed");
    info!(config = %cli.config.display(), "loading configuration");
    let config = Config::from_file(&cli.config)?;
    let host: IpAddr = config.host.parse()?;
    let address = SocketAddr::new(host, config.port);
    let state = AppState::new();
    let shutdown = state.shutdown_token();
    let listener = tokio::net::TcpListener::bind(address).await?;

    info!(%address, prefix = config.url_prefix, "server listening");
    let signal_shutdown = shutdown.clone();
    let _signal_task = tokio::spawn(async move {
        if let Err(error) = shutdown_signals.recv().await {
            tracing::error!(%error, "waiting for a shutdown signal failed");
        }
        signal_shutdown.cancel();
    });
    let graceful_shutdown = shutdown.clone();
    let server = axum::serve(listener, build_router(&config.url_prefix, state))
        .with_graceful_shutdown(graceful_shutdown.cancelled_owned())
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result?,
        _ = async {
            shutdown.cancelled().await;
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        } => {
            tracing::warn!("graceful shutdown deadline reached; forcing exit");
        }
    }
    Ok(())
}

fn default_log_filter(verbose: u8) -> &'static str {
    match verbose {
        0 | 1 => "info",
        2 => "debug",
        _ => "trace",
    }
}

/// The systemd unit maps `reload` to SIGHUP, whose default action terminates
/// the process and drops every worker connection without a close frame. The
/// server has no reloadable state, so the signal is logged and ignored.
#[cfg(unix)]
async fn ignore_reload_signal(mut hangup: tokio::signal::unix::Signal) {
    while hangup.recv().await.is_some() {
        info!("SIGHUP received; nothing to reload, connections are kept");
    }
}

/// SIGINT and SIGTERM streams created at startup. Creating a stream replaces
/// the default action immediately; `tokio::signal::ctrl_c()` or a stream
/// created later would leave a window in which the signal kills the process
/// without a graceful shutdown.
struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn install() -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt())?,
                terminate: signal(SignalKind::terminate())?,
            })
        }
        #[cfg(not(unix))]
        Ok(Self {})
    }

    async fn recv(self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            let Self {
                mut interrupt,
                mut terminate,
            } = self;
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
            Ok(())
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_accepts_positional_config_and_verbose_count() {
        let cli = Cli::try_parse_from(["proxy-server", "server.json", "-vvv"])
            .expect("legacy CLI arguments should parse");

        assert_eq!(cli.config, PathBuf::from("server.json"));
        assert_eq!(cli.verbose, 3);

        let cli = Cli::try_parse_from(["proxy-server", "server.json", "--verbose", "--verbose"])
            .expect("long verbose option should remain repeatable");
        assert_eq!(cli.verbose, 2);
    }

    #[test]
    fn verbosity_maps_to_expected_default_filter() {
        assert_eq!(default_log_filter(0), "info");
        assert_eq!(default_log_filter(1), "info");
        assert_eq!(default_log_filter(2), "debug");
        assert_eq!(default_log_filter(3), "trace");
        assert_eq!(default_log_filter(u8::MAX), "trace");
    }
}
