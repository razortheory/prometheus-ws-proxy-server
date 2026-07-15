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
        shutdown_signal().await;
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

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
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
