use anyhow::Context;
use rustzap::{AppState, build_router, config::AppConfig, db::MetadataDb};
use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = AppConfig::from_env();
    tracing_subscriber::registry()
        .with(EnvFilter::new(config.log_level.clone()))
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    if std::env::args().nth(1).as_deref() == Some("migrate") {
        let metadata_db = MetadataDb::connect(&config).await?;
        metadata_db.migrate().await?;
        tracing::info!("metadata migrations applied");
        return Ok(());
    }

    let bind_addr = config.bind_addr.clone();
    let state = AppState::from_config(config).await?;
    let restored_sessions = state
        .bootstrap_existing_sessions()
        .await
        .context("failed to bootstrap existing WhatsApp sessions")?;
    if restored_sessions > 0 {
        tracing::info!(restored_sessions, "restored WhatsApp sessions");
    }
    let app = build_router(state);
    let listener = TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind {bind_addr}"))?;

    tracing::info!(%bind_addr, "rustzap listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
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
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
