mod config;
mod http;
mod media;
mod state;
mod ws;

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{delete, get, post};
use axum::Router;
use chat_auth::AuthService;
use chat_common::snowflake::SnowflakeGenerator;
use chat_service::ChatService;
use sqlx::postgres::PgPoolOptions;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use config::Config;
use media::MediaStore;
use state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::from_env();

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;

    let nats = async_nats::connect(&config.nats_url).await?;

    let state = Arc::new(AppState {
        auth: AuthService::new(pool.clone(), SnowflakeGenerator::new(1), config.jwt_secret.clone()),
        chat: ChatService::new(pool, SnowflakeGenerator::new(2)),
        nats,
        media: MediaStore::new(config.media_dir.into(), config.jwt_secret),
        media_ids: SnowflakeGenerator::new(3),
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws::upgrade))
        .route("/auth/register", post(http::register))
        .route("/auth/login", post(http::login))
        .route("/me", get(http::me))
        .route("/users/lookup", get(http::lookup_user))
        .route("/users/search", get(http::search_users))
        .route("/chats", get(http::list_chats))
        .route("/chats/direct", post(http::create_direct_chat))
        .route("/chats/saved", post(http::get_or_create_saved_messages))
        .route("/chats/{chat_id}/messages", get(http::list_messages))
        .route("/chats/{chat_id}/clear", post(http::clear_chat))
        .route("/chats/{chat_id}/archive", post(http::archive_chat))
        .route("/chats/{chat_id}/unarchive", post(http::unarchive_chat))
        .route("/chats/{chat_id}", delete(http::leave_chat))
        .route(
            "/chats/{chat_id}/media",
            post(media::upload).route_layer(DefaultBodyLimit::max(media::MAX_UPLOAD_BYTES)),
        )
        .route("/chats/{chat_id}/media/forward", post(media::forward))
        .route("/media/{chat_id}/{media_id}", get(media::serve))
        .route("/users/{user_id}/block", post(http::block_user))
        .route("/users/{user_id}/unblock", post(http::unblock_user))
        .route("/users/{user_id}/report-spam", post(http::report_spam))
        .route("/blocks", get(http::list_blocked))
        .layer(CorsLayer::permissive()) // dev only — scope this down before Phase 1 ships
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&config.addr).await?;
    tracing::info!(addr = %config.addr, "gateway listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
