use std::{env, net::SocketAddr, path::Path};

use anyhow::Context;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    db: PgPool,
    boot_id: Uuid,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    boot_id: Uuid,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "backend=info,tower_http=info".into()),
        )
        .init();

    let database_url = env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let port = env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8080);

    let db = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .context("failed to connect to postgres")?;

    run_migrations(&db).await?;

    let boot_id = Uuid::new_v4();
    sqlx::query("INSERT INTO server_boots (id) VALUES ($1)")
        .bind(boot_id)
        .execute(&db)
        .await
        .context("failed to record server boot")?;

    let state = AppState { db, boot_id };
    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws))
        .route("/install.sh", get(install_sh))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    info!("game backend listening on http://{addr}");
    axum::serve(listener, app).await.context("server exited")?;
    Ok(())
}

async fn run_migrations(db: &PgPool) -> anyhow::Result<()> {
    let migrator = sqlx::migrate::Migrator::new(Path::new("./migrations"))
        .await
        .context("failed to load migrations")?;
    migrator.run(db).await.context("failed to run migrations")?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, AppError> {
    sqlx::query_scalar::<_, i64>("SELECT 1::BIGINT")
        .fetch_one(&state.db)
        .await?;
    Ok(Json(HealthResponse {
        ok: true,
        boot_id: state.boot_id,
    }))
}

async fn ws(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let hello = serde_json::json!({
        "type": "hello",
        "boot_id": state.boot_id,
    });

    if sender
        .send(Message::Text(hello.to_string().into()))
        .await
        .is_err()
    {
        return;
    }

    while let Some(message) = receiver.next().await {
        match message {
            Ok(Message::Text(text)) => {
                let reply = serde_json::json!({
                    "type": "echo",
                    "message": text.as_str(),
                });
                if sender
                    .send(Message::Text(reply.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok(Message::Ping(bytes)) => {
                if sender.send(Message::Pong(bytes)).await.is_err() {
                    break;
                }
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(err) => {
                error!("websocket error: {err}");
                break;
            }
        }
    }
}

async fn install_sh() -> Response {
    let script = r#"#!/usr/bin/env sh
set -eu

REPO="${GAME_CLI_REPO:-REPLACE_WITH_GITHUB_REPO}"
INSTALL_DIR="${GAME_INSTALL_DIR:-$HOME/.ascii/bin}"

case "$(uname -s)" in
  Linux) OS="linux" ;;
  Darwin) OS="darwin" ;;
  *) echo "Unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac

case "$(uname -m)" in
  x86_64|amd64) ARCH="x64" ;;
  arm64|aarch64) ARCH="arm64" ;;
  *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

ASSET="game-${OS}-${ARCH}"
URL="https://github.com/${REPO}/releases/latest/download/${ASSET}"

mkdir -p "$INSTALL_DIR"
TMP="$(mktemp)"
curl -fsSL "$URL" -o "$TMP"
chmod +x "$TMP"
mv "$TMP" "$INSTALL_DIR/game"

echo "Installed game to $INSTALL_DIR/game"
echo "Run: $INSTALL_DIR/game"
"#;
    ([("content-type", "text/x-shellscript")], script).into_response()
}

struct AppError(anyhow::Error);

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("request failed: {:#}", self.0);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "internal server error" })),
        )
            .into_response()
    }
}
