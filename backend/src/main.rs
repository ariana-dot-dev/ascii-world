use std::{
    collections::HashMap,
    env,
    f64::consts::{FRAC_PI_2, PI, TAU},
    net::SocketAddr,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path as AxumPath, Query, State, WebSocketUpgrade,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio::sync::{broadcast, Mutex};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    db: PgPool,
    boot_id: Uuid,
    game: GameHandle,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    boot_id: Uuid,
}

#[derive(Debug, Deserialize)]
struct AuthQuery {
    token: Option<String>,
}

#[derive(Debug, Serialize)]
struct LoginStartResponse {
    login_url: String,
    poll_token: String,
}

#[derive(Debug, Serialize)]
struct LoginPollResponse {
    status: String,
    token: Option<String>,
    username: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct XCallbackQuery {
    state: Option<String>,
    code: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct XTokenResponse {
    access_token: String,
}

#[derive(Debug, Deserialize)]
struct XMeResponse {
    data: XUserData,
}

#[derive(Debug, Deserialize)]
struct XUserData {
    id: String,
    username: String,
    name: String,
}

#[derive(Debug, Clone)]
struct GameUser {
    id: Uuid,
    username: String,
    economy: UserEconomy,
    rewards: Vec<RewardNotice>,
}

type GameHandle = Arc<Game>;

struct Game {
    world: Mutex<GameWorld>,
    snapshots: broadcast::Sender<Snapshot>,
}

struct GameWorld {
    players: HashMap<String, Player>,
    next_npc_id: u64,
    last_tick: Instant,
}

#[derive(Clone)]
struct Player {
    id: String,
    user_id: Option<Uuid>,
    name: String,
    planet_id: u32,
    position: Vec3,
    basis_up: Vec3,
    input: InputState,
    fake: bool,
    jump_height: f64,
    jump_velocity: f64,
    npc_jump_seconds: f64,
    total_tokens: u64,
    lobster_micros: u64,
    last_economy_at: Instant,
    equipped_head: String,
    owned_heads: Vec<String>,
    walking_phase: u64,
    npc_movement: Option<NpcMovement>,
}

#[derive(Debug, Clone)]
struct UserEconomy {
    total_tokens: u64,
    lobster_micros: u64,
    equipped_head: String,
    owned_heads: Vec<String>,
}

#[derive(Debug, Clone)]
struct EconomySave {
    user_id: Uuid,
    total_tokens: u64,
    lobster_micros: u64,
    equipped_head: String,
    owned_heads: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RewardNotice {
    label: String,
    lobsters: u64,
    streak: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MarketItemSnapshot {
    id: String,
    label: String,
    head: String,
    price_lobsters: u64,
}

#[derive(Clone)]
enum NpcMovement {
    Moving {
        start: Vec3,
        target: Vec3,
        lateral: Vec3,
        path_length: f64,
        distance: f64,
    },
    Idle {
        remaining_seconds: f64,
    },
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
struct InputState {
    #[serde(default)]
    up: bool,
    #[serde(default)]
    down: bool,
    #[serde(default)]
    left: bool,
    #[serde(default)]
    right: bool,
    #[serde(default)]
    jump: bool,
    #[serde(default)]
    camera_up: Option<[f64; 3]>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Input {
        #[serde(default)]
        up: bool,
        #[serde(default)]
        down: bool,
        #[serde(default)]
        left: bool,
        #[serde(default)]
        right: bool,
        #[serde(default)]
        jump: bool,
        #[serde(default)]
        camera_up: Option<[f64; 3]>,
    },
    TokenUsage {
        total_tokens: u64,
    },
    BuyHead {
        item_id: String,
    },
    EquipHead {
        item_id: String,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Welcome {
        self_id: String,
        rewards: Vec<RewardNotice>,
        market: Vec<MarketItemSnapshot>,
    },
    Snapshot(Snapshot),
}

#[derive(Debug, Clone, Serialize)]
struct Snapshot {
    server_time_ms: u64,
    players: Vec<PlayerSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
struct PlayerSnapshot {
    id: String,
    name: String,
    planet_id: u32,
    lat: f64,
    lon: f64,
    position: [f64; 3],
    basis_up: [f64; 3],
    fake: bool,
    total_tokens: u64,
    lobsters: u64,
    lobster_yield_per_hour: f64,
    equipped_head: String,
    owned_heads: Vec<String>,
    jump_height: f64,
    walking_phase: u64,
}

#[derive(Debug, Clone, Copy)]
struct Vec3 {
    x: f64,
    y: f64,
    z: f64,
}

impl Vec3 {
    const Z: Self = Self {
        x: 0.0,
        y: 0.0,
        z: 1.0,
    };
    const X: Self = Self {
        x: 1.0,
        y: 0.0,
        z: 0.0,
    };

    fn new(x: f64, y: f64, z: f64) -> Self {
        Self { x, y, z }
    }

    fn random_unit(rng: &mut impl Rng) -> Self {
        let z = rng.random_range(-1.0..=1.0);
        let theta = rng.random_range(0.0..TAU);
        let radius = (1.0_f64 - z * z).sqrt();
        Self::new(radius * theta.cos(), radius * theta.sin(), z)
    }

    fn dot(self, other: Self) -> f64 {
        self.x * other.x + self.y * other.y + self.z * other.z
    }

    fn angle_to(self, other: Self) -> f64 {
        self.dot(other).clamp(-1.0, 1.0).acos()
    }

    fn cross(self, other: Self) -> Self {
        Self::new(
            self.y * other.z - self.z * other.y,
            self.z * other.x - self.x * other.z,
            self.x * other.y - self.y * other.x,
        )
    }

    fn scale(self, factor: f64) -> Self {
        Self::new(self.x * factor, self.y * factor, self.z * factor)
    }

    fn add(self, other: Self) -> Self {
        Self::new(self.x + other.x, self.y + other.y, self.z + other.z)
    }

    fn length(self) -> f64 {
        self.dot(self).sqrt()
    }

    fn normalize(self) -> Self {
        let length = self.length();
        if length <= f64::EPSILON {
            self
        } else {
            self.scale(1.0 / length)
        }
    }

    fn any_tangent(self) -> Self {
        let seed = if self.z.abs() < 0.8 { Self::Z } else { Self::X };
        let tangent = seed.add(self.scale(-self.dot(seed)));
        if tangent.length() <= 1e-6 {
            self.cross(Self::new(0.0, 1.0, 0.0)).normalize()
        } else {
            tangent.normalize()
        }
    }

    fn from_array(value: [f64; 3]) -> Option<Self> {
        if !value.iter().all(|component| component.is_finite()) {
            return None;
        }
        let vector = Self::new(value[0], value[1], value[2]).normalize();
        if vector.length() <= 1e-6 {
            None
        } else {
            Some(vector)
        }
    }

    fn rotate_around(self, axis: Self, angle: f64) -> Self {
        let axis = axis.normalize();
        self.scale(angle.cos())
            .add(axis.cross(self).scale(angle.sin()))
            .add(axis.scale(axis.dot(self) * (1.0 - angle.cos())))
    }

    fn slerp(self, other: Self, t: f64) -> Self {
        let t = t.clamp(0.0, 1.0);
        let angle = self.angle_to(other);
        if angle <= 1e-6 {
            return self;
        }
        let sin_angle = angle.sin();
        self.scale(((1.0 - t) * angle).sin() / sin_angle)
            .add(other.scale((t * angle).sin() / sin_angle))
            .normalize()
    }

    fn to_array(self) -> [f64; 3] {
        [self.x, self.y, self.z]
    }

    fn lat_lon(self) -> (f64, f64) {
        let lat = self.z.clamp(-1.0, 1.0).asin();
        let lon = self.y.atan2(self.x);
        (lat, lon)
    }
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

    let game = Game::new();
    tokio::spawn(run_game_loop(game.clone()));

    let state = AppState { db, boot_id, game };
    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws))
        .route("/spectate", get(spectate))
        .route("/auth/x/start", axum::routing::post(auth_x_start))
        .route("/auth/x/poll/{poll_token}", get(auth_x_poll))
        .route("/auth/x/callback", get(auth_x_callback))
        .route("/terms", get(terms_page))
        .route("/privacy", get(privacy_page))
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

async fn auth_x_start(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<LoginStartResponse>, AppError> {
    let client_id = x_client_id()?;
    let poll_token = secret_token("poll");
    let oauth_state = secret_token("state");
    let code_verifier = secret_token("pkce");
    let code_challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()));
    let expires_at = chrono::Utc::now() + chrono::Duration::minutes(15);

    sqlx::query(
        r#"
        INSERT INTO x_login_sessions
          (id, poll_token, oauth_state, code_verifier, status, expires_at)
        VALUES ($1, $2, $3, $4, 'pending', $5)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(&poll_token)
    .bind(&oauth_state)
    .bind(&code_verifier)
    .bind(expires_at)
    .execute(&state.db)
    .await?;

    let mut login_url = url::Url::parse("https://x.com/i/oauth2/authorize")?;
    login_url
        .query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &client_id)
        .append_pair("redirect_uri", &x_redirect_uri(&headers)?)
        .append_pair("scope", "tweet.read users.read")
        .append_pair("state", &oauth_state)
        .append_pair("code_challenge", &code_challenge)
        .append_pair("code_challenge_method", "S256");

    Ok(Json(LoginStartResponse {
        login_url: login_url.to_string(),
        poll_token,
    }))
}

async fn auth_x_poll(
    AxumPath(poll_token): AxumPath<String>,
    State(state): State<AppState>,
) -> Result<Json<LoginPollResponse>, AppError> {
    let row = sqlx::query_as::<_, (String, Option<String>, Option<String>, Option<String>)>(
        r#"
        SELECT s.status, s.api_token, u.x_username, u.x_name
        FROM x_login_sessions s
        LEFT JOIN game_users u ON u.id = s.user_id
        WHERE s.poll_token = $1 AND s.expires_at > now()
        LIMIT 1
        "#,
    )
    .bind(poll_token)
    .fetch_optional(&state.db)
    .await?;

    let Some((status, token, username, name)) = row else {
        return Ok(Json(LoginPollResponse {
            status: "expired".to_string(),
            token: None,
            username: None,
            name: None,
        }));
    };

    Ok(Json(LoginPollResponse {
        status,
        token,
        username,
        name,
    }))
}

async fn auth_x_callback(
    headers: HeaderMap,
    Query(query): Query<XCallbackQuery>,
    State(state): State<AppState>,
) -> Result<Response, AppError> {
    if query.error.is_some() {
        return Ok(html(
            "X login was cancelled. Go back to your terminal.",
            StatusCode::BAD_REQUEST,
        ));
    }
    let Some(oauth_state) = query.state else {
        return Ok(html(
            "X login failed: missing state.",
            StatusCode::BAD_REQUEST,
        ));
    };
    let Some(code) = query.code else {
        return Ok(html(
            "X login failed: missing code.",
            StatusCode::BAD_REQUEST,
        ));
    };

    let session = sqlx::query_as::<_, (Uuid, String)>(
        "SELECT id, code_verifier FROM x_login_sessions WHERE oauth_state = $1 AND expires_at > now() LIMIT 1",
    )
    .bind(&oauth_state)
    .fetch_optional(&state.db)
    .await?;
    let Some((session_id, code_verifier)) = session else {
        return Ok(html(
            "X login expired. Go back to your terminal and try again.",
            StatusCode::BAD_REQUEST,
        ));
    };

    let access_token = match exchange_x_code(&headers, &code, &code_verifier).await {
        Ok(token) => token,
        Err(err) => {
            return Ok(html(
                &format!("X login failed during token exchange: {err}"),
                StatusCode::BAD_REQUEST,
            ));
        }
    };
    let x_user = match fetch_x_user(&access_token).await {
        Ok(user) => user,
        Err(err) => {
            return Ok(html(
                &format!("X login failed while reading your profile: {err}"),
                StatusCode::BAD_REQUEST,
            ));
        }
    };
    complete_x_login(&state.db, session_id, x_user).await?;

    Ok(html("All good. Go back to your terminal.", StatusCode::OK))
}

async fn complete_x_login(
    db: &PgPool,
    session_id: Uuid,
    x_user: XUserData,
) -> anyhow::Result<LoginPollResponse> {
    let api_token = secret_token("game");
    let user_id = Uuid::new_v4();
    let user_row = sqlx::query_as::<_, (Uuid, String, String, Option<String>)>(
        r#"
        INSERT INTO game_users (id, x_id, x_username, x_name, api_token)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (x_id) DO UPDATE SET
          x_username = EXCLUDED.x_username,
          x_name = EXCLUDED.x_name,
          updated_at = now()
        RETURNING id, api_token, x_username, x_name
        "#,
    )
    .bind(user_id)
    .bind(&x_user.id)
    .bind(&x_user.username)
    .bind(&x_user.name)
    .bind(&api_token)
    .fetch_one(db)
    .await?;

    sqlx::query(
        "UPDATE x_login_sessions SET status = 'complete', api_token = $1, user_id = $2, updated_at = now() WHERE id = $3",
    )
    .bind(&user_row.1)
    .bind(user_row.0)
    .bind(session_id)
    .execute(db)
    .await?;

    Ok(LoginPollResponse {
        status: "complete".to_string(),
        token: Some(user_row.1),
        username: Some(user_row.2),
        name: user_row.3,
    })
}

fn x_client_id() -> anyhow::Result<String> {
    env::var("X_CLIENT_ID")
        .or_else(|_| env::var("X_CONSUMER_KEY"))
        .context("X_CLIENT_ID is required")
}

fn x_client_secret() -> Option<String> {
    env::var("X_CLIENT_SECRET")
        .or_else(|_| env::var("X_CONSUMER_SECRET"))
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn x_redirect_uri(headers: &HeaderMap) -> anyhow::Result<String> {
    let explicit = env::var("X_REDIRECT_URI").ok();
    if let Some(value) = explicit.filter(|value| !value.trim().is_empty()) {
        return Ok(value);
    }
    if let Some(host) = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get("host"))
        .and_then(|value| value.to_str().ok())
    {
        let scheme = headers
            .get("x-forwarded-proto")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("https");
        return Ok(format!("{scheme}://{host}/auth/x/callback"));
    }
    let base = env::var("GAME_PUBLIC_URL")
        .or_else(|_| env::var("PUBLIC_URL"))
        .context("GAME_PUBLIC_URL or X_REDIRECT_URI is required")?;
    Ok(format!("{}/auth/x/callback", base.trim_end_matches('/')))
}

fn secret_token(prefix: &str) -> String {
    format!(
        "{prefix}_{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
}

async fn exchange_x_code(
    headers: &HeaderMap,
    code: &str,
    code_verifier: &str,
) -> anyhow::Result<String> {
    let client_id = x_client_id()?;
    let redirect_uri = x_redirect_uri(headers)?;
    let mut request = reqwest::Client::new()
        .post("https://api.twitter.com/2/oauth2/token")
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", client_id.as_str()),
            ("code", code),
            ("redirect_uri", redirect_uri.as_str()),
            ("code_verifier", code_verifier),
        ]);
    if let Some(secret) = x_client_secret() {
        request = request.basic_auth(client_id, Some(secret));
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        anyhow::bail!("X token exchange failed: {}", response.text().await?);
    }
    Ok(response.json::<XTokenResponse>().await?.access_token)
}

async fn fetch_x_user(access_token: &str) -> anyhow::Result<XUserData> {
    let response = reqwest::Client::new()
        .get("https://api.twitter.com/2/users/me")
        .query(&[("user.fields", "username,name")])
        .bearer_auth(access_token)
        .send()
        .await?;
    if !response.status().is_success() {
        anyhow::bail!("X user fetch failed: {}", response.text().await?);
    }
    Ok(response.json::<XMeResponse>().await?.data)
}

async fn user_by_api_token(db: &PgPool, token: &str) -> anyhow::Result<Option<GameUser>> {
    let row = sqlx::query_as::<
        _,
        (
            Uuid,
            String,
            Option<String>,
            i64,
            i64,
            Option<DateTime<Utc>>,
            Option<NaiveDate>,
            i32,
            Option<NaiveDate>,
            i32,
            String,
            Value,
        ),
    >(
        r#"
        SELECT id, x_username, x_name, total_tokens, lobster_micros, last_lobster_at,
               last_daily_reward_date, daily_streak_days,
               last_weekly_reward_monday, weekly_streak_weeks,
               equipped_head, owned_heads
        FROM game_users
        WHERE api_token = $1
        LIMIT 1
        "#,
    )
    .bind(token)
    .fetch_optional(db)
    .await?;

    let Some((
        id,
        username,
        _name,
        total_tokens,
        lobster_micros,
        _last_lobster_at,
        last_daily_reward_date,
        daily_streak_days,
        last_weekly_reward_monday,
        weekly_streak_weeks,
        equipped_head,
        owned_heads,
    )) = row
    else {
        return Ok(None);
    };

    let now = Utc::now();
    let total_tokens = total_tokens.max(0) as u64;
    let mut lobster_micros = lobster_micros.max(0) as u64;

    let mut rewards = Vec::new();
    let today = now.date_naive();
    let mut daily_streak_days = daily_streak_days.max(0) as u32;
    if last_daily_reward_date != Some(today) {
        daily_streak_days = if last_daily_reward_date
            .map(|date| date.succ_opt() == Some(today))
            .unwrap_or(false)
        {
            daily_streak_days.saturating_add(1)
        } else {
            1
        };
        let lobsters = 1_000_u64.saturating_add(1_000_u64.saturating_mul(daily_streak_days as u64));
        lobster_micros = lobster_micros.saturating_add(lobsters.saturating_mul(LOBSTER_MICROS));
        rewards.push(RewardNotice {
            label: "Daily check-in".to_string(),
            lobsters,
            streak: daily_streak_days,
        });
    }

    let monday = today - chrono::Duration::days(today.weekday().num_days_from_monday() as i64);
    let previous_monday = monday - chrono::Duration::weeks(1);
    let mut weekly_streak_weeks = weekly_streak_weeks.max(0) as u32;
    if last_weekly_reward_monday != Some(monday) {
        weekly_streak_weeks = if last_weekly_reward_monday == Some(previous_monday) {
            weekly_streak_weeks.saturating_add(1)
        } else {
            1
        };
        let lobsters = 5_000_u64.saturating_mul(weekly_streak_weeks.max(1) as u64);
        lobster_micros = lobster_micros.saturating_add(lobsters.saturating_mul(LOBSTER_MICROS));
        rewards.push(RewardNotice {
            label: "Weekly streak".to_string(),
            lobsters,
            streak: weekly_streak_weeks,
        });
    }

    let mut owned_heads = owned_heads
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec!["default".to_string()]);
    let had_default_head = owned_heads.iter().any(|item| item == "default");
    if !had_default_head {
        owned_heads.push("default".to_string());
    }
    let equipped_head = if !had_default_head && equipped_head == "box" {
        "default".to_string()
    } else if market_item(&equipped_head).is_some()
        && owned_heads.iter().any(|item| item == &equipped_head)
    {
        equipped_head
    } else {
        "default".to_string()
    };

    sqlx::query(
        r#"
        UPDATE game_users
        SET total_tokens = $2,
            lobster_micros = $3,
            last_lobster_at = now(),
            last_daily_reward_date = $4,
            daily_streak_days = $5,
            last_weekly_reward_monday = $6,
            weekly_streak_weeks = $7,
            equipped_head = $8,
            owned_heads = $9,
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(total_tokens as i64)
    .bind(lobster_micros as i64)
    .bind(today)
    .bind(daily_streak_days as i32)
    .bind(monday)
    .bind(weekly_streak_weeks as i32)
    .bind(&equipped_head)
    .bind(Value::Array(
        owned_heads
            .iter()
            .map(|item| Value::String(item.clone()))
            .collect(),
    ))
    .execute(db)
    .await?;

    Ok(Some(GameUser {
        id,
        username,
        economy: UserEconomy {
            total_tokens,
            lobster_micros,
            equipped_head,
            owned_heads,
        },
        rewards,
    }))
}

async fn terms_page() -> Response {
    html(
        "Terms of Service: Ascii World is an experimental multiplayer terminal game. Use it as-is.",
        StatusCode::OK,
    )
}

async fn privacy_page() -> Response {
    html(
        "Privacy Policy: Ascii World uses X login to identify your player name. Local token usage is tracked locally by the CLI for gameplay and is not uploaded.",
        StatusCode::OK,
    )
}

fn html(body: &str, status: StatusCode) -> Response {
    (
        status,
        [("content-type", "text/html; charset=utf-8")],
        format!(
            "<!doctype html><meta charset=\"utf-8\"><title>Ascii World</title><body style=\"font-family:system-ui;background:#111;color:#eee;padding:32px\"><h1>{}</h1></body>",
            body
        ),
    )
        .into_response()
}

async fn ws(
    ws: WebSocketUpgrade,
    Query(query): Query<AuthQuery>,
    State(state): State<AppState>,
) -> Response {
    let Some(token) = query.token else {
        return (StatusCode::UNAUTHORIZED, "missing token").into_response();
    };
    let Ok(Some(user)) = user_by_api_token(&state.db, &token).await else {
        return (StatusCode::UNAUTHORIZED, "bad token").into_response();
    };
    ws.on_upgrade(move |socket| handle_ws(socket, state, user))
        .into_response()
}

async fn spectate(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_spectator_ws(socket, state))
}

async fn handle_ws(socket: WebSocket, state: AppState, user: GameUser) {
    let player_id = Uuid::new_v4().to_string();
    let rewards = user.rewards.clone();
    state.game.add_player(player_id.clone(), user).await;

    let (mut sender, mut receiver) = socket.split();
    let mut snapshots = state.game.snapshots.subscribe();

    let welcome = ServerMessage::Welcome {
        self_id: player_id.clone(),
        rewards,
        market: market_items(),
    };
    if send_json(&mut sender, &welcome).await.is_err() {
        if let Some(save) = state.game.remove_player(&player_id).await {
            let _ = persist_economy(&state.db, save).await;
        }
        return;
    }

    loop {
        tokio::select! {
            message = receiver.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(ClientMessage::Input {
                                up,
                                down,
                                left,
                                right,
                                jump,
                                camera_up,
                            }) => {
                                state
                                    .game
                                    .set_input(
                                        &player_id,
                                        InputState {
                                            up,
                                            down,
                                            left,
                                            right,
                                            jump,
                                            camera_up,
                                        },
                                    )
                                    .await;
                            }
                            Ok(ClientMessage::TokenUsage { total_tokens }) => {
                                if let Some(save) =
                                    state.game.set_total_tokens(&player_id, total_tokens).await
                                {
                                    if let Err(err) = persist_economy(&state.db, save).await {
                                        error!("failed to persist token usage: {err}");
                                    }
                                }
                            }
                            Ok(ClientMessage::BuyHead { item_id }) => {
                                if let Some(save) =
                                    state.game.buy_head(&player_id, &item_id).await
                                {
                                    if let Err(err) = persist_economy(&state.db, save).await {
                                        error!("failed to persist head purchase: {err}");
                                    }
                                }
                            }
                            Ok(ClientMessage::EquipHead { item_id }) => {
                                if let Some(save) =
                                    state.game.equip_head(&player_id, &item_id).await
                                {
                                    if let Err(err) = persist_economy(&state.db, save).await {
                                        error!("failed to persist equipped head: {err}");
                                    }
                                }
                            }
                            Err(err) => error!("bad client message: {err}"),
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        if sender.send(Message::Pong(bytes)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(err)) => {
                        error!("websocket error: {err}");
                        break;
                    }
                }
            }
            snapshot = snapshots.recv() => {
                let Ok(snapshot) = snapshot else {
                    break;
                };
                if send_json(&mut sender, &ServerMessage::Snapshot(snapshot)).await.is_err() {
                    break;
                }
            }
        }
    }

    if let Some(save) = state.game.remove_player(&player_id).await {
        if let Err(err) = persist_economy(&state.db, save).await {
            error!("failed to persist player economy on disconnect: {err}");
        }
    }
}

async fn handle_spectator_ws(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut snapshots = state.game.snapshots.subscribe();

    loop {
        tokio::select! {
            message = receiver.next() => {
                match message {
                    Some(Ok(Message::Ping(bytes))) => {
                        if sender.send(Message::Pong(bytes)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(err)) => {
                        error!("spectator websocket error: {err}");
                        break;
                    }
                }
            }
            snapshot = snapshots.recv() => {
                let Ok(snapshot) = snapshot else {
                    break;
                };
                if send_json(&mut sender, &ServerMessage::Snapshot(snapshot)).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn send_json(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    value: &ServerMessage,
) -> Result<(), axum::Error> {
    sender
        .send(Message::Text(serde_json::to_string(value).unwrap().into()))
        .await
}

async fn persist_economy(db: &PgPool, save: EconomySave) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE game_users
        SET total_tokens = GREATEST(total_tokens, $2),
            lobster_micros = $3,
            last_lobster_at = now(),
            equipped_head = $4,
            owned_heads = $5,
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(save.user_id)
    .bind(save.total_tokens as i64)
    .bind(save.lobster_micros as i64)
    .bind(save.equipped_head)
    .bind(Value::Array(
        save.owned_heads
            .into_iter()
            .map(Value::String)
            .collect::<Vec<_>>(),
    ))
    .execute(db)
    .await?;
    Ok(())
}

impl Game {
    fn new() -> GameHandle {
        let (snapshots, _) = broadcast::channel(32);
        let players = HashMap::new();
        let mut world = GameWorld {
            players,
            next_npc_id: 0,
            last_tick: Instant::now(),
        };
        ensure_minimum_entities(&mut world);
        Arc::new(Self {
            world: Mutex::new(world),
            snapshots,
        })
    }

    async fn add_player(&self, id: String, user: GameUser) {
        let position = {
            let mut rng = rand::rng();
            Vec3::random_unit(&mut rng)
        };
        let mut world = self.world.lock().await;
        remove_random_npc(&mut world);
        world.players.insert(
            id.clone(),
            Player {
                id,
                user_id: Some(user.id),
                name: user.username,
                planet_id: 0,
                position,
                basis_up: position.any_tangent(),
                input: InputState::default(),
                fake: false,
                jump_height: 0.0,
                jump_velocity: 0.0,
                npc_jump_seconds: 0.0,
                total_tokens: user.economy.total_tokens,
                lobster_micros: user.economy.lobster_micros,
                last_economy_at: Instant::now(),
                equipped_head: user.economy.equipped_head,
                owned_heads: user.economy.owned_heads,
                walking_phase: 0,
                npc_movement: None,
            },
        );
        ensure_minimum_entities(&mut world);
    }

    async fn remove_player(&self, id: &str) -> Option<EconomySave> {
        let mut world = self.world.lock().await;
        let removed = world.players.remove(id);
        ensure_minimum_entities(&mut world);
        removed.and_then(|mut player| {
            accrue_lobsters(&mut player, Instant::now());
            player.user_id.map(|user_id| EconomySave {
                user_id,
                total_tokens: player.total_tokens,
                lobster_micros: player.lobster_micros,
                equipped_head: player.equipped_head,
                owned_heads: player.owned_heads,
            })
        })
    }

    async fn set_input(&self, id: &str, input: InputState) {
        let mut world = self.world.lock().await;
        if let Some(player) = world.players.get_mut(id) {
            let jump_started = input.jump && !player.input.jump;
            player.input = input;
            if jump_started && player.jump_height <= JUMP_GROUND_EPSILON {
                player.jump_velocity = JUMP_IMPULSE_CELLS_PER_SECOND;
            }
        }
    }

    async fn set_total_tokens(&self, id: &str, total_tokens: u64) -> Option<EconomySave> {
        let mut world = self.world.lock().await;
        let player = world.players.get_mut(id)?;
        accrue_lobsters(player, Instant::now());
        player.total_tokens = player.total_tokens.max(total_tokens);
        player.user_id.map(|user_id| EconomySave {
            user_id,
            total_tokens: player.total_tokens,
            lobster_micros: player.lobster_micros,
            equipped_head: player.equipped_head.clone(),
            owned_heads: player.owned_heads.clone(),
        })
    }

    async fn buy_head(&self, id: &str, item_id: &str) -> Option<EconomySave> {
        let item = market_item(item_id)?;
        let mut world = self.world.lock().await;
        let player = world.players.get_mut(id)?;
        accrue_lobsters(player, Instant::now());
        if player.owned_heads.iter().any(|owned| owned == item_id) {
            player.equipped_head = item.id;
        } else {
            let price_micros = item.price_lobsters.saturating_mul(LOBSTER_MICROS);
            if player.lobster_micros < price_micros {
                return None;
            }
            player.lobster_micros = player.lobster_micros.saturating_sub(price_micros);
            player.owned_heads.push(item.id.clone());
            player.equipped_head = item.id;
        }
        player.user_id.map(|user_id| EconomySave {
            user_id,
            total_tokens: player.total_tokens,
            lobster_micros: player.lobster_micros,
            equipped_head: player.equipped_head.clone(),
            owned_heads: player.owned_heads.clone(),
        })
    }

    async fn equip_head(&self, id: &str, item_id: &str) -> Option<EconomySave> {
        market_item(item_id)?;
        let mut world = self.world.lock().await;
        let player = world.players.get_mut(id)?;
        accrue_lobsters(player, Instant::now());
        if !player.owned_heads.iter().any(|owned| owned == item_id) {
            return None;
        }
        player.equipped_head = item_id.to_string();
        player.user_id.map(|user_id| EconomySave {
            user_id,
            total_tokens: player.total_tokens,
            lobster_micros: player.lobster_micros,
            equipped_head: player.equipped_head.clone(),
            owned_heads: player.owned_heads.clone(),
        })
    }
}

async fn run_game_loop(game: GameHandle) {
    let mut ticker = tokio::time::interval(Duration::from_millis(50));
    loop {
        ticker.tick().await;
        let snapshot = {
            let mut world = game.world.lock().await;
            let now = Instant::now();
            let dt = now.duration_since(world.last_tick).as_secs_f64().min(0.2);
            world.last_tick = now;
            tick_world(&mut world, dt);
            snapshot_world(&world)
        };
        let _ = game.snapshots.send(snapshot);
    }
}

const MIN_ENTITY_COUNT: usize = 30;
const ANGULAR_SPEED_RADIANS_PER_SECOND: f64 = 0.275;
const NPC_CURVE_AMPLITUDE_RADIANS: f64 = 0.18;
const NPC_PATH_SAMPLES: usize = 32;
const LOBSTER_MICROS: u64 = 1_000_000;
const LOBSTER_RATE_TOKEN_UNIT: u64 = 1_000_000_000;
const LOBSTER_ACCRUAL_TOKEN_MS_PER_MICRO: u64 = 60_000_000;
const JUMP_IMPULSE_CELLS_PER_SECOND: f64 = 7.6;
const JUMP_GRAVITY_CELLS_PER_SECOND2: f64 = 19.0;
const MAX_JUMP_HEIGHT_CELLS: f64 = 2.0;
const JUMP_GROUND_EPSILON: f64 = 0.02;
const NPC_NAMES: &[&str] = &[
    "Coco", "Lulu", "Rico", "Fifi", "Toto", "Mimi", "Pepe", "Gigi", "Bibi", "Nono", "Kiki", "Zaza",
    "Didi", "Paco", "Lola", "Nico", "Tina", "Jojo", "Pipo", "Nina", "Chico", "Mona", "Roxy",
    "Tony", "Candy", "Sunny", "Buddy", "Dolly", "Kitty", "Bobby", "Sandy", "Misty", "Ricky",
    "Betty", "Vicky", "Jimmy", "Penny", "Tammy", "Rusty", "Wendy", "Manny", "Cindy", "Randy",
    "Mindy", "Duke", "Daisy", "Spike", "Peggy", "Bonnie", "Clyde", "Pepa", "Nacho", "Chula",
    "Lalo", "Lita", "Tito", "Pili", "Yoyo", "Luci", "Beto", "Nena", "Chuy", "Tati", "Majo", "Fito",
    "Lili", "Cuca", "Panch", "Marga", "Chelo", "Rafa", "Conch", "Manu", "Vero", "Chava", "Cesar",
    "Dulce", "Renee", "Gaston", "Pierrot", "Bijou", "Fleur", "Babou", "Minou", "Chouch", "Nana",
    "Yvette", "Gabin", "Odette", "Marcel", "Lisou", "Claude", "Remy", "Colette", "Bruno", "Suzon",
    "Blaise", "Gogo", "Mado", "Loulou",
];

const MARKET_ITEMS: &[(&str, &str, &str, u64)] = &[
    ("default", "Default", "0", 0),
    ("box", "Package", "📦", 0),
    ("smile", "Smile", "🙂", 250),
    ("cowboy", "Cowboy", "🤠", 1_500),
    ("sunglasses", "Sunglasses", "😎", 7_500),
    ("frog", "Frog", "🐸", 25_000),
    ("lobster", "Lobster", "🦞", 100_000),
    ("sun", "Sun", "☀️", 500_000),
];

fn market_items() -> Vec<MarketItemSnapshot> {
    MARKET_ITEMS
        .iter()
        .map(|(id, label, head, price_lobsters)| MarketItemSnapshot {
            id: (*id).to_string(),
            label: (*label).to_string(),
            head: (*head).to_string(),
            price_lobsters: *price_lobsters,
        })
        .collect()
}

fn market_item(id: &str) -> Option<MarketItemSnapshot> {
    market_items().into_iter().find(|item| item.id == id)
}

fn lobster_yield_per_hour(total_tokens: u64) -> f64 {
    total_tokens as f64 / LOBSTER_RATE_TOKEN_UNIT as f64 * 60.0
}

fn accrue_lobsters(player: &mut Player, now: Instant) {
    let elapsed = now
        .checked_duration_since(player.last_economy_at)
        .unwrap_or_default();
    player.last_economy_at = now;
    let elapsed_ms = elapsed.as_millis() as u64;
    if elapsed_ms == 0 || player.total_tokens == 0 {
        return;
    }
    let gained = (player.total_tokens as u128)
        .saturating_mul(elapsed_ms as u128)
        .saturating_div(LOBSTER_ACCRUAL_TOKEN_MS_PER_MICRO as u128) as u64;
    player.lobster_micros = player.lobster_micros.saturating_add(gained);
}

fn lobster_balance(lobster_micros: u64) -> u64 {
    lobster_micros / LOBSTER_MICROS
}

fn ensure_minimum_entities(world: &mut GameWorld) {
    let mut rng = rand::rng();
    while world.players.len() < MIN_ENTITY_COUNT {
        spawn_npc(world, &mut rng);
    }
}

fn spawn_npc(world: &mut GameWorld, rng: &mut impl Rng) {
    let npc_id = world.next_npc_id;
    let id = format!("npc-{npc_id}");
    world.next_npc_id += 1;
    let position = Vec3::random_unit(rng);
    let name = NPC_NAMES[rng.random_range(0..NPC_NAMES.len())];
    world.players.insert(
        id.clone(),
        Player {
            id,
            user_id: None,
            name: name.to_string(),
            planet_id: 0,
            position,
            basis_up: position.any_tangent(),
            input: InputState::default(),
            fake: true,
            jump_height: 0.0,
            jump_velocity: 0.0,
            npc_jump_seconds: random_npc_jump_seconds(rng),
            total_tokens: 0,
            lobster_micros: 0,
            last_economy_at: Instant::now(),
            equipped_head: "default".to_string(),
            owned_heads: vec!["default".to_string()],
            walking_phase: 0,
            npc_movement: Some(NpcMovement::Idle {
                remaining_seconds: random_npc_idle_seconds(rng),
            }),
        },
    );
}

fn remove_random_npc(world: &mut GameWorld) {
    let npc_ids = world
        .players
        .values()
        .filter(|player| player.fake)
        .map(|player| player.id.clone())
        .collect::<Vec<_>>();
    if npc_ids.is_empty() {
        return;
    }
    let mut rng = rand::rng();
    let index = rng.random_range(0..npc_ids.len());
    world.players.remove(&npc_ids[index]);
}

fn random_npc_idle_seconds(rng: &mut impl Rng) -> f64 {
    rng.random_range(1.0..=10.0)
}

fn random_npc_jump_seconds(rng: &mut impl Rng) -> f64 {
    rng.random_range(1.4..=5.5)
}

fn tick_world(world: &mut GameWorld, dt: f64) {
    let economy_now = Instant::now();
    for player in world.players.values_mut().filter(|player| !player.fake) {
        accrue_lobsters(player, economy_now);
        tick_jump(player, dt);
        let camera_up = player
            .input
            .camera_up
            .and_then(Vec3::from_array)
            .unwrap_or(player.basis_up);
        let screen_up = camera_up
            .add(player.position.scale(-camera_up.dot(player.position)))
            .normalize();
        let screen_up = if screen_up.length() <= 1e-6 {
            player.basis_up
        } else {
            screen_up
        };
        let screen_right = screen_up.cross(player.position).normalize();
        let mut direction = Vec3::new(0.0, 0.0, 0.0);
        if player.input.up {
            direction = direction.add(screen_up);
        }
        if player.input.down {
            direction = direction.add(screen_up.scale(-1.0));
        }
        if player.input.right {
            direction = direction.add(screen_right);
        }
        if player.input.left {
            direction = direction.add(screen_right.scale(-1.0));
        }
        direction = direction.add(player.position.scale(-direction.dot(player.position)));
        if direction.length() > 1e-6 {
            let direction = direction.normalize();
            let angular_distance = ANGULAR_SPEED_RADIANS_PER_SECOND * dt;
            let rotation_axis = player.position.cross(direction).normalize();
            player.position = player
                .position
                .rotate_around(rotation_axis, angular_distance)
                .normalize();
            let transported_up = player
                .basis_up
                .rotate_around(rotation_axis, angular_distance)
                .normalize();
            player.basis_up = transported_up
                .add(player.position.scale(-transported_up.dot(player.position)))
                .normalize();
            player.walking_phase = player.walking_phase.wrapping_add(1);
        }
    }

    let mut rng = rand::rng();
    for player in world.players.values_mut().filter(|player| player.fake) {
        tick_npc_jump(player, dt, &mut rng);
        tick_jump(player, dt);
        tick_npc(player, dt, &mut rng);
    }
}

fn tick_npc_jump(player: &mut Player, dt: f64, rng: &mut impl Rng) {
    player.npc_jump_seconds -= dt;
    if player.npc_jump_seconds <= 0.0 {
        if player.jump_height <= JUMP_GROUND_EPSILON {
            player.jump_velocity = JUMP_IMPULSE_CELLS_PER_SECOND;
        }
        player.npc_jump_seconds = random_npc_jump_seconds(rng);
    }
}

fn tick_jump(player: &mut Player, dt: f64) {
    if player.jump_height <= 0.0 && player.jump_velocity <= 0.0 {
        player.jump_height = 0.0;
        player.jump_velocity = 0.0;
        return;
    }

    player.jump_velocity -= JUMP_GRAVITY_CELLS_PER_SECOND2 * dt;
    player.jump_height += player.jump_velocity * dt;

    if player.jump_height >= MAX_JUMP_HEIGHT_CELLS {
        player.jump_height = MAX_JUMP_HEIGHT_CELLS;
        player.jump_velocity = player.jump_velocity.min(0.0);
    }

    if player.jump_height <= 0.0 {
        player.jump_height = 0.0;
        player.jump_velocity = 0.0;
    }
}

fn tick_npc(player: &mut Player, dt: f64, rng: &mut impl Rng) {
    if player.npc_movement.is_none() {
        player.npc_movement = Some(NpcMovement::Idle {
            remaining_seconds: random_npc_idle_seconds(rng),
        });
    }

    let movement = player.npc_movement.take().unwrap();
    player.npc_movement = Some(match movement {
        NpcMovement::Idle { remaining_seconds } => {
            let remaining_seconds = remaining_seconds - dt;
            if remaining_seconds > 0.0 {
                NpcMovement::Idle { remaining_seconds }
            } else {
                new_npc_move(player.position, rng)
            }
        }
        NpcMovement::Moving {
            start,
            target,
            lateral,
            path_length,
            distance,
        } => {
            let distance = distance + ANGULAR_SPEED_RADIANS_PER_SECOND * dt;
            let t = (distance / path_length.max(1e-6)).clamp(0.0, 1.0);
            let next_position = npc_path_point(start, target, lateral, t);
            move_player_to_position(player, next_position);
            player.walking_phase = player.walking_phase.wrapping_add(1);

            if t >= 1.0 {
                player.position = target;
                player.basis_up = target.any_tangent();
                NpcMovement::Idle {
                    remaining_seconds: random_npc_idle_seconds(rng),
                }
            } else {
                NpcMovement::Moving {
                    start,
                    target,
                    lateral,
                    path_length,
                    distance,
                }
            }
        }
    });
}

fn new_npc_move(start: Vec3, rng: &mut impl Rng) -> NpcMovement {
    let mut target = Vec3::random_unit(rng);
    for _ in 0..8 {
        if start.angle_to(target) >= 0.35 {
            break;
        }
        target = Vec3::random_unit(rng);
    }

    let lateral = start
        .cross(target)
        .normalize()
        .add(start.scale(-start.cross(target).normalize().dot(start)))
        .normalize();
    let lateral = if lateral.length() <= 1e-6 {
        start.any_tangent()
    } else {
        lateral
    };
    let path_length = estimate_npc_path_length(start, target, lateral);

    NpcMovement::Moving {
        start,
        target,
        lateral,
        path_length,
        distance: 0.0,
    }
}

fn estimate_npc_path_length(start: Vec3, target: Vec3, lateral: Vec3) -> f64 {
    let mut length = 0.0;
    let mut previous = npc_path_point(start, target, lateral, 0.0);
    for sample in 1..=NPC_PATH_SAMPLES {
        let t = sample as f64 / NPC_PATH_SAMPLES as f64;
        let next = npc_path_point(start, target, lateral, t);
        length += previous.angle_to(next);
        previous = next;
    }
    length.max(start.angle_to(target))
}

fn npc_path_point(start: Vec3, target: Vec3, lateral: Vec3, t: f64) -> Vec3 {
    let base = start.slerp(target, t);
    let wave = (TAU * t).sin();
    base.add(lateral.scale(wave * NPC_CURVE_AMPLITUDE_RADIANS))
        .normalize()
}

fn move_player_to_position(player: &mut Player, next_position: Vec3) {
    let angle = player.position.angle_to(next_position);
    if angle <= 1e-9 {
        player.position = next_position;
        return;
    }
    let rotation_axis = player.position.cross(next_position).normalize();
    player.position = next_position.normalize();
    let transported_up = player
        .basis_up
        .rotate_around(rotation_axis, angle)
        .normalize();
    player.basis_up = transported_up
        .add(player.position.scale(-transported_up.dot(player.position)))
        .normalize();
}

fn snapshot_world(world: &GameWorld) -> Snapshot {
    let players = world
        .players
        .values()
        .map(|player| {
            let (lat, lon) = player.position.lat_lon();
            PlayerSnapshot {
                id: player.id.clone(),
                name: player.name.clone(),
                planet_id: player.planet_id,
                lat: lat.clamp(-FRAC_PI_2, FRAC_PI_2),
                lon: wrap_pi(lon),
                position: player.position.to_array(),
                basis_up: player.basis_up.to_array(),
                fake: player.fake,
                total_tokens: player.total_tokens,
                lobsters: lobster_balance(player.lobster_micros),
                lobster_yield_per_hour: lobster_yield_per_hour(player.total_tokens),
                equipped_head: player.equipped_head.clone(),
                owned_heads: player.owned_heads.clone(),
                jump_height: player.jump_height,
                walking_phase: player.walking_phase,
            }
        })
        .collect();
    Snapshot {
        server_time_ms: 0,
        players,
    }
}

fn wrap_pi(value: f64) -> f64 {
    (value + PI).rem_euclid(TAU) - PI
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lobster_income_is_scaled_down_by_thousand() {
        assert!((lobster_yield_per_hour(1_000_000) - 0.06).abs() < f64::EPSILON);
        assert!((lobster_yield_per_hour(1_000_000_000) - 60.0).abs() < f64::EPSILON);

        let position = Vec3::new(1.0, 0.0, 0.0);
        let now = Instant::now();
        let mut player = Player {
            id: "player".to_string(),
            user_id: Some(Uuid::new_v4()),
            name: "player".to_string(),
            planet_id: 0,
            position,
            basis_up: position.any_tangent(),
            input: InputState::default(),
            fake: false,
            jump_height: 0.0,
            jump_velocity: 0.0,
            npc_jump_seconds: 0.0,
            total_tokens: 1_000_000_000,
            lobster_micros: 0,
            last_economy_at: now - Duration::from_secs(60),
            equipped_head: "default".to_string(),
            owned_heads: vec!["default".to_string()],
            walking_phase: 0,
            npc_movement: None,
        };

        accrue_lobsters(&mut player, now);

        assert_eq!(lobster_balance(player.lobster_micros), 1);
    }

    #[test]
    fn movement_near_poles_keeps_unit_position_and_tangent_basis() {
        let position = Vec3::new(0.001, 0.0, 0.9999995).normalize();
        let mut world = GameWorld {
            players: HashMap::from([(
                "player".to_string(),
                Player {
                    id: "player".to_string(),
                    user_id: Some(Uuid::new_v4()),
                    name: "player".to_string(),
                    planet_id: 0,
                    position,
                    basis_up: position.any_tangent(),
                    input: InputState {
                        up: true,
                        down: false,
                        left: true,
                        right: false,
                        jump: false,
                        camera_up: Some([0.0, 1.0, 0.0]),
                    },
                    fake: false,
                    jump_height: 0.0,
                    jump_velocity: 0.0,
                    npc_jump_seconds: 0.0,
                    total_tokens: 0,
                    lobster_micros: 0,
                    last_economy_at: Instant::now(),
                    equipped_head: "default".to_string(),
                    owned_heads: vec!["default".to_string()],
                    walking_phase: 0,
                    npc_movement: None,
                },
            )]),
            next_npc_id: 0,
            last_tick: Instant::now(),
        };

        for _ in 0..500 {
            tick_world(&mut world, 1.0 / 60.0);
            let player = world.players.get("player").unwrap();
            assert!((player.position.length() - 1.0).abs() < 1e-9);
            assert!((player.basis_up.length() - 1.0).abs() < 1e-9);
            assert!(player.position.dot(player.basis_up).abs() < 1e-9);
            assert!(player.position.x.is_finite());
            assert!(player.position.y.is_finite());
            assert!(player.position.z.is_finite());
            assert!(player.basis_up.x.is_finite());
            assert!(player.basis_up.y.is_finite());
            assert!(player.basis_up.z.is_finite());
        }
    }

    #[tokio::test]
    async fn player_join_replaces_npc_and_leave_restores_minimum_entities() {
        let game = Game::new();

        {
            let world = game.world.lock().await;
            assert_eq!(world.players.len(), MIN_ENTITY_COUNT);
            assert_eq!(
                world.players.values().filter(|player| player.fake).count(),
                MIN_ENTITY_COUNT
            );
            assert!(world
                .players
                .values()
                .filter(|player| player.fake)
                .all(|player| player.equipped_head == "default"));
        }

        let player_id = "real-player".to_string();
        game.add_player(
            player_id.clone(),
            GameUser {
                id: Uuid::new_v4(),
                username: "real".to_string(),
                economy: UserEconomy {
                    total_tokens: 0,
                    lobster_micros: 0,
                    equipped_head: "default".to_string(),
                    owned_heads: vec!["default".to_string()],
                },
                rewards: Vec::new(),
            },
        )
        .await;

        {
            let world = game.world.lock().await;
            assert_eq!(world.players.len(), MIN_ENTITY_COUNT);
            assert_eq!(
                world.players.values().filter(|player| player.fake).count(),
                MIN_ENTITY_COUNT - 1
            );
            assert!(world.players.contains_key(&player_id));
        }

        game.remove_player(&player_id).await;

        {
            let world = game.world.lock().await;
            assert_eq!(world.players.len(), MIN_ENTITY_COUNT);
            assert_eq!(
                world.players.values().filter(|player| player.fake).count(),
                MIN_ENTITY_COUNT
            );
        }
    }

    #[test]
    fn npc_leaves_idle_and_walks_on_unit_sphere() {
        let position = Vec3::new(1.0, 0.0, 0.0);
        let mut world = GameWorld {
            players: HashMap::from([(
                "npc".to_string(),
                Player {
                    id: "npc".to_string(),
                    user_id: None,
                    name: "npc".to_string(),
                    planet_id: 0,
                    position,
                    basis_up: position.any_tangent(),
                    input: InputState::default(),
                    fake: true,
                    jump_height: 0.0,
                    jump_velocity: 0.0,
                    npc_jump_seconds: 0.0,
                    total_tokens: 0,
                    lobster_micros: 0,
                    last_economy_at: Instant::now(),
                    equipped_head: "default".to_string(),
                    owned_heads: vec!["default".to_string()],
                    walking_phase: 0,
                    npc_movement: Some(NpcMovement::Idle {
                        remaining_seconds: 0.0,
                    }),
                },
            )]),
            next_npc_id: 0,
            last_tick: Instant::now(),
        };

        for _ in 0..120 {
            tick_world(&mut world, 1.0 / 60.0);
        }

        let npc = world.players.get("npc").unwrap();
        assert!(npc.walking_phase > 0);
        assert!(position.angle_to(npc.position) > 0.1);
        assert!((npc.position.length() - 1.0).abs() < 1e-9);
        assert!((npc.basis_up.length() - 1.0).abs() < 1e-9);
        assert!(npc.position.dot(npc.basis_up).abs() < 1e-9);
    }
}
