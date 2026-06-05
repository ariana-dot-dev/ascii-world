use std::{
    collections::HashMap,
    env,
    f64::consts::{FRAC_PI_2, PI, TAU},
    net::SocketAddr,
    path::{Path, PathBuf},
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
    response::{Html, IntoResponse, Response},
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
struct SpectatorFrameQuery {
    width: Option<u16>,
    height: Option<u16>,
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
    placed_pixels: HashMap<String, PlacedPixel>,
    pickups: HashMap<u64, Pickup>,
    pickup_rewards: Vec<PickupReward>,
    next_pickup_id: u64,
    last_pickup_spawn: Instant,
    next_npc_id: u64,
    last_tick: Instant,
}

#[derive(Clone)]
struct Player {
    id: String,
    user_id: Option<Uuid>,
    last_seen: Instant,
    name: String,
    planet_id: u32,
    position: Vec3,
    basis_up: Vec3,
    input: InputState,
    fake: bool,
    jump_height: f64,
    jump_velocity: f64,
    jump_momentum: Option<Vec3>,
    last_jump_started_at: Option<Instant>,
    last_jump_momentum: Option<Vec3>,
    jump_leg_pose: i8,
    momentum_jump_chain: u8,
    jump_momentum_multiplier: f64,
    npc_jump_seconds: f64,
    total_tokens: u64,
    all_time_tokens: u64,
    lobster_micros: u64,
    last_economy_at: Instant,
    equipped_head: String,
    owned_heads: Vec<String>,
    owned_pixels: [u64; PIXEL_COLOR_COUNT],
    facing: i8,
    walking_phase: u64,
    npc_movement: Option<NpcMovement>,
}

#[derive(Debug, Clone)]
struct UserEconomy {
    total_tokens: u64,
    all_time_tokens: u64,
    lobster_micros: u64,
    equipped_head: String,
    owned_heads: Vec<String>,
    owned_pixels: [u64; PIXEL_COLOR_COUNT],
}

#[derive(Debug, Clone)]
struct EconomySave {
    user_id: Uuid,
    total_tokens: u64,
    all_time_tokens: u64,
    lobster_micros: u64,
    equipped_head: String,
    owned_heads: Vec<String>,
    owned_pixels: [u64; PIXEL_COLOR_COUNT],
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
    kind: String,
    pixel_color: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct PlacedPixel {
    position: [f64; 3],
    color: usize,
}

#[derive(Clone)]
struct Pickup {
    id: u64,
    position: Vec3,
    emoji: &'static str,
    expires_at: Instant,
}

#[derive(Debug, Clone, Serialize)]
struct PickupSnapshot {
    id: u64,
    position: [f64; 3],
    emoji: String,
}

#[derive(Clone)]
struct PickupReward {
    player_id: String,
    lobsters: u64,
    expires_at: Instant,
}

#[derive(Debug, Clone, Serialize)]
struct PickupRewardSnapshot {
    player_id: String,
    lobsters: u64,
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
        all_time_tokens: u64,
    },
    BuyHead {
        item_id: String,
    },
    EquipHead {
        item_id: String,
    },
    BuyPixel {
        color: usize,
    },
    PlacePixel {
        color: usize,
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
    leaderboard: Vec<LeaderboardEntry>,
    placed_pixels: Vec<PlacedPixel>,
    pickups: Vec<PickupSnapshot>,
    pickup_rewards: Vec<PickupRewardSnapshot>,
    economy_rules: EconomyRulesSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct EconomyRulesSnapshot {
    lobster_rate_token_unit: u64,
}

#[derive(Debug, Clone, Serialize)]
struct LeaderboardEntry {
    username: String,
    lobsters: u64,
    all_time_tokens: u64,
    profile_url: String,
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
    all_time_tokens: u64,
    lobsters: u64,
    equipped_head: String,
    owned_heads: Vec<String>,
    owned_pixels: [u64; PIXEL_COLOR_COUNT],
    jump_height: f64,
    jump_leg_pose: i8,
    facing: i8,
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
    close_stale_player_sessions_on_boot(&db, boot_id).await?;

    let game = Game::new();
    tokio::spawn(run_game_loop(game.clone(), db.clone()));

    let state = AppState { db, boot_id, game };
    let app = Router::new()
        .route("/", get(landing_page))
        .route("/health", get(health))
        .route("/ws", get(ws))
        .route("/spectate", get(spectate))
        .route("/spectate-frame", get(spectate_frame))
        .route("/auth/x/start", axum::routing::post(auth_x_start))
        .route("/auth/x/poll/{poll_token}", get(auth_x_poll))
        .route("/auth/x/callback", get(auth_x_callback))
        .route("/terms", get(terms_page))
        .route("/privacy", get(privacy_page))
        .route("/robots.txt", get(robots_txt))
        .route("/sitemap.xml", get(sitemap_xml))
        .route("/og.png", get(og_png))
        .route("/assets/og.png", get(og_png))
        .route("/install.sh", get(install_sh))
        .route("/install.ps1", get(install_ps1))
        .route("/download/{asset}", get(download_cli_asset))
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
            i64,
            Option<DateTime<Utc>>,
            Option<NaiveDate>,
            i32,
            Option<NaiveDate>,
            i32,
            String,
            Value,
            Value,
        ),
    >(
        r#"
        SELECT id, x_username, x_name, total_tokens, all_time_tokens, lobster_micros, last_lobster_at,
               last_daily_reward_date, daily_streak_days,
               last_weekly_reward_monday, weekly_streak_weeks,
               equipped_head, owned_heads, owned_pixels
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
        all_time_tokens,
        lobster_micros,
        _last_lobster_at,
        last_daily_reward_date,
        daily_streak_days,
        last_weekly_reward_monday,
        weekly_streak_weeks,
        equipped_head,
        owned_heads,
        owned_pixels,
    )) = row
    else {
        return Ok(None);
    };

    let now = Utc::now();
    let total_tokens = total_tokens.max(0) as u64;
    let all_time_tokens = all_time_tokens.max(0) as u64;
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
        if weekly_streak_weeks >= 2 {
            let lobsters = 5_000_u64.saturating_mul(weekly_streak_weeks as u64);
            lobster_micros = lobster_micros.saturating_add(lobsters.saturating_mul(LOBSTER_MICROS));
            rewards.push(RewardNotice {
                label: "Weekly streak".to_string(),
                lobsters,
                streak: weekly_streak_weeks,
            });
        } else {
            rewards.push(RewardNotice {
                label: "Weekly starts next week".to_string(),
                lobsters: 0,
                streak: weekly_streak_weeks,
            });
        }
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
    let owned_pixels = parse_pixel_inventory(&owned_pixels)?;

    sqlx::query(
        r#"
        UPDATE game_users
        SET total_tokens = $2,
            all_time_tokens = $3,
            lobster_micros = $4,
            last_lobster_at = now(),
            last_daily_reward_date = $5,
            daily_streak_days = $6,
            last_weekly_reward_monday = $7,
            weekly_streak_weeks = $8,
            equipped_head = $9,
            owned_heads = $10,
            owned_pixels = $11,
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(total_tokens as i64)
    .bind(all_time_tokens as i64)
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
    .bind(pixel_inventory_json(owned_pixels))
    .execute(db)
    .await?;

    Ok(Some(GameUser {
        id,
        username,
        economy: UserEconomy {
            total_tokens,
            all_time_tokens,
            lobster_micros,
            equipped_head,
            owned_heads,
            owned_pixels,
        },
        rewards,
    }))
}

fn parse_pixel_inventory(value: &Value) -> anyhow::Result<[u64; PIXEL_COLOR_COUNT]> {
    let mut inventory = [0; PIXEL_COLOR_COUNT];
    let items = value
        .as_array()
        .context("owned_pixels must be a JSON array")?;
    if items.len() != PIXEL_COLOR_COUNT {
        anyhow::bail!(
            "owned_pixels must have {PIXEL_COLOR_COUNT} entries, got {}",
            items.len()
        );
    }
    for (index, item) in items.iter().enumerate() {
        inventory[index] = item
            .as_u64()
            .with_context(|| format!("owned_pixels[{index}] must be a non-negative integer"))?;
    }
    Ok(inventory)
}

fn pixel_inventory_json(inventory: [u64; PIXEL_COLOR_COUNT]) -> Value {
    Value::Array(inventory.into_iter().map(Value::from).collect())
}

async fn landing_page() -> Response {
    let page = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Ascii World is an idle multiplayer terminal game for vibe coders. Join from your terminal, earn tokens, collect lobsters, and leave pixels on a shared ASCII planet.">
<meta name="robots" content="index,follow">
<meta name="theme-color" content="#1e1e1e">
<link rel="canonical" href="https://world.ascii.dev/">
<meta property="og:type" content="website">
<meta property="og:site_name" content="Ascii World">
<meta property="og:title" content="Ascii World">
<meta property="og:description" content="The idle multiplayer terminal game for vibe coders.">
<meta property="og:url" content="https://world.ascii.dev/">
<meta property="og:image" content="https://world.ascii.dev/og.png">
<meta property="og:image:secure_url" content="https://world.ascii.dev/og.png">
<meta property="og:image:type" content="image/png">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta property="og:image:alt" content="Ascii World terminal game preview with an ASCII Earth.">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:site" content="@asciidotdev">
<meta name="twitter:title" content="Ascii World">
<meta name="twitter:description" content="The idle multiplayer terminal game for vibe coders.">
<meta name="twitter:image" content="https://world.ascii.dev/og.png">
<meta name="twitter:image:alt" content="Ascii World terminal game preview with an ASCII Earth.">
<title>Ascii World</title>
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "SoftwareApplication",
  "name": "Ascii World",
  "applicationCategory": "GameApplication",
  "operatingSystem": "Linux, macOS, Windows",
  "url": "https://world.ascii.dev/",
  "image": "https://world.ascii.dev/og.png",
  "description": "Ascii World is an idle multiplayer terminal game for vibe coders.",
  "offers": {
    "@type": "Offer",
    "price": "0",
    "priceCurrency": "USD"
  }
}
</script>
<style>
:root { color-scheme: dark; }
* { box-sizing: border-box; }
html, body { margin: 0; min-height: 100%; background: #1e1e1e; color: #cccccc; }
body {
  --earth-x: 0ch;
  --earth-y: 0em;
  position: relative;
  isolation: isolate;
  display: grid;
  grid-template-rows: 1fr auto;
  place-items: center start;
  min-height: 100svh;
  padding: 2ch;
  font-family: "Lilex Mono", "Lilex Nerd Font Mono", "Lilex", ui-monospace, monospace;
  font-size: clamp(13px, 1.7vw, 18px);
  font-style: italic;
  font-weight: 400;
  line-height: 1.45;
  overflow: hidden;
}
.ascii-sky {
  position: fixed;
  inset: 0;
  z-index: -1;
  margin: 0;
  opacity: .7;
  transform: translate(var(--earth-x), var(--earth-y));
  color: #3f4658;
  background: #1e1e1e;
  font: inherit;
  font-style: normal;
  font-weight: 400;
  line-height: 1;
  letter-spacing: 0;
  white-space: pre;
  pointer-events: none;
  user-select: none;
  overflow: hidden;
}
.ascii-mask {
  position: fixed;
  inset: 0;
  z-index: 0;
  margin: 0;
  color: transparent;
  background: transparent;
  font: inherit;
  font-style: normal;
  font-weight: 400;
  line-height: 1;
  letter-spacing: 0;
  white-space: pre;
  pointer-events: none;
  user-select: none;
  overflow: hidden;
}
.ascii-mask .m { background: #1e1e1e; }
.ascii-sky .s0 { color: #465064; }
.ascii-sky .s1 { color: #6f7d98; }
.ascii-sky .s2 { color: #d4d4d4; }
.ascii-sky .po { color: #5fa55f; }
.ascii-sky .pl { color: #509150; }
.ascii-sky .pw { color: #2d4b6e; }
.cell-probe {
  position: fixed;
  left: -1000px;
  top: -1000px;
  visibility: hidden;
  font: inherit;
  font-style: normal;
  line-height: 1;
}
.screen {
  position: relative;
  z-index: 10;
  width: min(96vw, 96ch);
  min-height: 0;
  display: grid;
  place-items: center;
  padding: 0;
  margin-inline: auto;
}
.term {
  position: relative;
  width: min(100%, 72ch);
  padding: 0;
  background: transparent;
  box-shadow: none;
  color: #cccccc;
  text-align: center;
  line-height: inherit;
}
.wordmark-wrap,
.dim,
.cmd,
.toggle,
.palette,
.footer {
  background: transparent;
  box-decoration-break: clone;
  -webkit-box-decoration-break: clone;
  box-shadow: none;
}
.title { color: #569cd6; font: inherit; font-weight: 400; letter-spacing: 0; }
.wordmark {
  margin: 0 0 1.45em;
  color: #569cd6;
  font: inherit;
  font-style: normal;
  font-weight: 400;
  line-height: 1;
  letter-spacing: 0;
  text-align: center;
  white-space: pre;
  overflow: visible;
}
.wordmark-wrap {
  position: relative;
  display: inline-block;
  margin: 0 0 1.45em;
}
.wordmark-wrap .wordmark {
  margin: 0;
}
.wordmark-shine {
  position: absolute;
  inset: 0;
  color: #ffffff;
  pointer-events: none;
  clip-path: inset(0 100% 0 0);
  animation: wordmark-shine 4.2s linear infinite;
}
.dim { color: #858585; }
.tagline { color: #569cd6; }
.cmd {
  display: block;
  width: 100%;
  margin: .3rem auto 1rem;
  padding: 0;
  color: #cccccc;
  background: transparent;
  border: 0;
  font: inherit;
  text-align: center;
  overflow-wrap: anywhere;
}
.toggle {
  display: inline-grid;
  grid-auto-flow: column;
  gap: 2ch;
  margin: .35rem 0 .4rem;
}
button {
  appearance: none;
  background: transparent;
  border: 0;
  color: #858585;
  font: inherit;
  font-style: normal;
  cursor: pointer;
  padding: 0;
}
button::before { content: " "; }
button::after { content: " "; }
button[aria-pressed="true"] { color: #cccccc; }
button[aria-pressed="true"]::before { content: "["; }
button[aria-pressed="true"]::after { content: "]"; }
button[aria-pressed="true"]::before,
button[aria-pressed="true"]::after { color: #569cd6; }
button:focus-visible { outline: 1px solid #569cd6; outline-offset: 2px; }
.palette {
  position: fixed;
  right: 0;
  bottom: 0;
  z-index: 10;
  display: inline-grid;
  grid-template-columns: repeat(8, 3ch);
  grid-template-rows: repeat(2, 1em);
  gap: 0;
  margin: 0;
  opacity: .7;
  line-height: 1;
  font-style: normal;
}
.swatch {
  width: 3ch;
  height: 1em;
  display: inline-block;
  color: transparent;
  overflow: hidden;
}
.c0 { background: #000000; }
.c1 { background: #cd3131; }
.c2 { background: #0dbc79; }
.c3 { background: #e5e510; }
.c4 { background: #2472c8; }
.c5 { background: #bc3fbc; }
.c6 { background: #11a8cd; }
.c7 { background: #e5e5e5; }
.c8 { background: #666666; }
.c9 { background: #f14c4c; }
.c10 { background: #23d18b; }
.c11 { background: #f5f543; }
.c12 { background: #3b8eea; }
.c13 { background: #d670d6; }
.c14 { background: #29b8db; }
.c15 { background: #ffffff; }
.footer {
  position: relative;
  z-index: 10;
  color: #858585;
  text-align: center;
  padding-right: 26ch;
}
.footer a {
  color: #569cd6;
  text-decoration: underline;
  text-underline-offset: .18em;
}
@media (max-width: 700px) {
  body {
    --earth-x: 0ch;
    --earth-y: 10em;
    place-items: start center;
    padding-top: max(4em, env(safe-area-inset-top));
  }
  .screen {
    margin-inline: auto;
  }
}
@media (min-width: 1000px) {
  body {
    --earth-x: 22ch;
    --earth-y: 0em;
    place-items: center start;
    padding-left: 7vw;
  }
  .screen {
    width: min(52vw, 76ch);
    margin-inline: 0;
  }
}
@media (min-width: 701px) and (max-width: 999px) {
  body {
    --earth-x: 0ch;
    --earth-y: 0em;
    place-items: center;
  }
  .screen {
    margin-inline: auto;
  }
}
@keyframes wordmark-shine {
  0%, 24% { clip-path: inset(0 100% 0 0); }
  30% { clip-path: inset(0 88% 0 0); }
  58% { clip-path: inset(0 0 0 88%); }
  64%, 100% { clip-path: inset(0 0 0 100%); }
}
</style>
</head>
<body>
<pre id="ascii-sky" class="ascii-sky" aria-hidden="true"></pre>
<pre id="ascii-mask" class="ascii-mask" aria-hidden="true"></pre>
<span id="cell-probe" class="cell-probe" aria-hidden="true">M</span>
<main class="screen">
<section class="term" aria-label="Ascii World landing page">
<div class="wordmark-wrap">
<pre class="wordmark" aria-label="Ascii World">   ___           _ _   _      __         __   __
  / _ | ___ ____(_|_) | | /| / /__  ____/ /__/ /
  / __ |(_-&lt;/ __/ / /  | |/ |/ / _ \/ __/ / _  / 
/_/ |_/___/\__/_/_/   |__/|__/\___/_/ /_/\_,_/</pre>
<pre class="wordmark wordmark-shine" aria-hidden="true">   ___           _ _   _      __         __   __
  / _ | ___ ____(_|_) | | /| / /__  ____/ /__/ /
  / __ |(_-&lt;/ __/ / /  | |/ |/ / _ \/ __/ / _  / 
/_/ |_/___/\__/_/_/   |__/|__/\___/_/ /_/\_,_/</pre>
</div>
<div class="dim tagline">The idle multiplayer game for vibe coders</div>
<div class="dim">join from your terminal:</div>
<div class="toggle" role="group" aria-label="platform">
<button type="button" data-platform="mac" aria-pressed="false" aria-label="macOS">🍎</button>
<button type="button" data-platform="linux" aria-pressed="true" aria-label="Linux">🐧</button>
<button type="button" data-platform="windows" aria-pressed="false" aria-label="Windows">🪟</button>
</div>
<code id="install" class="cmd"></code>
<div class="dim">once installed, run:</div>
<code id="run" class="cmd"></code>
</section>
</main>
<footer class="footer">Made and hosted by agents on <a href="https://box.ascii.dev">box.ascii.dev</a>, the cheapest and most powerful AI sandboxes</footer>
<div class="palette" aria-label="Dark+ terminal colors">
<span class="swatch c0">###</span><span class="swatch c1">###</span><span class="swatch c2">###</span><span class="swatch c3">###</span><span class="swatch c4">###</span><span class="swatch c5">###</span><span class="swatch c6">###</span><span class="swatch c7">###</span>
<span class="swatch c8">###</span><span class="swatch c9">###</span><span class="swatch c10">###</span><span class="swatch c11">###</span><span class="swatch c12">###</span><span class="swatch c13">###</span><span class="swatch c14">###</span><span class="swatch c15">###</span>
</div>
<script>
const sky = document.getElementById("ascii-sky");
const mask = document.getElementById("ascii-mask");
const probe = document.getElementById("cell-probe");
let skySize = { cols: 0, rows: 0, cellW: 1, cellH: 1 };
let lastSkyFrame = 0;
let maskQueued = false;

function hash2(x, y, seed) {
  let value = ((x * 0x8da6b343) ^ (y * 0xd8163841) ^ seed) >>> 0;
  value ^= value >>> 16;
  value = Math.imul(value, 0x7feb352d) >>> 0;
  value ^= value >>> 15;
  value = Math.imul(value, 0x846ca68b) >>> 0;
  value ^= value >>> 16;
  return value >>> 0;
}

function unit(seed) {
  return seed / 4294967295;
}

function smooth(t) {
  return t * t * t * (t * (t * 6 - 15) + 10);
}

function lerp(a, b, t) {
  return a + (b - a) * t;
}

function valueNoise3(x, y, z) {
  const xi = Math.floor(x);
  const yi = Math.floor(y);
  const zi = Math.floor(z);
  const xf = x - xi;
  const yf = y - yi;
  const zf = z - zi;
  const u = smooth(xf);
  const v = smooth(yf);
  const w = smooth(zf);
  const h = (xx, yy, zz) => unit(hash2(xx, yy, 0x9e3779b9 ^ Math.imul(zz, 0x85ebca6b)));
  const x00 = lerp(h(xi, yi, zi), h(xi + 1, yi, zi), u);
  const x10 = lerp(h(xi, yi + 1, zi), h(xi + 1, yi + 1, zi), u);
  const x01 = lerp(h(xi, yi, zi + 1), h(xi + 1, yi, zi + 1), u);
  const x11 = lerp(h(xi, yi + 1, zi + 1), h(xi + 1, yi + 1, zi + 1), u);
  return lerp(lerp(x00, x10, v), lerp(x01, x11, v), w) * 2 - 1;
}

function escapeStar(ch) {
  return ch === "*" ? "*" : ch;
}

function vec(x, y, z) {
  return { x, y, z };
}

function dot(a, b) {
  return a.x * b.x + a.y * b.y + a.z * b.z;
}

function add(a, b) {
  return vec(a.x + b.x, a.y + b.y, a.z + b.z);
}

function scale(a, s) {
  return vec(a.x * s, a.y * s, a.z * s);
}

function cross(a, b) {
  return vec(a.y * b.z - a.z * b.y, a.z * b.x - a.x * b.z, a.x * b.y - a.y * b.x);
}

function normalize(a) {
  const len = Math.hypot(a.x, a.y, a.z) || 1;
  return vec(a.x / len, a.y / len, a.z / len);
}

function rotateY(a, angle) {
  const c = Math.cos(angle);
  const s = Math.sin(angle);
  return vec(a.x * c + a.z * s, a.y, -a.x * s + a.z * c);
}

function rotateZ(a, angle) {
  const c = Math.cos(angle);
  const s = Math.sin(angle);
  return vec(a.x * c - a.y * s, a.x * s + a.y * c, a.z);
}

function proceduralLand(p) {
  const lat = Math.asin(Math.max(-1, Math.min(1, p.z)));
  const lon = Math.atan2(p.y, p.x);
  const n1 = valueNoise3(lon * 1.7 + 2.0, lat * 3.1, 0.15);
  const n2 = valueNoise3(lon * 3.4 - 1.5, lat * 5.8 + 4.0, 1.7) * 0.42;
  const n3 = valueNoise3(lon * 7.5 + 0.7, lat * 9.0 - 2.0, 3.1) * 0.18;
  const polar = Math.abs(lat) > 1.18 ? 0.55 : 0;
  return n1 + n2 + n3 + polar > 0.05;
}

function landChar(p) {
  const lat = Math.asin(Math.max(-1, Math.min(1, p.z)));
  if (Math.abs(lat) > 1.15) {
    return "*";
  }
  return Math.sin(lat * 31 + p.x * 17 + p.y * 13) > 0.25 ? "#" : "+";
}

function skyCell(x, y, time) {
  const seed = hash2(x, y, 0x5f3759df);
  const placement = unit(seed);
  const large = placement < 0.0022;
  const small = placement < 0.019;
  if (!large && !small) {
    return " ";
  }
  if (large) {
    const phase = unit(seed ^ 0x9e3779b9) * 12;
    const noise = valueNoise3(x * 0.075, y * 0.12, time * 0.18 + phase);
    const level = Math.max(0, Math.min(2, Math.floor((noise + 1) * 1.5)));
    const ch = level === 0 ? "+" : "*";
    return `<span class="s${level}">${escapeStar(ch)}</span>`;
  }
  const level = placement < 0.006 ? 1 : 0;
  return `<span class="s${level}">.</span>`;
}

function measureSky() {
  const rect = probe.getBoundingClientRect();
  const cellW = Math.max(1, rect.width);
  const measuredCellH = Math.max(1, rect.height);
  const cellH = Math.max(measuredCellH, cellW * 2);
  skySize = {
    cols: Math.ceil(window.innerWidth / cellW),
    rows: Math.ceil(window.innerHeight / cellH),
    cellW,
    cellH
  };
  sky.style.lineHeight = `${cellH}px`;
  mask.style.lineHeight = `${cellH}px`;
}

function foregroundTextNodes() {
  const roots = Array.from(document.querySelectorAll(".screen, .footer"));
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
    acceptNode(node) {
      if (!node.nodeValue || !/\S/.test(node.nodeValue)) {
        return NodeFilter.FILTER_REJECT;
      }
      if (!roots.some(root => root.contains(node.parentElement))) {
        return NodeFilter.FILTER_REJECT;
      }
      const style = window.getComputedStyle(node.parentElement);
      if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") {
        return NodeFilter.FILTER_REJECT;
      }
      return NodeFilter.FILTER_ACCEPT;
    }
  });
  const nodes = [];
  while (walker.nextNode()) {
    nodes.push(walker.currentNode);
  }
  return nodes;
}

function markMask(maskCells, col, row) {
  for (let dy = -2; dy <= 2; dy++) {
    const yy = row + dy;
    if (yy < 0 || yy >= skySize.rows) {
      continue;
    }
    for (let dx = -2; dx <= 2; dx++) {
      const xx = col + dx;
      if (xx >= 0 && xx < skySize.cols) {
        maskCells[yy * skySize.cols + xx] = true;
      }
    }
  }
}

function renderForegroundMask() {
  maskQueued = false;
  if (skySize.cols <= 0 || skySize.rows <= 0) {
    measureSky();
  }
  const maskCells = new Array(skySize.cols * skySize.rows).fill(false);
  for (const node of foregroundTextNodes()) {
    const text = node.nodeValue;
    for (let index = 0; index < text.length;) {
      const glyph = Array.from(text.slice(index))[0];
      if (glyph === " " || glyph === "\n" || glyph === "\t" || glyph === "\r") {
        index += glyph.length;
        continue;
      }
      const range = document.createRange();
      range.setStart(node, index);
      range.setEnd(node, index + glyph.length);
      for (const rect of range.getClientRects()) {
        if (rect.width <= 0 || rect.height <= 0) {
          continue;
        }
        const col = Math.floor((rect.left + rect.width / 2) / skySize.cellW);
        const row = Math.floor((rect.top + rect.height / 2) / skySize.cellH);
        markMask(maskCells, col, row);
      }
      range.detach();
      index += glyph.length;
    }
  }

  const lines = [];
  for (let row = 0; row < skySize.rows; row++) {
    let line = "";
    let inMask = false;
    for (let col = 0; col < skySize.cols; col++) {
      const filled = maskCells[row * skySize.cols + col];
      if (filled && !inMask) {
        line += `<span class="m">`;
        inMask = true;
      } else if (!filled && inMask) {
        line += `</span>`;
        inMask = false;
      }
      line += " ";
    }
    if (inMask) {
      line += `</span>`;
    }
    lines.push(line);
  }
  mask.innerHTML = lines.join("\n");
}

function queueForegroundMask() {
  if (maskQueued) {
    return;
  }
  maskQueued = true;
  requestAnimationFrame(renderForegroundMask);
}

async function renderSky(now) {
  if (now - lastSkyFrame < 90) {
    requestAnimationFrame(renderSky);
    return;
  }
  lastSkyFrame = now;
  if (skySize.cols <= 0 || skySize.rows <= 0) {
    measureSky();
  }
  try {
    const response = await fetch(`/spectate-frame?width=${skySize.cols}&height=${skySize.rows}`, {
      cache: "no-store"
    });
    if (response.ok) {
      sky.innerHTML = await response.text();
    }
  } catch {
    // Keep the previous frame visible if the server restarts between polls.
  }
  requestAnimationFrame(renderSky);
}

window.addEventListener("resize", () => {
  measureSky();
  lastSkyFrame = 0;
  queueForegroundMask();
});
measureSky();
queueForegroundMask();
requestAnimationFrame(renderSky);

const installOrigin = "https://world.ascii.dev";
const commands = {
  mac: {
    install: `curl -fsSL ${installOrigin}/install.sh | sh`,
    run: `world`
  },
  linux: {
    install: `curl -fsSL ${installOrigin}/install.sh | sh`,
    run: `world`
  },
  windows: {
    install: `powershell -ExecutionPolicy Bypass -c "irm ${installOrigin}/install.ps1 | iex"`,
    run: `world`
  }
};
const install = document.getElementById("install");
const run = document.getElementById("run");
function pick(platform) {
  for (const button of document.querySelectorAll("button[data-platform]")) {
    button.setAttribute("aria-pressed", String(button.dataset.platform === platform));
  }
  install.textContent = commands[platform].install;
  run.textContent = commands[platform].run;
  queueForegroundMask();
}
for (const button of document.querySelectorAll("button[data-platform]")) {
  button.addEventListener("click", () => pick(button.dataset.platform));
}
pick(navigator.platform.toLowerCase().includes("win") ? "windows" : navigator.platform.toLowerCase().includes("mac") ? "mac" : "linux");
</script>
</body>
</html>"##;
    ([("content-type", "text/html; charset=utf-8")], page).into_response()
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

async fn robots_txt() -> Response {
    const ROBOTS: &str = "User-agent: *\nAllow: /\nSitemap: https://world.ascii.dev/sitemap.xml\n";
    ([("content-type", "text/plain; charset=utf-8")], ROBOTS).into_response()
}

async fn sitemap_xml() -> Response {
    const SITEMAP: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://world.ascii.dev/</loc>
    <changefreq>daily</changefreq>
    <priority>1.0</priority>
  </url>
  <url>
    <loc>https://world.ascii.dev/terms</loc>
    <changefreq>monthly</changefreq>
    <priority>0.3</priority>
  </url>
  <url>
    <loc>https://world.ascii.dev/privacy</loc>
    <changefreq>monthly</changefreq>
    <priority>0.3</priority>
  </url>
</urlset>
"#;
    (
        [("content-type", "application/xml; charset=utf-8")],
        SITEMAP,
    )
        .into_response()
}

async fn og_png() -> Response {
    const OG_PNG: &[u8] = include_bytes!("../../assets/og.png");
    (
        [
            ("content-type", "image/png"),
            ("cache-control", "public, max-age=86400"),
        ],
        OG_PNG,
    )
        .into_response()
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

async fn spectate_frame(
    Query(query): Query<SpectatorFrameQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let width = query.width.unwrap_or(120).clamp(40, 240);
    let height = query.height.unwrap_or(40).clamp(18, 80);
    let leaderboard = fetch_lobster_leaderboard(&state.db)
        .await
        .unwrap_or_default();
    let snapshot = {
        let world = state.game.world.lock().await;
        snapshot_world(&world, leaderboard)
    };
    Html(render_spectator_frame_html(&snapshot, width, height))
}

fn render_spectator_frame_html(snapshot: &Snapshot, width: u16, height: u16) -> String {
    let reward_by_player =
        snapshot
            .pickup_rewards
            .iter()
            .fold(HashMap::<&str, u64>::new(), |mut rewards, reward| {
                *rewards.entry(reward.player_id.as_str()).or_default() += reward.lobsters;
                rewards
            });
    let (camera_focus, camera_up) = world_render::orbit_camera_now();
    let players = snapshot
        .players
        .iter()
        .filter(|player| player.planet_id == 0)
        .filter_map(|player| {
            let position = world_render::Vec3::from_array(player.position)?;
            Some(world_render::VisiblePlayer {
                name: player.name.clone(),
                position,
                is_self: false,
                is_fake: player.fake,
                points: player.all_time_tokens,
                lobsters: player.lobsters,
                lobster_yield_per_hour: player.total_tokens as f64
                    / snapshot.economy_rules.lobster_rate_token_unit as f64
                    * 60.0,
                equipped_head: world_render::equipped_head_glyph(&player.equipped_head).to_string(),
                jump_height: player.jump_height,
                jump_leg_pose: player.jump_leg_pose,
                pickup_reward_lobsters: reward_by_player
                    .get(player.id.as_str())
                    .copied()
                    .unwrap_or(0),
                facing: player.facing,
                walking_phase: player.walking_phase,
            })
        })
        .collect();
    let state = world_render::VisibleGameState {
        width,
        height,
        planet_diameter_cells: 90.0,
        camera_focus,
        camera_up,
        tokens_since_joining: 0,
        tokens_all_time: 0,
        lobsters: 0,
        lobster_yield_per_hour: 0.0,
        leaderboard: snapshot
            .leaderboard
            .iter()
            .map(|entry| world_render::LeaderboardEntry {
                username: entry.username.clone(),
                lobsters: entry.lobsters,
                all_time_tokens: entry.all_time_tokens,
                profile_url: entry.profile_url.clone(),
            })
            .collect(),
        placed_pixels: snapshot
            .placed_pixels
            .iter()
            .map(|pixel| world_render::PlacedPixel {
                position: pixel.position,
                color: pixel.color,
            })
            .collect(),
        pickups: snapshot
            .pickups
            .iter()
            .map(|pickup| world_render::PickupSnapshot {
                position: pickup.position,
                emoji: pickup.emoji.clone(),
            })
            .collect(),
        players,
    };
    let frame = world_render::render_game_frame(
        &state,
        world_render::GameRenderOptions {
            show_header: false,
            show_footer: false,
            show_pixel_inventory: false,
            show_lobster_leaderboard: false,
        },
        0,
        [0; world_render::PIXEL_COLOR_COUNT],
    );
    world_render::frame_to_html(&frame)
}

async fn handle_ws(socket: WebSocket, state: AppState, user: GameUser) {
    let player_id = Uuid::new_v4().to_string();
    let session_id = Uuid::new_v4();
    let user_id = user.id;
    let rewards = user.rewards.clone();
    if let Err(err) = close_open_player_sessions_for_user(&state.db, user_id).await {
        error!("failed to close replaced player sessions: {err}");
    }
    if let Err(err) = start_player_session(&state.db, session_id, user_id, state.boot_id).await {
        error!("failed to record player session start: {err}");
    }
    if let Some(save) = state.game.add_player(player_id.clone(), user).await {
        if let Err(err) = persist_economy(&state.db, save).await {
            error!("failed to persist replaced player economy on reconnect: {err}");
        }
    }

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
        if let Err(err) = end_player_session(&state.db, session_id, "welcome_failed").await {
            error!("failed to record player session end: {err}");
        }
        return;
    }

    let mut last_session_seen_write = Instant::now();
    let mut end_reason = "disconnect";
    loop {
        tokio::select! {
            message = receiver.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        touch_player_session_throttled(
                            &state.db,
                            session_id,
                            &mut last_session_seen_write,
                        )
                        .await;
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
                            Ok(ClientMessage::TokenUsage {
                                total_tokens,
                                all_time_tokens,
                            }) => {
                                if let Some(save) = state
                                    .game
                                    .set_token_totals(&player_id, total_tokens, all_time_tokens)
                                    .await
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
                            Ok(ClientMessage::BuyPixel { color }) => {
                                if let Some(save) = state.game.buy_pixel(&player_id, color).await {
                                    if let Err(err) = persist_economy(&state.db, save).await {
                                        error!("failed to persist pixel purchase: {err}");
                                    }
                                }
                            }
                            Ok(ClientMessage::PlacePixel { color }) => {
                                if let Some(save) = state.game.place_pixel(&player_id, color).await {
                                    if let Err(err) = persist_economy(&state.db, save).await {
                                        error!("failed to persist pixel placement: {err}");
                                    }
                                }
                            }
                            Err(err) => error!("bad client message: {err}"),
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        state.game.touch_player(&player_id).await;
                        touch_player_session_throttled(
                            &state.db,
                            session_id,
                            &mut last_session_seen_write,
                        )
                        .await;
                        if sender.send(Message::Pong(bytes)).await.is_err() {
                            end_reason = "send_failed";
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(err)) => {
                        error!("websocket error: {err}");
                        end_reason = "websocket_error";
                        break;
                    }
                }
            }
            snapshot = snapshots.recv() => {
                let Ok(snapshot) = snapshot else {
                    end_reason = "snapshot_channel_closed";
                    break;
                };
                if send_json(&mut sender, &ServerMessage::Snapshot(snapshot)).await.is_err() {
                    end_reason = "send_failed";
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(20)) => {
                if state
                    .game
                    .is_player_stale(&player_id, Duration::from_secs(20))
                    .await
                {
                    end_reason = "stale";
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
    if let Err(err) = end_player_session(&state.db, session_id, end_reason).await {
        error!("failed to record player session end: {err}");
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
            all_time_tokens = GREATEST(all_time_tokens, $3),
            lobster_micros = $4,
            last_lobster_at = now(),
            equipped_head = $5,
            owned_heads = $6,
            owned_pixels = $7,
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(save.user_id)
    .bind(save.total_tokens as i64)
    .bind(save.all_time_tokens as i64)
    .bind(save.lobster_micros as i64)
    .bind(save.equipped_head)
    .bind(Value::Array(
        save.owned_heads
            .into_iter()
            .map(Value::String)
            .collect::<Vec<_>>(),
    ))
    .bind(pixel_inventory_json(save.owned_pixels))
    .execute(db)
    .await?;
    Ok(())
}

async fn close_stale_player_sessions_on_boot(db: &PgPool, boot_id: Uuid) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE player_sessions
        SET ended_at = COALESCE(ended_at, last_seen_at),
            end_reason = COALESCE(end_reason, 'server_restarted')
        WHERE ended_at IS NULL AND boot_id <> $1
        "#,
    )
    .bind(boot_id)
    .execute(db)
    .await?;
    Ok(())
}

async fn close_open_player_sessions_for_user(db: &PgPool, user_id: Uuid) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE player_sessions
        SET ended_at = now(),
            last_seen_at = now(),
            end_reason = COALESCE(end_reason, 'replaced')
        WHERE user_id = $1 AND ended_at IS NULL
        "#,
    )
    .bind(user_id)
    .execute(db)
    .await?;
    Ok(())
}

async fn start_player_session(
    db: &PgPool,
    session_id: Uuid,
    user_id: Uuid,
    boot_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO player_sessions (id, user_id, boot_id)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(session_id)
    .bind(user_id)
    .bind(boot_id)
    .execute(db)
    .await?;
    Ok(())
}

async fn touch_player_session_throttled(db: &PgPool, session_id: Uuid, last_write: &mut Instant) {
    if last_write.elapsed() < Duration::from_secs(15) {
        return;
    }
    *last_write = Instant::now();
    if let Err(err) = touch_player_session(db, session_id).await {
        error!("failed to record player session activity: {err}");
    }
}

async fn touch_player_session(db: &PgPool, session_id: Uuid) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE player_sessions
        SET last_seen_at = now()
        WHERE id = $1 AND ended_at IS NULL
        "#,
    )
    .bind(session_id)
    .execute(db)
    .await?;
    Ok(())
}

async fn end_player_session(db: &PgPool, session_id: Uuid, reason: &str) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE player_sessions
        SET ended_at = COALESCE(ended_at, now()),
            last_seen_at = now(),
            end_reason = COALESCE(end_reason, $2)
        WHERE id = $1
        "#,
    )
    .bind(session_id)
    .bind(reason)
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
            placed_pixels: HashMap::new(),
            pickups: HashMap::new(),
            pickup_rewards: Vec::new(),
            next_pickup_id: 0,
            last_pickup_spawn: Instant::now(),
            next_npc_id: 0,
            last_tick: Instant::now(),
        };
        ensure_minimum_entities(&mut world);
        Arc::new(Self {
            world: Mutex::new(world),
            snapshots,
        })
    }

    async fn add_player(&self, id: String, user: GameUser) -> Option<EconomySave> {
        let mut world = self.world.lock().await;
        let replaced_id = world.players.iter().find_map(|(player_id, player)| {
            if !player.fake && player.user_id == Some(user.id) {
                Some(player_id.clone())
            } else {
                None
            }
        });
        let replaced = replaced_id.and_then(|player_id| world.players.remove(&player_id));

        if replaced.is_none() {
            remove_random_npc(&mut world);
        }

        let mut replaced_save = None;
        let (
            position,
            basis_up,
            total_tokens,
            all_time_tokens,
            lobster_micros,
            equipped_head,
            owned_heads,
            owned_pixels,
            jump_height,
            jump_velocity,
            jump_momentum,
            last_jump_started_at,
            last_jump_momentum,
            jump_leg_pose,
            momentum_jump_chain,
            jump_momentum_multiplier,
            facing,
            walking_phase,
        ) = if let Some(mut player) = replaced {
            accrue_lobsters(&mut player, Instant::now());
            replaced_save = player.user_id.map(|user_id| EconomySave {
                user_id,
                total_tokens: player.total_tokens,
                all_time_tokens: player.all_time_tokens,
                lobster_micros: player.lobster_micros,
                equipped_head: player.equipped_head.clone(),
                owned_heads: player.owned_heads.clone(),
                owned_pixels: player.owned_pixels,
            });
            (
                player.position,
                player.basis_up,
                player.total_tokens,
                player.all_time_tokens,
                player.lobster_micros,
                player.equipped_head,
                player.owned_heads,
                player.owned_pixels,
                player.jump_height,
                player.jump_velocity,
                player.jump_momentum,
                player.last_jump_started_at,
                player.last_jump_momentum,
                player.jump_leg_pose,
                player.momentum_jump_chain,
                player.jump_momentum_multiplier,
                player.facing,
                player.walking_phase,
            )
        } else {
            let position = {
                let mut rng = rand::rng();
                Vec3::random_unit(&mut rng)
            };
            (
                position,
                position.any_tangent(),
                user.economy.total_tokens,
                user.economy.all_time_tokens,
                user.economy.lobster_micros,
                user.economy.equipped_head,
                user.economy.owned_heads,
                user.economy.owned_pixels,
                0.0,
                0.0,
                None,
                None,
                None,
                0,
                0,
                1.0,
                0,
                0,
            )
        };

        world.players.insert(
            id.clone(),
            Player {
                id,
                user_id: Some(user.id),
                last_seen: Instant::now(),
                name: user.username,
                planet_id: 0,
                position,
                basis_up,
                input: InputState::default(),
                fake: false,
                jump_height,
                jump_velocity,
                jump_momentum,
                last_jump_started_at,
                last_jump_momentum,
                jump_leg_pose,
                momentum_jump_chain,
                jump_momentum_multiplier,
                npc_jump_seconds: 0.0,
                total_tokens,
                all_time_tokens,
                lobster_micros,
                last_economy_at: Instant::now(),
                equipped_head,
                owned_heads,
                owned_pixels,
                facing,
                walking_phase,
                npc_movement: None,
            },
        );
        ensure_minimum_entities(&mut world);
        replaced_save
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
                all_time_tokens: player.all_time_tokens,
                lobster_micros: player.lobster_micros,
                equipped_head: player.equipped_head,
                owned_heads: player.owned_heads,
                owned_pixels: player.owned_pixels,
            })
        })
    }

    async fn touch_player(&self, id: &str) {
        let mut world = self.world.lock().await;
        if let Some(player) = world.players.get_mut(id) {
            player.last_seen = Instant::now();
        }
    }

    async fn is_player_stale(&self, id: &str, max_idle: Duration) -> bool {
        let world = self.world.lock().await;
        world
            .players
            .get(id)
            .map(|player| {
                !player.fake && player.last_seen.elapsed().checked_sub(max_idle).is_some()
            })
            .unwrap_or(false)
    }

    async fn set_input(&self, id: &str, input: InputState) {
        let mut world = self.world.lock().await;
        if let Some(player) = world.players.get_mut(id) {
            player.last_seen = Instant::now();
            let jump_started = input.jump && !player.input.jump;
            player.input = input;
            if jump_started && player.jump_height <= JUMP_GROUND_EPSILON {
                start_player_jump(player, input);
            }
        }
    }

    async fn set_token_totals(
        &self,
        id: &str,
        total_tokens: u64,
        all_time_tokens: u64,
    ) -> Option<EconomySave> {
        let mut world = self.world.lock().await;
        let player = world.players.get_mut(id)?;
        player.last_seen = Instant::now();
        accrue_lobsters(player, Instant::now());
        player.total_tokens = player.total_tokens.max(total_tokens);
        player.all_time_tokens = player.all_time_tokens.max(all_time_tokens);
        player.user_id.map(|user_id| EconomySave {
            user_id,
            total_tokens: player.total_tokens,
            all_time_tokens: player.all_time_tokens,
            lobster_micros: player.lobster_micros,
            equipped_head: player.equipped_head.clone(),
            owned_heads: player.owned_heads.clone(),
            owned_pixels: player.owned_pixels,
        })
    }

    async fn buy_head(&self, id: &str, item_id: &str) -> Option<EconomySave> {
        let item = market_item(item_id)?;
        if item.kind != "head" {
            return None;
        }
        let mut world = self.world.lock().await;
        let player = world.players.get_mut(id)?;
        player.last_seen = Instant::now();
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
            all_time_tokens: player.all_time_tokens,
            lobster_micros: player.lobster_micros,
            equipped_head: player.equipped_head.clone(),
            owned_heads: player.owned_heads.clone(),
            owned_pixels: player.owned_pixels,
        })
    }

    async fn equip_head(&self, id: &str, item_id: &str) -> Option<EconomySave> {
        if market_item(item_id)?.kind != "head" {
            return None;
        }
        let mut world = self.world.lock().await;
        let player = world.players.get_mut(id)?;
        player.last_seen = Instant::now();
        accrue_lobsters(player, Instant::now());
        if !player.owned_heads.iter().any(|owned| owned == item_id) {
            return None;
        }
        player.equipped_head = item_id.to_string();
        player.user_id.map(|user_id| EconomySave {
            user_id,
            total_tokens: player.total_tokens,
            all_time_tokens: player.all_time_tokens,
            lobster_micros: player.lobster_micros,
            equipped_head: player.equipped_head.clone(),
            owned_heads: player.owned_heads.clone(),
            owned_pixels: player.owned_pixels,
        })
    }

    async fn buy_pixel(&self, id: &str, color: usize) -> Option<EconomySave> {
        if color >= PIXEL_COLOR_COUNT {
            return None;
        }
        let mut world = self.world.lock().await;
        let player = world.players.get_mut(id)?;
        player.last_seen = Instant::now();
        accrue_lobsters(player, Instant::now());
        let price_micros = PIXEL_PACK_PRICE_LOBSTERS.saturating_mul(LOBSTER_MICROS);
        if player.lobster_micros < price_micros {
            return None;
        }
        player.lobster_micros = player.lobster_micros.saturating_sub(price_micros);
        player.owned_pixels[color] = player.owned_pixels[color].saturating_add(PIXEL_PACK_SIZE);
        player.user_id.map(|user_id| EconomySave {
            user_id,
            total_tokens: player.total_tokens,
            all_time_tokens: player.all_time_tokens,
            lobster_micros: player.lobster_micros,
            equipped_head: player.equipped_head.clone(),
            owned_heads: player.owned_heads.clone(),
            owned_pixels: player.owned_pixels,
        })
    }

    async fn place_pixel(&self, id: &str, color: usize) -> Option<EconomySave> {
        if color >= PIXEL_COLOR_COUNT {
            return None;
        }
        let mut world = self.world.lock().await;
        let player = world.players.get_mut(id)?;
        player.last_seen = Instant::now();
        if player.owned_pixels[color] == 0 {
            return None;
        }
        player.owned_pixels[color] -= 1;
        let position = player.position.normalize();
        let save = player.user_id.map(|user_id| EconomySave {
            user_id,
            total_tokens: player.total_tokens,
            all_time_tokens: player.all_time_tokens,
            lobster_micros: player.lobster_micros,
            equipped_head: player.equipped_head.clone(),
            owned_heads: player.owned_heads.clone(),
            owned_pixels: player.owned_pixels,
        });
        world.placed_pixels.insert(
            placed_pixel_key(position),
            PlacedPixel {
                position: position.to_array(),
                color,
            },
        );
        save
    }
}

async fn run_game_loop(game: GameHandle, db: PgPool) {
    let mut ticker = tokio::time::interval(Duration::from_millis(50));
    let mut leaderboard = Vec::new();
    let mut next_leaderboard_refresh = Instant::now();
    loop {
        ticker.tick().await;
        let now = Instant::now();
        if now >= next_leaderboard_refresh {
            match fetch_lobster_leaderboard(&db).await {
                Ok(next_leaderboard) => leaderboard = next_leaderboard,
                Err(err) => error!("failed to refresh lobster leaderboard: {err}"),
            }
            next_leaderboard_refresh = now + Duration::from_secs(5);
        }
        let saves = {
            let mut world = game.world.lock().await;
            let dt = now.duration_since(world.last_tick).as_secs_f64().min(0.2);
            world.last_tick = now;
            tick_world(&mut world, dt)
        };
        for save in saves {
            if let Err(err) = persist_economy(&db, save).await {
                error!("failed to persist pickup reward: {err}");
            }
        }
        let snapshot = {
            let world = game.world.lock().await;
            snapshot_world(&world, leaderboard.clone())
        };
        let _ = game.snapshots.send(snapshot);
    }
}

async fn fetch_lobster_leaderboard(db: &PgPool) -> anyhow::Result<Vec<LeaderboardEntry>> {
    let rows = sqlx::query_as::<_, (String, i64, i64)>(
        r#"
        SELECT x_username, lobster_micros, all_time_tokens
        FROM game_users
        WHERE x_username <> ''
        ORDER BY lobster_micros DESC, lower(x_username) ASC
        LIMIT 10
        "#,
    )
    .fetch_all(db)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(username, lobster_micros, all_time_tokens)| {
            let username = username.trim().trim_start_matches('@').to_string();
            LeaderboardEntry {
                profile_url: format!("https://x.com/{username}"),
                username,
                lobsters: lobster_balance(lobster_micros.max(0) as u64),
                all_time_tokens: all_time_tokens.max(0) as u64,
            }
        })
        .filter(|entry| !entry.username.is_empty())
        .collect())
}

const MIN_ENTITY_COUNT: usize = 30;
const ANGULAR_SPEED_RADIANS_PER_SECOND: f64 = 0.275;
const NPC_CURVE_AMPLITUDE_RADIANS: f64 = 0.18;
const NPC_PATH_SAMPLES: usize = 32;
const PIXEL_COLOR_COUNT: usize = 5;
const PIXEL_PACK_SIZE: u64 = 1;
const PIXEL_PACK_PRICE_LOBSTERS: u64 = 1_000;
const PICKUP_SPAWN_SECONDS: u64 = 15;
const PICKUP_TTL_SECONDS: u64 = 60;
const PICKUP_REWARD_LOBSTERS: u64 = 100;
const PICKUP_COLLISION_RADIANS: f64 = 0.045;
const PICKUP_REWARD_DISPLAY_MS: u64 = 1_200;
const LOBSTER_MICROS: u64 = 1_000_000;
const LOBSTER_RATE_TOKEN_UNIT: u64 = 6_000_000;
const LOBSTER_ACCRUAL_TOKEN_MS_PER_MICRO: u64 = 360_000;
const JUMP_IMPULSE_CELLS_PER_SECOND: f64 = 7.6;
const JUMP_GRAVITY_CELLS_PER_SECOND2: f64 = 19.0;
const MAX_JUMP_HEIGHT_CELLS: f64 = 2.0;
const JUMP_GROUND_EPSILON: f64 = 0.02;
const THIRD_MOMENTUM_JUMP_MULTIPLIER: f64 = 6.0;
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

const PICKUP_EMOJIS: &[&str] = &[
    "🍎", "🍌", "🍇", "🍓", "🍕", "🥐", "🥕", "🌽", "🍄", "🐟", "🐢", "🐸", "🐧", "🦊", "🐼", "🐙",
];

const MARKET_ITEMS: &[(&str, &str, &str, u64)] = &[
    ("default", "Default", "0", 0),
    ("box", "Package", "📦", 0),
    ("smile", "Smile", "🙂", 250),
    ("cowboy", "Cowboy", "🤠", 1_500),
    ("sunglasses", "Sunglasses", "😎", 7_500),
    ("frog", "Frog", "🐸", 25_000),
    ("lobster", "🦞", "🦞", 100_000),
    ("sun", "Sun", "☀️", 500_000),
];

fn market_items() -> Vec<MarketItemSnapshot> {
    let mut items = MARKET_ITEMS
        .iter()
        .map(|(id, label, head, price_lobsters)| MarketItemSnapshot {
            id: (*id).to_string(),
            label: (*label).to_string(),
            head: (*head).to_string(),
            price_lobsters: *price_lobsters,
            kind: "head".to_string(),
            pixel_color: None,
        })
        .collect::<Vec<_>>();
    for color in 0..PIXEL_COLOR_COUNT {
        items.push(MarketItemSnapshot {
            id: format!("pixel-{color}"),
            label: format!("Pixel pack {}", color + 1),
            head: "█".to_string(),
            price_lobsters: PIXEL_PACK_PRICE_LOBSTERS,
            kind: "pixel".to_string(),
            pixel_color: Some(color),
        });
    }
    items
}

fn market_item(id: &str) -> Option<MarketItemSnapshot> {
    market_items().into_iter().find(|item| item.id == id)
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

fn placed_pixel_key(position: Vec3) -> String {
    let (lat, lon) = position.lat_lon();
    let lat_bucket = ((lat + FRAC_PI_2) * 96.0 / PI).floor() as i32;
    let lon_bucket = ((wrap_pi(lon) + PI) * 192.0 / TAU).floor() as i32;
    format!("{lat_bucket}:{lon_bucket}")
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
            last_seen: Instant::now(),
            name: name.to_string(),
            planet_id: 0,
            position,
            basis_up: position.any_tangent(),
            input: InputState::default(),
            fake: true,
            jump_height: 0.0,
            jump_velocity: 0.0,
            jump_momentum: None,
            last_jump_started_at: None,
            last_jump_momentum: None,
            jump_leg_pose: 0,
            momentum_jump_chain: 0,
            jump_momentum_multiplier: 1.0,
            npc_jump_seconds: random_npc_jump_seconds(rng),
            total_tokens: 0,
            all_time_tokens: 0,
            lobster_micros: 0,
            last_economy_at: Instant::now(),
            equipped_head: "default".to_string(),
            owned_heads: vec!["default".to_string()],
            owned_pixels: [0; PIXEL_COLOR_COUNT],
            facing: 0,
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

fn start_player_jump(player: &mut Player, input: InputState) {
    let now = Instant::now();
    player.jump_velocity = JUMP_IMPULSE_CELLS_PER_SECOND;

    let pose = jump_leg_pose_from_input(input);
    let has_direction_input = input.up || input.down || input.left || input.right;
    let continues_momentum_chain = player.last_jump_momentum.is_some();
    if has_direction_input {
        player.jump_leg_pose = pose;
        player.jump_momentum = player.last_jump_momentum;
        player.momentum_jump_chain = if continues_momentum_chain {
            player.momentum_jump_chain.saturating_add(1).max(1)
        } else {
            1
        };
        player.jump_momentum_multiplier = momentum_jump_multiplier(player.momentum_jump_chain);
    } else if continues_momentum_chain {
        player.jump_momentum = player.last_jump_momentum;
        player.momentum_jump_chain = player.momentum_jump_chain.saturating_add(1).max(1);
        player.jump_momentum_multiplier = momentum_jump_multiplier(player.momentum_jump_chain);
        if player.jump_leg_pose == 0 || player.jump_leg_pose == 1 {
            player.jump_leg_pose = jump_leg_pose_from_momentum(player);
        }
    } else {
        player.jump_momentum = None;
        player.momentum_jump_chain = 0;
        player.jump_momentum_multiplier = 1.0;
        player.jump_leg_pose = if rand::rng().random_range(0..2) == 0 {
            0
        } else {
            1
        };
    }

    player.last_jump_started_at = Some(now);
}

fn momentum_jump_multiplier(chain: u8) -> f64 {
    if chain > 0 && chain % 3 == 0 {
        THIRD_MOMENTUM_JUMP_MULTIPLIER
    } else {
        1.0
    }
}

fn jump_leg_pose_from_input(input: InputState) -> i8 {
    let left_or_top = input.left || input.up;
    let right_or_bottom = input.right || input.down;
    if left_or_top && !right_or_bottom {
        -1
    } else if right_or_bottom && !left_or_top {
        2
    } else if rand::rng().random_range(0..2) == 0 {
        0
    } else {
        1
    }
}

fn jump_leg_pose_from_momentum(player: &Player) -> i8 {
    let Some(momentum) = player.last_jump_momentum else {
        return player.jump_leg_pose;
    };
    let right = screen_right_for_player(player);
    if momentum.dot(right) < 0.0 {
        -1
    } else {
        2
    }
}

fn screen_up_for_player(player: &Player) -> Vec3 {
    let camera_up = player
        .input
        .camera_up
        .and_then(Vec3::from_array)
        .unwrap_or(player.basis_up);
    let screen_up = camera_up
        .add(player.position.scale(-camera_up.dot(player.position)))
        .normalize();
    if screen_up.length() <= 1e-6 {
        player.basis_up
    } else {
        screen_up
    }
}

fn screen_right_for_player(player: &Player) -> Vec3 {
    screen_up_for_player(player)
        .cross(player.position)
        .normalize()
}

fn tick_world(world: &mut GameWorld, dt: f64) -> Vec<EconomySave> {
    let mut saves = Vec::new();
    let economy_now = Instant::now();
    for player in world.players.values_mut().filter(|player| !player.fake) {
        accrue_lobsters(player, economy_now);
        tick_jump(player, dt);
        let screen_up = screen_up_for_player(player);
        let screen_right = screen_right_for_player(player);
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
        let airborne = player.jump_height > JUMP_GROUND_EPSILON || player.jump_velocity > 0.0;
        let movement_direction = if direction.length() > 1e-6 {
            let direction = direction.normalize();
            if airborne {
                player.jump_momentum = Some(direction);
                player.last_jump_momentum = Some(direction);
            }
            Some(direction)
        } else if airborne {
            player.jump_momentum.map(|momentum| {
                momentum
                    .add(player.position.scale(-momentum.dot(player.position)))
                    .normalize()
            })
        } else {
            player.jump_momentum = None;
            None
        };
        if let Some(direction) = movement_direction.filter(|direction| direction.length() > 1e-6) {
            let movement_multiplier = if airborne && player.jump_momentum.is_some() {
                player.jump_momentum_multiplier
            } else {
                1.0
            };
            let angular_distance = ANGULAR_SPEED_RADIANS_PER_SECOND * dt * movement_multiplier;
            let rotation_axis = player.position.cross(direction).normalize();
            let previous_position = player.position;
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
            let screen_right = screen_right_for_player(player);
            update_facing_from_movement(player, previous_position, player.position, screen_right);
            player.walking_phase = player.walking_phase.wrapping_add(1);
        }
    }

    let mut rng = rand::rng();
    for player in world.players.values_mut().filter(|player| player.fake) {
        tick_npc_jump(player, dt, &mut rng);
        tick_jump(player, dt);
        tick_npc(player, dt, &mut rng);
    }
    tick_pickups(world, economy_now, &mut rng, &mut saves);
    saves
}

fn tick_pickups(
    world: &mut GameWorld,
    now: Instant,
    rng: &mut impl Rng,
    saves: &mut Vec<EconomySave>,
) {
    world.pickups.retain(|_, pickup| pickup.expires_at > now);
    world
        .pickup_rewards
        .retain(|reward| reward.expires_at > now);

    if now.duration_since(world.last_pickup_spawn) >= Duration::from_secs(PICKUP_SPAWN_SECONDS) {
        let active_players = world.players.values().filter(|player| !player.fake).count();
        let spawn_count = (active_players as f64).sqrt().ceil() as usize;
        for _ in 0..spawn_count {
            spawn_pickup(world, now, rng);
        }
        world.last_pickup_spawn = now;
    }

    let mut collected = Vec::new();
    for (pickup_id, pickup) in &world.pickups {
        if let Some(player) = world
            .players
            .values_mut()
            .filter(|player| !player.fake)
            .find(|player| player.position.angle_to(pickup.position) <= PICKUP_COLLISION_RADIANS)
        {
            player.lobster_micros = player
                .lobster_micros
                .saturating_add(PICKUP_REWARD_LOBSTERS.saturating_mul(LOBSTER_MICROS));
            if let Some(save) = economy_save(player) {
                saves.push(save);
            }
            world.pickup_rewards.push(PickupReward {
                player_id: player.id.clone(),
                lobsters: PICKUP_REWARD_LOBSTERS,
                expires_at: now + Duration::from_millis(PICKUP_REWARD_DISPLAY_MS),
            });
            collected.push(*pickup_id);
        }
    }
    for pickup_id in collected {
        world.pickups.remove(&pickup_id);
    }
}

fn spawn_pickup(world: &mut GameWorld, now: Instant, rng: &mut impl Rng) {
    let id = world.next_pickup_id;
    world.next_pickup_id = world.next_pickup_id.wrapping_add(1);
    let emoji = PICKUP_EMOJIS[rng.random_range(0..PICKUP_EMOJIS.len())];
    world.pickups.insert(
        id,
        Pickup {
            id,
            position: Vec3::random_unit(rng),
            emoji,
            expires_at: now + Duration::from_secs(PICKUP_TTL_SECONDS),
        },
    );
}

fn economy_save(player: &Player) -> Option<EconomySave> {
    player.user_id.map(|user_id| EconomySave {
        user_id,
        total_tokens: player.total_tokens,
        all_time_tokens: player.all_time_tokens,
        lobster_micros: player.lobster_micros,
        equipped_head: player.equipped_head.clone(),
        owned_heads: player.owned_heads.clone(),
        owned_pixels: player.owned_pixels,
    })
}

fn tick_npc_jump(player: &mut Player, dt: f64, rng: &mut impl Rng) {
    player.npc_jump_seconds -= dt;
    if player.npc_jump_seconds <= 0.0 {
        if player.jump_height <= JUMP_GROUND_EPSILON {
            player.jump_velocity = JUMP_IMPULSE_CELLS_PER_SECOND;
            player.last_jump_started_at = Some(Instant::now());
            player.momentum_jump_chain = 0;
            player.jump_momentum_multiplier = 1.0;
            player.jump_leg_pose = if rng.random_range(0..2) == 0 { 0 } else { 1 };
        }
        player.npc_jump_seconds = random_npc_jump_seconds(rng);
    }
}

fn tick_jump(player: &mut Player, dt: f64) {
    if player.jump_height <= 0.0 && player.jump_velocity <= 0.0 {
        player.jump_height = 0.0;
        player.jump_velocity = 0.0;
        player.jump_momentum = None;
        player.jump_momentum_multiplier = 1.0;
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
        player.jump_momentum = None;
        player.jump_momentum_multiplier = 1.0;
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
    let previous_position = player.position;
    let rotation_axis = player.position.cross(next_position).normalize();
    player.position = next_position.normalize();
    let transported_up = player
        .basis_up
        .rotate_around(rotation_axis, angle)
        .normalize();
    player.basis_up = transported_up
        .add(player.position.scale(-transported_up.dot(player.position)))
        .normalize();
    let local_right = player.basis_up.cross(player.position).normalize();
    update_facing_from_movement(player, previous_position, player.position, local_right);
}

fn update_facing_from_movement(
    player: &mut Player,
    previous_position: Vec3,
    next_position: Vec3,
    right_basis: Vec3,
) {
    let delta = next_position.add(previous_position.scale(-1.0));
    let movement = delta.add(next_position.scale(-delta.dot(next_position)));
    if movement.length() <= 1e-9 {
        return;
    }

    let right_basis = right_basis
        .add(next_position.scale(-right_basis.dot(next_position)))
        .normalize();
    let right_amount = movement.normalize().dot(right_basis);
    if right_amount > 0.15 {
        player.facing = 1;
    } else if right_amount < -0.15 {
        player.facing = -1;
    }
}

fn snapshot_world(world: &GameWorld, leaderboard: Vec<LeaderboardEntry>) -> Snapshot {
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
                all_time_tokens: player.all_time_tokens,
                lobsters: lobster_balance(player.lobster_micros),
                equipped_head: player.equipped_head.clone(),
                owned_heads: player.owned_heads.clone(),
                owned_pixels: player.owned_pixels,
                jump_height: player.jump_height,
                jump_leg_pose: player.jump_leg_pose,
                facing: player.facing,
                walking_phase: player.walking_phase,
            }
        })
        .collect();
    let placed_pixels = world.placed_pixels.values().cloned().collect();
    let pickups = world
        .pickups
        .values()
        .map(|pickup| PickupSnapshot {
            id: pickup.id,
            position: pickup.position.to_array(),
            emoji: pickup.emoji.to_string(),
        })
        .collect();
    let pickup_rewards = world
        .pickup_rewards
        .iter()
        .map(|reward| PickupRewardSnapshot {
            player_id: reward.player_id.clone(),
            lobsters: reward.lobsters,
        })
        .collect();
    Snapshot {
        server_time_ms: 0,
        players,
        leaderboard,
        placed_pixels,
        pickups,
        pickup_rewards,
        economy_rules: EconomyRulesSnapshot {
            lobster_rate_token_unit: LOBSTER_RATE_TOKEN_UNIT,
        },
    }
}

fn wrap_pi(value: f64) -> f64 {
    (value + PI).rem_euclid(TAU) - PI
}

async fn install_sh() -> Response {
    let script = r#"#!/usr/bin/env sh
set -eu

INSTALL_DIR="${GAME_INSTALL_DIR:-}"
DOWNLOAD_BASE="${GAME_DOWNLOAD_BASE:-https://world.ascii.dev/download}"

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

ASSET="world-${OS}-${ARCH}"
URL="${DOWNLOAD_BASE}/${ASSET}"

if [ -z "$INSTALL_DIR" ]; then
  if [ -w /usr/local/bin ]; then
    INSTALL_DIR="/usr/local/bin"
  else
    INSTALL_DIR="$HOME/.local/bin"
  fi
fi

mkdir -p "$INSTALL_DIR"
TMP="$(mktemp)"
curl -fsSL "$URL" -o "$TMP"
chmod +x "$TMP"
mv "$TMP" "$INSTALL_DIR/world"

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) echo "Add $INSTALL_DIR to PATH to run world from any terminal." ;;
esac

echo "Installed world to $INSTALL_DIR/world"
echo "Run: world"
"#;
    ([("content-type", "text/x-shellscript")], script).into_response()
}

async fn install_ps1() -> Response {
    let script = r#"$ErrorActionPreference = "Stop"

$installDir = if ($env:GAME_INSTALL_DIR) { $env:GAME_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps" }
$downloadBase = if ($env:GAME_DOWNLOAD_BASE) { $env:GAME_DOWNLOAD_BASE } else { "https://world.ascii.dev/download" }
$asset = "world-windows-x64.exe"
$url = "$downloadBase/$asset"
$target = Join-Path $installDir "world.exe"
$tmp = New-TemporaryFile

New-Item -ItemType Directory -Force -Path $installDir | Out-Null
Invoke-WebRequest -Uri $url -OutFile $tmp
Move-Item -Force -Path $tmp -Destination $target

Write-Host "Installed world to $target"
Write-Host "Run: world"
"#;
    ([("content-type", "text/plain; charset=utf-8")], script).into_response()
}

async fn download_cli_asset(AxumPath(asset): AxumPath<String>) -> Response {
    const ALLOWED_ASSETS: &[&str] = &[
        "world-linux-x64",
        "world-linux-arm64",
        "world-darwin-x64",
        "world-darwin-arm64",
        "world-windows-x64.exe",
    ];

    if !ALLOWED_ASSETS.contains(&asset.as_str()) {
        return (StatusCode::NOT_FOUND, "asset not found").into_response();
    }

    let asset_dir = env::var("GAME_CLI_ASSET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/opt/ascii-game/releases/latest"));
    let path = asset_dir.join(&asset);
    let Ok(bytes) = tokio::fs::read(path).await else {
        return (StatusCode::NOT_FOUND, "asset not found").into_response();
    };

    let content_type = if asset.ends_with(".exe") {
        "application/vnd.microsoft.portable-executable"
    } else {
        "application/octet-stream"
    };
    ([("content-type", content_type)], bytes).into_response()
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
    fn lobster_income_uses_current_market_rate() {
        assert_eq!(LOBSTER_RATE_TOKEN_UNIT, 6_000_000);
        assert_eq!(LOBSTER_ACCRUAL_TOKEN_MS_PER_MICRO, 360_000);

        let position = Vec3::new(1.0, 0.0, 0.0);
        let now = Instant::now();
        let mut player = Player {
            id: "player".to_string(),
            user_id: Some(Uuid::new_v4()),
            last_seen: now,
            name: "player".to_string(),
            planet_id: 0,
            position,
            basis_up: position.any_tangent(),
            input: InputState::default(),
            fake: false,
            jump_height: 0.0,
            jump_velocity: 0.0,
            jump_momentum: None,
            last_jump_started_at: None,
            last_jump_momentum: None,
            jump_leg_pose: 0,
            momentum_jump_chain: 0,
            jump_momentum_multiplier: 1.0,
            npc_jump_seconds: 0.0,
            total_tokens: 1_000_000,
            all_time_tokens: 1_000_000,
            lobster_micros: 0,
            last_economy_at: now - Duration::from_secs(60 * 60),
            equipped_head: "default".to_string(),
            owned_heads: vec!["default".to_string()],
            owned_pixels: [0; PIXEL_COLOR_COUNT],
            facing: 0,
            walking_phase: 0,
            npc_movement: None,
        };

        accrue_lobsters(&mut player, now);

        assert_eq!(lobster_balance(player.lobster_micros), 10);
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
                    last_seen: Instant::now(),
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
                    jump_momentum: None,
                    last_jump_started_at: None,
                    last_jump_momentum: None,
                    jump_leg_pose: 0,
                    momentum_jump_chain: 0,
                    jump_momentum_multiplier: 1.0,
                    npc_jump_seconds: 0.0,
                    total_tokens: 0,
                    all_time_tokens: 0,
                    lobster_micros: 0,
                    last_economy_at: Instant::now(),
                    equipped_head: "default".to_string(),
                    owned_heads: vec!["default".to_string()],
                    owned_pixels: [0; PIXEL_COLOR_COUNT],
                    facing: 0,
                    walking_phase: 0,
                    npc_movement: None,
                },
            )]),
            next_npc_id: 0,
            last_tick: Instant::now(),
            placed_pixels: HashMap::new(),
            pickups: HashMap::new(),
            pickup_rewards: Vec::new(),
            next_pickup_id: 0,
            last_pickup_spawn: Instant::now(),
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
                    all_time_tokens: 0,
                    lobster_micros: 0,
                    equipped_head: "default".to_string(),
                    owned_heads: vec!["default".to_string()],
                    owned_pixels: [0; PIXEL_COLOR_COUNT],
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

    #[tokio::test]
    async fn reconnect_replaces_existing_player_for_same_user() {
        let game = Game::new();
        let user_id = Uuid::new_v4();
        let user = GameUser {
            id: user_id,
            username: "real".to_string(),
            economy: UserEconomy {
                total_tokens: 0,
                all_time_tokens: 0,
                lobster_micros: 0,
                equipped_head: "default".to_string(),
                owned_heads: vec!["default".to_string()],
                owned_pixels: [0; PIXEL_COLOR_COUNT],
            },
            rewards: Vec::new(),
        };

        game.add_player("old-session".to_string(), user.clone())
            .await;
        {
            let mut world = game.world.lock().await;
            let player = world.players.get_mut("old-session").unwrap();
            player.position = Vec3::new(1.0, 0.0, 0.0);
            player.basis_up = Vec3::new(0.0, 0.0, 1.0);
            player.total_tokens = 42;
        }

        let save = game.add_player("new-session".to_string(), user).await;

        let world = game.world.lock().await;
        assert!(save.is_some());
        assert!(!world.players.contains_key("old-session"));
        assert!(world.players.contains_key("new-session"));
        assert_eq!(
            world
                .players
                .values()
                .filter(|player| !player.fake && player.user_id == Some(user_id))
                .count(),
            1
        );
        let player = world.players.get("new-session").unwrap();
        assert!(player.position.angle_to(Vec3::new(1.0, 0.0, 0.0)) < 1e-9);
        assert_eq!(player.total_tokens, 42);
    }

    #[test]
    fn movement_updates_facing_in_screen_tangent_frame() {
        let position = Vec3::new(1.0, 0.0, 0.0);
        let mut player = Player {
            id: "player".to_string(),
            user_id: None,
            last_seen: Instant::now(),
            name: "player".to_string(),
            planet_id: 0,
            position,
            basis_up: Vec3::new(0.0, 0.0, 1.0),
            input: InputState::default(),
            fake: false,
            jump_height: 0.0,
            jump_velocity: 0.0,
            jump_momentum: None,
            last_jump_started_at: None,
            last_jump_momentum: None,
            jump_leg_pose: 0,
            momentum_jump_chain: 0,
            jump_momentum_multiplier: 1.0,
            npc_jump_seconds: 0.0,
            total_tokens: 0,
            all_time_tokens: 0,
            lobster_micros: 0,
            last_economy_at: Instant::now(),
            equipped_head: "default".to_string(),
            owned_heads: vec!["default".to_string()],
            owned_pixels: [0; PIXEL_COLOR_COUNT],
            facing: 0,
            walking_phase: 0,
            npc_movement: None,
        };

        let right = Vec3::new(0.0, 1.0, 0.0);
        update_facing_from_movement(
            &mut player,
            position,
            Vec3::new(1.0, 0.2, 0.0).normalize(),
            right,
        );
        assert_eq!(player.facing, 1);

        update_facing_from_movement(
            &mut player,
            position,
            Vec3::new(1.0, -0.2, 0.0).normalize(),
            right,
        );
        assert_eq!(player.facing, -1);

        player.position = Vec3::new(-1.0, 0.0, 0.0);
        player.basis_up = Vec3::new(0.0, 0.0, -1.0);
        player.input.camera_up = Some(Vec3::Z.to_array());
        let far_side_position = player.position;
        let far_side_right = screen_right_for_player(&player);
        update_facing_from_movement(
            &mut player,
            far_side_position,
            Vec3::new(-1.0, -0.2, 0.0).normalize(),
            far_side_right,
        );
        assert_eq!(player.facing, 1);
    }

    #[test]
    fn third_chained_momentum_jump_uses_fast_multiplier() {
        let position = Vec3::new(1.0, 0.0, 0.0);
        let mut player = Player {
            id: "player".to_string(),
            user_id: None,
            last_seen: Instant::now(),
            name: "player".to_string(),
            planet_id: 0,
            position,
            basis_up: Vec3::new(0.0, 0.0, 1.0),
            input: InputState::default(),
            fake: false,
            jump_height: 0.0,
            jump_velocity: 0.0,
            jump_momentum: None,
            last_jump_started_at: None,
            last_jump_momentum: None,
            jump_leg_pose: 0,
            momentum_jump_chain: 0,
            jump_momentum_multiplier: 1.0,
            npc_jump_seconds: 0.0,
            total_tokens: 0,
            all_time_tokens: 0,
            lobster_micros: 0,
            last_economy_at: Instant::now(),
            equipped_head: "default".to_string(),
            owned_heads: vec!["default".to_string()],
            owned_pixels: [0; PIXEL_COLOR_COUNT],
            facing: 0,
            walking_phase: 0,
            npc_movement: None,
        };
        let momentum = Vec3::new(0.0, 1.0, 0.0);

        start_player_jump(
            &mut player,
            InputState {
                right: true,
                ..InputState::default()
            },
        );
        player.last_jump_momentum = Some(momentum);
        player.jump_height = 0.0;
        player.jump_velocity = 0.0;

        start_player_jump(&mut player, InputState::default());
        assert_eq!(player.momentum_jump_chain, 2);
        assert_eq!(player.jump_momentum_multiplier, 1.0);
        player.jump_height = 0.0;
        player.jump_velocity = 0.0;

        start_player_jump(&mut player, InputState::default());
        assert_eq!(player.momentum_jump_chain, 3);
        assert_eq!(
            player.jump_momentum_multiplier,
            THIRD_MOMENTUM_JUMP_MULTIPLIER
        );

        player.jump_height = 0.0;
        player.jump_velocity = 0.0;
        player.momentum_jump_chain = 2;
        player.jump_momentum_multiplier = 1.0;
        player.last_jump_momentum = Some(momentum);

        start_player_jump(
            &mut player,
            InputState {
                right: true,
                ..InputState::default()
            },
        );
        assert_eq!(player.momentum_jump_chain, 3);
        assert_eq!(
            player.jump_momentum_multiplier,
            THIRD_MOMENTUM_JUMP_MULTIPLIER
        );

        for expected_chain in 4..=6 {
            player.jump_height = 0.0;
            player.jump_velocity = 0.0;
            player.jump_momentum_multiplier = 1.0;
            player.last_jump_momentum = Some(momentum);
            start_player_jump(&mut player, InputState::default());
            assert_eq!(player.momentum_jump_chain, expected_chain);
        }
        assert_eq!(
            player.jump_momentum_multiplier,
            THIRD_MOMENTUM_JUMP_MULTIPLIER
        );
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
                    last_seen: Instant::now(),
                    name: "npc".to_string(),
                    planet_id: 0,
                    position,
                    basis_up: position.any_tangent(),
                    input: InputState::default(),
                    fake: true,
                    jump_height: 0.0,
                    jump_velocity: 0.0,
                    jump_momentum: None,
                    last_jump_started_at: None,
                    last_jump_momentum: None,
                    jump_leg_pose: 0,
                    momentum_jump_chain: 0,
                    jump_momentum_multiplier: 1.0,
                    npc_jump_seconds: 0.0,
                    total_tokens: 0,
                    all_time_tokens: 0,
                    lobster_micros: 0,
                    last_economy_at: Instant::now(),
                    equipped_head: "default".to_string(),
                    owned_heads: vec!["default".to_string()],
                    owned_pixels: [0; PIXEL_COLOR_COUNT],
                    facing: 0,
                    walking_phase: 0,
                    npc_movement: Some(NpcMovement::Idle {
                        remaining_seconds: 0.0,
                    }),
                },
            )]),
            next_npc_id: 0,
            last_tick: Instant::now(),
            placed_pixels: HashMap::new(),
            pickups: HashMap::new(),
            pickup_rewards: Vec::new(),
            next_pickup_id: 0,
            last_pickup_spawn: Instant::now(),
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
