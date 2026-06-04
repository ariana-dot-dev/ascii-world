use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc as std_mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal,
};
use futures_util::{SinkExt, StreamExt};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc as tokio_mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use walkdir::WalkDir;

mod land_mask;

const ONBOARDING_VERSION: u32 = 3;

#[derive(Parser)]
#[command(name = "Tokenizers", version, about = "Multiplayer token game")]
struct Cli {
    #[arg(long, env = "GAME_BACKEND_URL")]
    backend_url: Option<String>,
    #[arg(long, help = "Skip production self-update check.")]
    no_update: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedState {
    counter: u64,
    #[serde(default)]
    onboarding_completed: bool,
    #[serde(default)]
    onboarding_version: u32,
    #[serde(default)]
    ai_usage_consent: bool,
    #[serde(default)]
    enabled_ai_tools: Vec<String>,
    #[serde(default)]
    ai_usage_baseline: Option<ai_usage::UsageSnapshot>,
    #[serde(default)]
    game_api_token: Option<String>,
    #[serde(default)]
    x_username: Option<String>,
    #[serde(default)]
    x_name: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_dotenv();
    let cli = Cli::parse();

    let backend_url = resolve_backend_url(cli.backend_url.clone())?;
    if should_self_update(&cli) {
        try_self_update().await;
    }

    let state_path = state_path()?;
    let mut state = load_state(&state_path)?;
    let detected_ai_tools = ai_usage::detect_supported_tools();

    if state.ai_usage_consent && state.ai_usage_baseline.is_none() {
        state.ai_usage_baseline = Some(ai_usage::scan_enabled(
            &detected_ai_tools,
            &state.enabled_ai_tools,
        ));
        save_state(&state_path, &state)?;
    }

    run_client(backend_url, state_path, state, detected_ai_tools).await
}

enum ClientPhase {
    Onboarding,
    XLoginPending {
        poll_token: String,
        next_poll: Instant,
    },
    Welcome {
        started: Instant,
    },
    Collapsing {
        started: Instant,
    },
    Playing,
}

#[derive(Debug, Deserialize)]
struct LoginStartResponse {
    login_url: String,
    poll_token: String,
}

#[derive(Debug, Deserialize)]
struct LoginPollResponse {
    status: String,
    token: Option<String>,
    username: Option<String>,
    name: Option<String>,
}

struct TokenScanWorker {
    receiver: std_mpsc::Receiver<ai_usage::UsageSnapshot>,
    stop: Arc<AtomicBool>,
}

impl TokenScanWorker {
    fn start(
        detected: Vec<ai_usage::DetectedTool>,
        enabled: Vec<String>,
        baseline: Option<ai_usage::UsageSnapshot>,
    ) -> Self {
        let (tx, receiver) = std_mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        thread::spawn(move || {
            while !stop_thread.load(Ordering::Relaxed) {
                let snapshot =
                    ai_usage::scan_enabled(&detected, &enabled).saturating_sub(baseline.as_ref());
                if tx.send(snapshot).is_err() {
                    break;
                }
                for _ in 0..20 {
                    if stop_thread.load(Ordering::Relaxed) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            }
        });
        Self { receiver, stop }
    }
}

impl Drop for TokenScanWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

async fn run_client(
    backend_url: String,
    state_path: PathBuf,
    mut state: PersistedState,
    detected: Vec<ai_usage::DetectedTool>,
) -> anyhow::Result<()> {
    let needs_onboarding = !state.onboarding_completed
        || !state.ai_usage_consent
        || state.game_api_token.is_none()
        || state.onboarding_version != ONBOARDING_VERSION;
    let spectator_state = if needs_onboarding {
        start_spectator(&backend_url).await
    } else {
        None
    };
    let mut active_state = spectator_state
        .clone()
        .unwrap_or_else(|| Arc::new(Mutex::new(ClientGameState::default())));
    let mut player_tx: Option<tokio_mpsc::UnboundedSender<ClientMessage>> = None;
    let mut token_worker: Option<TokenScanWorker> = None;
    let mut phase = if needs_onboarding {
        ClientPhase::Onboarding
    } else {
        let token = state
            .game_api_token
            .as_deref()
            .context("missing game API token")?;
        let (joined_state, tx) = start_player_connection(&backend_url, token).await?;
        active_state = joined_state;
        player_tx = Some(tx);
        token_worker = Some(TokenScanWorker::start(
            detected.clone(),
            state.enabled_ai_tools.clone(),
            state.ai_usage_baseline.clone(),
        ));
        ClientPhase::Playing
    };

    let _terminal = TerminalGuard::enter()?;
    let mut input_tracker = InputTracker::default();
    let mut camera = CameraState::default();
    let mut renderer = SmartRenderer::default();
    let mut last_sent = InputState::default();
    let mut last_input_send = Instant::now() - Duration::from_millis(50);
    let mut last_frame = Instant::now() - Duration::from_millis(16);
    let mut token_delta = ai_usage::UsageSnapshot::default();
    let onboarding_panel = UiPanel::onboarding(&detected);

    loop {
        while event::poll(Duration::from_millis(0))? {
            match event::read()? {
                Event::Key(key)
                    if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
                {
                    match (&phase, key.code) {
                        (ClientPhase::Onboarding, KeyCode::Enter) if !detected.is_empty() => {
                            state.ai_usage_consent = true;
                            state.enabled_ai_tools =
                                detected.iter().map(|tool| tool.id.to_string()).collect();
                            state.ai_usage_baseline = Some(
                                scan_usage_blocking(
                                    detected.clone(),
                                    state.enabled_ai_tools.clone(),
                                )
                                .await?,
                            );
                            save_state(&state_path, &state)?;
                            let start = start_x_login(&backend_url).await?;
                            let _ = webbrowser::open(&start.login_url);
                            phase = ClientPhase::XLoginPending {
                                poll_token: start.poll_token,
                                next_poll: Instant::now(),
                            };
                        }
                        (ClientPhase::Onboarding, KeyCode::Esc) => {
                            state.onboarding_completed = false;
                            state.onboarding_version = 0;
                            state.ai_usage_consent = false;
                            state.enabled_ai_tools.clear();
                            state.ai_usage_baseline = None;
                            state.game_api_token = None;
                            state.x_username = None;
                            state.x_name = None;
                            save_state(&state_path, &state)?;
                            return Ok(());
                        }
                        (_, KeyCode::Esc) => return Ok(()),
                        (ClientPhase::Playing, KeyCode::Up) => {
                            input_tracker.up = Some(Instant::now())
                        }
                        (ClientPhase::Playing, KeyCode::Down) => {
                            input_tracker.down = Some(Instant::now())
                        }
                        (ClientPhase::Playing, KeyCode::Left) => {
                            input_tracker.left = Some(Instant::now())
                        }
                        (ClientPhase::Playing, KeyCode::Right) => {
                            input_tracker.right = Some(Instant::now())
                        }
                        _ => {}
                    }
                }
                Event::Key(key) if matches!(key.kind, KeyEventKind::Release) => {
                    if matches!(phase, ClientPhase::Playing) {
                        match key.code {
                            KeyCode::Up => input_tracker.up = None,
                            KeyCode::Down => input_tracker.down = None,
                            KeyCode::Left => input_tracker.left = None,
                            KeyCode::Right => input_tracker.right = None,
                            _ => {}
                        }
                    }
                }
                Event::Resize(_, _) => {
                    last_frame = Instant::now() - Duration::from_millis(16);
                }
                _ => {}
            }
        }

        if matches!(phase, ClientPhase::Playing) {
            let input = input_tracker.current();
            let input_active = input.up || input.down || input.left || input.right;
            if input != last_sent
                || (input_active && last_input_send.elapsed() >= Duration::from_millis(50))
            {
                if let Some(tx) = &player_tx {
                    let _ = tx.send(ClientMessage::Input {
                        up: input.up,
                        down: input.down,
                        left: input.left,
                        right: input.right,
                        camera_up: camera.up.to_array(),
                    });
                }
                last_sent = input;
                last_input_send = Instant::now();
            }
        }

        if let ClientPhase::XLoginPending {
            ref poll_token,
            ref mut next_poll,
        } = phase
        {
            if Instant::now() >= *next_poll {
                let poll = poll_x_login(&backend_url, poll_token).await?;
                *next_poll = Instant::now() + Duration::from_secs(2);
                if poll.status == "expired" {
                    state.game_api_token = None;
                    save_state(&state_path, &state)?;
                    phase = ClientPhase::Onboarding;
                } else if poll.status == "complete" {
                    state.game_api_token = poll.token;
                    state.x_username = poll.username;
                    state.x_name = poll.name;
                    state.onboarding_completed = true;
                    state.onboarding_version = ONBOARDING_VERSION;
                    save_state(&state_path, &state)?;
                    phase = ClientPhase::Welcome {
                        started: Instant::now(),
                    };
                }
            }
        }

        if let ClientPhase::Welcome { started } = phase {
            if started.elapsed() >= Duration::from_millis(900) {
                phase = ClientPhase::Collapsing {
                    started: Instant::now(),
                };
            }
        }

        if let ClientPhase::Collapsing { started } = phase {
            if started.elapsed() >= Duration::from_millis(420) {
                let token = state
                    .game_api_token
                    .as_deref()
                    .context("missing game API token")?;
                let (joined_state, tx) = start_player_connection(&backend_url, token).await?;
                active_state = joined_state;
                player_tx = Some(tx);
                token_worker = Some(TokenScanWorker::start(
                    detected.clone(),
                    state.enabled_ai_tools.clone(),
                    state.ai_usage_baseline.clone(),
                ));
                phase = ClientPhase::Playing;
                renderer.reset();
            }
        }

        if last_frame.elapsed() >= Duration::from_micros(16_667) {
            if let Some(worker) = &token_worker {
                while let Ok(snapshot) = worker.receiver.try_recv() {
                    token_delta = snapshot;
                }
            }
            let (cols, rows) = terminal::size()?;
            let live_state = active_state
                .lock()
                .map(|state| state.clone())
                .unwrap_or_default();
            let visible = VisibleGameState::from_client_state(&live_state, &mut camera, cols, rows);
            let transient_panel = match phase {
                ClientPhase::XLoginPending { .. } => Some(UiPanel::x_login_pending()),
                ClientPhase::Welcome { .. } => {
                    let name = state
                        .x_username
                        .as_deref()
                        .or(state.x_name.as_deref())
                        .unwrap_or("player");
                    Some(UiPanel::welcome(name))
                }
                _ => None,
            };
            let panel_progress = match phase {
                ClientPhase::Onboarding => Some((&onboarding_panel, 0.0)),
                ClientPhase::XLoginPending { .. } | ClientPhase::Welcome { .. } => {
                    transient_panel.as_ref().map(|panel| (panel, 0.0))
                }
                ClientPhase::Collapsing { started } => {
                    let t = (started.elapsed().as_secs_f64() / 0.42).clamp(0.0, 1.0);
                    Some((&onboarding_panel, t))
                }
                ClientPhase::Playing => None,
            };
            let mut visible = visible;
            visible.token_delta = token_delta.total_tokens();
            let actions = renderer.render_app(&visible, panel_progress);
            apply_ansi_actions(&actions)?;
            last_frame = Instant::now();
        }

        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn scan_usage_blocking(
    detected: Vec<ai_usage::DetectedTool>,
    enabled: Vec<String>,
) -> anyhow::Result<ai_usage::UsageSnapshot> {
    tokio::task::spawn_blocking(move || ai_usage::scan_enabled(&detected, &enabled))
        .await
        .context("token usage scan task failed")
}

async fn start_spectator(backend_url: &str) -> Option<Arc<Mutex<ClientGameState>>> {
    let ws_url = endpoint_url(backend_url, "spectate", true).ok()?;
    let (socket, _) = connect_async(ws_url).await.ok()?;
    let (ws_tx, mut ws_rx) = socket.split();
    let shared = Arc::new(Mutex::new(ClientGameState::default()));
    let reader_state = shared.clone();
    tokio::spawn(async move {
        let _keepalive = ws_tx;
        while let Some(message) = ws_rx.next().await {
            let Ok(Message::Text(text)) = message else {
                continue;
            };
            if let Ok(ServerMessage::Snapshot(snapshot)) =
                serde_json::from_str::<ServerMessage>(&text)
            {
                if let Ok(mut state) = reader_state.lock() {
                    state.snapshot = Some(snapshot);
                    state.self_id = None;
                }
            }
        }
    });
    Some(shared)
}

async fn start_player_connection(
    backend_url: &str,
    api_token: &str,
) -> anyhow::Result<(
    Arc<Mutex<ClientGameState>>,
    tokio_mpsc::UnboundedSender<ClientMessage>,
)> {
    let mut ws_url = url::Url::parse(&endpoint_url(backend_url, "ws", true)?)?;
    ws_url.query_pairs_mut().append_pair("token", api_token);
    let (socket, _) = connect_async(ws_url.to_string())
        .await
        .context("failed to connect websocket")?;
    let (mut ws_tx, mut ws_rx) = socket.split();
    let shared = Arc::new(Mutex::new(ClientGameState::default()));
    let reader_state = shared.clone();
    tokio::spawn(async move {
        while let Some(message) = ws_rx.next().await {
            let Ok(Message::Text(text)) = message else {
                continue;
            };
            match serde_json::from_str::<ServerMessage>(&text) {
                Ok(ServerMessage::Welcome { self_id }) => {
                    if let Ok(mut state) = reader_state.lock() {
                        state.self_id = Some(self_id);
                    }
                }
                Ok(ServerMessage::Snapshot(snapshot)) => {
                    if let Ok(mut state) = reader_state.lock() {
                        state.snapshot = Some(snapshot);
                    }
                }
                Err(_) => {}
            }
        }
    });

    let (tx, mut rx) = tokio_mpsc::unbounded_channel::<ClientMessage>();
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            let Ok(text) = serde_json::to_string(&message) else {
                continue;
            };
            if ws_tx.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    Ok((shared, tx))
}

async fn start_x_login(backend_url: &str) -> anyhow::Result<LoginStartResponse> {
    let url = endpoint_url(backend_url, "auth/x/start", false)?;
    let response = reqwest::Client::new().post(url).send().await?;
    if !response.status().is_success() {
        anyhow::bail!("failed to start X login: {}", response.text().await?);
    }
    Ok(response.json().await?)
}

async fn poll_x_login(backend_url: &str, poll_token: &str) -> anyhow::Result<LoginPollResponse> {
    let url = endpoint_url(backend_url, &format!("auth/x/poll/{poll_token}"), false)?;
    let response = reqwest::Client::new().get(url).send().await?;
    if !response.status().is_success() {
        anyhow::bail!("failed to poll X login: {}", response.text().await?);
    }
    Ok(response.json().await?)
}

fn load_dotenv() {
    dotenvy::dotenv().ok();
    if env::var("GAME_BACKEND_URL").is_err() {
        dotenvy::from_path("cli/.env").ok();
    }
    if env::var("GAME_BACKEND_URL").is_err() {
        dotenvy::from_path(".env.production").ok();
    }
    if env::var("GAME_BACKEND_URL").is_err() {
        dotenvy::from_path("cli/.env.production").ok();
    }
}

fn resolve_backend_url(arg: Option<String>) -> anyhow::Result<String> {
    if let Some(url) = arg {
        return Ok(trim_url(url));
    }
    if let Ok(url) = env::var("GAME_BACKEND_URL") {
        return Ok(trim_url(url));
    }
    if let Some(url) = option_env!("GAME_BACKEND_URL") {
        if !url.trim().is_empty() {
            return Ok(trim_url(url.to_string()));
        }
    }
    Err(anyhow!(
        "GAME_BACKEND_URL is required. Put it in cli/.env for dev or cli/.env.production for release builds."
    ))
}

fn trim_url(url: String) -> String {
    url.trim().trim_end_matches('/').to_string()
}

fn state_path() -> anyhow::Result<PathBuf> {
    let base = dirs::data_local_dir()
        .or_else(dirs::data_dir)
        .context("could not determine local data directory")?;
    let dir = base.join("ascii-game");
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    Ok(dir.join("state.json"))
}

fn load_state(path: &PathBuf) -> anyhow::Result<PersistedState> {
    if !path.exists() {
        return Ok(PersistedState::default());
    }
    let data =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&data).with_context(|| format!("failed to parse {}", path.display()))
}

fn save_state(path: &PathBuf, state: &PersistedState) -> anyhow::Result<()> {
    let temp = path.with_extension("json.tmp");
    let data = serde_json::to_vec_pretty(state)?;
    fs::write(&temp, data).with_context(|| format!("failed to write {}", temp.display()))?;
    fs::rename(&temp, path).with_context(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

fn endpoint_url(base: &str, path: &str, websocket: bool) -> anyhow::Result<String> {
    let mut url = url::Url::parse(base)?;
    url.set_path(path);
    if websocket {
        let scheme = match url.scheme() {
            "https" => "wss",
            "http" => "ws",
            other => other,
        }
        .to_string();
        url.set_scheme(&scheme)
            .map_err(|_| anyhow!("unsupported URL scheme"))?;
    }
    Ok(url.to_string())
}

fn should_self_update(cli: &Cli) -> bool {
    !cli.no_update && runtime_environment() == "production"
}

fn runtime_environment() -> String {
    env::var("GAME_ENV")
        .ok()
        .or_else(|| option_env!("GAME_ENV").map(str::to_string))
        .unwrap_or_else(|| "development".to_string())
}

async fn try_self_update() {
    let Some(repo) = option_env!("GAME_CLI_REPO") else {
        return;
    };
    if repo.trim().is_empty() || repo == "REPLACE_WITH_GITHUB_REPO" {
        return;
    }

    let Some(asset) = current_asset_name() else {
        return;
    };
    let url = format!("https://github.com/{repo}/releases/latest/download/{asset}");
    let Ok(response) = reqwest::get(url).await else {
        return;
    };
    if !response.status().is_success() {
        return;
    }
    let Ok(bytes) = response.bytes().await else {
        return;
    };
    let Ok(current_exe) = env::current_exe() else {
        return;
    };
    let temp = current_exe.with_extension("new");
    if fs::write(&temp, bytes).is_err() {
        return;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&temp, fs::Permissions::from_mode(0o755));
    }
    let _ = fs::rename(temp, current_exe);
}

fn current_asset_name() -> Option<&'static str> {
    match (env::consts::OS, env::consts::ARCH) {
        ("linux", "x86_64") => Some("game-linux-x64"),
        ("linux", "aarch64") => Some("game-linux-arm64"),
        ("macos", "x86_64") => Some("game-darwin-x64"),
        ("macos", "aarch64") => Some("game-darwin-arm64"),
        ("windows", "x86_64") => Some("game-windows-x64.exe"),
        _ => None,
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Welcome { self_id: String },
    Snapshot(Snapshot),
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Input {
        up: bool,
        down: bool,
        left: bool,
        right: bool,
        camera_up: [f64; 3],
    },
}

#[derive(Debug, Clone, Default, Deserialize)]
struct Snapshot {
    players: Vec<PlayerSnapshot>,
}

#[derive(Debug, Clone, Deserialize)]
struct PlayerSnapshot {
    id: String,
    #[serde(default)]
    name: String,
    planet_id: u32,
    lat: f64,
    lon: f64,
    #[serde(default)]
    position: Option<[f64; 3]>,
    #[serde(default)]
    basis_up: Option<[f64; 3]>,
    fake: bool,
    walking_phase: u64,
}

impl PlayerSnapshot {
    fn position_vec(&self) -> Vec3 {
        self.position
            .and_then(Vec3::from_array)
            .unwrap_or_else(|| Vec3::from_lat_lon(self.lat, self.lon))
    }

    fn basis_up_vec(&self) -> Vec3 {
        let position = self.position_vec();
        self.basis_up
            .and_then(Vec3::from_array)
            .map(|basis_up| {
                basis_up
                    .add(position.scale(-basis_up.dot(position)))
                    .normalize()
            })
            .filter(|basis_up| basis_up.length() > 1e-6)
            .unwrap_or_else(|| position.any_tangent())
    }
}

#[derive(Debug, Clone, Default)]
struct ClientGameState {
    self_id: Option<String>,
    snapshot: Option<Snapshot>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct InputState {
    up: bool,
    down: bool,
    left: bool,
    right: bool,
}

#[derive(Debug, Default)]
struct InputTracker {
    up: Option<Instant>,
    down: Option<Instant>,
    left: Option<Instant>,
    right: Option<Instant>,
}

impl InputTracker {
    fn current(&mut self) -> InputState {
        let now = Instant::now();
        let expiry = Duration::from_millis(140);
        let active = |last: &mut Option<Instant>| {
            if last
                .map(|instant| now.duration_since(instant) <= expiry)
                .unwrap_or(false)
            {
                true
            } else {
                *last = None;
                false
            }
        };
        InputState {
            up: active(&mut self.up),
            down: active(&mut self.down),
            left: active(&mut self.left),
            right: active(&mut self.right),
        }
    }
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

    fn from_lat_lon(lat: f64, lon: f64) -> Self {
        let cos_lat = lat.cos();
        Self::new(cos_lat * lon.cos(), cos_lat * lon.sin(), lat.sin())
    }

    fn dot(self, other: Self) -> f64 {
        self.x * other.x + self.y * other.y + self.z * other.z
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

    fn rotate_around(self, axis: Self, angle: f64) -> Self {
        let axis = axis.normalize();
        self.scale(angle.cos())
            .add(axis.cross(self).scale(angle.sin()))
            .add(axis.scale(axis.dot(self) * (1.0 - angle.cos())))
    }

    fn to_array(self) -> [f64; 3] {
        [self.x, self.y, self.z]
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
}

#[derive(Debug, Clone, Copy)]
struct CameraState {
    focus: Vec3,
    up: Vec3,
}

impl Default for CameraState {
    fn default() -> Self {
        Self {
            focus: Vec3::X,
            up: Vec3::Z,
        }
    }
}

#[derive(Debug, Clone)]
struct VisibleGameState {
    width: u16,
    height: u16,
    planet_diameter_cells: f64,
    camera_focus: Vec3,
    camera_up: Vec3,
    token_delta: u64,
    players: Vec<VisiblePlayer>,
}

#[derive(Debug, Clone)]
struct VisiblePlayer {
    name: String,
    position: Vec3,
    is_self: bool,
    is_fake: bool,
    walking_phase: u64,
}

impl VisibleGameState {
    fn from_client_state(
        state: &ClientGameState,
        camera: &mut CameraState,
        width: u16,
        height: u16,
    ) -> Self {
        let snapshot = state.snapshot.clone().unwrap_or_default();
        let self_position = state
            .self_id
            .as_ref()
            .and_then(|self_id| {
                snapshot
                    .players
                    .iter()
                    .find(|player| &player.id == self_id)
                    .map(PlayerSnapshot::position_vec)
            })
            .unwrap_or(camera.focus);
        let self_up = state
            .self_id
            .as_ref()
            .and_then(|self_id| {
                snapshot
                    .players
                    .iter()
                    .find(|player| &player.id == self_id)
                    .map(PlayerSnapshot::basis_up_vec)
            })
            .unwrap_or(camera.up);

        let angle_from_focus = camera.focus.dot(self_position).clamp(-1.0, 1.0).acos();
        let follow_radius = 0.38;
        if angle_from_focus > follow_radius {
            let step = angle_from_focus - follow_radius;
            let tangent_to_self = self_position
                .add(camera.focus.scale(-self_position.dot(camera.focus)))
                .normalize();
            let rotation_axis = camera.focus.cross(tangent_to_self).normalize();
            camera.focus = camera.focus.rotate_around(rotation_axis, step).normalize();
            let transported_up = camera.up.rotate_around(rotation_axis, step).normalize();
            camera.up = transported_up
                .add(camera.focus.scale(-transported_up.dot(camera.focus)))
                .normalize();
        }
        if angle_from_focus <= follow_radius || camera.up.length() <= 1e-6 {
            camera.up = self_up
                .add(camera.focus.scale(-self_up.dot(camera.focus)))
                .normalize();
        }

        let players = snapshot
            .players
            .iter()
            .filter(|player| player.planet_id == 0)
            .map(|player| VisiblePlayer {
                name: player.name.clone(),
                position: player.position_vec(),
                is_self: state
                    .self_id
                    .as_ref()
                    .map(|self_id| self_id == &player.id)
                    .unwrap_or(false),
                is_fake: player.fake,
                walking_phase: player.walking_phase,
            })
            .collect();

        Self {
            width,
            height,
            planet_diameter_cells: 67.5,
            camera_focus: camera.focus,
            camera_up: camera.up,
            token_delta: 0,
            players,
        }
    }
}

#[derive(Debug, Clone)]
enum AnsiAction {
    Clear,
    HideCursor,
    ShowCursor,
    Move { x: u16, y: u16 },
    Fg(Color),
    Reset,
    Text(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Color(u8, u8, u8);

const PLANET_OUTLINE: Color = Color(95, 165, 95);
const PLANET_LAND: Color = Color(80, 145, 80);
const PLANET_WATER: Color = Color(45, 75, 110);
const PLAYER_SELF: Color = Color(170, 235, 170);
const PLAYER_OTHER: Color = Color(255, 190, 125);
const HUD: Color = Color(120, 120, 120);
const FG: Color = Color(245, 245, 245);
const FG_DIM: Color = Color(190, 190, 190);
const FG_V_DIM: Color = Color(120, 120, 120);
const ACCENT_1: Color = Color(170, 235, 170);
const ACCENT_1_DIM: Color = Color(95, 165, 95);
const ACCENT_2: Color = Color(255, 190, 125);
const ACCENT_2_DIM: Color = Color(180, 125, 70);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cell {
    ch: char,
    fg: Option<Color>,
}

impl Default for Cell {
    fn default() -> Self {
        Self { ch: ' ', fg: None }
    }
}

#[derive(Debug, Clone)]
struct FrameBuffer {
    width: u16,
    height: u16,
    cells: Vec<Cell>,
}

impl FrameBuffer {
    fn new(width: u16, height: u16) -> Self {
        Self {
            width,
            height,
            cells: vec![Cell::default(); width as usize * height as usize],
        }
    }

    fn index(&self, x: u16, y: u16) -> usize {
        y as usize * self.width as usize + x as usize
    }

    fn get(&self, x: u16, y: u16) -> Cell {
        self.cells[self.index(x, y)]
    }

    fn put(&mut self, x: i32, y: i32, ch: char, fg: Color) {
        if x < 0 || y < 0 || x >= self.width as i32 || y >= self.height as i32 {
            return;
        }
        let index = self.index(x as u16, y as u16);
        self.cells[index] = Cell { ch, fg: Some(fg) };
    }

    fn put_cell(&mut self, x: i32, y: i32, cell: Cell) {
        if x < 0 || y < 0 || x >= self.width as i32 || y >= self.height as i32 {
            return;
        }
        let index = self.index(x as u16, y as u16);
        self.cells[index] = cell;
    }

    fn text(&mut self, x: i32, y: i32, text: &str, fg: Color) {
        for (offset, ch) in text.chars().enumerate() {
            self.put(x + offset as i32, y, ch, fg);
        }
    }

    fn clear_rect(&mut self, x: i32, y: i32, width: u16, height: u16) {
        for yy in 0..height {
            for xx in 0..width {
                self.put_cell(x + xx as i32, y + yy as i32, Cell::default());
            }
        }
    }

    fn blit(&mut self, src: &FrameBuffer, dst_x: i32, dst_y: i32) {
        for y in 0..src.height {
            for x in 0..src.width {
                self.put_cell(dst_x + x as i32, dst_y + y as i32, src.get(x, y));
            }
        }
    }

    fn box_border(&mut self, x: i32, y: i32, width: u16, height: u16, fg: Color) {
        if width < 2 || height < 2 {
            return;
        }
        let right = x + width as i32 - 1;
        let bottom = y + height as i32 - 1;
        self.put(x, y, '+', fg);
        self.put(right, y, '+', fg);
        self.put(x, bottom, '+', fg);
        self.put(right, bottom, '+', fg);
        for xx in x + 1..right {
            self.put(xx, y, '-', fg);
            self.put(xx, bottom, '-', fg);
        }
        for yy in y + 1..bottom {
            self.put(x, yy, '|', fg);
            self.put(right, yy, '|', fg);
        }
    }
}

#[derive(Debug, Default)]
struct SmartRenderer {
    previous: Option<FrameBuffer>,
}

impl SmartRenderer {
    fn render_app(
        &mut self,
        state: &VisibleGameState,
        panel: Option<(&UiPanel, f64)>,
    ) -> Vec<AnsiAction> {
        let current = render_app_frame(state, panel);
        let actions = diff_frames(self.previous.as_ref(), &current);
        self.previous = Some(current);
        actions
    }

    fn reset(&mut self) {
        self.previous = None;
    }
}

#[derive(Debug, Clone)]
struct UiPanel {
    title: String,
    blocks: Vec<UiBlock>,
    prompt: Option<String>,
}

#[derive(Debug, Clone)]
enum UiBlock {
    Paragraph {
        lines: Vec<String>,
        color: Color,
    },
    Table {
        columns: Vec<String>,
        rows: Vec<UiRow>,
    },
}

#[derive(Debug, Clone)]
struct UiRow {
    cells: Vec<String>,
    color: Color,
}

impl UiPanel {
    fn onboarding(detected: &[ai_usage::DetectedTool]) -> Self {
        let mut blocks = vec![UiBlock::Paragraph {
            color: FG,
            lines: vec![
                "By logging in with X, you accept local token tracking for gameplay.".to_string(),
                "We start counting now, then use new token activity to trigger game events and award points."
                    .to_string(),
                "The multiplayer world is hosted, but your local usage data is not uploaded or stored in the cloud."
                    .to_string(),
            ],
        }];

        if detected.is_empty() {
            blocks.push(UiBlock::Paragraph {
                color: ACCENT_2,
                lines: vec!["No supported local token source.".to_string()],
            });
            return Self {
                title: "Welcome to Tokenizers".to_string(),
                blocks,
                prompt: Some("No supported token source was found. Press Escape.".to_string()),
            };
        }

        blocks.push(UiBlock::Table {
            columns: vec![
                "AI tool".to_string(),
                "records".to_string(),
                "what Tokenizers can read".to_string(),
            ],
            rows: detected
                .iter()
                .map(|tool| UiRow {
                    cells: vec![
                        tool.display_name.to_string(),
                        tool.sources.len().to_string(),
                        short_method(tool.collection_method).to_string(),
                    ],
                    color: if tool.collection_method.contains("SQLite") {
                        ACCENT_2_DIM
                    } else {
                        FG_DIM
                    },
                })
                .collect(),
        });

        Self {
            title: "Welcome to Tokenizers".to_string(),
            blocks,
            prompt: Some(">> Press Enter to log in with X <<".to_string()),
        }
    }

    fn x_login_pending() -> Self {
        Self {
            title: "Connect your player profile".to_string(),
            blocks: vec![UiBlock::Paragraph {
                color: FG,
                lines: vec![
                    "A browser window opened for X login.".to_string(),
                    "Complete the login there, then return to this terminal.".to_string(),
                    "The planet stays live while Tokenizers waits.".to_string(),
                ],
            }],
            prompt: Some("Finish login in your browser, or press Escape to quit.".to_string()),
        }
    }

    fn welcome(name: &str) -> Self {
        Self {
            title: format!("Welcome, {name}"),
            blocks: vec![UiBlock::Paragraph {
                color: ACCENT_1,
                lines: vec![
                    "Your player profile is connected.".to_string(),
                    "Tokenizers is joining the multiplayer world now.".to_string(),
                ],
            }],
            prompt: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Rect {
    x: u16,
    y: u16,
    width: u16,
    height: u16,
}

#[derive(Debug, Clone, Copy)]
struct AppLayout {
    game: Rect,
    panel: Option<Rect>,
}

fn app_layout(width: u16, height: u16, panel_progress: Option<f64>) -> AppLayout {
    let Some(progress) = panel_progress else {
        return AppLayout {
            game: Rect {
                x: 0,
                y: 0,
                width: width.max(1),
                height: height.max(1),
            },
            panel: None,
        };
    };
    let progress = progress.clamp(0.0, 1.0);
    if progress >= 1.0 {
        return AppLayout {
            game: Rect {
                x: 0,
                y: 0,
                width: width.max(1),
                height: height.max(1),
            },
            panel: None,
        };
    }

    if width >= 86 {
        let full_panel_width = if width >= 200 {
            88
        } else {
            width.saturating_mul(52).saturating_div(100).clamp(48, 78)
        }
        .min(width.saturating_sub(24).max(1));
        let panel_width = ((full_panel_width as f64) * (1.0 - progress)).round() as u16;
        let panel_width = panel_width.max(1);
        let gutter = if progress >= 0.98 {
            0
        } else {
            1.min(width.saturating_sub(panel_width))
        };
        let game_x = panel_width.saturating_add(gutter);
        AppLayout {
            game: Rect {
                x: game_x,
                y: 0,
                width: width.saturating_sub(game_x).max(1),
                height: height.max(1),
            },
            panel: Some(Rect {
                x: 1,
                y: 1,
                width: panel_width.saturating_sub(2).max(1),
                height: height.saturating_sub(2).max(1),
            }),
        }
    } else {
        AppLayout {
            game: Rect {
                x: 0,
                y: 0,
                width: width.max(1),
                height: height.max(1),
            },
            panel: Some(Rect {
                x: 1.min(width.saturating_sub(1)),
                y: 1.min(height.saturating_sub(1)),
                width: width.saturating_sub(2).max(1),
                height: height.saturating_sub(2).max(1),
            }),
        }
    }
}

fn render_app_frame(state: &VisibleGameState, panel: Option<(&UiPanel, f64)>) -> FrameBuffer {
    let width = state.width.max(1);
    let height = state.height.max(1);
    let layout = app_layout(width, height, panel.map(|(_, progress)| progress));
    let game_state = VisibleGameState {
        width: layout.game.width.max(1),
        height: layout.game.height.max(1),
        ..state.clone()
    };
    let game = render_game_frame(&game_state);
    let mut frame = FrameBuffer::new(width, height);
    frame.blit(&game, layout.game.x as i32, layout.game.y as i32);
    if let (Some(rect), Some((panel, _))) = (layout.panel, panel) {
        render_panel(&mut frame, rect, panel);
    }
    frame
}

fn render_panel(frame: &mut FrameBuffer, rect: Rect, panel: &UiPanel) {
    let x = rect.x as i32;
    let y = rect.y as i32;
    let width = rect.width.max(1);
    let height = rect.height.max(1);
    frame.clear_rect(x, y, width, height);
    frame.box_border(x, y, width, height, ACCENT_1_DIM);
    if width < 8 || height < 6 {
        return;
    }
    let inner_x = x + 2;
    let mut line = y + 2;
    let inner_width = width.saturating_sub(4) as usize;
    draw_clipped_text(frame, inner_x, line, &panel.title, inner_width, ACCENT_1);
    line += 2;
    let max_content_y = y + height as i32 - 4;
    for block in &panel.blocks {
        if line > max_content_y {
            break;
        }
        draw_block(frame, inner_x, &mut line, inner_width, max_content_y, block);
        line += 1;
    }
    let prompt_y = y + height as i32 - 2;
    if prompt_y > line {
        let prompt = panel.prompt.as_deref().unwrap_or("");
        draw_clipped_text(frame, inner_x, prompt_y, prompt, inner_width, ACCENT_2);
    }
}

fn draw_block(
    frame: &mut FrameBuffer,
    x: i32,
    line: &mut i32,
    inner_width: usize,
    max_y: i32,
    block: &UiBlock,
) {
    match block {
        UiBlock::Paragraph { lines, color } => {
            for text in lines {
                for wrapped in wrap_text(text, inner_width) {
                    if *line > max_y {
                        return;
                    }
                    draw_clipped_text(frame, x, *line, &wrapped, inner_width, *color);
                    *line += 1;
                }
            }
        }
        UiBlock::Table { columns, rows } => {
            if *line > max_y {
                return;
            }
            draw_clipped_text(
                frame,
                x,
                *line,
                &format_table_row(columns),
                inner_width,
                FG_V_DIM,
            );
            *line += 1;
            for row in rows {
                if *line > max_y {
                    draw_clipped_text(frame, x, *line, "...", inner_width, FG_V_DIM);
                    return;
                }
                draw_clipped_text(
                    frame,
                    x,
                    *line,
                    &format_table_row(&row.cells),
                    inner_width,
                    row.color,
                );
                *line += 1;
            }
        }
    }
}

fn format_table_row(cells: &[String]) -> String {
    let first = cells.first().map(String::as_str).unwrap_or("");
    let second = cells.get(1).map(String::as_str).unwrap_or("");
    let third = cells.get(2).map(String::as_str).unwrap_or("");
    format!("{first:<14} {second:>7}  {third}")
}

fn draw_clipped_text(frame: &mut FrameBuffer, x: i32, y: i32, text: &str, width: usize, fg: Color) {
    let clipped = text.chars().take(width).collect::<String>();
    frame.text(x, y, &clipped, fg);
}

fn wrap_text(text: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return Vec::new();
    }
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        let next_len = if current.is_empty() {
            word.chars().count()
        } else {
            current.chars().count() + 1 + word.chars().count()
        };
        if next_len > width && !current.is_empty() {
            lines.push(current);
            current = word.to_string();
        } else {
            if !current.is_empty() {
                current.push(' ');
            }
            current.push_str(word);
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}

fn short_method(method: &str) -> &'static str {
    if method.contains("SQLite") {
        "read-only local database and files"
    } else if method.contains("rollout") {
        "local Codex session files"
    } else if method.contains("JSONL") || method.contains("transcript") {
        "local conversation files"
    } else {
        "local files"
    }
}

fn render_game_frame(state: &VisibleGameState) -> FrameBuffer {
    let width = state.width.max(1);
    let height = state.height.max(1);
    let mut frame = FrameBuffer::new(width, height);
    let cx = width as f64 / 2.0;
    let cy = height as f64 / 2.0;
    let radius_x = (state.planet_diameter_cells / 2.0).min((width as f64 - 4.0).max(4.0) / 2.0);
    let radius_y = (radius_x / 2.0).min((height as f64 - 6.0).max(3.0) / 2.0);
    let view_normal = state.camera_focus.normalize();
    let up = state
        .camera_up
        .add(view_normal.scale(-state.camera_up.dot(view_normal)))
        .normalize();
    let right = up.cross(view_normal).normalize();

    for y in 0..height {
        for x in 0..width {
            let nx = (x as f64 + 0.5 - cx) / radius_x;
            let ny = (y as f64 + 0.5 - cy) / radius_y;
            let d = nx * nx + ny * ny;
            if d < 0.88 {
                let py = -ny;
                let depth = (1.0 - nx * nx - py * py).max(0.0).sqrt();
                let world = right
                    .scale(nx)
                    .add(up.scale(py))
                    .add(view_normal.scale(depth))
                    .normalize();
                if earth_land(world) {
                    frame.put(x as i32, y as i32, land_char(world), PLANET_LAND);
                } else if ((x as u32 + y as u32) % 5) == 0 {
                    frame.put(x as i32, y as i32, '.', PLANET_WATER);
                }
            }
            if (0.88..=1.08).contains(&d) {
                frame.put(x as i32, y as i32, '.', PLANET_OUTLINE);
            }
        }
    }

    let mut players = state.players.clone();
    players.sort_by(|a, b| {
        a.position
            .dot(view_normal)
            .partial_cmp(&b.position.dot(view_normal))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for player in &players {
        if player.position.dot(view_normal) <= 0.0 {
            continue;
        }
        let px = player.position.dot(right);
        let py = player.position.dot(up);
        let sx = (cx + px * radius_x).round() as i32;
        let sy = (cy - py * radius_y).round() as i32;
        draw_player(&mut frame, sx, sy, player);
    }

    frame.text(
        0,
        0,
        &format!("arrows move, esc exits   tokens +{}", state.token_delta),
        HUD,
    );
    frame
}

fn earth_land(position: Vec3) -> bool {
    let lat = position.z.asin();
    let lon = position.y.atan2(position.x);
    let x = (((lon + std::f64::consts::PI) / std::f64::consts::TAU) * land_mask::LAND_MASK_W as f64)
        .floor()
        .rem_euclid(land_mask::LAND_MASK_W as f64) as usize;
    let y = (((std::f64::consts::FRAC_PI_2 - lat) / std::f64::consts::PI)
        * land_mask::LAND_MASK_H as f64)
        .floor()
        .clamp(0.0, (land_mask::LAND_MASK_H - 1) as f64) as usize;
    land_mask::LAND_MASK_ROWS[y].as_bytes()[x] == b'1'
}

fn land_char(position: Vec3) -> char {
    let lat = position.z.asin();
    if lat.abs() > 1.15 {
        '*'
    } else if ((lat * 31.0 + position.x * 17.0 + position.y * 13.0).sin()) > 0.25 {
        '#'
    } else {
        '+'
    }
}

fn draw_player(frame: &mut FrameBuffer, x: i32, y: i32, player: &VisiblePlayer) {
    let color = if player.is_self {
        PLAYER_SELF
    } else if player.is_fake {
        PLAYER_OTHER
    } else {
        Color(245, 245, 245)
    };
    let leg = match player.walking_phase % 3 {
        0 => "/",
        1 => "|",
        _ => "\\",
    };
    let rows = [
        (0, " 0 ".to_string()),
        (1, "-|-".to_string()),
        (2, format!(" |{leg}")),
    ];
    for (dy, text) in rows {
        frame.text(x - 1, y - 2 + dy, &text, color);
    }
    if !player.name.is_empty() {
        let label = format!("@{}", player.name);
        let label_x = x - (label.chars().count() as i32 / 2);
        frame.text(label_x, y - 3, &label, HUD);
    }
}

fn diff_frames(previous: Option<&FrameBuffer>, current: &FrameBuffer) -> Vec<AnsiAction> {
    let full_redraw = previous
        .map(|previous| previous.width != current.width || previous.height != current.height)
        .unwrap_or(true);
    let mut actions = Vec::new();
    if full_redraw {
        actions.push(AnsiAction::HideCursor);
        actions.push(AnsiAction::Clear);
    }
    let mut cursor: Option<(u16, u16)> = None;
    let mut active_fg: Option<Option<Color>> = Some(None);

    for y in 0..current.height {
        let mut x = 0;
        while x < current.width {
            let cell = current.get(x, y);
            let changed = full_redraw
                || previous
                    .map(|previous| previous.get(x, y) != cell)
                    .unwrap_or(true);
            if !changed || (full_redraw && cell == Cell::default()) {
                x += 1;
                continue;
            }

            let run_fg = cell.fg;
            let start_x = x;
            let mut text = String::new();
            while x < current.width {
                let next = current.get(x, y);
                let next_changed = full_redraw
                    || previous
                        .map(|previous| previous.get(x, y) != next)
                        .unwrap_or(true);
                if !next_changed || next.fg != run_fg {
                    break;
                }
                if full_redraw && next == Cell::default() {
                    break;
                }
                text.push(next.ch);
                x += 1;
            }

            if text.is_empty() {
                x += 1;
                continue;
            }
            if cursor != Some((start_x, y)) {
                actions.push(AnsiAction::Move { x: start_x, y });
            }
            match run_fg {
                Some(color) if active_fg != Some(Some(color)) => {
                    actions.push(AnsiAction::Fg(color));
                    active_fg = Some(Some(color));
                }
                None if active_fg != Some(None) => {
                    actions.push(AnsiAction::Reset);
                    active_fg = Some(None);
                }
                _ => {}
            }
            let text_width = text.chars().count() as u16;
            actions.push(AnsiAction::Text(text));
            cursor = Some((start_x.saturating_add(text_width), y));
        }
    }

    if active_fg != Some(None) {
        actions.push(AnsiAction::Reset);
    }
    actions
}

fn apply_ansi_actions(actions: &[AnsiAction]) -> anyhow::Result<()> {
    let mut out = String::with_capacity(actions.len() * 8);
    for action in actions {
        match action {
            AnsiAction::Clear => out.push_str("\x1b[2J\x1b[H"),
            AnsiAction::HideCursor => out.push_str("\x1b[?25l"),
            AnsiAction::ShowCursor => out.push_str("\x1b[?25h"),
            AnsiAction::Move { x, y } => out.push_str(&format!("\x1b[{};{}H", y + 1, x + 1)),
            AnsiAction::Fg(Color(r, g, b)) => out.push_str(&format!("\x1b[38;2;{r};{g};{b}m")),
            AnsiAction::Reset => out.push_str("\x1b[0m"),
            AnsiAction::Text(text) => out.push_str(text),
        }
    }
    print!("{out}");
    io::stdout().flush()?;
    Ok(())
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> anyhow::Result<Self> {
        terminal::enable_raw_mode()?;
        print!("\x1b[2J\x1b[H\x1b[?25l");
        io::stdout().flush()?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = terminal::disable_raw_mode();
        let _ = apply_ansi_actions(&[AnsiAction::Reset, AnsiAction::ShowCursor, AnsiAction::Clear]);
    }
}

#[allow(dead_code)]
mod ai_usage {
    use super::*;

    #[derive(Debug, Clone)]
    pub struct DetectedTool {
        pub id: &'static str,
        pub display_name: &'static str,
        pub collection_method: &'static str,
        pub sources: Vec<PathBuf>,
    }

    #[derive(Debug, Default, Clone, Serialize, Deserialize)]
    pub struct UsageSnapshot {
        pub input_tokens: u64,
        pub output_tokens: u64,
        pub cached_tokens: u64,
        pub reasoning_tokens: u64,
        pub tools: HashMap<String, ToolUsage>,
    }

    #[derive(Debug, Default, Clone, Serialize, Deserialize)]
    pub struct ToolUsage {
        pub input_tokens: u64,
        pub output_tokens: u64,
        pub cached_tokens: u64,
        pub reasoning_tokens: u64,
        pub sources: usize,
    }

    #[derive(Debug, Default, Clone, Copy)]
    struct TokenCounts {
        input: u64,
        output: u64,
        cached: u64,
        reasoning: u64,
    }

    impl UsageSnapshot {
        pub fn total_tokens(&self) -> u64 {
            self.input_tokens
                .saturating_add(self.output_tokens)
                .saturating_add(self.cached_tokens)
                .saturating_add(self.reasoning_tokens)
        }

        pub fn saturating_sub(&self, baseline: Option<&UsageSnapshot>) -> UsageSnapshot {
            let Some(baseline) = baseline else {
                return self.clone();
            };
            let mut tools = HashMap::new();
            for (id, usage) in &self.tools {
                let tool_baseline = baseline.tools.get(id);
                tools.insert(id.clone(), usage.saturating_sub(tool_baseline));
            }
            UsageSnapshot {
                input_tokens: self.input_tokens.saturating_sub(baseline.input_tokens),
                output_tokens: self.output_tokens.saturating_sub(baseline.output_tokens),
                cached_tokens: self.cached_tokens.saturating_sub(baseline.cached_tokens),
                reasoning_tokens: self
                    .reasoning_tokens
                    .saturating_sub(baseline.reasoning_tokens),
                tools,
            }
        }

        fn add(&mut self, tool: &DetectedTool, counts: TokenCounts) {
            self.input_tokens = self.input_tokens.saturating_add(counts.input);
            self.output_tokens = self.output_tokens.saturating_add(counts.output);
            self.cached_tokens = self.cached_tokens.saturating_add(counts.cached);
            self.reasoning_tokens = self.reasoning_tokens.saturating_add(counts.reasoning);
            let entry = self.tools.entry(tool.id.to_string()).or_default();
            entry.input_tokens = entry.input_tokens.saturating_add(counts.input);
            entry.output_tokens = entry.output_tokens.saturating_add(counts.output);
            entry.cached_tokens = entry.cached_tokens.saturating_add(counts.cached);
            entry.reasoning_tokens = entry.reasoning_tokens.saturating_add(counts.reasoning);
            entry.sources = tool.sources.len();
        }
    }

    impl ToolUsage {
        fn saturating_sub(&self, baseline: Option<&ToolUsage>) -> ToolUsage {
            let Some(baseline) = baseline else {
                return self.clone();
            };
            ToolUsage {
                input_tokens: self.input_tokens.saturating_sub(baseline.input_tokens),
                output_tokens: self.output_tokens.saturating_sub(baseline.output_tokens),
                cached_tokens: self.cached_tokens.saturating_sub(baseline.cached_tokens),
                reasoning_tokens: self
                    .reasoning_tokens
                    .saturating_sub(baseline.reasoning_tokens),
                sources: self.sources,
            }
        }
    }

    pub fn detect_supported_tools() -> Vec<DetectedTool> {
        let candidates = [
            DetectedTool {
                id: "claude-code",
                display_name: "Claude Code",
                collection_method: "passive JSONL session reader",
                sources: discover_claude_sources(),
            },
            DetectedTool {
                id: "codex",
                display_name: "Codex CLI",
                collection_method: "passive rollout JSONL reader",
                sources: discover_codex_sources(),
            },
            DetectedTool {
                id: "cursor",
                display_name: "Cursor",
                collection_method: "bundled read-only SQLite plus transcript reader",
                sources: discover_cursor_sources(),
            },
            DetectedTool {
                id: "copilot",
                display_name: "GitHub Copilot",
                collection_method: "passive VS Code transcript/session reader",
                sources: discover_copilot_sources(),
            },
        ];

        candidates
            .into_iter()
            .filter(|tool| !tool.sources.is_empty())
            .collect()
    }

    pub fn scan_enabled(detected: &[DetectedTool], enabled_ids: &[String]) -> UsageSnapshot {
        let enabled = enabled_ids
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let mut snapshot = UsageSnapshot::default();

        for tool in detected.iter().filter(|tool| enabled.contains(tool.id)) {
            let counts = match tool.id {
                "claude-code" => scan_claude(tool),
                "codex" => scan_codex(tool),
                "cursor" => scan_cursor(tool),
                "copilot" => scan_copilot(tool),
                _ => TokenCounts::default(),
            };
            snapshot.add(tool, counts);
        }

        snapshot
    }

    fn discover_claude_sources() -> Vec<PathBuf> {
        let mut roots = Vec::new();
        if let Some(raw) = env::var_os("CLAUDE_CONFIG_DIR") {
            roots.extend(
                raw.to_string_lossy()
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(PathBuf::from),
            );
        }
        if roots.is_empty() {
            if let Some(config) = env_path("XDG_CONFIG_HOME") {
                roots.push(config.join("claude"));
            } else if let Some(home) = dirs::home_dir() {
                roots.push(home.join(".config").join("claude"));
            }
            if let Some(home) = dirs::home_dir() {
                roots.push(home.join(".claude"));
                roots.push(claude_desktop_sessions_dir(&home));
            }
        }

        let mut sources = Vec::new();
        for root in roots {
            let candidate = if root.file_name().and_then(|s| s.to_str()) == Some("projects") {
                root
            } else {
                root.join("projects")
            };
            if candidate.exists() {
                sources.extend(find_files(&candidate, 5, |path| {
                    path.extension().and_then(|s| s.to_str()) == Some("jsonl")
                }));
            }
        }
        dedup_paths(sources)
    }

    fn claude_desktop_sessions_dir(home: &Path) -> PathBuf {
        if cfg!(target_os = "macos") {
            home.join("Library/Application Support/Claude/local-agent-mode-sessions")
        } else if cfg!(target_os = "windows") {
            home.join("AppData/Roaming/Claude/local-agent-mode-sessions")
        } else {
            home.join(".config/Claude/local-agent-mode-sessions")
        }
    }

    fn discover_codex_sources() -> Vec<PathBuf> {
        let root = env_path("CODEX_HOME")
            .or_else(|| dirs::home_dir().map(|home| home.join(".codex")))
            .map(|home| home.join("sessions"));
        let Some(root) = root else {
            return Vec::new();
        };
        find_files(&root, 12, |path| {
            path.file_name()
                .and_then(|s| s.to_str())
                .map(|name| name.starts_with("rollout-"))
                .unwrap_or(false)
                && path.extension().and_then(|s| s.to_str()) == Some("jsonl")
        })
    }

    fn discover_cursor_sources() -> Vec<PathBuf> {
        let mut sources = Vec::new();
        if let Some(db) = cursor_state_db_path() {
            if db.exists() {
                sources.push(db);
            }
        }
        if let Some(agent_home) =
            env_path("CURSOR_AGENT_HOME").or_else(|| dirs::home_dir().map(|h| h.join(".cursor")))
        {
            let projects = agent_home.join("projects");
            sources.extend(find_files(&projects, 8, |path| {
                path.extension().and_then(|s| s.to_str()) == Some("jsonl")
            }));
        }
        dedup_paths(sources)
    }

    fn cursor_state_db_path() -> Option<PathBuf> {
        let home = dirs::home_dir()?;
        let base = if cfg!(target_os = "macos") {
            home.join("Library/Application Support/Cursor/User/globalStorage")
        } else if cfg!(target_os = "windows") {
            home.join("AppData/Roaming/Cursor/User/globalStorage")
        } else {
            home.join(".config/Cursor/User/globalStorage")
        };
        Some(base.join("state.vscdb"))
    }

    fn discover_copilot_sources() -> Vec<PathBuf> {
        let mut sources = Vec::new();
        if let Some(home) = dirs::home_dir() {
            let legacy = home.join(".copilot/session-state");
            if legacy.exists() {
                sources.extend(find_files(&legacy, 4, |path| {
                    path.extension().and_then(|s| s.to_str()) == Some("jsonl")
                }));
            }
            for root in vscode_workspace_storage_dirs(&home) {
                sources.extend(find_files(&root, 5, |path| {
                    path.extension().and_then(|s| s.to_str()) == Some("jsonl")
                        && path.components().any(|c| c.as_os_str() == "transcripts")
                }));
            }
        }
        dedup_paths(sources)
    }

    fn vscode_workspace_storage_dirs(home: &Path) -> Vec<PathBuf> {
        if cfg!(target_os = "macos") {
            return vec![
                home.join("Library/Application Support/Code/User/workspaceStorage"),
                home.join("Library/Application Support/Code - Insiders/User/workspaceStorage"),
            ];
        }
        if cfg!(target_os = "windows") {
            return vec![
                home.join("AppData/Roaming/Code/User/workspaceStorage"),
                home.join("AppData/Roaming/Code - Insiders/User/workspaceStorage"),
            ];
        }
        vec![
            home.join(".config/Code/User/workspaceStorage"),
            home.join(".config/Code - Insiders/User/workspaceStorage"),
            home.join(".vscode-server/data/User/workspaceStorage"),
        ]
    }

    fn scan_claude(tool: &DetectedTool) -> TokenCounts {
        let mut seen = HashSet::new();
        let mut counts = TokenCounts::default();
        for path in &tool.sources {
            for value in read_jsonl_values(path) {
                if value.get("type").and_then(Value::as_str) != Some("assistant") {
                    continue;
                }
                let Some(usage) = value.pointer("/message/usage") else {
                    continue;
                };
                let dedup = value
                    .pointer("/message/id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| {
                        value
                            .get("uuid")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    })
                    .unwrap_or_else(|| format!("{}:{}", path.display(), seen.len()));
                if seen.insert(dedup) {
                    counts.input = counts
                        .input
                        .saturating_add(token_field(usage, "input_tokens"));
                    counts.output = counts
                        .output
                        .saturating_add(token_field(usage, "output_tokens"));
                    counts.cached = counts
                        .cached
                        .saturating_add(token_field(usage, "cache_creation_input_tokens"))
                        .saturating_add(token_field(usage, "cache_read_input_tokens"));
                }
            }
        }
        counts
    }

    fn scan_codex(tool: &DetectedTool) -> TokenCounts {
        let mut counts = TokenCounts::default();
        for path in &tool.sources {
            let mut previous_total: Option<TokenCounts> = None;
            for value in read_jsonl_values(path) {
                if value.get("type").and_then(Value::as_str) != Some("event_msg") {
                    continue;
                }
                if value.pointer("/payload/type").and_then(Value::as_str) != Some("token_count") {
                    continue;
                }
                let usage = value
                    .pointer("/payload/info/last_token_usage")
                    .map(tokens_from_codex_usage)
                    .or_else(|| {
                        value
                            .pointer("/payload/info/total_token_usage")
                            .map(tokens_from_codex_usage)
                            .map(|total| delta(total, previous_total))
                    });
                if let Some(delta) = usage {
                    counts.input = counts.input.saturating_add(delta.input);
                    counts.output = counts.output.saturating_add(delta.output);
                    counts.cached = counts.cached.saturating_add(delta.cached);
                    counts.reasoning = counts.reasoning.saturating_add(delta.reasoning);
                }
                if let Some(total) = value
                    .pointer("/payload/info/total_token_usage")
                    .map(tokens_from_codex_usage)
                {
                    previous_total = Some(total);
                }
            }
        }
        counts
    }

    fn scan_cursor(tool: &DetectedTool) -> TokenCounts {
        let mut counts = TokenCounts::default();
        for path in &tool.sources {
            if path.file_name().and_then(|s| s.to_str()) == Some("state.vscdb") {
                add(&mut counts, scan_cursor_db(path));
            } else {
                add(&mut counts, scan_jsonl_token_fields(path));
            }
        }
        counts
    }

    fn scan_cursor_db(path: &Path) -> TokenCounts {
        let uri = format!("file:{}?immutable=1", path.display());
        let Ok(conn) = Connection::open_with_flags(
            &uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        ) else {
            return TokenCounts::default();
        };
        let Ok(mut stmt) = conn.prepare(
            "SELECT value FROM cursorDiskKV WHERE key LIKE 'bubbleId:%' OR key LIKE 'agentKv:blob:%'",
        ) else {
            return TokenCounts::default();
        };
        let rows = stmt.query_map([], |row| row.get::<_, String>(0));
        let Ok(rows) = rows else {
            return TokenCounts::default();
        };
        let mut counts = TokenCounts::default();
        for raw in rows.flatten() {
            if let Ok(value) = serde_json::from_str::<Value>(&raw) {
                add(&mut counts, generic_token_walk(&value));
            }
        }
        counts
    }

    fn scan_copilot(tool: &DetectedTool) -> TokenCounts {
        let mut counts = TokenCounts::default();
        for path in &tool.sources {
            for value in read_jsonl_values(path) {
                add(&mut counts, generic_token_walk(&value));
            }
        }
        counts
    }

    fn scan_jsonl_token_fields(path: &Path) -> TokenCounts {
        let mut counts = TokenCounts::default();
        for value in read_jsonl_values(path) {
            add(&mut counts, generic_token_walk(&value));
        }
        counts
    }

    fn read_jsonl_values(path: &Path) -> Vec<Value> {
        let Ok(raw) = fs::read_to_string(path) else {
            return Vec::new();
        };
        raw.lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect()
    }

    fn generic_token_walk(value: &Value) -> TokenCounts {
        let mut counts = TokenCounts::default();
        walk_value(value, &mut |key, value| {
            let Some(n) = value.as_u64() else {
                return;
            };
            match key {
                "input_tokens" | "inputTokens" | "prompt_tokens" => {
                    counts.input = counts.input.saturating_add(n)
                }
                "output_tokens" | "outputTokens" | "completion_tokens" => {
                    counts.output = counts.output.saturating_add(n)
                }
                "cached_input_tokens"
                | "cache_read_input_tokens"
                | "cache_creation_input_tokens" => counts.cached = counts.cached.saturating_add(n),
                "reasoning_tokens" | "reasoning_output_tokens" => {
                    counts.reasoning = counts.reasoning.saturating_add(n)
                }
                _ => {}
            }
        });
        counts
    }

    fn tokens_from_codex_usage(value: &Value) -> TokenCounts {
        let cached = token_field(value, "cached_input_tokens")
            .saturating_add(token_field(value, "cache_read_input_tokens"));
        TokenCounts {
            input: token_field(value, "input_tokens").saturating_sub(cached),
            output: token_field(value, "output_tokens"),
            cached,
            reasoning: token_field(value, "reasoning_output_tokens"),
        }
    }

    fn delta(total: TokenCounts, previous: Option<TokenCounts>) -> TokenCounts {
        let Some(previous) = previous else {
            return total;
        };
        TokenCounts {
            input: total.input.saturating_sub(previous.input),
            output: total.output.saturating_sub(previous.output),
            cached: total.cached.saturating_sub(previous.cached),
            reasoning: total.reasoning.saturating_sub(previous.reasoning),
        }
    }

    fn token_field(value: &Value, name: &str) -> u64 {
        value.get(name).and_then(Value::as_u64).unwrap_or(0)
    }

    fn add(target: &mut TokenCounts, extra: TokenCounts) {
        target.input = target.input.saturating_add(extra.input);
        target.output = target.output.saturating_add(extra.output);
        target.cached = target.cached.saturating_add(extra.cached);
        target.reasoning = target.reasoning.saturating_add(extra.reasoning);
    }

    fn walk_value(value: &Value, visitor: &mut impl FnMut(&str, &Value)) {
        match value {
            Value::Object(map) => {
                for (key, child) in map {
                    visitor(key, child);
                    walk_value(child, visitor);
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk_value(item, visitor);
                }
            }
            _ => {}
        }
    }

    fn find_files(
        root: &Path,
        max_depth: usize,
        predicate: impl Fn(&Path) -> bool,
    ) -> Vec<PathBuf> {
        if !root.exists() {
            return Vec::new();
        }
        WalkDir::new(root)
            .max_depth(max_depth)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| {
                let name = entry.file_name().to_string_lossy();
                name != "node_modules" && name != ".git"
            })
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file() && predicate(entry.path()))
            .map(|entry| entry.path().to_path_buf())
            .collect()
    }

    fn dedup_paths(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
        paths.sort();
        paths.dedup();
        paths
    }

    fn env_path(var: &str) -> Option<PathBuf> {
        env::var_os(var)
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty())
    }
}
