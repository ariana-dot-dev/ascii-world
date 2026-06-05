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
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
use walkdir::WalkDir;

mod land_mask;

const ONBOARDING_VERSION: u32 = 3;
const PIXEL_COLOR_COUNT: usize = 5;
const JUMP_GROUND_EPSILON: f64 = 0.02;
const CAMERA_FOLLOW_STIFFNESS: f64 = 3.6;
const CAMERA_CENTERING_STIFFNESS: f64 = 0.55;
const CAMERA_FOLLOW_DAMPING: f64 = 5.4;
const CAMERA_PREDICTION_SECONDS: f64 = 0.9;
const CAMERA_MAX_PREDICTION_RADIANS: f64 = 0.34;
const CAMERA_MAX_SPEED_RADIANS_PER_SECOND: f64 = 0.95;
const CAMERA_MAX_LAG_RADIANS: f64 = 0.92;
const CAMERA_SOFT_LAG_RADIANS: f64 = 0.58;
const HEADER_CONTROLS: &str = "arrows move, space jumps, M market, esc exits";
const FOOTER_TEXT: &str =
    "Made and hosted by agents on https://box.ascii.dev, the cheapest and most powerful AI sandboxes";

#[derive(Parser)]
#[command(name = "Ascii World", version, about = "Multiplayer token game")]
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
    receiver: std_mpsc::Receiver<TokenScanSnapshot>,
    stop: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Default)]
struct TokenScanSnapshot {
    since_joining: ai_usage::UsageSnapshot,
    all_time: ai_usage::UsageSnapshot,
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
                let all_time = ai_usage::scan_enabled(&detected, &enabled);
                let snapshot = TokenScanSnapshot {
                    since_joining: all_time.saturating_sub(baseline.as_ref()),
                    all_time,
                };
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
        match start_player_connection(&backend_url, token).await {
            Ok((joined_state, tx)) => {
                active_state = joined_state;
                player_tx = Some(tx);
                token_worker = Some(TokenScanWorker::start(
                    detected.clone(),
                    state.enabled_ai_tools.clone(),
                    state.ai_usage_baseline.clone(),
                ));
                ClientPhase::Playing
            }
            Err(error) if is_auth_rejected(&error) => {
                state.game_api_token = None;
                state.x_username = None;
                state.x_name = None;
                state.onboarding_completed = false;
                save_state(&state_path, &state)?;
                if let Some(spectator_state) = start_spectator(&backend_url).await {
                    active_state = spectator_state;
                }
                ClientPhase::Onboarding
            }
            Err(error) => return Err(error),
        }
    };

    let _terminal = TerminalGuard::enter()?;
    let mut input_tracker = InputTracker::default();
    let mut camera = CameraState::default();
    let mut renderer = SmartRenderer::default();
    let mut last_sent = InputState::default();
    let mut last_input_send = Instant::now() - Duration::from_millis(50);
    let mut last_frame = Instant::now() - Duration::from_millis(16);
    let mut tokens_since_joining = ai_usage::UsageSnapshot::default();
    let mut tokens_all_time = ai_usage::UsageSnapshot::default();
    let mut last_reported_tokens: Option<u64> = None;
    let mut last_token_report = Instant::now() - Duration::from_secs(5);
    let mut reward_dialog_closed = false;
    let mut reward_dialog_opened_at: Option<Instant> = None;
    let mut market_open = false;
    let mut market_selected = 0usize;
    let mut selected_pixel_color = 0usize;
    let onboarding_panel = UiPanel::onboarding(&detected);

    loop {
        while event::poll(Duration::from_millis(0))? {
            match event::read()? {
                Event::Key(key)
                    if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
                {
                    if matches!(phase, ClientPhase::Playing) && market_open {
                        let live_state = active_state
                            .lock()
                            .map(|state| state.clone())
                            .unwrap_or_default();
                        let market_len = live_state.market.len().max(1);
                        match key.code {
                            KeyCode::Char('m') | KeyCode::Char('M') | KeyCode::Esc => {
                                market_open = false;
                                renderer.reset();
                            }
                            KeyCode::Up => {
                                market_selected = market_selected.saturating_sub(1);
                            }
                            KeyCode::Down => {
                                market_selected = (market_selected + 1).min(market_len - 1);
                            }
                            KeyCode::Enter => {
                                if let Some(item) = live_state.market.get(market_selected) {
                                    if let Some(self_player) = live_state.self_player() {
                                        if let Some(tx) = &player_tx {
                                            let _ = if item.kind == "pixel" {
                                                item.pixel_color.map(|color| {
                                                    tx.send(ClientMessage::BuyPixel { color })
                                                })
                                            } else {
                                                let owned = self_player
                                                    .owned_heads
                                                    .iter()
                                                    .any(|owned| owned == &item.id);
                                                let equipped = self_player.equipped_head == item.id;
                                                Some(if owned && !equipped {
                                                    tx.send(ClientMessage::EquipHead {
                                                        item_id: item.id.clone(),
                                                    })
                                                } else {
                                                    tx.send(ClientMessage::BuyHead {
                                                        item_id: item.id.clone(),
                                                    })
                                                })
                                            };
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                        continue;
                    }
                    if matches!(phase, ClientPhase::Playing) && key.code == KeyCode::Enter {
                        let has_rewards = active_state
                            .lock()
                            .map(|state| !state.rewards.is_empty())
                            .unwrap_or(false);
                        let reward_can_close = reward_dialog_opened_at
                            .map(|opened| opened.elapsed() >= Duration::from_secs(5))
                            .unwrap_or(false);
                        if has_rewards && !reward_dialog_closed && reward_can_close {
                            reward_dialog_closed = true;
                            reward_dialog_opened_at = None;
                            renderer.reset();
                            continue;
                        }
                    }
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
                        (ClientPhase::Playing, KeyCode::Char('m') | KeyCode::Char('M')) => {
                            market_open = true;
                            market_selected = 0;
                            renderer.reset();
                        }
                        (ClientPhase::Playing, KeyCode::Char(ch))
                            if pixel_shortcut(ch).is_some() =>
                        {
                            if let Some(color) = pixel_shortcut(ch) {
                                selected_pixel_color = color;
                                renderer.reset();
                            }
                        }
                        (ClientPhase::Playing, KeyCode::Char('p') | KeyCode::Char('P')) => {
                            if let Some(tx) = &player_tx {
                                let _ = tx.send(ClientMessage::PlacePixel {
                                    color: selected_pixel_color,
                                });
                            }
                        }
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
                        (ClientPhase::Playing, KeyCode::Char(' '))
                            if key.kind == KeyEventKind::Press =>
                        {
                            input_tracker.jump = Some(Instant::now());
                            if let Some(tx) = &player_tx {
                                let input = input_tracker.current();
                                let _ = tx.send(ClientMessage::Input {
                                    up: input.up,
                                    down: input.down,
                                    left: input.left,
                                    right: input.right,
                                    jump: true,
                                    camera_up: camera.up.to_array(),
                                });
                                last_sent = input;
                                last_input_send = Instant::now();
                            }
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
                            KeyCode::Char(' ') => input_tracker.jump = None,
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
            let input_active = input.up || input.down || input.left || input.right || input.jump;
            if input != last_sent
                || (input_active && last_input_send.elapsed() >= Duration::from_millis(50))
            {
                if let Some(tx) = &player_tx {
                    let _ = tx.send(ClientMessage::Input {
                        up: input.up,
                        down: input.down,
                        left: input.left,
                        right: input.right,
                        jump: input.jump,
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
            if started.elapsed() >= Duration::from_secs(5) {
                let token = state
                    .game_api_token
                    .as_deref()
                    .context("missing game API token")?;
                match start_player_connection(&backend_url, token).await {
                    Ok((joined_state, tx)) => {
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
                    Err(error) if is_auth_rejected(&error) => {
                        state.game_api_token = None;
                        state.x_username = None;
                        state.x_name = None;
                        state.onboarding_completed = false;
                        save_state(&state_path, &state)?;
                        if let Some(spectator_state) = start_spectator(&backend_url).await {
                            active_state = spectator_state;
                        }
                        phase = ClientPhase::Onboarding;
                        renderer.reset();
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        if last_frame.elapsed() >= Duration::from_micros(16_667) {
            let frame_started = Instant::now();
            let frame_dt = frame_started
                .duration_since(last_frame)
                .as_secs_f64()
                .clamp(0.0, 0.05);
            if let Some(worker) = &token_worker {
                while let Ok(snapshot) = worker.receiver.try_recv() {
                    tokens_since_joining = snapshot.since_joining;
                    tokens_all_time = snapshot.all_time;
                }
            }
            let token_total = tokens_since_joining.total_tokens();
            let all_time_token_total = tokens_all_time.total_tokens();
            if matches!(phase, ClientPhase::Playing)
                && (last_reported_tokens != Some(token_total)
                    || last_token_report.elapsed() >= Duration::from_secs(2))
            {
                if let Some(tx) = &player_tx {
                    let _ = tx.send(ClientMessage::TokenUsage {
                        total_tokens: token_total,
                        all_time_tokens: all_time_token_total,
                    });
                    last_reported_tokens = Some(token_total);
                    last_token_report = Instant::now();
                }
            }
            let (cols, rows) = terminal::size()?;
            let live_state = active_state
                .lock()
                .map(|state| state.clone())
                .map_err(|_| anyhow!("game state lock is poisoned"))?;
            if let Some(error) = &live_state.protocol_error {
                return Err(anyhow!(error.clone()));
            }
            let rewards_visible = matches!(phase, ClientPhase::Playing)
                && !reward_dialog_closed
                && !live_state.rewards.is_empty();
            if rewards_visible && reward_dialog_opened_at.is_none() {
                reward_dialog_opened_at = Some(Instant::now());
            }
            let visible = VisibleGameState::from_client_state(
                &live_state,
                &mut camera,
                cols,
                rows,
                frame_dt,
                token_total,
                all_time_token_total,
            );
            let self_pixels = live_state
                .self_player()
                .map(|player| player.owned_pixels)
                .unwrap_or([0; PIXEL_COLOR_COUNT]);
            let gameplay_panel = if matches!(phase, ClientPhase::Playing) && market_open {
                Some(UiPanel::market(
                    &live_state.market,
                    live_state.self_player(),
                    market_selected,
                ))
            } else if rewards_visible {
                let can_close = reward_dialog_opened_at
                    .map(|opened| opened.elapsed() >= Duration::from_secs(5))
                    .unwrap_or(false);
                Some(UiPanel::rewards(&live_state.rewards, can_close))
            } else {
                None
            };
            let transient_panel = match &phase {
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
                ClientPhase::Playing => gameplay_panel.as_ref().map(|panel| (panel, 0.0)),
            };
            let actions =
                renderer.render_app(&visible, panel_progress, selected_pixel_color, self_pixels);
            apply_ansi_actions(&actions)?;
            last_frame = frame_started;
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
            match serde_json::from_str::<ServerMessage>(&text) {
                Ok(ServerMessage::Snapshot(snapshot)) => {
                    let result = validate_snapshot(&snapshot);
                    if let Ok(mut state) = reader_state.lock() {
                        match result {
                            Ok(()) => {
                                state.snapshot = Some(snapshot);
                                state.self_id = None;
                            }
                            Err(err) => state.protocol_error = Some(err.to_string()),
                        }
                    }
                }
                Ok(ServerMessage::Welcome { .. }) => {
                    if let Ok(mut state) = reader_state.lock() {
                        state.protocol_error =
                            Some("backend sent welcome message on spectator websocket".to_string());
                    }
                }
                Err(err) => {
                    if let Ok(mut state) = reader_state.lock() {
                        state.protocol_error = Some(format!("backend protocol error: {err}"));
                    }
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
                Ok(ServerMessage::Welcome {
                    self_id,
                    rewards,
                    market,
                }) => {
                    if let Ok(mut state) = reader_state.lock() {
                        state.self_id = Some(self_id);
                        state.rewards = rewards;
                        state.market = market;
                    }
                }
                Ok(ServerMessage::Snapshot(snapshot)) => {
                    let result = validate_snapshot(&snapshot);
                    if let Ok(mut state) = reader_state.lock() {
                        match result {
                            Ok(()) => state.snapshot = Some(snapshot),
                            Err(err) => state.protocol_error = Some(err.to_string()),
                        }
                    }
                }
                Err(err) => {
                    if let Ok(mut state) = reader_state.lock() {
                        state.protocol_error = Some(format!("backend protocol error: {err}"));
                    }
                }
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

fn is_auth_rejected(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("401") || message.contains("Unauthorized")
    })
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
    if option_env!("GAME_ENV") == Some("production") {
        return;
    }

    dotenvy::dotenv().ok();
    dotenvy::from_path_override("cli/.env").ok();
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
    let Some(asset) = current_asset_name() else {
        return;
    };
    let update_base =
        option_env!("GAME_DOWNLOAD_BASE_URL").unwrap_or("https://world.ascii.dev/download");
    let url = format!("{}/{asset}", update_base.trim_end_matches('/'));
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
        ("linux", "x86_64") => Some("world-linux-x64"),
        ("linux", "aarch64") => Some("world-linux-arm64"),
        ("macos", "x86_64") => Some("world-darwin-x64"),
        ("macos", "aarch64") => Some("world-darwin-arm64"),
        ("windows", "x86_64") => Some("world-windows-x64.exe"),
        _ => None,
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Welcome {
        self_id: String,
        rewards: Vec<RewardNotice>,
        market: Vec<MarketItem>,
    },
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
        jump: bool,
        camera_up: [f64; 3],
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

#[derive(Debug, Clone, Deserialize)]
struct Snapshot {
    players: Vec<PlayerSnapshot>,
    leaderboard: Vec<LeaderboardEntry>,
    placed_pixels: Vec<PlacedPixel>,
    pickups: Vec<PickupSnapshot>,
    pickup_rewards: Vec<PickupRewardSnapshot>,
    economy_rules: EconomyRulesSnapshot,
}

#[derive(Debug, Clone, Deserialize)]
struct EconomyRulesSnapshot {
    lobster_rate_token_unit: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct PlacedPixel {
    position: [f64; 3],
    color: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct PickupSnapshot {
    id: u64,
    position: [f64; 3],
    emoji: String,
}

#[derive(Debug, Clone, Deserialize)]
struct PickupRewardSnapshot {
    player_id: String,
    lobsters: u64,
}

fn pixel_shortcut(ch: char) -> Option<usize> {
    match ch {
        '1' | '&' => Some(0),
        '2' | 'é' => Some(1),
        '3' | '"' => Some(2),
        '4' | '\'' => Some(3),
        '5' | '(' => Some(4),
        _ => None,
    }
}

#[derive(Debug, Clone, Deserialize)]
struct LeaderboardEntry {
    username: String,
    lobsters: u64,
    all_time_tokens: u64,
    profile_url: String,
}

#[derive(Debug, Clone, Deserialize)]
struct PlayerSnapshot {
    id: String,
    name: String,
    planet_id: u32,
    lat: f64,
    lon: f64,
    #[serde(default)]
    position: Option<[f64; 3]>,
    fake: bool,
    total_tokens: u64,
    all_time_tokens: u64,
    lobsters: u64,
    equipped_head: String,
    owned_heads: Vec<String>,
    owned_pixels: [u64; PIXEL_COLOR_COUNT],
    jump_height: f64,
    #[serde(default)]
    jump_leg_pose: i8,
    #[serde(default)]
    facing: i8,
    walking_phase: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct RewardNotice {
    label: String,
    lobsters: u64,
    streak: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct MarketItem {
    id: String,
    label: String,
    head: String,
    price_lobsters: u64,
    kind: String,
    pixel_color: Option<usize>,
}

impl PlayerSnapshot {
    fn position_vec(&self) -> Vec3 {
        self.position
            .and_then(Vec3::from_array)
            .unwrap_or_else(|| Vec3::from_lat_lon(self.lat, self.lon))
    }
}

#[derive(Debug, Clone, Default)]
struct ClientGameState {
    self_id: Option<String>,
    snapshot: Option<Snapshot>,
    rewards: Vec<RewardNotice>,
    market: Vec<MarketItem>,
    protocol_error: Option<String>,
}

impl ClientGameState {
    fn self_player(&self) -> Option<&PlayerSnapshot> {
        let self_id = self.self_id.as_ref()?;
        self.snapshot
            .as_ref()?
            .players
            .iter()
            .find(|player| &player.id == self_id)
    }
}

fn validate_snapshot(snapshot: &Snapshot) -> anyhow::Result<()> {
    if snapshot.economy_rules.lobster_rate_token_unit == 0 {
        anyhow::bail!("backend sent invalid economy rule: lobster_rate_token_unit is zero");
    }
    for pixel in &snapshot.placed_pixels {
        if pixel.color >= PIXEL_COLOR_COUNT {
            anyhow::bail!("backend sent invalid placed pixel color {}", pixel.color);
        }
    }
    for pickup in &snapshot.pickups {
        if Vec3::from_array(pickup.position).is_none() {
            anyhow::bail!("backend sent invalid pickup position {}", pickup.id);
        }
    }
    for player in &snapshot.players {
        if player.owned_pixels.len() != PIXEL_COLOR_COUNT {
            anyhow::bail!(
                "backend sent invalid pixel inventory for player {}",
                player.id
            );
        }
        if player.jump_height < 0.0 || !player.jump_height.is_finite() {
            anyhow::bail!("backend sent invalid jump height for player {}", player.id);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct InputState {
    up: bool,
    down: bool,
    left: bool,
    right: bool,
    jump: bool,
}

#[derive(Debug, Default)]
struct InputTracker {
    up: Option<Instant>,
    down: Option<Instant>,
    left: Option<Instant>,
    right: Option<Instant>,
    jump: Option<Instant>,
}

impl InputTracker {
    fn current(&mut self) -> InputState {
        let now = Instant::now();
        let movement_expiry = Duration::from_millis(1_500);
        let jump_expiry = Duration::from_millis(180);
        let active = |last: &mut Option<Instant>, expiry: Duration| {
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
            up: active(&mut self.up, movement_expiry),
            down: active(&mut self.down, movement_expiry),
            left: active(&mut self.left, movement_expiry),
            right: active(&mut self.right, movement_expiry),
            jump: active(&mut self.jump, jump_expiry),
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
    velocity: Vec3,
    previous_self_position: Option<Vec3>,
}

impl Default for CameraState {
    fn default() -> Self {
        Self {
            focus: Vec3::X,
            up: Vec3::Z,
            velocity: Vec3::new(0.0, 0.0, 0.0),
            previous_self_position: None,
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
    tokens_since_joining: u64,
    tokens_all_time: u64,
    lobsters: u64,
    lobster_yield_per_hour: f64,
    leaderboard: Vec<LeaderboardEntry>,
    placed_pixels: Vec<PlacedPixel>,
    pickups: Vec<PickupSnapshot>,
    players: Vec<VisiblePlayer>,
}

#[derive(Debug, Clone)]
struct VisiblePlayer {
    name: String,
    position: Vec3,
    is_self: bool,
    is_fake: bool,
    points: u64,
    lobsters: u64,
    lobster_yield_per_hour: f64,
    equipped_head: String,
    jump_height: f64,
    jump_leg_pose: i8,
    pickup_reward_lobsters: u64,
    facing: i8,
    walking_phase: u64,
}

impl VisibleGameState {
    fn from_client_state(
        state: &ClientGameState,
        camera: &mut CameraState,
        width: u16,
        height: u16,
        dt: f64,
        tokens_since_joining: u64,
        tokens_all_time: u64,
    ) -> Self {
        let Some(snapshot) = state.snapshot.clone() else {
            return Self {
                width,
                height,
                planet_diameter_cells: 90.0,
                camera_focus: camera.focus,
                camera_up: camera.up,
                tokens_since_joining: 0,
                tokens_all_time: 0,
                lobsters: 0,
                lobster_yield_per_hour: 0.0,
                leaderboard: Vec::new(),
                placed_pixels: Vec::new(),
                pickups: Vec::new(),
                players: Vec::new(),
            };
        };
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
        update_camera_follow(camera, self_position, dt);
        camera.up = stable_camera_up(camera.focus);

        let players: Vec<VisiblePlayer> = snapshot
            .players
            .iter()
            .filter(|player| player.planet_id == 0)
            .map(|player| {
                let equipped_head = equipped_head_glyph(&player.equipped_head).to_string();
                VisiblePlayer {
                    name: player.name.clone(),
                    position: player.position_vec(),
                    is_self: state
                        .self_id
                        .as_ref()
                        .map(|self_id| self_id == &player.id)
                        .unwrap_or(false),
                    is_fake: player.fake,
                    points: player.all_time_tokens,
                    lobsters: player.lobsters,
                    lobster_yield_per_hour: lobster_yield_per_hour(
                        player.total_tokens,
                        snapshot.economy_rules.lobster_rate_token_unit,
                    ),
                    equipped_head,
                    jump_height: player.jump_height,
                    jump_leg_pose: player.jump_leg_pose,
                    pickup_reward_lobsters: snapshot
                        .pickup_rewards
                        .iter()
                        .filter(|reward| reward.player_id == player.id)
                        .map(|reward| reward.lobsters)
                        .sum(),
                    facing: player.facing,
                    walking_phase: player.walking_phase,
                }
            })
            .collect();
        let self_economy = players
            .iter()
            .find(|player: &&VisiblePlayer| player.is_self)
            .map(|player| {
                (
                    player.lobsters,
                    player.lobster_yield_per_hour,
                    player.points,
                )
            })
            .unwrap_or((0, 0.0, 0));

        Self {
            width,
            height,
            planet_diameter_cells: 90.0,
            camera_focus: camera.focus,
            camera_up: camera.up,
            tokens_since_joining,
            tokens_all_time,
            lobsters: self_economy.0,
            lobster_yield_per_hour: self_economy.1,
            leaderboard: snapshot.leaderboard,
            placed_pixels: snapshot.placed_pixels,
            pickups: snapshot.pickups,
            players,
        }
    }
}

fn update_camera_follow(camera: &mut CameraState, self_position: Vec3, dt: f64) {
    if dt <= 0.0 {
        return;
    }

    camera.focus = camera.focus.normalize();
    let current_self_position = self_position.normalize();
    let target = predicted_camera_target(camera, current_self_position, dt);
    let tangent_to_target = target.add(camera.focus.scale(-target.dot(camera.focus)));
    let distance = tangent_to_target.length();

    let mut angle = camera.focus.dot(target).clamp(-1.0, 1.0).acos();
    if distance > 1e-6 && angle.is_finite() {
        let direction = tangent_to_target.scale(1.0 / distance);
        let soft_angle = (angle - CAMERA_SOFT_LAG_RADIANS).max(0.0);
        camera.velocity = camera.velocity.add(
            direction
                .scale(soft_angle * CAMERA_FOLLOW_STIFFNESS)
                .scale(dt),
        );
    }

    let tangent_to_self =
        current_self_position.add(camera.focus.scale(-current_self_position.dot(camera.focus)));
    let self_distance = tangent_to_self.length();
    let self_angle = camera
        .focus
        .dot(current_self_position)
        .clamp(-1.0, 1.0)
        .acos();
    if self_distance > 1e-6 && self_angle.is_finite() {
        let center_direction = tangent_to_self.scale(1.0 / self_distance);
        camera.velocity = camera.velocity.add(
            center_direction
                .scale(self_angle * CAMERA_CENTERING_STIFFNESS)
                .scale(dt),
        );
    }

    if distance <= 1e-6 || !angle.is_finite() {
        camera.velocity = camera
            .velocity
            .scale((1.0 - CAMERA_FOLLOW_DAMPING * dt).max(0.0));
    } else {
        camera.velocity = camera
            .velocity
            .add(camera.velocity.scale(-CAMERA_FOLLOW_DAMPING * dt));
    }

    camera.velocity = camera
        .velocity
        .add(camera.focus.scale(-camera.velocity.dot(camera.focus)));
    let speed = camera.velocity.length();
    if speed > CAMERA_MAX_SPEED_RADIANS_PER_SECOND {
        camera.velocity = camera
            .velocity
            .scale(CAMERA_MAX_SPEED_RADIANS_PER_SECOND / speed);
    }

    let step = camera.velocity.length() * dt;
    if step > 1e-6 {
        let direction = camera.velocity.normalize();
        let rotation_axis = camera.focus.cross(direction).normalize();
        camera.focus = camera.focus.rotate_around(rotation_axis, step).normalize();
        camera.velocity = camera
            .velocity
            .add(camera.focus.scale(-camera.velocity.dot(camera.focus)));
    }

    angle = camera.focus.dot(target).clamp(-1.0, 1.0).acos();
    if angle > CAMERA_MAX_LAG_RADIANS {
        let tangent = target
            .add(camera.focus.scale(-target.dot(camera.focus)))
            .normalize();
        let rotation_axis = camera.focus.cross(tangent).normalize();
        camera.focus = camera
            .focus
            .rotate_around(rotation_axis, angle - CAMERA_MAX_LAG_RADIANS)
            .normalize();
        camera.velocity = camera
            .velocity
            .add(camera.focus.scale(-camera.velocity.dot(camera.focus)))
            .scale(0.65);
    }

    camera.previous_self_position = Some(current_self_position);
}

fn predicted_camera_target(camera: &CameraState, self_position: Vec3, dt: f64) -> Vec3 {
    let Some(previous) = camera.previous_self_position else {
        return self_position;
    };
    if dt <= 0.0 {
        return self_position;
    }

    let motion_distance = previous.dot(self_position).clamp(-1.0, 1.0).acos();
    let tangent_motion = self_position
        .scale(previous.dot(self_position))
        .add(previous.scale(-1.0));
    let motion_length = tangent_motion.length();
    if motion_length <= 1e-6 || !motion_distance.is_finite() {
        return self_position;
    }

    let lead =
        (motion_distance / dt * CAMERA_PREDICTION_SECONDS).min(CAMERA_MAX_PREDICTION_RADIANS);
    let motion_direction = tangent_motion.scale(1.0 / motion_length);
    let rotation_axis = self_position.cross(motion_direction).normalize();
    self_position.rotate_around(rotation_axis, lead).normalize()
}

fn stable_camera_up(focus: Vec3) -> Vec3 {
    let north = Vec3::Z;
    let up = north.add(focus.scale(-north.dot(focus)));
    if up.length() > 1e-6 {
        up.normalize()
    } else {
        let east = Vec3::new(0.0, 1.0, 0.0);
        east.add(focus.scale(-east.dot(focus))).normalize()
    }
}

fn shared_vec3(value: Vec3) -> world_render::Vec3 {
    world_render::Vec3::new(value.x, value.y, value.z)
}

#[allow(dead_code)]
fn builtin_head(id: &str) -> &str {
    match id {
        "default" => "0",
        "box" => "📦",
        "smile" => "🙂",
        "cowboy" => "🤠",
        "sunglasses" => "😎",
        "frog" => "🐸",
        "lobster" => "🦞",
        "sun" => "☀️",
        other => other,
    }
}

fn equipped_head_glyph(id: &str) -> &str {
    world_render::equipped_head_glyph(id)
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
const PLAYER_SELF: Color = Color(25, 215, 255);
const PLAYER_NPC: Color = Color(255, 190, 125);
const PLAYER_OTHER: Color = Color(245, 245, 245);
const HUD: Color = Color(120, 120, 120);
const STAR_DIM: Color = Color(70, 76, 92);
const STAR_MID: Color = Color(105, 114, 135);
const STAR_BRIGHT: Color = Color(155, 166, 190);
const FG: Color = Color(245, 245, 245);
const FG_DIM: Color = Color(190, 190, 190);
const FG_V_DIM: Color = Color(120, 120, 120);
const ACCENT_1: Color = Color(170, 235, 170);
const ACCENT_1_DIM: Color = Color(95, 165, 95);
const ACCENT_2: Color = Color(255, 190, 125);
const ACCENT_2_DIM: Color = Color(180, 125, 70);
const WIDE_CONTINUATION: char = '\0';
const PIXEL_COLORS: [Color; PIXEL_COLOR_COUNT] = [
    Color(255, 80, 80),
    Color(80, 180, 255),
    Color(255, 220, 80),
    Color(120, 235, 120),
    Color(220, 120, 255),
];

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
        let mut cursor = x;
        for ch in text.chars() {
            let width = char_display_width(ch);
            if width == 0 {
                continue;
            }
            if cursor + width as i32 > self.width as i32 {
                break;
            }
            self.put(cursor, y, ch, fg);
            for offset in 1..width {
                self.put_cell(
                    cursor + offset as i32,
                    y,
                    Cell {
                        ch: WIDE_CONTINUATION,
                        fg: Some(fg),
                    },
                );
            }
            cursor += width as i32;
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
        selected_pixel_color: usize,
        pixel_inventory: [u64; PIXEL_COLOR_COUNT],
    ) -> Vec<AnsiAction> {
        let current = render_app_frame(state, panel, selected_pixel_color, pixel_inventory);
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
    Preformatted {
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
    swatch_color: Option<Color>,
}

impl UiPanel {
    fn onboarding(detected: &[ai_usage::DetectedTool]) -> Self {
        let mut blocks = vec![
            UiBlock::Preformatted {
                color: ACCENT_1,
                lines: vec![
                    "   ___           _ _   _      __         __   __".to_string(),
                    "  / _ | ___ ____(_|_) | | /| / /__  ____/ /__/ /".to_string(),
                    " / __ |(_-</ __/ / /  | |/ |/ / _ \\/ __/ / _  / ".to_string(),
                    "/_/ |_/___/\\__/_/_/   |__/|__/\\___/_/ /_/\\_,_/".to_string(),
                ],
            },
            UiBlock::Paragraph {
                color: FG,
                lines: vec![
                    "- We read supported local coding agents CLI logs on this machine.".to_string(),
                    "- New local tokens become Ŧ points while you play.".to_string(),
                    "- Ŧ points create 🦞 yield.".to_string(),
                    "- 🦞 buy heads to look cool: 0, 📦, 🙂, 🤠, 😎, 🐸, 🦞, ☀️.".to_string(),
                    "- 🦞 buy pixel packs to leave a mark on the world.".to_string(),
                    "- The 🦞 leaderboard links each player to their X page.".to_string(),
                ],
            },
        ];

        if detected.is_empty() {
            blocks.push(UiBlock::Paragraph {
                color: ACCENT_2,
                lines: vec!["No supported local token source.".to_string()],
            });
            return Self {
                title: "Welcome to Ascii World".to_string(),
                blocks,
                prompt: Some("No supported token source was found. Press Escape.".to_string()),
            };
        }

        blocks.push(UiBlock::Table {
            columns: vec![
                "AI tool".to_string(),
                "records".to_string(),
                "what Ascii World can read".to_string(),
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
                    swatch_color: None,
                })
                .collect(),
        });

        Self {
            title: "Welcome to Ascii World".to_string(),
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
                    "Ascii World is joining the multiplayer world now.".to_string(),
                ],
            }],
            prompt: None,
        }
    }

    fn rewards(rewards: &[RewardNotice], can_close: bool) -> Self {
        let mut lines = vec!["Check-in rewards:".to_string()];
        for reward in rewards {
            if reward.lobsters == 0 {
                lines.push(format!("{} (streak {})", reward.label, reward.streak));
            } else {
                lines.push(format!(
                    "{}: +{} (streak {})",
                    reward.label,
                    format_lobsters(reward.lobsters),
                    reward.streak
                ));
            }
        }
        lines.push("Daily: 1000 plus 1000 per streak day.".to_string());
        lines.push("Weekly: starts on week 2, then 5000 per streak week.".to_string());

        Self {
            title: "Rewards".to_string(),
            blocks: vec![UiBlock::Paragraph {
                lines,
                color: ACCENT_1,
            }],
            prompt: Some(if can_close {
                "Press Enter to close.".to_string()
            } else {
                "Rewards will stay visible for a moment.".to_string()
            }),
        }
    }

    fn market(items: &[MarketItem], self_player: Option<&PlayerSnapshot>, selected: usize) -> Self {
        let balance = self_player.map(|player| player.lobsters).unwrap_or(0);
        let owned = self_player
            .map(|player| player.owned_heads.as_slice())
            .unwrap_or(&[]);
        let equipped = self_player
            .map(|player| player.equipped_head.as_str())
            .unwrap_or("default");
        let rows = items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let is_pixel = item.kind == "pixel";
                let pixel_count = item
                    .pixel_color
                    .and_then(|color| self_player.map(|player| player.owned_pixels[color]))
                    .unwrap_or(0);
                let is_owned = !is_pixel && owned.iter().any(|owned| owned == &item.id);
                let is_equipped = !is_pixel && equipped == item.id;
                let action = if is_pixel {
                    format!("owned {}", format_count(pixel_count))
                } else if is_equipped {
                    "equipped".to_string()
                } else if is_owned {
                    "owned".to_string()
                } else if balance >= item.price_lobsters {
                    "buy".to_string()
                } else {
                    "locked".to_string()
                };
                UiRow {
                    cells: vec![
                        if index == selected { ">" } else { " " }.to_string(),
                        item.head.clone(),
                        item.label.clone(),
                        format_lobsters(item.price_lobsters),
                        action,
                    ],
                    color: if index == selected {
                        ACCENT_1
                    } else if is_owned || is_pixel {
                        FG
                    } else {
                        FG_DIM
                    },
                    swatch_color: item
                        .pixel_color
                        .filter(|_| is_pixel)
                        .and_then(|color| PIXEL_COLORS.get(color).copied()),
                }
            })
            .collect();

        Self {
            title: format!("Market  balance {}", format_lobsters(balance)),
            blocks: vec![UiBlock::Table {
                columns: vec![
                    "".to_string(),
                    "head".to_string(),
                    "item".to_string(),
                    "price".to_string(),
                    "state".to_string(),
                ],
                rows,
            }],
            prompt: Some("Up/down select, Enter buy/equip, M close.".to_string()),
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

#[derive(Debug, Clone, Copy, Default)]
struct GameRenderOptions {
    show_header: bool,
    show_footer: bool,
    show_pixel_inventory: bool,
    show_lobster_leaderboard: bool,
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

fn render_app_frame(
    state: &VisibleGameState,
    panel: Option<(&UiPanel, f64)>,
    selected_pixel_color: usize,
    pixel_inventory: [u64; PIXEL_COLOR_COUNT],
) -> FrameBuffer {
    let width = state.width.max(1);
    let height = state.height.max(1);
    let layout = app_layout(width, height, panel.map(|(_, progress)| progress));
    let left_panel_visible = layout.panel.is_some() && layout.game.x > 0;
    let game_options = GameRenderOptions {
        show_header: true,
        show_footer: true,
        show_pixel_inventory: true,
        show_lobster_leaderboard: width > 150 && !left_panel_visible,
    };
    let game_state = VisibleGameState {
        width: layout.game.width.max(1),
        height: layout.game.height.max(1),
        ..state.clone()
    };
    let game = render_game_frame(
        &game_state,
        game_options,
        selected_pixel_color,
        pixel_inventory,
    );
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
        UiBlock::Preformatted { lines, color } => {
            for text in lines {
                if *line > max_y {
                    return;
                }
                draw_clipped_text(frame, x, *line, text, inner_width, *color);
                *line += 1;
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
                if let Some(color) = row.swatch_color {
                    frame.put(x + 4, *line, '█', color);
                    frame.put(x + 5, *line, '█', color);
                }
                *line += 1;
            }
        }
    }
}

fn format_table_row(cells: &[String]) -> String {
    if cells.len() == 5 {
        let marker = cells.first().map(String::as_str).unwrap_or("");
        let head = cells.get(1).map(String::as_str).unwrap_or("");
        let item = cells.get(2).map(String::as_str).unwrap_or("");
        let price = cells.get(3).map(String::as_str).unwrap_or("");
        let state = cells.get(4).map(String::as_str).unwrap_or("");
        return format!("{marker:<1}  {head:<3}  {item:<12}  {price:>9}  {state:<8}");
    }
    let first = cells.first().map(String::as_str).unwrap_or("");
    let second = cells.get(1).map(String::as_str).unwrap_or("");
    let third = cells.get(2).map(String::as_str).unwrap_or("");
    format!("{first:<14} {second:>7}  {third}")
}

fn draw_clipped_text(frame: &mut FrameBuffer, x: i32, y: i32, text: &str, width: usize, fg: Color) {
    let clipped = clip_to_display_width(text, width);
    frame.text(x, y, &clipped, fg);
}

fn wrap_text(text: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return Vec::new();
    }
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        let word_width = display_width(word);
        let next_len = if current.is_empty() {
            word_width
        } else {
            display_width(&current) + 1 + word_width
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

#[allow(unreachable_code)]
fn render_game_frame(
    state: &VisibleGameState,
    options: GameRenderOptions,
    selected_pixel_color: usize,
    pixel_inventory: [u64; PIXEL_COLOR_COUNT],
) -> FrameBuffer {
    let shared_state = world_render::VisibleGameState {
        width: state.width,
        height: state.height,
        planet_diameter_cells: state.planet_diameter_cells,
        camera_focus: shared_vec3(state.camera_focus),
        camera_up: shared_vec3(state.camera_up),
        tokens_since_joining: state.tokens_since_joining,
        tokens_all_time: state.tokens_all_time,
        lobsters: state.lobsters,
        lobster_yield_per_hour: state.lobster_yield_per_hour,
        leaderboard: state
            .leaderboard
            .iter()
            .map(|entry| world_render::LeaderboardEntry {
                username: entry.username.clone(),
                lobsters: entry.lobsters,
                all_time_tokens: entry.all_time_tokens,
                profile_url: entry.profile_url.clone(),
            })
            .collect(),
        placed_pixels: state
            .placed_pixels
            .iter()
            .map(|pixel| world_render::PlacedPixel {
                position: pixel.position,
                color: pixel.color,
            })
            .collect(),
        pickups: state
            .pickups
            .iter()
            .map(|pickup| world_render::PickupSnapshot {
                position: pickup.position,
                emoji: pickup.emoji.clone(),
            })
            .collect(),
        players: state
            .players
            .iter()
            .map(|player| world_render::VisiblePlayer {
                name: player.name.clone(),
                position: shared_vec3(player.position),
                is_self: player.is_self,
                is_fake: player.is_fake,
                points: player.points,
                lobsters: player.lobsters,
                lobster_yield_per_hour: player.lobster_yield_per_hour,
                equipped_head: player.equipped_head.clone(),
                jump_height: player.jump_height,
                jump_leg_pose: player.jump_leg_pose,
                pickup_reward_lobsters: player.pickup_reward_lobsters,
                facing: player.facing,
                walking_phase: player.walking_phase,
            })
            .collect(),
    };
    let shared = world_render::render_game_frame(
        &shared_state,
        world_render::GameRenderOptions {
            show_header: options.show_header,
            show_footer: options.show_footer,
            show_pixel_inventory: options.show_pixel_inventory,
            show_lobster_leaderboard: options.show_lobster_leaderboard,
        },
        selected_pixel_color,
        pixel_inventory,
    );
    return FrameBuffer {
        width: shared.width,
        height: shared.height,
        cells: shared
            .cells
            .into_iter()
            .map(|cell| Cell {
                ch: cell.ch,
                fg: cell.fg.map(|world_render::Color(r, g, b)| Color(r, g, b)),
            })
            .collect(),
    };

    let width = state.width.max(1);
    let height = state.height.max(1);
    let mut frame = FrameBuffer::new(width, height);
    draw_starfield(&mut frame);
    let leaderboard_visible =
        options.show_lobster_leaderboard && should_render_lobster_leaderboard(width, height);
    let leaderboard_width = lobster_leaderboard_width(width);
    let cx = if leaderboard_visible {
        width as f64 / 2.0 - leaderboard_width as f64 * 0.28
    } else {
        width as f64 / 2.0
    };
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
                frame.put_cell(x as i32, y as i32, Cell::default());
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

    for pixel in &state.placed_pixels {
        if pixel.color >= PIXEL_COLOR_COUNT {
            continue;
        }
        let Some(position) = Vec3::from_array(pixel.position) else {
            continue;
        };
        if position.dot(view_normal) <= 0.0 {
            continue;
        }
        let px = position.dot(right);
        let py = position.dot(up);
        let sx = (cx + px * radius_x).round() as i32;
        let sy = (cy - py * radius_y).round() as i32;
        draw_pixel_block(&mut frame, sx, sy, PIXEL_COLORS[pixel.color]);
    }

    for pickup in &state.pickups {
        let Some(position) = Vec3::from_array(pickup.position) else {
            continue;
        };
        if position.dot(view_normal) <= 0.0 {
            continue;
        }
        let px = position.dot(right);
        let py = position.dot(up);
        let sx = (cx + px * radius_x).round() as i32;
        let sy = (cy - py * radius_y).round() as i32;
        frame.text(sx, sy, &pickup.emoji, ACCENT_2);
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

    frame.text(0, 0, HEADER_CONTROLS, HUD);
    if let Some(economy) = economy_header(state, width as usize) {
        let economy_x = width as i32 - display_width(&economy) as i32 - 1;
        frame.text(economy_x.max(0), 0, &economy, HUD);
    }
    draw_pixel_inventory(&mut frame, selected_pixel_color, pixel_inventory);
    if leaderboard_visible {
        draw_lobster_leaderboard(&mut frame, &state.leaderboard);
    }
    draw_footer(&mut frame);
    frame
}

fn economy_header(state: &VisibleGameState, width: usize) -> Option<String> {
    let available = width
        .saturating_sub(display_width(HEADER_CONTROLS))
        .saturating_sub(3);
    if available == 0 {
        return None;
    }

    let since = format_token_points(state.tokens_since_joining);
    let all_time = format_token_points(state.tokens_all_time);
    let yield_rate = format_lobsters_per_hour(state.lobster_yield_per_hour);
    let balance = format_lobsters(state.lobsters);
    let options = [
        format!(
            "tokens all time {all_time}  tokens since joining {since}  => yield {yield_rate}/h  balance {balance}"
        ),
        format!("tokens since joining {since}  => yield {yield_rate}/h  balance {balance}"),
        format!("{since}  => {yield_rate}/h  {balance}"),
    ];
    options
        .into_iter()
        .find(|option| display_width(option) <= available)
}

fn draw_pixel_inventory(
    frame: &mut FrameBuffer,
    selected_color: usize,
    inventory: [u64; PIXEL_COLOR_COUNT],
) {
    if frame.height < 3 || !inventory.iter().any(|count| *count > 0) {
        return;
    }
    let y = frame.height as i32 - 2;
    let mut x = 0;
    frame.text(x, y, "pixels ", HUD);
    x += 7;
    for color in 0..PIXEL_COLOR_COUNT {
        let count = inventory[color];
        if count == 0 {
            continue;
        }
        let shortcut = color + 1;
        let label = if color == selected_color {
            format!("[{shortcut}:{}]", format_count(count))
        } else {
            format!("{shortcut}:{}", format_count(count))
        };
        draw_pixel_block(frame, x, y, PIXEL_COLORS[color]);
        x += 3;
        frame.text(x, y, &label, HUD);
        x += display_width(&label) as i32 + 1;
    }
    frame.text(x, y, "P place", HUD);
}

fn draw_pixel_block(frame: &mut FrameBuffer, x: i32, y: i32, color: Color) {
    frame.put(x, y, '█', color);
    frame.put(x + 1, y, '█', color);
}

fn draw_footer(frame: &mut FrameBuffer) {
    if frame.height == 0 {
        return;
    }
    let width = frame.width as usize;
    let text_width = display_width(FOOTER_TEXT);
    let x = if text_width >= width {
        0
    } else {
        ((width - text_width) / 2) as i32
    };
    draw_clipped_text(
        frame,
        x,
        frame.height as i32 - 1,
        FOOTER_TEXT,
        width.saturating_sub(x.max(0) as usize),
        HUD,
    );
}

fn display_width(text: &str) -> usize {
    UnicodeWidthStr::width(text)
}

fn char_display_width(ch: char) -> usize {
    UnicodeWidthChar::width(ch).unwrap_or(0)
}

fn clip_to_display_width(text: &str, width: usize) -> String {
    let mut clipped = String::new();
    let mut used = 0;
    for ch in text.chars() {
        let ch_width = char_display_width(ch);
        if ch_width == 0 {
            clipped.push(ch);
            continue;
        }
        if used + ch_width > width {
            break;
        }
        clipped.push(ch);
        used += ch_width;
    }
    clipped
}

fn draw_lobster_leaderboard(frame: &mut FrameBuffer, leaderboard: &[LeaderboardEntry]) {
    if !should_render_lobster_leaderboard(frame.width, frame.height) {
        return;
    }

    let leaders = lobster_leaders(leaderboard);
    if leaders.is_empty() {
        return;
    }

    let panel_width = lobster_leaderboard_width(frame.width);
    let x = frame.width as i32 - panel_width as i32 - 1;
    let mut line_y = 3;
    let max_y = frame.height as i32 - 2;
    draw_clipped_text(frame, x, 2, "🦞 leaders", panel_width, ACCENT_2);

    for (index, player) in leaders.into_iter().enumerate() {
        if line_y > max_y {
            break;
        }
        let line = format!(
            "{:>2}. @{:<12} {:>8} {}",
            index + 1,
            player.username,
            format_lobsters(player.lobsters),
            player.profile_url
        );
        draw_clipped_text(frame, x, line_y, &line, panel_width, HUD);
        line_y += 1;
        if index < 3 && line_y <= max_y {
            let tokens = format!(
                "    tokens all time {}",
                format_token_points(player.all_time_tokens)
            );
            draw_clipped_text(frame, x, line_y, &tokens, panel_width, FG_DIM);
            line_y += 1;
        }
    }
}

fn should_render_lobster_leaderboard(width: u16, height: u16) -> bool {
    width > 150 && height >= 7
}

fn lobster_leaderboard_width(width: u16) -> usize {
    (width as usize - 2).min(58)
}

fn lobster_leaders(leaderboard: &[LeaderboardEntry]) -> Vec<&LeaderboardEntry> {
    let mut leaders = leaderboard
        .iter()
        .filter(|entry| !entry.username.trim().is_empty())
        .collect::<Vec<_>>();
    leaders.sort_by(|a, b| {
        b.lobsters
            .cmp(&a.lobsters)
            .then_with(|| a.username.to_lowercase().cmp(&b.username.to_lowercase()))
    });
    leaders.truncate(10);
    leaders
}

fn draw_starfield(frame: &mut FrameBuffer) {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0);
    for y in 0..frame.height {
        for x in 0..frame.width {
            let seed = hash_coords(x as i32, y as i32, 0x5f3759df);
            let placement = unit_from_hash(seed);
            let large = placement < 0.0022;
            let small = placement < 0.019;
            if !large && !small {
                continue;
            }

            if large {
                let phase = unit_from_hash(seed ^ 0x9e3779b9) * 12.0;
                let noise = perlin3(x as f64 * 0.075, y as f64 * 0.12, time * 0.18 + phase);
                let level = (((noise + 1.0) * 1.5).floor() as i32).clamp(0, 2);
                let (ch, color) = match level {
                    0 => ('+', STAR_MID),
                    1 => ('*', STAR_MID),
                    _ => ('*', STAR_BRIGHT),
                };
                frame.put(x as i32, y as i32, ch, color);
            } else {
                let color = if placement < 0.006 {
                    STAR_MID
                } else {
                    STAR_DIM
                };
                frame.put(x as i32, y as i32, '.', color);
            }
        }
    }
}

fn perlin3(x: f64, y: f64, z: f64) -> f64 {
    let xi = x.floor() as i32;
    let yi = y.floor() as i32;
    let zi = z.floor() as i32;
    let xf = x - xi as f64;
    let yf = y - yi as f64;
    let zf = z - zi as f64;
    let u = perlin_fade(xf);
    let v = perlin_fade(yf);
    let w = perlin_fade(zf);

    let n000 = gradient_dot(xi, yi, zi, xf, yf, zf);
    let n100 = gradient_dot(xi + 1, yi, zi, xf - 1.0, yf, zf);
    let n010 = gradient_dot(xi, yi + 1, zi, xf, yf - 1.0, zf);
    let n110 = gradient_dot(xi + 1, yi + 1, zi, xf - 1.0, yf - 1.0, zf);
    let n001 = gradient_dot(xi, yi, zi + 1, xf, yf, zf - 1.0);
    let n101 = gradient_dot(xi + 1, yi, zi + 1, xf - 1.0, yf, zf - 1.0);
    let n011 = gradient_dot(xi, yi + 1, zi + 1, xf, yf - 1.0, zf - 1.0);
    let n111 = gradient_dot(xi + 1, yi + 1, zi + 1, xf - 1.0, yf - 1.0, zf - 1.0);

    let x00 = lerp(n000, n100, u);
    let x10 = lerp(n010, n110, u);
    let x01 = lerp(n001, n101, u);
    let x11 = lerp(n011, n111, u);
    let y0 = lerp(x00, x10, v);
    let y1 = lerp(x01, x11, v);
    lerp(y0, y1, w).clamp(-1.0, 1.0)
}

fn gradient_dot(ix: i32, iy: i32, iz: i32, x: f64, y: f64, z: f64) -> f64 {
    (match hash_coords3(ix, iy, iz) & 15 {
        0 => x + y,
        1 => -x + y,
        2 => x - y,
        3 => -x - y,
        4 => x + z,
        5 => -x + z,
        6 => x - z,
        7 => -x - z,
        8 => y + z,
        9 => -y + z,
        10 => y - z,
        11 => -y - z,
        12 => x + y,
        13 => -x + y,
        14 => -y + z,
        _ => -y - z,
    }) * 0.7071067811865475
}

fn perlin_fade(t: f64) -> f64 {
    t * t * t * (t * (t * 6.0 - 15.0) + 10.0)
}

fn lerp(a: f64, b: f64, t: f64) -> f64 {
    a + (b - a) * t
}

fn hash_coords(x: i32, y: i32, seed: u32) -> u32 {
    hash_mix((x as u32).wrapping_mul(0x8da6b343) ^ (y as u32).wrapping_mul(0xd8163841) ^ seed)
}

fn hash_coords3(x: i32, y: i32, z: i32) -> u32 {
    hash_mix(
        (x as u32).wrapping_mul(0x8da6b343)
            ^ (y as u32).wrapping_mul(0xd8163841)
            ^ (z as u32).wrapping_mul(0xcb1ab31f),
    )
}

fn hash_mix(mut value: u32) -> u32 {
    value ^= value >> 16;
    value = value.wrapping_mul(0x7feb352d);
    value ^= value >> 15;
    value = value.wrapping_mul(0x846ca68b);
    value ^ (value >> 16)
}

fn unit_from_hash(value: u32) -> f64 {
    value as f64 / u32::MAX as f64
}

fn format_count(value: u64) -> String {
    let (scaled, suffix) = if value >= 1_000_000_000 {
        (value as f64 / 1_000_000_000.0, "B")
    } else if value >= 1_000_000 {
        (value as f64 / 1_000_000.0, "M")
    } else if value >= 1_000 {
        (value as f64 / 1_000.0, "k")
    } else {
        return value.to_string();
    };

    let tenths = (scaled * 10.0).round() as u64;
    if tenths % 10 == 0 {
        format!("{}{suffix}", tenths / 10)
    } else {
        format!("{}.{}{suffix}", tenths / 10, tenths % 10)
    }
}

fn format_token_points(tokens: u64) -> String {
    format!("Ŧ{}", format_count(tokens))
}

fn format_lobsters(lobsters: u64) -> String {
    format!("🦞{}", format_count(lobsters))
}

fn lobster_yield_per_hour(total_tokens: u64, rate_token_unit: u64) -> f64 {
    assert!(
        rate_token_unit > 0,
        "economy rate token unit must be nonzero"
    );
    total_tokens as f64 / rate_token_unit as f64 * 60.0
}

fn format_lobsters_per_hour(lobsters_per_hour: f64) -> String {
    format!("🦞{}", format_rate(lobsters_per_hour))
}

fn format_rate(value: f64) -> String {
    if !value.is_finite() || value < 0.0 {
        return "invalid".to_string();
    }
    if value == 0.0 {
        return "0".to_string();
    }

    let rounded = (value * 1_000.0).round() / 1_000.0;
    let mut text = format!("{rounded:.3}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
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
        PLAYER_NPC
    } else {
        PLAYER_OTHER
    };
    let facing_right = player.facing > 0;
    let is_airborne = player.jump_height > JUMP_GROUND_EPSILON;
    let legs = if is_airborne {
        match player.jump_leg_pose {
            -1 => "//",
            2 => "\\\\",
            1 => "/\\",
            _ => "||",
        }
    } else if facing_right {
        match player.walking_phase % 4 {
            0 => "/|",
            1 => "|/",
            2 => "|\\",
            _ => "||",
        }
    } else {
        match player.walking_phase % 4 {
            0 => "|\\",
            1 => "\\|",
            2 => "/|",
            _ => "||",
        }
    };
    let lift = player.jump_height.ceil().clamp(0.0, 2.0) as i32;
    if lift > 0 {
        let shadow = if player.jump_height > 1.2 {
            " . "
        } else {
            "..."
        };
        frame.text(x - 1, y, shadow, FG_V_DIM);
    }
    let body_y = y - lift;
    let head = if player.equipped_head.trim().is_empty() {
        "0"
    } else {
        player.equipped_head.as_str()
    };
    let head_row = if head.is_ascii() {
        format!(" {head} ")
    } else {
        format!("{head} ")
    };
    let head_shift = facing_right && head != "0";
    let head_x = if head.is_ascii() { x - 1 } else { x } - i32::from(head_shift);
    let chest = if facing_right { "-]-" } else { "-[-" };
    let legs_x = if facing_right { x - 2 } else { x - 1 };
    let rows = [
        (0, head_x, head_row),
        (1, x - 1, chest.to_string()),
        (2, legs_x, format!(" {legs}")),
    ];
    for (dy, row_x, text) in rows {
        frame.text(row_x, body_y - 2 + dy, &text, color);
    }
    if !player.name.is_empty() {
        let label = format!("@{}", player.name);
        let label_x = x - (display_width(&label) as i32 / 2);
        if player.pickup_reward_lobsters > 0 {
            let reward = format!("+{}", format_lobsters(player.pickup_reward_lobsters));
            let reward_x = x - (display_width(&reward) as i32 / 2);
            frame.text(reward_x, body_y - 5, &reward, ACCENT_2);
        }
        if !player.is_fake {
            let points = format_token_points(player.points);
            let points_x = x - (display_width(&points) as i32 / 2);
            let points_color = if player.is_self { ACCENT_1 } else { FG_DIM };
            frame.text(points_x, body_y - 3, &points, points_color);
            frame.text(label_x, body_y - 4, &label, HUD);
        } else {
            frame.text(label_x, body_y - 3, &label, HUD);
        }
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
            if cell.ch == WIDE_CONTINUATION {
                x += 1;
                continue;
            }
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
                if next.ch == WIDE_CONTINUATION {
                    break;
                }
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
            let text_width = display_width(&text) as u16;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_lobsters_are_abbreviated() {
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(7_500), "7.5k");
        assert_eq!(format_count(10_000), "10k");
        assert_eq!(format_count(10_500), "10.5k");
        assert_eq!(format_count(10_000_000), "10M");
        assert_eq!(format_count(10_500_000), "10.5M");
        assert_eq!(format_token_points(10_500_000), "Ŧ10.5M");
        assert_eq!(format_lobsters(10_000_000_000), "🦞10B");
        assert_eq!(format_lobsters_per_hour(0.0), "🦞0");
        assert_eq!(format_lobsters_per_hour(0.06), "🦞0.06");
        assert_eq!(format_lobsters_per_hour(0.0014), "🦞0.001");
        assert_eq!(format_lobsters_per_hour(7.5), "🦞7.5");
        assert_eq!(format_lobsters_per_hour(f64::NAN), "🦞invalid");
        assert_eq!(
            format_lobsters_per_hour(lobster_yield_per_hour(1_800_000, 6_000_000,)),
            "🦞18"
        );
        assert_eq!(
            format_lobsters_per_hour(lobster_yield_per_hour(1_000_000, 6_000_000,)),
            "🦞10"
        );
        assert_eq!(builtin_head("default"), "0");
        assert_eq!(builtin_head("box"), "📦");
        assert_eq!(equipped_head_glyph("default"), "0");
    }

    #[test]
    fn wide_emoji_occupy_two_terminal_cells() {
        assert_eq!(display_width("balance 🦞2312k"), 15);

        let mut frame = FrameBuffer::new(8, 1);
        frame.text(0, 0, "a🦞b", HUD);
        assert_eq!(frame.get(0, 0).ch, 'a');
        assert_eq!(frame.get(1, 0).ch, '🦞');
        assert_eq!(frame.get(2, 0).ch, WIDE_CONTINUATION);
        assert_eq!(frame.get(3, 0).ch, 'b');
    }

    #[test]
    fn websocket_unauthorized_is_auth_rejected_through_error_chain() {
        let error = anyhow!("HTTP error: 401 Unauthorized").context("failed to connect websocket");
        assert!(is_auth_rejected(&error));
    }
}
