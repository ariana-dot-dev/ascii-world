use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, Context};
use clap::Parser;
use futures_util::SinkExt;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use walkdir::WalkDir;

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
    ai_usage_consent: bool,
    #[serde(default)]
    enabled_ai_tools: Vec<String>,
    #[serde(default)]
    ai_usage_baseline: Option<ai_usage::UsageSnapshot>,
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

    if !state.onboarding_completed || !state.ai_usage_consent {
        let should_play = run_onboarding(&mut state, &detected_ai_tools)?;
        save_state(&state_path, &state)?;
        if !should_play {
            return Ok(());
        }
    }
    if state.ai_usage_consent && state.ai_usage_baseline.is_none() {
        state.ai_usage_baseline = Some(ai_usage::scan_enabled(
            &detected_ai_tools,
            &state.enabled_ai_tools,
        ));
        save_state(&state_path, &state)?;
    }

    loop {
        state.counter += 1;
        save_state(&state_path, &state)?;

        let http_connected = check_http(&backend_url).await;
        let ws_connected = check_ws(&backend_url).await;
        let connected = http_connected || ws_connected;
        let status = if connected {
            "connected"
        } else {
            "disconnected"
        };
        let usage = if state.ai_usage_consent {
            let snapshot = ai_usage::scan_enabled(&detected_ai_tools, &state.enabled_ai_tools);
            let snapshot = snapshot.saturating_sub(state.ai_usage_baseline.as_ref());
            format!(
                " ai_tokens total={} input={} output={} cached={} tools={}",
                snapshot.total_tokens(),
                snapshot.input_tokens,
                snapshot.output_tokens,
                snapshot.cached_tokens,
                snapshot.tools.len()
            )
        } else {
            String::new()
        };
        println!("ws/http {status} hello world {}{usage}", state.counter);

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn run_onboarding(
    state: &mut PersistedState,
    detected: &[ai_usage::DetectedTool],
) -> anyhow::Result<bool> {
    ui::clear();
    ui::title();

    if detected.is_empty() {
        ui::empty_state();
        state.onboarding_completed = true;
        state.ai_usage_consent = false;
        state.enabled_ai_tools.clear();
        state.ai_usage_baseline = None;
        return Ok(false);
    }

    ui::tool_table(detected);
    let consent = ui::confirm("start local token tracking?");

    state.ai_usage_consent = consent;
    state.enabled_ai_tools.clear();
    state.ai_usage_baseline = None;

    if !consent {
        state.onboarding_completed = false;
        ui::bye();
        return Ok(false);
    }

    state.onboarding_completed = true;
    state.enabled_ai_tools = detected.iter().map(|tool| tool.id.to_string()).collect();
    state.ai_usage_baseline = Some(ai_usage::scan_enabled(detected, &state.enabled_ai_tools));
    Ok(true)
}

mod ui {
    use super::*;

    const RESET: &str = "\x1b[0m";
    const FG: &str = "\x1b[38;2;245;245;245m";
    const FG_DIM: &str = "\x1b[38;2;190;190;190m";
    const FG_V_DIM: &str = "\x1b[38;2;120;120;120m";
    const ACCENT_1: &str = "\x1b[38;2;170;235;170m";
    const ACCENT_1_DIM: &str = "\x1b[38;2;95;165;95m";
    const ACCENT_2: &str = "\x1b[38;2;255;190;125m";
    const ACCENT_2_DIM: &str = "\x1b[38;2;180;125;70m";

    pub fn clear() {
        print!("\x1b[2J\x1b[H");
        let _ = io::stdout().flush();
    }

    pub fn title() {
        println!("{FG}Welcome to {ACCENT_1}Tokenizers{RESET}");
        println!("{FG}To play the game, we need to track your local token consumption.{RESET}");
        println!("{FG_DIM}We count from now on to trigger game elements and award points.{RESET}");
        println!("{FG_DIM}Multiplayer is hosted, but your local usage data is not uploaded or stored in the cloud.{RESET}\n");
    }

    pub fn empty_state() {
        println!("{FG}+----------------------+----------+{RESET}");
        println!("{FG}| source               | status   |{RESET}");
        println!("{FG}+----------------------+----------+{RESET}");
        println!("{FG}| ai tools             | {ACCENT_2}none{FG}     |{RESET}");
        println!("{FG}+----------------------+----------+{RESET}");
        println!("{FG_DIM}No supported local token source was found.{RESET}");
    }

    pub fn bye() {
        println!("{FG_DIM}Tokenizers needs local token tracking to play.{RESET}");
    }

    pub fn tool_table(detected: &[ai_usage::DetectedTool]) {
        println!("{FG}+----------------+---------+----------------------+{RESET}");
        println!("{FG}| tool           | sources | access               |{RESET}");
        println!("{FG}+----------------+---------+----------------------+{RESET}");
        for tool in detected {
            let count = color_pad(&tool.sources.len().to_string(), 7, ACCENT_1);
            let (method, color) = short_method(tool.collection_method);
            let access = color_pad(method, 20, color);
            println!(
                "{FG}| {tool:<14} | {count} | {access} |{RESET}",
                tool = tool.display_name,
            );
        }
        println!("{FG}+----------------+---------+----------------------+{RESET}");
        println!("{FG_V_DIM}These sources stay on this machine.{RESET}\n");
    }

    pub fn confirm(prompt: &str) -> bool {
        print!("{ACCENT_1}?{RESET} {FG}{prompt}{RESET} {FG_DIM}[y/N]{RESET} ");
        let _ = io::stdout().flush();

        let mut answer = String::new();
        if io::stdin().read_line(&mut answer).is_err() {
            println!();
            return false;
        }
        matches!(answer.trim(), "y" | "Y" | "yes" | "YES" | "Yes")
    }

    fn color_pad(value: &str, width: usize, color: &str) -> String {
        format!("{color}{value:<width$}{FG}", width = width)
    }

    fn short_method(method: &str) -> (&'static str, &'static str) {
        if method.contains("SQLite") {
            ("sqlite+files", ACCENT_2_DIM)
        } else if method.contains("rollout") {
            ("jsonl", ACCENT_1_DIM)
        } else if method.contains("JSONL") || method.contains("transcript") {
            ("jsonl files", ACCENT_1_DIM)
        } else {
            ("files", FG_DIM)
        }
    }
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

async fn check_http(backend_url: &str) -> bool {
    let Ok(url) = endpoint_url(backend_url, "health", false) else {
        return false;
    };
    let client = reqwest::Client::new();
    let Ok(response) = client.get(url).timeout(Duration::from_secs(3)).send().await else {
        return false;
    };
    response.status().is_success()
}

async fn check_ws(backend_url: &str) -> bool {
    let Ok(ws_url) = endpoint_url(backend_url, "ws", true) else {
        return false;
    };

    let Ok(Ok((mut socket, _))) =
        tokio::time::timeout(Duration::from_secs(3), connect_async(ws_url)).await
    else {
        return false;
    };
    let _ = socket.send(Message::Text("ping".into())).await;
    true
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
