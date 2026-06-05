use std::{
    cmp::Ordering,
    f64::consts::{FRAC_PI_2, TAU},
    time::{SystemTime, UNIX_EPOCH},
};

use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

mod land_mask;

pub const PIXEL_COLOR_COUNT: usize = 5;
const JUMP_GROUND_EPSILON: f64 = 0.02;
const HEADER_CONTROLS: &str = "arrows move, space jumps, M market, esc exits";
const FOOTER_TEXT: &str =
    "Made and hosted by agents on https://box.ascii.dev, the cheapest and most powerful AI sandboxes";
const WIDE_CONTINUATION: char = '\0';

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Vec3 {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Vec3 {
    pub const X: Self = Self {
        x: 1.0,
        y: 0.0,
        z: 0.0,
    };
    pub const Z: Self = Self {
        x: 0.0,
        y: 0.0,
        z: 1.0,
    };

    pub fn new(x: f64, y: f64, z: f64) -> Self {
        Self { x, y, z }
    }

    pub fn from_lat_lon(lat: f64, lon: f64) -> Self {
        let lat = lat.clamp(-FRAC_PI_2, FRAC_PI_2);
        let cos_lat = lat.cos();
        Self::new(cos_lat * lon.cos(), cos_lat * lon.sin(), lat.sin())
    }

    pub fn from_array(value: [f64; 3]) -> Option<Self> {
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

    pub fn dot(self, other: Self) -> f64 {
        self.x * other.x + self.y * other.y + self.z * other.z
    }

    pub fn cross(self, other: Self) -> Self {
        Self {
            x: self.y * other.z - self.z * other.y,
            y: self.z * other.x - self.x * other.z,
            z: self.x * other.y - self.y * other.x,
        }
    }

    pub fn add(self, other: Self) -> Self {
        Self {
            x: self.x + other.x,
            y: self.y + other.y,
            z: self.z + other.z,
        }
    }

    pub fn scale(self, scalar: f64) -> Self {
        Self {
            x: self.x * scalar,
            y: self.y * scalar,
            z: self.z * scalar,
        }
    }

    pub fn length(self) -> f64 {
        self.dot(self).sqrt()
    }

    pub fn normalize(self) -> Self {
        let length = self.length();
        if length <= 1e-9 {
            Self::X
        } else {
            self.scale(1.0 / length)
        }
    }
}

pub fn stable_camera_up(focus: Vec3) -> Vec3 {
    let north = Vec3::Z;
    let up = north.add(focus.scale(-north.dot(focus)));
    if up.length() > 1e-6 {
        up.normalize()
    } else {
        let east = Vec3::new(0.0, 1.0, 0.0);
        east.add(focus.scale(-east.dot(focus))).normalize()
    }
}

pub fn orbit_camera(elapsed_seconds: f64) -> (Vec3, Vec3) {
    let seconds = if elapsed_seconds.is_finite() {
        elapsed_seconds.max(0.0)
    } else {
        0.0
    };
    let longitude = seconds * TAU / 96.0;
    let latitude = 0.18 * (seconds * TAU / 64.0).sin();
    let focus = Vec3::from_lat_lon(latitude, longitude).normalize();
    (focus, stable_camera_up(focus))
}

pub fn orbit_camera_now() -> (Vec3, Vec3) {
    let elapsed_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs_f64())
        .unwrap_or(0.0);
    orbit_camera(elapsed_seconds)
}

pub fn equipped_head_glyph(id: &str) -> &str {
    match id {
        "default" => "0",
        "box" => "\u{1f4e6}",
        "smile" => "\u{1f642}",
        "cowboy" => "\u{1f920}",
        "sunglasses" => "\u{1f60e}",
        "frog" => "\u{1f438}",
        "lobster" => "\u{1f99e}",
        "sun" => "\u{2600}\u{fe0f}",
        other => other,
    }
}

#[derive(Debug, Clone)]
pub struct VisibleGameState {
    pub width: u16,
    pub height: u16,
    pub planet_diameter_cells: f64,
    pub camera_focus: Vec3,
    pub camera_up: Vec3,
    pub tokens_since_joining: u64,
    pub tokens_all_time: u64,
    pub lobsters: u64,
    pub lobster_yield_per_hour: f64,
    pub leaderboard: Vec<LeaderboardEntry>,
    pub placed_pixels: Vec<PlacedPixel>,
    pub pickups: Vec<PickupSnapshot>,
    pub players: Vec<VisiblePlayer>,
}

#[derive(Debug, Clone)]
pub struct VisiblePlayer {
    pub name: String,
    pub position: Vec3,
    pub is_self: bool,
    pub is_fake: bool,
    pub points: u64,
    pub lobsters: u64,
    pub lobster_yield_per_hour: f64,
    pub equipped_head: String,
    pub jump_height: f64,
    pub jump_leg_pose: i8,
    pub pickup_reward_lobsters: u64,
    pub facing: i8,
    pub walking_phase: u64,
}

#[derive(Debug, Clone)]
pub struct LeaderboardEntry {
    pub username: String,
    pub lobsters: u64,
    pub all_time_tokens: u64,
    pub profile_url: String,
}

#[derive(Debug, Clone)]
pub struct PlacedPixel {
    pub position: [f64; 3],
    pub color: usize,
}

#[derive(Debug, Clone)]
pub struct PickupSnapshot {
    pub position: [f64; 3],
    pub emoji: String,
}

#[derive(Debug, Clone, Copy)]
pub struct GameRenderOptions {
    pub show_header: bool,
    pub show_footer: bool,
    pub show_pixel_inventory: bool,
    pub show_lobster_leaderboard: bool,
}

impl Default for GameRenderOptions {
    fn default() -> Self {
        Self {
            show_header: true,
            show_footer: true,
            show_pixel_inventory: true,
            show_lobster_leaderboard: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Color(pub u8, pub u8, pub u8);

const PLANET_OUTLINE: Color = Color(95, 165, 95);
const PLANET_LAND: Color = Color(80, 145, 80);
const PLANET_WATER: Color = Color(45, 75, 110);
const PLAYER_SELF: Color = Color(80, 180, 255);
const PLAYER_NPC: Color = Color(255, 190, 125);
const PLAYER_OTHER: Color = Color(245, 245, 245);
const HUD: Color = Color(120, 120, 120);
const STAR_DIM: Color = Color(70, 76, 92);
const STAR_MID: Color = Color(105, 114, 135);
const STAR_BRIGHT: Color = Color(155, 166, 190);
const FG_V_DIM: Color = Color(120, 120, 120);
const ACCENT_1: Color = Color(170, 235, 170);
const ACCENT_2: Color = Color(255, 190, 125);
const PIXEL_COLORS: [Color; PIXEL_COLOR_COUNT] = [
    Color(255, 80, 80),
    Color(80, 180, 255),
    Color(255, 220, 80),
    Color(120, 235, 120),
    Color(220, 120, 255),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cell {
    pub ch: char,
    pub fg: Option<Color>,
}

impl Default for Cell {
    fn default() -> Self {
        Self { ch: ' ', fg: None }
    }
}

impl Cell {
    pub fn is_wide_continuation(self) -> bool {
        self.ch == WIDE_CONTINUATION
    }
}

#[derive(Debug, Clone)]
pub struct FrameBuffer {
    pub width: u16,
    pub height: u16,
    pub cells: Vec<Cell>,
}

impl FrameBuffer {
    pub fn new(width: u16, height: u16) -> Self {
        Self {
            width,
            height,
            cells: vec![Cell::default(); width as usize * height as usize],
        }
    }

    fn index(&self, x: u16, y: u16) -> usize {
        y as usize * self.width as usize + x as usize
    }

    pub fn get(&self, x: u16, y: u16) -> Cell {
        self.cells[self.index(x, y)]
    }

    pub fn put(&mut self, x: i32, y: i32, ch: char, fg: Color) {
        if x < 0 || y < 0 || x >= self.width as i32 || y >= self.height as i32 {
            return;
        }
        let index = self.index(x as u16, y as u16);
        self.cells[index] = Cell { ch, fg: Some(fg) };
    }

    pub fn put_cell(&mut self, x: i32, y: i32, cell: Cell) {
        if x < 0 || y < 0 || x >= self.width as i32 || y >= self.height as i32 {
            return;
        }
        let index = self.index(x as u16, y as u16);
        self.cells[index] = cell;
    }

    pub fn text(&mut self, x: i32, y: i32, text: &str, fg: Color) {
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
}

pub fn render_game_frame(
    state: &VisibleGameState,
    options: GameRenderOptions,
    selected_pixel_color: usize,
    pixel_inventory: [u64; PIXEL_COLOR_COUNT],
) -> FrameBuffer {
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
            .unwrap_or(Ordering::Equal)
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

    if options.show_header {
        frame.text(0, 0, HEADER_CONTROLS, HUD);
        if let Some(economy) = economy_header(state, width as usize) {
            let economy_x = width as i32 - display_width(&economy) as i32 - 1;
            frame.text(economy_x.max(0), 0, &economy, HUD);
        }
    }
    if options.show_pixel_inventory {
        draw_pixel_inventory(&mut frame, selected_pixel_color, pixel_inventory);
    }
    if leaderboard_visible {
        draw_lobster_leaderboard(&mut frame, &state.leaderboard);
    }
    if options.show_footer {
        draw_footer(&mut frame);
    }
    frame
}

pub fn frame_to_html(frame: &FrameBuffer) -> String {
    let mut html = String::with_capacity(frame.cells.len() * 18);
    for y in 0..frame.height {
        if y > 0 {
            html.push('\n');
        }
        let mut active: Option<Color> = None;
        for x in 0..frame.width {
            let cell = frame.get(x, y);
            if cell.ch == WIDE_CONTINUATION {
                continue;
            }
            if cell.fg != active {
                if active.is_some() {
                    html.push_str("</span>");
                }
                active = cell.fg;
                if let Some(Color(r, g, b)) = active {
                    html.push_str(&format!("<span style=\"color:rgb({r},{g},{b})\">"));
                }
            }
            push_escaped_char(&mut html, cell.ch);
        }
        if active.is_some() {
            html.push_str("</span>");
        }
    }
    html
}

fn push_escaped_char(out: &mut String, ch: char) {
    match ch {
        '&' => out.push_str("&amp;"),
        '<' => out.push_str("&lt;"),
        '>' => out.push_str("&gt;"),
        '"' => out.push_str("&quot;"),
        _ => out.push(ch),
    }
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
    frame.put(x, y, '\u{2588}', color);
    frame.put(x + 1, y, '\u{2588}', color);
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

fn draw_clipped_text(frame: &mut FrameBuffer, x: i32, y: i32, text: &str, width: usize, fg: Color) {
    let clipped = clip_to_display_width(text, width);
    frame.text(x, y, &clipped, fg);
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
    draw_clipped_text(frame, x, 2, "\u{1f99e} leaders", panel_width, ACCENT_2);
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
            draw_clipped_text(frame, x, line_y, &tokens, panel_width, Color(190, 190, 190));
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
    }) * std::f64::consts::FRAC_1_SQRT_2
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
    format!("\u{0166}{}", format_count(tokens))
}

fn format_lobsters(lobsters: u64) -> String {
    format!("\u{1f99e}{}", format_count(lobsters))
}

fn format_lobsters_per_hour(lobsters_per_hour: f64) -> String {
    format!("\u{1f99e}{}", format_rate(lobsters_per_hour))
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
            let points_color = if player.is_self {
                ACCENT_1
            } else {
                Color(190, 190, 190)
            };
            frame.text(points_x, body_y - 3, &points, points_color);
            frame.text(label_x, body_y - 4, &label, HUD);
        } else {
            frame.text(label_x, body_y - 3, &label, HUD);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orbit_camera_stays_on_unit_sphere_with_tangent_up() {
        let (focus, up) = orbit_camera(12.0);

        assert!((focus.length() - 1.0).abs() < 1e-9);
        assert!((up.length() - 1.0).abs() < 1e-9);
        assert!(focus.dot(up).abs() < 1e-9);
    }

    #[test]
    fn orbit_camera_moves_around_equator_over_time() {
        let (start, _) = orbit_camera(0.0);
        let (half_orbit, _) = orbit_camera(48.0);

        assert!(start.dot(half_orbit) < -0.9);
    }
}
