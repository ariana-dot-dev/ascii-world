# Ascii World

The idle multiplayer game for vibe coders.

Join from your terminal:

```sh
curl -fsSL https://world.ascii.dev/install.sh | sh
world
```

Windows:

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://world.ascii.dev/install.ps1 | iex"
world
```

Ascii World turns local coding-agent token activity into points, lobster yield, cosmetics, pixels, and a shared ASCII planet. Your local usage data stays local; the CLI reports gameplay totals to the multiplayer backend.

## Build from source

Install Rust with [rustup](https://rustup.rs/), then clone this repository and build the terminal client:

```sh
git clone https://github.com/ariana-dot-dev/ascii-world.git
cd ascii-world
cp cli/.env.production.example cli/.env.production
cargo build --release --manifest-path cli/Cargo.toml
./target/release/world
```

On Windows PowerShell:

```powershell
git clone https://github.com/ariana-dot-dev/ascii-world.git
cd ascii-world
Copy-Item cli/.env.production.example cli/.env.production
cargo build --release --manifest-path cli/Cargo.toml
.\target\release\world.exe
```

`cli/.env.production` should point at the public backend:

```env
GAME_BACKEND_URL=https://world.ascii.dev
GAME_ENV=production
```

You can also run from source without a release build:

```sh
GAME_BACKEND_URL=https://world.ascii.dev cargo run --manifest-path cli/Cargo.toml
```

On Windows PowerShell:

```powershell
$env:GAME_BACKEND_URL = "https://world.ascii.dev"
cargo run --manifest-path cli/Cargo.toml
```

If you do not want to pipe an install script into your shell, download and inspect it first:

```sh
curl -fsSLO https://world.ascii.dev/install.sh
less install.sh
sh install.sh
```

On Windows PowerShell:

```powershell
irm https://world.ascii.dev/install.ps1 -OutFile install.ps1
notepad .\install.ps1
powershell -ExecutionPolicy Bypass -File .\install.ps1
```

## Development

Run the CLI locally:

```sh
cargo run --manifest-path cli/Cargo.toml
```

Deploy the dev backend:

```sh
scripts/backend-dev.sh
```

Deploy production:

```sh
scripts/backend-prod.sh
```

Release the CLI:

```sh
scripts/game-release-cli.sh
```

## Repository

- `backend/` runs the Axum websocket/API server, X login, economy, and landing page.
- `cli/` contains the terminal client.
- `world-render/` contains the shared ASCII planet renderer used by the CLI and landing page.
- `assets/` contains generated world mask assets.
- `scripts/` contains deployment, release, and local reset helpers.

Runtime secrets and machine-local state belong in ignored files such as `.env`, `.env.production`, `cli/.env`, `cli/.env.production`, and `.box-state/`.
