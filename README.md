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
