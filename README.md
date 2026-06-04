# Terminal Multiplayer Game

Rust backend plus Rust terminal CLI.

## Backend

Dev deploy:

```sh
scripts/backend-dev.sh
```

Prod deploy:

```sh
scripts/backend-prod.sh
```

Both scripts use `BOX_API_KEY` from `.env` or `.env.production`, copy the latest backend files to a Box, run Postgres in Docker, run backend migrations, start the Rust backend detached, start `host`, and print the HTTPS URL.

The dev script writes that URL to `cli/.env`. The prod script writes it to `cli/.env.production`.

## CLI

Run locally in dev:

```sh
cargo run --manifest-path cli/Cargo.toml
```

Release the CLI:

```sh
scripts/game-release-cli.sh
```

Set the GitHub Actions variable `GAME_BACKEND_URL` to the production backend URL before cutting a release, or build locally with `cli/.env.production` present.

