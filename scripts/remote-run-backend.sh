#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/ascii-game"
PORT="${PORT:-8080}"
RESET_DB="${RESET_DB:-0}"
DB_CONTAINER="${DB_CONTAINER:-game-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
DATABASE_URL="${DATABASE_URL:-postgres://postgres:${DB_PASSWORD}@127.0.0.1:5432/postgres}"

cd "$APP_DIR"

if [ "$RESET_DB" = "1" ]; then
  docker rm -f "$DB_CONTAINER" >/dev/null 2>&1 || true
fi

if ! docker inspect "$DB_CONTAINER" >/dev/null 2>&1; then
  docker run -d \
    --name "$DB_CONTAINER" \
    -e POSTGRES_PASSWORD="$DB_PASSWORD" \
    -p 127.0.0.1:5432:5432 \
    postgres:16 >/dev/null
else
  docker start "$DB_CONTAINER" >/dev/null 2>&1 || true
fi

for _ in $(seq 1 60); do
  if docker exec "$DB_CONTAINER" pg_isready -U postgres >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

pkill -f "target/debug/backend" >/dev/null 2>&1 || true
pkill -f "cargo run --manifest-path backend/Cargo.toml" >/dev/null 2>&1 || true

mkdir -p "$APP_DIR/logs"
(
  cd "$APP_DIR/backend"
  export DATABASE_URL PORT RUST_LOG="${RUST_LOG:-backend=info,tower_http=info}"
  nohup cargo run > "$APP_DIR/logs/backend.log" 2>&1 &
  echo "$!" > "$APP_DIR/.backend.pid"
)

for _ in $(seq 1 180); do
  if curl -fsS "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

curl -fsS "http://127.0.0.1:${PORT}/health" >/dev/null

host "$PORT" --public --title "Ascii game backend" > "$APP_DIR/logs/host.log" 2>&1 || true
awk '/^https:\/\// { print $1; exit }' "$APP_DIR/logs/host.log" > "$APP_DIR/.box-host-url"

test -s "$APP_DIR/.box-host-url"
