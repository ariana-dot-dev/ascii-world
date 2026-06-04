#!/usr/bin/env bash
set -euo pipefail

ENVIRONMENT="${1:-dev}"
if [ "$ENVIRONMENT" != "dev" ] && [ "$ENVIRONMENT" != "prod" ]; then
  echo "Usage: $0 dev|prod" >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="$HOME/.ascii/bin:$PATH"
BOX_BIN="${BOX_BIN:-box}"
BOX_API_BASE="${BOX_API_BASE:-https://ascii.dev/api/box/v1}"
PORT="${PORT:-8080}"

if [ "$ENVIRONMENT" = "dev" ]; then
  BOX_NAME="${BOX_NAME:-game-dev-anicet}"
  RESET_DB="1"
  ENV_FILE="$ROOT_DIR/.env"
  CLI_ENV_FILE="$ROOT_DIR/cli/.env"
else
  BOX_NAME="${BOX_NAME:-game-prod-anicet}"
  RESET_DB="0"
  ENV_FILE="$ROOT_DIR/.env.production"
  CLI_ENV_FILE="$ROOT_DIR/cli/.env.production"
fi
export BOX_NAME

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck source=/dev/null
  . "$ENV_FILE"
  set +a
fi

if [ -z "${BOX_API_KEY:-}" ]; then
  echo "BOX_API_KEY is required in $ENV_FILE" >&2
  exit 1
fi
BOX_AUTH_TOKEN="$BOX_API_KEY"

STATE_DIR="$ROOT_DIR/.box-state"
mkdir -p "$STATE_DIR"
STATE_FILE="$STATE_DIR/${ENVIRONMENT}-box-id"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "$1 is required" >&2
    exit 1
  }
}

need python3
need tar
need curl
need "$BOX_BIN"

api() {
  local method="$1"
  local path="$2"
  local data="${3:-}"
  if [ -n "$data" ]; then
    curl -fsS \
      -X "$method" \
      "$BOX_API_BASE$path" \
      -H "Authorization: Bearer $BOX_AUTH_TOKEN" \
      -H "Content-Type: application/json" \
      -d "$data"
  else
    curl -fsS \
      -X "$method" \
      "$BOX_API_BASE$path" \
      -H "Authorization: Bearer $BOX_AUTH_TOKEN"
  fi
}

config_token() {
  python3 -c 'import json, pathlib
path = pathlib.Path.home() / ".config" / "ascii" / "box" / "config.json"
try:
    print(json.loads(path.read_text()).get("token", ""))
except Exception:
    print("")
'
}

if ! api GET "/me" >/dev/null 2>&1; then
  saved_token="$(config_token)"
  if [ -n "$saved_token" ]; then
    BOX_AUTH_TOKEN="$saved_token"
  fi
fi

if ! api GET "/me" >/dev/null 2>&1; then
  echo "Box API authentication failed. Refresh BOX_API_KEY in $ENV_FILE or run: box login <key>" >&2
  exit 1
fi

"$BOX_BIN" config --json >/dev/null || "$BOX_BIN" login "$BOX_AUTH_TOKEN" --json >/dev/null

find_box_by_name() {
  api GET "/boxes?limit=200" \
    | python3 -c 'import json, os, sys
name = os.environ["BOX_NAME"]
data = json.load(sys.stdin)
for box in data.get("boxes", []):
    if box.get("name") == name and str(box.get("state", "")).lower() != "deleted":
        print(box.get("id", ""))
        break
' \
    | head -n 1
}

json_field() {
  local path="$1"
  python3 -c 'import json, sys
path = sys.argv[1].split(".")
value = json.load(sys.stdin)
for key in path:
    if isinstance(value, dict):
        value = value.get(key)
    else:
        value = None
    if value is None:
        break
print("" if value is None else value)
' "$path"
}

box_patch_body() {
  python3 -c 'import json, os
print(json.dumps({"name": os.environ["BOX_NAME"], "ttlSeconds": None}))
'
}

wait_for_box_ready() {
  local id="$1"
  local state=""
  for _ in $(seq 1 240); do
    info="$(api GET "/boxes/$id")"
    state="$(printf '%s' "$info" | json_field "box.state")"
    case "$state" in
      ready|idle|running|provisioned)
        printf '%s' "$info"
        return 0
        ;;
      error)
        printf '%s\n' "$info" >&2
        return 1
        ;;
    esac
    sleep 2
  done
  echo "Box $id did not become ready; last state: ${state:-unknown}" >&2
  return 1
}

box_id="$(find_box_by_name || true)"
if [ -z "$box_id" ] && [ -f "$STATE_FILE" ]; then
  candidate="$(cat "$STATE_FILE")"
  if [ -n "$candidate" ]; then
    candidate_info="$(api GET "/boxes/$candidate" 2>/dev/null || true)"
    candidate_name="$(printf '%s' "$candidate_info" | json_field "box.name" 2>/dev/null || true)"
    if [ "$candidate_name" = "$BOX_NAME" ]; then
      box_id="$candidate"
    fi
  fi
fi

if [ -z "$box_id" ]; then
  echo "Creating Box $BOX_NAME..."
  created="$(api POST "/boxes" '{"ttlSeconds":null}')"
  printf '%s\n' "$created" > "$STATE_DIR/new-${ENVIRONMENT}.json"
  box_id="$(printf '%s' "$created" | json_field "box.id")"
  if [ -z "$box_id" ]; then
    echo "Could not determine created Box id" >&2
    exit 1
  fi
  api PATCH "/boxes/$box_id" "$(box_patch_body)" >/dev/null
else
  api PATCH "/boxes/$box_id" "$(box_patch_body)" >/dev/null || true
fi

printf '%s' "$box_id" > "$STATE_FILE"

info="$(api GET "/boxes/$box_id")"
state="$(printf '%s' "$info" | json_field "box.state")"
case "$state" in
  archived|stopped)
    echo "Resuming Box $box_id..."
    api POST "/boxes/$box_id/resume" >/dev/null
    ;;
esac
info="$(wait_for_box_ready "$box_id")"
ip="$(printf '%s' "$info" | json_field "box.ip")"
if [ -z "$ip" ]; then
  echo "Box $box_id is ready but has no IP in the API response" >&2
  exit 1
fi
echo "Using Box $box_id ${ip:+($ip)}"

archive="$(mktemp -t ascii-game.XXXXXX.tgz)"
(
  cd "$ROOT_DIR"
  tar \
    --exclude='.git' \
    --exclude='target' \
    --exclude='backend/target' \
    --exclude='cli/target' \
    --exclude='.box-state' \
    --exclude='.env' \
    --exclude='.env.production' \
    --exclude='cli/.env' \
    --exclude='cli/.env.production' \
    -czf "$archive" .
)

"$BOX_BIN" scp "$archive" "$box_id:/tmp/ascii-game.tgz"
rm -f "$archive"

"$BOX_BIN" ssh "$box_id" -- bash --noprofile --norc -s <<EOF
set -euo pipefail
mkdir -p /opt/ascii-game
tar -xzf /tmp/ascii-game.tgz -C /opt/ascii-game
cd /opt/ascii-game
chmod +x scripts/remote-run-backend.sh
RESET_DB=$RESET_DB PORT=$PORT ./scripts/remote-run-backend.sh
EOF

host_url="$("$BOX_BIN" ssh "$box_id" -- bash --noprofile --norc -lc "cat /opt/ascii-game/.box-host-url 2>/dev/null || true")"
if [ -z "$host_url" ]; then
  echo "Hosted URL was not produced" >&2
  exit 1
fi

tmp_env="$(mktemp)"
if [ -f "$CLI_ENV_FILE" ]; then
  grep -v '^GAME_BACKEND_URL=' "$CLI_ENV_FILE" > "$tmp_env" || true
fi
printf 'GAME_BACKEND_URL=%s\n' "$host_url" >> "$tmp_env"
if [ "$ENVIRONMENT" = "prod" ]; then
  grep -q '^GAME_ENV=' "$tmp_env" 2>/dev/null || printf 'GAME_ENV=production\n' >> "$tmp_env"
fi
mv "$tmp_env" "$CLI_ENV_FILE"

echo "$host_url"
