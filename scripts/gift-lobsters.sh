#!/usr/bin/env bash
set -euo pipefail

# Gift lobsters to one prod player by X username.
#
# Operational notes from the first manual run:
# - The game prod box is the Box recorded in .box-state/prod-box-id.
# - The host does not have psql installed. psql is inside the Postgres Docker
#   container named game-postgres.
# - Piping SQL through `box ssh ... docker exec -i ... psql` can hang through the
#   Box SSH wrapper, and inline SQL is fragile because PowerShell/local shells
#   eat quotes, pipes, and $() before the remote shell sees them.
# - The reliable path is: write a SQL file locally, box scp it to /tmp, docker cp
#   it into game-postgres, then run `docker exec game-postgres psql -f ...`.
# - Lobsters are stored as lobster_micros. 1 lobster = 1,000,000 micros.
# - Use explicit BIGINT arithmetic. `20000 * 1000000` overflows as a default SQL
#   integer expression before being assigned to the BIGINT column.
# - The backend keeps active players in memory and persists economy later. If a
#   user is online while this runs, a later in-memory save may overwrite the DB
#   gift. The script warns when a recent live session is visible; for heavy
#   campaign use, the better long-term fix is an admin API or ledger table that
#   the game loop applies in memory.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -n "${BOX_BIN:-}" ]; then
  BOX_BIN="$BOX_BIN"
elif command -v box >/dev/null 2>&1; then
  BOX_BIN="box"
elif [ -x "$HOME/.ascii/bin/box" ]; then
  BOX_BIN="$HOME/.ascii/bin/box"
elif [ -x "$HOME/.ascii/bin/box.exe" ]; then
  BOX_BIN="$HOME/.ascii/bin/box.exe"
else
  BOX_BIN="box"
fi
BOX_ID="${BOX_ID:-}"
DB_CONTAINER="${DB_CONTAINER:-game-postgres}"
DB_NAME="${DB_NAME:-postgres}"
DB_USER="${DB_USER:-postgres}"
LOBSTER_MICROS="${LOBSTER_MICROS:-1000000}"

usage() {
  cat >&2 <<EOF
Usage: $0 <x_username> <lobsters>

Examples:
  $0 Nicolas_MVD 20000
  BOX_ID=bx_z2ax4z87 $0 Nicolas_MVD 20000
EOF
}

sql_literal() {
  # PostgreSQL single-quoted string literal.
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/''/g")"
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ $# -ne 2 ]; then
  usage
  exit 2
fi

USERNAME="$1"
AMOUNT="$2"

case "$AMOUNT" in
  ''|*[!0-9]*)
    echo "lobsters must be a positive integer" >&2
    exit 2
    ;;
esac

if [ "$AMOUNT" = "0" ]; then
  echo "lobsters must be greater than zero" >&2
  exit 2
fi

if [ -z "$BOX_ID" ]; then
  if [ ! -f "$ROOT_DIR/.box-state/prod-box-id" ]; then
    echo "BOX_ID is required and .box-state/prod-box-id is missing" >&2
    exit 1
  fi
  BOX_ID="$(cat "$ROOT_DIR/.box-state/prod-box-id")"
fi

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "$1 is required" >&2
    exit 1
  }
}

need "$BOX_BIN"
need mktemp

sql_file="$(mktemp -t gift-lobsters.XXXXXX.sql)"
remote_file="/tmp/$(basename "$sql_file")"
container_file="/tmp/$(basename "$sql_file")"
username_sql="$(sql_literal "$USERNAME")"

cleanup() {
  rm -f "$sql_file"
}
trap cleanup EXIT

cat > "$sql_file" <<EOF
\\pset pager off
\\timing on

BEGIN;

CREATE TEMP TABLE gift_target AS
SELECT id
FROM game_users
WHERE lower(x_username) = lower(${username_sql});

DO \$\$
DECLARE
  target_count integer;
BEGIN
  SELECT count(*) INTO target_count FROM gift_target;
  IF target_count = 0 THEN
    RAISE EXCEPTION 'no game_users row found for X username %', ${username_sql};
  END IF;
  IF target_count > 1 THEN
    RAISE EXCEPTION 'multiple game_users rows found for X username %', ${username_sql};
  END IF;
END
\$\$;

SELECT
  'before' AS phase,
  game_users.id,
  x_username,
  x_name,
  lobster_micros,
  lobster_micros / ${LOBSTER_MICROS}::bigint AS lobsters,
  updated_at
FROM game_users
JOIN gift_target USING (id);

SELECT
  'recent_session' AS phase,
  game_users.x_username,
  player_sessions.player_id,
  player_sessions.connected_at,
  player_sessions.disconnected_at
FROM player_sessions
JOIN game_users ON game_users.id = player_sessions.user_id
JOIN gift_target ON gift_target.id = game_users.id
WHERE player_sessions.disconnected_at IS NULL
   OR player_sessions.connected_at > now() - interval '10 minutes'
ORDER BY player_sessions.connected_at DESC
LIMIT 5;

UPDATE game_users
SET lobster_micros = lobster_micros + (${AMOUNT}::bigint * ${LOBSTER_MICROS}::bigint),
    updated_at = now()
FROM gift_target
WHERE game_users.id = gift_target.id
RETURNING
  'after' AS phase,
  game_users.id,
  x_username,
  x_name,
  lobster_micros,
  lobster_micros / ${LOBSTER_MICROS}::bigint AS lobsters,
  updated_at;

COMMIT;
EOF

echo "Gifting +${AMOUNT} lobster(s) to X username ${USERNAME} on Box ${BOX_ID}."
"$BOX_BIN" scp "$sql_file" "$BOX_ID:$remote_file"
"$BOX_BIN" ssh "$BOX_ID" "docker cp '$remote_file' '$DB_CONTAINER:$container_file' && docker exec '$DB_CONTAINER' psql -v ON_ERROR_STOP=1 -U '$DB_USER' -d '$DB_NAME' -f '$container_file'"
