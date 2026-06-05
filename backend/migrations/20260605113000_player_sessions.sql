CREATE TABLE IF NOT EXISTS player_sessions (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES game_users(id),
  boot_id UUID NOT NULL REFERENCES server_boots(id),
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ,
  end_reason TEXT
);

CREATE INDEX IF NOT EXISTS player_sessions_user_started_idx
  ON player_sessions (user_id, started_at DESC);

CREATE INDEX IF NOT EXISTS player_sessions_started_idx
  ON player_sessions (started_at DESC);

CREATE INDEX IF NOT EXISTS player_sessions_live_idx
  ON player_sessions (boot_id, last_seen_at DESC)
  WHERE ended_at IS NULL;
