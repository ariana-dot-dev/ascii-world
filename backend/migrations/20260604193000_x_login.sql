CREATE TABLE IF NOT EXISTS game_users (
  id UUID PRIMARY KEY,
  x_id TEXT NOT NULL UNIQUE,
  x_username TEXT NOT NULL,
  x_name TEXT,
  api_token TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS x_login_sessions (
  id UUID PRIMARY KEY,
  poll_token TEXT NOT NULL UNIQUE,
  oauth_state TEXT NOT NULL UNIQUE,
  code_verifier TEXT NOT NULL,
  status TEXT NOT NULL,
  api_token TEXT,
  user_id UUID REFERENCES game_users(id),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE game_users ADD COLUMN IF NOT EXISTS total_tokens BIGINT NOT NULL DEFAULT 0;
ALTER TABLE game_users ADD COLUMN IF NOT EXISTS lobster_micros BIGINT NOT NULL DEFAULT 0;
ALTER TABLE game_users ADD COLUMN IF NOT EXISTS last_lobster_at TIMESTAMPTZ;
ALTER TABLE game_users ADD COLUMN IF NOT EXISTS last_daily_reward_date DATE;
ALTER TABLE game_users ADD COLUMN IF NOT EXISTS daily_streak_days INTEGER NOT NULL DEFAULT 0;
ALTER TABLE game_users ADD COLUMN IF NOT EXISTS last_weekly_reward_monday DATE;
ALTER TABLE game_users ADD COLUMN IF NOT EXISTS weekly_streak_weeks INTEGER NOT NULL DEFAULT 0;
ALTER TABLE game_users ADD COLUMN IF NOT EXISTS equipped_head TEXT NOT NULL DEFAULT 'default';
ALTER TABLE game_users ADD COLUMN IF NOT EXISTS owned_heads JSONB NOT NULL DEFAULT '["default"]'::jsonb;
ALTER TABLE game_users ADD COLUMN IF NOT EXISTS owned_pixels JSONB NOT NULL DEFAULT '[0,0,0,0,0]'::jsonb;
UPDATE game_users SET equipped_head = 'default' WHERE equipped_head = 'box';
