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
