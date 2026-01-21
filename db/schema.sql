CREATE TABLE IF NOT EXISTS repos (
    repo_id BIGINT PRIMARY KEY,
    repo_name TEXT,
    repo_url TEXT,
    language TEXT,
    stars INT,
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS actors (
    actor_id BIGINT PRIMARY KEY,
    login TEXT,
    actor_type TEXT
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT,
    repo_id BIGINT REFERENCES repos(repo_id),
    actor_id BIGINT REFERENCES actors(actor_id),
    created_at TIMESTAMP,
    payload JSONB
);

CREATE INDEX IF NOT EXISTS idx_events_created_at
ON events(created_at);

CREATE INDEX IF NOT EXISTS idx_events_repo_time
ON events(repo_id, created_at);