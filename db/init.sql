-- =========================
-- FACT TABLE
-- =========================
CREATE TABLE IF NOT EXISTS github_events_fact (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_created_at TIMESTAMP NOT NULL,

    actor_id BIGINT,
    actor_login TEXT,

    repo_id BIGINT,
    repo_name TEXT,

    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_created_at
    ON github_events_fact (event_created_at);

CREATE INDEX IF NOT EXISTS idx_events_type
    ON github_events_fact (event_type);

CREATE INDEX IF NOT EXISTS idx_repo_name
    ON github_events_fact (repo_name);

CREATE INDEX IF NOT EXISTS idx_actor_login
    ON github_events_fact (actor_login);


-- =========================
-- AGGREGATION TABLES
-- =========================

-- Events per minute
CREATE TABLE IF NOT EXISTS github_events_minute (
    minute TIMESTAMP PRIMARY KEY,
    event_count BIGINT
);

-- Event types
CREATE TABLE IF NOT EXISTS github_event_types (
    event_type TEXT PRIMARY KEY,
    event_count BIGINT,
    last_updated TIMESTAMP
);

-- Repo leaderboard
CREATE TABLE IF NOT EXISTS github_repo_activity (
    repo_name TEXT PRIMARY KEY,
    event_count BIGINT,
    last_updated TIMESTAMP
);

-- Actor leaderboard
CREATE TABLE IF NOT EXISTS github_actor_activity (
    actor_login TEXT PRIMARY KEY,
    event_count BIGINT,
    last_updated TIMESTAMP
);