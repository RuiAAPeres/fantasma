CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(id),
    key_prefix TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events_raw (
    id BIGSERIAL PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(id),
    event_name TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    install_id TEXT NOT NULL,
    user_id TEXT,
    session_id TEXT,
    platform TEXT NOT NULL,
    app_version TEXT,
    os_version TEXT,
    locale TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_project_time
    ON events_raw(project_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_install
    ON events_raw(install_id);
CREATE INDEX IF NOT EXISTS idx_events_project_install_timestamp
    ON events_raw(project_id, install_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_event_name
    ON events_raw(event_name);
CREATE INDEX IF NOT EXISTS idx_events_received_at
    ON events_raw(received_at);
CREATE INDEX IF NOT EXISTS idx_events_platform
    ON events_raw(platform);

CREATE TABLE IF NOT EXISTS sessions (
    id BIGSERIAL PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(id),
    install_id TEXT NOT NULL,
    user_id TEXT,
    session_id TEXT NOT NULL,
    session_start TIMESTAMPTZ NOT NULL,
    session_end TIMESTAMPTZ NOT NULL,
    event_count INTEGER NOT NULL,
    duration_seconds INTEGER NOT NULL,
    platform TEXT,
    app_version TEXT,
    UNIQUE (project_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_sessions_project_time
    ON sessions(project_id, session_start);
CREATE INDEX IF NOT EXISTS idx_sessions_project_install_start
    ON sessions(project_id, install_id, session_start);
CREATE INDEX IF NOT EXISTS idx_sessions_project_install_end
    ON sessions(project_id, install_id, session_end);

CREATE TABLE IF NOT EXISTS worker_offsets (
    worker_name TEXT PRIMARY KEY,
    last_processed_event_id BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
