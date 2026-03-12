CREATE TABLE IF NOT EXISTS session_daily (
    project_id UUID NOT NULL REFERENCES projects(id),
    day DATE NOT NULL,
    sessions_count BIGINT NOT NULL,
    active_installs BIGINT NOT NULL,
    total_duration_seconds BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, day)
);

CREATE INDEX IF NOT EXISTS idx_session_daily_project_day
    ON session_daily(project_id, day);
