CREATE TABLE IF NOT EXISTS session_daily_installs (
    project_id UUID NOT NULL REFERENCES projects(id),
    day DATE NOT NULL,
    install_id TEXT NOT NULL,
    session_count INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, day, install_id)
);

CREATE INDEX IF NOT EXISTS idx_session_daily_installs_project_day
    ON session_daily_installs(project_id, day);
