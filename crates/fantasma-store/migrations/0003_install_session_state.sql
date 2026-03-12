CREATE TABLE IF NOT EXISTS install_session_state (
    project_id UUID NOT NULL REFERENCES projects(id),
    install_id TEXT NOT NULL,
    tail_session_id TEXT NOT NULL,
    tail_session_start TIMESTAMPTZ NOT NULL,
    tail_session_end TIMESTAMPTZ NOT NULL,
    tail_event_count INTEGER NOT NULL,
    tail_duration_seconds INTEGER NOT NULL,
    tail_day DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, install_id)
);

CREATE INDEX IF NOT EXISTS idx_install_session_state_project_install
    ON install_session_state(project_id, install_id);
