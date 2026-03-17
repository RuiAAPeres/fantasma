CREATE TABLE IF NOT EXISTS live_install_state (
    project_id UUID NOT NULL REFERENCES projects(id),
    install_id TEXT NOT NULL,
    last_received_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, install_id)
);

CREATE INDEX IF NOT EXISTS idx_live_install_state_project_received_at
    ON live_install_state(project_id, last_received_at DESC);
