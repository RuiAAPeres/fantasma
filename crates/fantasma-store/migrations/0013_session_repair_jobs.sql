CREATE TABLE session_repair_jobs (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    install_id TEXT NOT NULL,
    target_event_id BIGINT NOT NULL,
    claimed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, install_id)
);

CREATE INDEX idx_session_repair_jobs_claimable
    ON session_repair_jobs(claimed_at ASC, updated_at ASC, project_id ASC, install_id ASC);

CREATE INDEX idx_events_project_install_id
    ON events_raw(project_id, install_id, id);
