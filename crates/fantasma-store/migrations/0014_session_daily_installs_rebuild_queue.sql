CREATE TABLE session_daily_installs_rebuild_queue (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    day DATE NOT NULL,
    install_id TEXT NOT NULL,
    PRIMARY KEY (project_id, day, install_id)
);

CREATE INDEX idx_session_daily_installs_rebuild_queue_project_day
    ON session_daily_installs_rebuild_queue(project_id, day);
