ALTER TABLE projects
    ADD COLUMN state TEXT NOT NULL DEFAULT 'active',
    ADD COLUMN fence_epoch BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT projects_state_check
        CHECK (state IN ('active', 'range_deleting', 'pending_deletion'));

CREATE TABLE IF NOT EXISTS project_deletions (
    id UUID PRIMARY KEY,
    project_id UUID NOT NULL,
    project_name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('project_purge', 'range_delete')),
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    scope JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_code TEXT,
    error_message TEXT,
    deleted_raw_events BIGINT NOT NULL DEFAULT 0,
    deleted_sessions BIGINT NOT NULL DEFAULT 0,
    rebuilt_installs BIGINT NOT NULL DEFAULT 0,
    rebuilt_days BIGINT NOT NULL DEFAULT 0,
    rebuilt_hours BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_project_deletions_project_created_at
    ON project_deletions(project_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_project_deletions_project_id
    ON project_deletions(project_id, id);

CREATE TABLE IF NOT EXISTS project_processing_leases (
    project_id UUID NOT NULL,
    actor_kind TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    fence_epoch BIGINT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, actor_kind, actor_id)
);

CREATE INDEX IF NOT EXISTS idx_project_processing_leases_project_epoch
    ON project_processing_leases(project_id, fence_epoch);
