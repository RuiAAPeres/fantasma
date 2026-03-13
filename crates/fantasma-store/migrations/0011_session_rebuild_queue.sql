CREATE TABLE session_rebuild_queue (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    granularity TEXT NOT NULL CHECK (granularity IN ('day', 'hour')),
    bucket_start TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, granularity, bucket_start)
);
