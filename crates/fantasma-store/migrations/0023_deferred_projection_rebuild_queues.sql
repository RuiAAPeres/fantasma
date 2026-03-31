DROP INDEX IF EXISTS idx_events_device;
DROP TABLE IF EXISTS event_metric_buckets_dim4;
DROP TABLE IF EXISTS event_metric_buckets_dim3;
DROP TABLE IF EXISTS session_metric_buckets_dim4;
DROP TABLE IF EXISTS session_metric_buckets_dim3;

CREATE TABLE event_metric_deferred_rebuild_queue (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    granularity TEXT NOT NULL CHECK (granularity IN ('day', 'hour')),
    bucket_start TIMESTAMPTZ NOT NULL,
    event_name TEXT NOT NULL,
    PRIMARY KEY (project_id, granularity, bucket_start, event_name)
);

CREATE TABLE session_projection_deferred_rebuild_queue (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    granularity TEXT NOT NULL CHECK (granularity IN ('day', 'hour')),
    bucket_start TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, granularity, bucket_start)
);

CREATE TABLE install_activity_deferred_rebuild_queue (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    install_id TEXT NOT NULL,
    PRIMARY KEY (project_id, install_id)
);
