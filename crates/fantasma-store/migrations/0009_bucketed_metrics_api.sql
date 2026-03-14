CREATE TABLE IF NOT EXISTS event_metric_buckets_total (
    project_id UUID NOT NULL REFERENCES projects(id),
    granularity TEXT NOT NULL CHECK (granularity IN ('hour', 'day')),
    bucket_start TIMESTAMPTZ NOT NULL,
    event_name TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, granularity, bucket_start, event_name)
);

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_total_project_event_bucket
    ON event_metric_buckets_total(project_id, granularity, event_name, bucket_start);

CREATE TABLE IF NOT EXISTS event_metric_buckets_dim1 (
    project_id UUID NOT NULL REFERENCES projects(id),
    granularity TEXT NOT NULL CHECK (granularity IN ('hour', 'day')),
    bucket_start TIMESTAMPTZ NOT NULL,
    event_name TEXT NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        project_id,
        granularity,
        bucket_start,
        event_name,
        dim1_key,
        dim1_value
    )
);

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim1_project_event_bucket
    ON event_metric_buckets_dim1(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim1_value,
        bucket_start
    );

CREATE TABLE IF NOT EXISTS event_metric_buckets_dim2 (
    project_id UUID NOT NULL REFERENCES projects(id),
    granularity TEXT NOT NULL CHECK (granularity IN ('hour', 'day')),
    bucket_start TIMESTAMPTZ NOT NULL,
    event_name TEXT NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL,
    dim2_key TEXT NOT NULL,
    dim2_value TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        project_id,
        granularity,
        bucket_start,
        event_name,
        dim1_key,
        dim1_value,
        dim2_key,
        dim2_value
    ),
    CHECK (dim1_key < dim2_key)
);

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim2_project_event_bucket
    ON event_metric_buckets_dim2(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim1_value,
        dim2_key,
        dim2_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim2_project_event_dim1_value_bucket
    ON event_metric_buckets_dim2(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim2_key,
        dim1_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim2_project_event_dim2_value_bucket
    ON event_metric_buckets_dim2(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim2_key,
        dim2_value,
        bucket_start
    );

CREATE TABLE IF NOT EXISTS event_metric_buckets_dim3 (
    project_id UUID NOT NULL REFERENCES projects(id),
    granularity TEXT NOT NULL CHECK (granularity IN ('hour', 'day')),
    bucket_start TIMESTAMPTZ NOT NULL,
    event_name TEXT NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL,
    dim2_key TEXT NOT NULL,
    dim2_value TEXT NOT NULL,
    dim3_key TEXT NOT NULL,
    dim3_value TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        project_id,
        granularity,
        bucket_start,
        event_name,
        dim1_key,
        dim1_value,
        dim2_key,
        dim2_value,
        dim3_key,
        dim3_value
    ),
    CHECK (dim1_key < dim2_key AND dim2_key < dim3_key)
);

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim3_project_event_bucket
    ON event_metric_buckets_dim3(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim1_value,
        dim2_key,
        dim2_value,
        dim3_key,
        dim3_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim3_project_event_dim1_value_bucket
    ON event_metric_buckets_dim3(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim2_key,
        dim3_key,
        dim1_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim3_project_event_dim2_value_bucket
    ON event_metric_buckets_dim3(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim2_key,
        dim3_key,
        dim2_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim3_project_event_dim3_value_bucket
    ON event_metric_buckets_dim3(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim2_key,
        dim3_key,
        dim3_value,
        bucket_start
    );

CREATE TABLE IF NOT EXISTS session_metric_buckets_total (
    project_id UUID NOT NULL REFERENCES projects(id),
    granularity TEXT NOT NULL CHECK (granularity IN ('hour', 'day')),
    bucket_start TIMESTAMPTZ NOT NULL,
    session_count BIGINT NOT NULL,
    duration_total_seconds BIGINT NOT NULL,
    new_installs BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, granularity, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_total_project_bucket
    ON session_metric_buckets_total(project_id, granularity, bucket_start);

CREATE TABLE IF NOT EXISTS session_metric_buckets_dim1 (
    project_id UUID NOT NULL REFERENCES projects(id),
    granularity TEXT NOT NULL CHECK (granularity IN ('hour', 'day')),
    bucket_start TIMESTAMPTZ NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL,
    session_count BIGINT NOT NULL,
    duration_total_seconds BIGINT NOT NULL,
    new_installs BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        project_id,
        granularity,
        bucket_start,
        dim1_key,
        dim1_value
    )
);

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_dim1_project_bucket
    ON session_metric_buckets_dim1(
        project_id,
        granularity,
        dim1_key,
        dim1_value,
        bucket_start
    );

CREATE TABLE IF NOT EXISTS session_metric_buckets_dim2 (
    project_id UUID NOT NULL REFERENCES projects(id),
    granularity TEXT NOT NULL CHECK (granularity IN ('hour', 'day')),
    bucket_start TIMESTAMPTZ NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL,
    dim2_key TEXT NOT NULL,
    dim2_value TEXT NOT NULL,
    session_count BIGINT NOT NULL,
    duration_total_seconds BIGINT NOT NULL,
    new_installs BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        project_id,
        granularity,
        bucket_start,
        dim1_key,
        dim1_value,
        dim2_key,
        dim2_value
    ),
    CHECK (dim1_key < dim2_key)
);

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_dim2_project_bucket
    ON session_metric_buckets_dim2(
        project_id,
        granularity,
        dim1_key,
        dim1_value,
        dim2_key,
        dim2_value,
        bucket_start
    );

CREATE TABLE IF NOT EXISTS install_first_seen (
    project_id UUID NOT NULL REFERENCES projects(id),
    install_id TEXT NOT NULL,
    first_seen_event_id BIGINT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    platform TEXT NOT NULL,
    app_version TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, install_id)
);

CREATE INDEX IF NOT EXISTS idx_install_first_seen_project_time
    ON install_first_seen(project_id, first_seen_at);
