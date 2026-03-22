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
        dim2_key,
        dim3_key,
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

CREATE TABLE IF NOT EXISTS event_metric_buckets_dim4 (
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
    dim4_key TEXT NOT NULL,
    dim4_value TEXT NOT NULL,
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
        dim3_value,
        dim4_key,
        dim4_value
    ),
    CHECK (dim1_key < dim2_key AND dim2_key < dim3_key AND dim3_key < dim4_key)
);

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim4_project_event_bucket
    ON event_metric_buckets_dim4(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim2_key,
        dim3_key,
        dim4_key,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim4_project_event_dim1_value_bucket
    ON event_metric_buckets_dim4(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim2_key,
        dim3_key,
        dim4_key,
        dim1_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim4_project_event_dim2_value_bucket
    ON event_metric_buckets_dim4(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim2_key,
        dim3_key,
        dim4_key,
        dim2_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim4_project_event_dim3_value_bucket
    ON event_metric_buckets_dim4(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim2_key,
        dim3_key,
        dim4_key,
        dim3_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim4_project_event_dim4_value_bucket
    ON event_metric_buckets_dim4(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim2_key,
        dim3_key,
        dim4_key,
        dim4_value,
        bucket_start
    );

CREATE TABLE IF NOT EXISTS session_metric_buckets_dim3 (
    project_id UUID NOT NULL REFERENCES projects(id),
    granularity TEXT NOT NULL CHECK (granularity IN ('hour', 'day')),
    bucket_start TIMESTAMPTZ NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL,
    dim2_key TEXT NOT NULL,
    dim2_value TEXT NOT NULL,
    dim3_key TEXT NOT NULL,
    dim3_value TEXT NOT NULL,
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
        dim2_value,
        dim3_key,
        dim3_value
    ),
    CHECK (dim1_key < dim2_key AND dim2_key < dim3_key)
);

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_dim3_project_bucket
    ON session_metric_buckets_dim3(
        project_id,
        granularity,
        dim1_key,
        dim2_key,
        dim3_key,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_dim3_dim1_value_bucket
    ON session_metric_buckets_dim3(
        project_id,
        granularity,
        dim1_key,
        dim2_key,
        dim3_key,
        dim1_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_dim3_dim2_value_bucket
    ON session_metric_buckets_dim3(
        project_id,
        granularity,
        dim1_key,
        dim2_key,
        dim3_key,
        dim2_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_dim3_dim3_value_bucket
    ON session_metric_buckets_dim3(
        project_id,
        granularity,
        dim1_key,
        dim2_key,
        dim3_key,
        dim3_value,
        bucket_start
    );

CREATE TABLE IF NOT EXISTS session_metric_buckets_dim4 (
    project_id UUID NOT NULL REFERENCES projects(id),
    granularity TEXT NOT NULL CHECK (granularity IN ('hour', 'day')),
    bucket_start TIMESTAMPTZ NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL,
    dim2_key TEXT NOT NULL,
    dim2_value TEXT NOT NULL,
    dim3_key TEXT NOT NULL,
    dim3_value TEXT NOT NULL,
    dim4_key TEXT NOT NULL,
    dim4_value TEXT NOT NULL,
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
        dim2_value,
        dim3_key,
        dim3_value,
        dim4_key,
        dim4_value
    ),
    CHECK (dim1_key < dim2_key AND dim2_key < dim3_key AND dim3_key < dim4_key)
);

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_dim4_project_bucket
    ON session_metric_buckets_dim4(
        project_id,
        granularity,
        dim1_key,
        dim2_key,
        dim3_key,
        dim4_key,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_dim4_dim1_value_bucket
    ON session_metric_buckets_dim4(
        project_id,
        granularity,
        dim1_key,
        dim2_key,
        dim3_key,
        dim4_key,
        dim1_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_dim4_dim2_value_bucket
    ON session_metric_buckets_dim4(
        project_id,
        granularity,
        dim1_key,
        dim2_key,
        dim3_key,
        dim4_key,
        dim2_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_dim4_dim3_value_bucket
    ON session_metric_buckets_dim4(
        project_id,
        granularity,
        dim1_key,
        dim2_key,
        dim3_key,
        dim4_key,
        dim3_value,
        bucket_start
    );

CREATE INDEX IF NOT EXISTS idx_session_metric_buckets_dim4_dim4_value_bucket
    ON session_metric_buckets_dim4(
        project_id,
        granularity,
        dim1_key,
        dim2_key,
        dim3_key,
        dim4_key,
        dim4_value,
        bucket_start
    );
