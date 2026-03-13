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

DROP INDEX IF EXISTS idx_event_metric_buckets_dim2_project_event_bucket;

CREATE INDEX IF NOT EXISTS idx_event_metric_buckets_dim2_project_event_bucket
    ON event_metric_buckets_dim2(
        project_id,
        granularity,
        event_name,
        dim1_key,
        dim2_key,
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

DROP INDEX IF EXISTS idx_event_metric_buckets_dim3_project_event_bucket;

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

DROP INDEX IF EXISTS idx_event_metric_buckets_dim4_project_event_bucket;

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

DROP INDEX IF EXISTS idx_event_count_daily_total_project_event_day;
DROP INDEX IF EXISTS idx_event_count_daily_dim1_project_event_dims_day;
DROP INDEX IF EXISTS idx_event_count_daily_dim2_project_event_dims_day;
DROP INDEX IF EXISTS idx_event_count_daily_dim2_project_event_keys_day;
DROP INDEX IF EXISTS idx_event_count_daily_dim2_project_event_dim1_value_day;
DROP INDEX IF EXISTS idx_event_count_daily_dim2_project_event_dim2_value_day;
DROP INDEX IF EXISTS idx_event_count_daily_dim3_project_event_dims_day;
DROP INDEX IF EXISTS idx_event_count_daily_dim3_project_event_keys_day;
DROP INDEX IF EXISTS idx_event_count_daily_dim3_project_event_dim1_value_day;
DROP INDEX IF EXISTS idx_event_count_daily_dim3_project_event_dim2_value_day;
DROP INDEX IF EXISTS idx_event_count_daily_dim3_project_event_dim3_value_day;

DROP TABLE IF EXISTS event_count_daily_dim3;
DROP TABLE IF EXISTS event_count_daily_dim2;
DROP TABLE IF EXISTS event_count_daily_dim1;
DROP TABLE IF EXISTS event_count_daily_total;
