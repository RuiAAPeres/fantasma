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
DROP INDEX IF EXISTS idx_event_metric_buckets_dim3_project_event_dim1_value_bucket;
DROP INDEX IF EXISTS idx_event_metric_buckets_dim3_project_event_dim2_value_bucket;
DROP INDEX IF EXISTS idx_event_metric_buckets_dim3_project_event_dim3_value_bucket;
DROP INDEX IF EXISTS idx_event_metric_buckets_dim4_project_event_bucket;
DROP INDEX IF EXISTS idx_event_metric_buckets_dim4_project_event_dim1_value_bucket;
DROP INDEX IF EXISTS idx_event_metric_buckets_dim4_project_event_dim2_value_bucket;
DROP INDEX IF EXISTS idx_event_metric_buckets_dim4_project_event_dim3_value_bucket;
DROP INDEX IF EXISTS idx_event_metric_buckets_dim4_project_event_dim4_value_bucket;

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

DROP TABLE IF EXISTS event_metric_buckets_dim4;
DROP TABLE IF EXISTS event_metric_buckets_dim3;
DROP TABLE IF EXISTS event_count_daily_dim3;
DROP TABLE IF EXISTS event_count_daily_dim2;
DROP TABLE IF EXISTS event_count_daily_dim1;
DROP TABLE IF EXISTS event_count_daily_total;
