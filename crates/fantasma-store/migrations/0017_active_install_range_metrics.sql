CREATE TABLE active_install_range_rebuild_queue (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    granularity TEXT NOT NULL CHECK (granularity IN ('week', 'month', 'year')),
    bucket_start TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, granularity, bucket_start)
);

CREATE TABLE active_install_metric_buckets_total (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    granularity TEXT NOT NULL CHECK (granularity IN ('week', 'month', 'year')),
    bucket_start TIMESTAMPTZ NOT NULL,
    active_installs BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, granularity, bucket_start)
);

CREATE TABLE active_install_metric_buckets_dim1 (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    granularity TEXT NOT NULL CHECK (granularity IN ('week', 'month', 'year')),
    bucket_start TIMESTAMPTZ NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL DEFAULT '',
    dim1_value_is_null BOOLEAN NOT NULL DEFAULT false,
    active_installs BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        project_id,
        granularity,
        bucket_start,
        dim1_key,
        dim1_value_is_null,
        dim1_value
    )
);

CREATE TABLE active_install_metric_buckets_dim2 (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    granularity TEXT NOT NULL CHECK (granularity IN ('week', 'month', 'year')),
    bucket_start TIMESTAMPTZ NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL DEFAULT '',
    dim1_value_is_null BOOLEAN NOT NULL DEFAULT false,
    dim2_key TEXT NOT NULL,
    dim2_value TEXT NOT NULL DEFAULT '',
    dim2_value_is_null BOOLEAN NOT NULL DEFAULT false,
    active_installs BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        project_id,
        granularity,
        bucket_start,
        dim1_key,
        dim1_value_is_null,
        dim1_value,
        dim2_key,
        dim2_value_is_null,
        dim2_value
    )
);

CREATE INDEX idx_active_install_metric_buckets_total_project_bucket
    ON active_install_metric_buckets_total(project_id, granularity, bucket_start);

CREATE INDEX idx_active_install_metric_buckets_dim1_value
    ON active_install_metric_buckets_dim1(
        project_id,
        granularity,
        dim1_key,
        dim1_value,
        bucket_start
    );

CREATE INDEX idx_active_install_metric_buckets_dim2_value
    ON active_install_metric_buckets_dim2(
        project_id,
        granularity,
        dim1_key,
        dim1_value,
        dim2_key,
        dim2_value,
        bucket_start
    );

CREATE INDEX idx_active_install_metric_buckets_dim2_filter_dim1
    ON active_install_metric_buckets_dim2(
        project_id,
        granularity,
        dim1_key,
        dim1_value,
        bucket_start
    );

CREATE INDEX idx_active_install_metric_buckets_dim2_filter_dim2
    ON active_install_metric_buckets_dim2(
        project_id,
        granularity,
        dim2_key,
        dim2_value,
        bucket_start
    );
