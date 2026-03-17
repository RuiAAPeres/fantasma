CREATE TABLE IF NOT EXISTS project_install_ordinals (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    install_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, install_id),
    UNIQUE (project_id, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_project_install_ordinals_project_ordinal
    ON project_install_ordinals(project_id, ordinal);

CREATE TABLE IF NOT EXISTS project_install_ordinal_state (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    next_ordinal BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id)
);

CREATE TABLE IF NOT EXISTS active_install_bitmap_rebuild_queue (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    day DATE NOT NULL,
    PRIMARY KEY (project_id, day)
);

CREATE TABLE IF NOT EXISTS active_install_daily_bitmaps_total (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    day DATE NOT NULL,
    bitmap BYTEA NOT NULL,
    cardinality BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, day)
);

CREATE TABLE IF NOT EXISTS active_install_daily_bitmaps_dim1 (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    day DATE NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL DEFAULT '',
    dim1_value_is_null BOOLEAN NOT NULL DEFAULT false,
    bitmap BYTEA NOT NULL,
    cardinality BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        project_id,
        day,
        dim1_key,
        dim1_value_is_null,
        dim1_value
    )
);

CREATE TABLE IF NOT EXISTS active_install_daily_bitmaps_dim2 (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    day DATE NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL DEFAULT '',
    dim1_value_is_null BOOLEAN NOT NULL DEFAULT false,
    dim2_key TEXT NOT NULL,
    dim2_value TEXT NOT NULL DEFAULT '',
    dim2_value_is_null BOOLEAN NOT NULL DEFAULT false,
    bitmap BYTEA NOT NULL,
    cardinality BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        project_id,
        day,
        dim1_key,
        dim1_value_is_null,
        dim1_value,
        dim2_key,
        dim2_value_is_null,
        dim2_value
    )
);

CREATE INDEX IF NOT EXISTS idx_active_install_daily_bitmaps_dim1_value
    ON active_install_daily_bitmaps_dim1(
        project_id,
        dim1_key,
        dim1_value_is_null,
        dim1_value,
        day
    );

CREATE INDEX IF NOT EXISTS idx_active_install_daily_bitmaps_dim1_filter
    ON active_install_daily_bitmaps_dim1(
        project_id,
        dim1_key,
        day
    );

CREATE INDEX IF NOT EXISTS idx_active_install_daily_bitmaps_dim2_value
    ON active_install_daily_bitmaps_dim2(
        project_id,
        dim1_key,
        dim1_value_is_null,
        dim1_value,
        dim2_key,
        dim2_value_is_null,
        dim2_value,
        day
    );

CREATE INDEX IF NOT EXISTS idx_active_install_daily_bitmaps_dim2_filter_dim1
    ON active_install_daily_bitmaps_dim2(
        project_id,
        dim1_key,
        dim1_value_is_null,
        dim1_value,
        dim2_key,
        day
    );

CREATE INDEX IF NOT EXISTS idx_active_install_daily_bitmaps_dim2_filter_dim2
    ON active_install_daily_bitmaps_dim2(
        project_id,
        dim2_key,
        dim2_value_is_null,
        dim2_value,
        dim1_key,
        day
    );
