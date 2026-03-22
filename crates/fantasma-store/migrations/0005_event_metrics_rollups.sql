ALTER TABLE events_raw
    ADD COLUMN IF NOT EXISTS os_version TEXT,
    ADD COLUMN IF NOT EXISTS locale TEXT;

CREATE TABLE IF NOT EXISTS event_count_daily_total (
    project_id UUID NOT NULL REFERENCES projects(id),
    day DATE NOT NULL,
    event_name TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, day, event_name)
);

CREATE INDEX IF NOT EXISTS idx_event_count_daily_total_project_event_day
    ON event_count_daily_total(project_id, event_name, day);

CREATE TABLE IF NOT EXISTS event_count_daily_dim1 (
    project_id UUID NOT NULL REFERENCES projects(id),
    day DATE NOT NULL,
    event_name TEXT NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, day, event_name, dim1_key, dim1_value)
);

CREATE INDEX IF NOT EXISTS idx_event_count_daily_dim1_project_event_dims_day
    ON event_count_daily_dim1(project_id, event_name, dim1_key, dim1_value, day);

CREATE TABLE IF NOT EXISTS event_count_daily_dim2 (
    project_id UUID NOT NULL REFERENCES projects(id),
    day DATE NOT NULL,
    event_name TEXT NOT NULL,
    dim1_key TEXT NOT NULL,
    dim1_value TEXT NOT NULL,
    dim2_key TEXT NOT NULL,
    dim2_value TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        project_id,
        day,
        event_name,
        dim1_key,
        dim1_value,
        dim2_key,
        dim2_value
    ),
    CHECK (dim1_key < dim2_key)
);

CREATE INDEX IF NOT EXISTS idx_event_count_daily_dim2_project_event_dims_day
    ON event_count_daily_dim2(
        project_id,
        event_name,
        dim1_key,
        dim1_value,
        dim2_key,
        dim2_value,
        day
    );

CREATE TABLE IF NOT EXISTS event_count_daily_dim3 (
    project_id UUID NOT NULL REFERENCES projects(id),
    day DATE NOT NULL,
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
        day,
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

CREATE INDEX IF NOT EXISTS idx_event_count_daily_dim3_project_event_dims_day
    ON event_count_daily_dim3(
        project_id,
        event_name,
        dim1_key,
        dim1_value,
        dim2_key,
        dim2_value,
        dim3_key,
        dim3_value,
        day
    );
