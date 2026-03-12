DROP INDEX IF EXISTS idx_event_count_daily_dim2_project_event_dims_day;
DROP INDEX IF EXISTS idx_event_count_daily_dim3_project_event_dims_day;

CREATE INDEX IF NOT EXISTS idx_event_count_daily_dim2_project_event_keys_day
    ON event_count_daily_dim2(project_id, event_name, dim1_key, dim2_key, day);

CREATE INDEX IF NOT EXISTS idx_event_count_daily_dim2_project_event_dim1_value_day
    ON event_count_daily_dim2(project_id, event_name, dim1_key, dim2_key, dim1_value, day);

CREATE INDEX IF NOT EXISTS idx_event_count_daily_dim2_project_event_dim2_value_day
    ON event_count_daily_dim2(project_id, event_name, dim1_key, dim2_key, dim2_value, day);

CREATE INDEX IF NOT EXISTS idx_event_count_daily_dim3_project_event_keys_day
    ON event_count_daily_dim3(project_id, event_name, dim1_key, dim2_key, dim3_key, day);

CREATE INDEX IF NOT EXISTS idx_event_count_daily_dim3_project_event_dim1_value_day
    ON event_count_daily_dim3(project_id, event_name, dim1_key, dim2_key, dim3_key, dim1_value, day);

CREATE INDEX IF NOT EXISTS idx_event_count_daily_dim3_project_event_dim2_value_day
    ON event_count_daily_dim3(project_id, event_name, dim1_key, dim2_key, dim3_key, dim2_value, day);

CREATE INDEX IF NOT EXISTS idx_event_count_daily_dim3_project_event_dim3_value_day
    ON event_count_daily_dim3(project_id, event_name, dim1_key, dim2_key, dim3_key, dim3_value, day);
