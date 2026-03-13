ALTER TABLE api_keys
    ADD COLUMN name TEXT NOT NULL,
    ADD COLUMN kind TEXT NOT NULL,
    ADD COLUMN revoked_at TIMESTAMPTZ;

ALTER TABLE api_keys
    ADD CONSTRAINT api_keys_kind_check
    CHECK (kind IN ('ingest', 'read'));

CREATE INDEX idx_api_keys_project_id_created_at
    ON api_keys(project_id, created_at DESC);
