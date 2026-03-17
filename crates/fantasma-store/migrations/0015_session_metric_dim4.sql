ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS os_version TEXT,
    ADD COLUMN IF NOT EXISTS properties JSONB;

ALTER TABLE install_first_seen
    ADD COLUMN IF NOT EXISTS os_version TEXT,
    ADD COLUMN IF NOT EXISTS properties JSONB;
