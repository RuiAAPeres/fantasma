ALTER TABLE events_raw
    DROP COLUMN IF EXISTS user_id,
    DROP COLUMN IF EXISTS session_id;

ALTER TABLE sessions
    DROP COLUMN IF EXISTS user_id;
