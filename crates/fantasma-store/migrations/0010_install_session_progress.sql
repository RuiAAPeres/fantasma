ALTER TABLE install_session_state
    ADD COLUMN IF NOT EXISTS last_processed_event_id BIGINT NOT NULL DEFAULT 0;
