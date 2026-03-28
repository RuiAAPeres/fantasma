ALTER TABLE events_raw
    ADD COLUMN IF NOT EXISTS device TEXT NOT NULL DEFAULT 'unknown';

UPDATE events_raw
SET device = 'unknown'
WHERE device IS NULL
   OR device = '';

CREATE INDEX IF NOT EXISTS idx_events_device
    ON events_raw(device);

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS device TEXT NOT NULL DEFAULT 'unknown';

UPDATE sessions
SET device = 'unknown'
WHERE device IS NULL
   OR device = '';

ALTER TABLE install_first_seen
    ADD COLUMN IF NOT EXISTS device TEXT NOT NULL DEFAULT 'unknown';

UPDATE install_first_seen
SET device = 'unknown'
WHERE device IS NULL
   OR device = '';

ALTER TABLE session_active_install_slices
    ADD COLUMN IF NOT EXISTS device TEXT NOT NULL DEFAULT 'unknown';

UPDATE session_active_install_slices
SET device = 'unknown'
WHERE device IS NULL
   OR device = '';

ALTER TABLE session_active_install_slices
    DROP CONSTRAINT IF EXISTS session_active_install_slices_pkey;

ALTER TABLE session_active_install_slices
    ADD PRIMARY KEY (
        project_id,
        day,
        install_id,
        platform,
        device,
        app_version_is_null,
        app_version,
        os_version_is_null,
        os_version,
        locale_is_null,
        locale
    );

CREATE INDEX IF NOT EXISTS idx_session_active_install_slices_project_day_device
    ON session_active_install_slices(
        project_id,
        day,
        device,
        install_id
    );
