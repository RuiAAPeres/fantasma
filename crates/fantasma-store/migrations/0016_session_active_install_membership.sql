CREATE TABLE IF NOT EXISTS session_active_install_slices (
    project_id UUID NOT NULL REFERENCES projects(id),
    day DATE NOT NULL,
    install_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    app_version TEXT NOT NULL DEFAULT '',
    app_version_is_null BOOLEAN NOT NULL,
    os_version TEXT NOT NULL DEFAULT '',
    os_version_is_null BOOLEAN NOT NULL,
    locale TEXT NOT NULL DEFAULT '',
    locale_is_null BOOLEAN NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        project_id,
        day,
        install_id,
        platform,
        app_version_is_null,
        app_version,
        os_version_is_null,
        os_version,
        locale_is_null,
        locale
    )
);

CREATE INDEX IF NOT EXISTS idx_session_active_install_slices_project_day
    ON session_active_install_slices(project_id, day, install_id);

CREATE INDEX IF NOT EXISTS idx_session_active_install_slices_project_day_platform
    ON session_active_install_slices(
        project_id,
        day,
        platform,
        install_id
    );
