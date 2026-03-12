use std::time::Duration;

use chrono::{DateTime, Utc};
use fantasma_auth::{derive_key_prefix, hash_ingest_key};
use fantasma_core::{EventCountQuery, EventPayload, Platform};
pub use sqlx::PgPool;
use sqlx::{Postgres, QueryBuilder, Row, Transaction, postgres::PgPoolOptions};
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 5;

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub connect_timeout: Duration,
}

impl DatabaseConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            connect_timeout: Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    pub project_id: Uuid,
    pub project_name: String,
    pub ingest_key: Option<String>,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("invalid platform value: {0}")]
    InvalidPlatform(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEventRecord {
    pub id: i64,
    pub project_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub install_id: String,
    pub user_id: Option<String>,
    pub platform: Platform,
    pub app_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub project_id: Uuid,
    pub install_id: String,
    pub user_id: Option<String>,
    pub session_id: String,
    pub session_start: DateTime<Utc>,
    pub session_end: DateTime<Utc>,
    pub event_count: i32,
    pub duration_seconds: i32,
    pub platform: Platform,
    pub app_version: Option<String>,
}

pub async fn connect(config: &DatabaseConfig) -> Result<PgPool, StoreError> {
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(config.connect_timeout)
        .connect(&config.url)
        .await?;

    Ok(pool)
}

pub async fn bootstrap(pool: &PgPool, config: &BootstrapConfig) -> Result<(), StoreError> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS projects (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS api_keys (
            id UUID PRIMARY KEY,
            project_id UUID NOT NULL REFERENCES projects(id),
            key_prefix TEXT NOT NULL,
            key_hash TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS events_raw (
            id BIGSERIAL PRIMARY KEY,
            project_id UUID NOT NULL REFERENCES projects(id),
            event_name TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            install_id TEXT NOT NULL,
            user_id TEXT,
            session_id TEXT,
            platform TEXT NOT NULL,
            app_version TEXT,
            properties JSONB,
            received_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        "#,
    )
    .execute(pool)
    .await?;

    for statement in [
        "CREATE INDEX IF NOT EXISTS idx_events_project_time ON events_raw(project_id, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_events_install ON events_raw(install_id)",
        "CREATE INDEX IF NOT EXISTS idx_events_project_install_timestamp ON events_raw(project_id, install_id, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_events_event_name ON events_raw(event_name)",
        "CREATE INDEX IF NOT EXISTS idx_events_received_at ON events_raw(received_at)",
        "CREATE INDEX IF NOT EXISTS idx_events_platform ON events_raw(platform)",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sessions (
            id BIGSERIAL PRIMARY KEY,
            project_id UUID NOT NULL REFERENCES projects(id),
            install_id TEXT NOT NULL,
            user_id TEXT,
            session_id TEXT NOT NULL,
            session_start TIMESTAMPTZ NOT NULL,
            session_end TIMESTAMPTZ NOT NULL,
            event_count INTEGER NOT NULL,
            duration_seconds INTEGER NOT NULL,
            platform TEXT,
            app_version TEXT,
            UNIQUE (project_id, session_id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    for statement in [
        "CREATE INDEX IF NOT EXISTS idx_sessions_project_time ON sessions(project_id, session_start)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_project_install_start ON sessions(project_id, install_id, session_start)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_project_install_end ON sessions(project_id, install_id, session_end)",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS worker_offsets (
            worker_name TEXT PRIMARY KEY,
            last_processed_event_id BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO projects (id, name)
        VALUES ($1, $2)
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
        "#,
    )
    .bind(config.project_id)
    .bind(&config.project_name)
    .execute(pool)
    .await?;

    if let Some(ingest_key) = config.ingest_key.as_deref() {
        let key_hash = hash_ingest_key(ingest_key);
        let key_prefix = derive_key_prefix(ingest_key);

        sqlx::query(
            r#"
            INSERT INTO api_keys (id, project_id, key_prefix, key_hash)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (key_hash) DO UPDATE
            SET project_id = EXCLUDED.project_id,
                key_prefix = EXCLUDED.key_prefix
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(config.project_id)
        .bind(key_prefix)
        .bind(key_hash)
        .execute(pool)
        .await?;
    }

    Ok(())
}

pub async fn resolve_project_id_for_ingest_key(
    pool: &PgPool,
    ingest_key: &str,
) -> Result<Option<Uuid>, StoreError> {
    let project_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT project_id FROM api_keys WHERE key_hash = $1 LIMIT 1",
    )
    .bind(hash_ingest_key(ingest_key))
    .fetch_optional(pool)
    .await?;

    Ok(project_id)
}

pub async fn insert_events(
    pool: &PgPool,
    project_id: Uuid,
    events: &[EventPayload],
) -> Result<u64, StoreError> {
    let mut builder = QueryBuilder::<sqlx::Postgres>::new(
        "INSERT INTO events_raw (project_id, event_name, timestamp, install_id, user_id, session_id, platform, app_version, properties) ",
    );

    builder.push_values(events, |mut separated, event| {
        separated.push_bind(project_id);
        separated.push_bind(&event.event);
        separated.push_bind(event.timestamp);
        separated.push_bind(&event.install_id);
        separated.push_bind(&event.user_id);
        separated.push_bind(&event.session_id);
        separated.push_bind(match event.platform {
            fantasma_core::Platform::Ios => "ios",
            fantasma_core::Platform::Android => "android",
        });
        separated.push_bind(&event.app_version);
        separated.push_bind(sqlx::types::Json(serde_json::Value::Object(
            event.properties.clone(),
        )));
    });

    let result = builder.build().execute(pool).await?;

    Ok(result.rows_affected())
}

pub async fn count_events(pool: &PgPool, query: &EventCountQuery) -> Result<u64, StoreError> {
    let count = sqlx::query(
        r#"
        SELECT count(*) AS count
        FROM events_raw
        WHERE project_id = $1
          AND timestamp BETWEEN $2 AND $3
        "#,
    )
    .bind(query.project_id)
    .bind(query.start)
    .bind(query.end)
    .fetch_one(pool)
    .await?
    .try_get::<i64, _>("count")? as u64;

    Ok(count)
}

pub async fn fetch_events_after(
    pool: &PgPool,
    last_processed_event_id: i64,
    limit: i64,
) -> Result<Vec<RawEventRecord>, StoreError> {
    let rows = sqlx::query(
        r#"
        SELECT id, project_id, timestamp, install_id, user_id, platform, app_version
        FROM events_raw
        WHERE id > $1
        ORDER BY id ASC
        LIMIT $2
        "#,
    )
    .bind(last_processed_event_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(raw_event_from_row).collect()
}

pub async fn fetch_events_for_install_between(
    pool: &PgPool,
    project_id: Uuid,
    install_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<RawEventRecord>, StoreError> {
    let rows = sqlx::query(
        r#"
        SELECT id, project_id, timestamp, install_id, user_id, platform, app_version
        FROM events_raw
        WHERE project_id = $1
          AND install_id = $2
          AND timestamp BETWEEN $3 AND $4
        ORDER BY timestamp ASC, id ASC
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(raw_event_from_row).collect()
}

pub async fn fetch_latest_session_for_install(
    pool: &PgPool,
    project_id: Uuid,
    install_id: &str,
) -> Result<Option<SessionRecord>, StoreError> {
    let row = sqlx::query(
        r#"
        SELECT project_id, install_id, user_id, session_id, session_start, session_end,
               event_count, duration_seconds, platform, app_version
        FROM sessions
        WHERE project_id = $1
          AND install_id = $2
        ORDER BY session_start DESC, id DESC
        LIMIT 1
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .fetch_optional(pool)
    .await?;

    row.map(session_from_row).transpose()
}

pub async fn fetch_sessions_overlapping_window(
    pool: &PgPool,
    project_id: Uuid,
    install_id: &str,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Result<Vec<SessionRecord>, StoreError> {
    let rows = sqlx::query(
        r#"
        SELECT project_id, install_id, user_id, session_id, session_start, session_end,
               event_count, duration_seconds, platform, app_version
        FROM sessions
        WHERE project_id = $1
          AND install_id = $2
          AND session_start <= $3
          AND session_end >= $4
        ORDER BY session_start ASC, id ASC
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .bind(window_end)
    .bind(window_start)
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(session_from_row).collect()
}

pub async fn upsert_sessions(pool: &PgPool, sessions: &[SessionRecord]) -> Result<u64, StoreError> {
    upsert_sessions_in_executor(pool, sessions).await
}

pub async fn upsert_sessions_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    sessions: &[SessionRecord],
) -> Result<u64, StoreError> {
    upsert_sessions_in_executor(&mut **tx, sessions).await
}

async fn upsert_sessions_in_executor<'a, E>(
    executor: E,
    sessions: &[SessionRecord],
) -> Result<u64, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    if sessions.is_empty() {
        return Ok(0);
    }

    let mut builder = QueryBuilder::<sqlx::Postgres>::new(
        "INSERT INTO sessions (project_id, install_id, user_id, session_id, session_start, session_end, event_count, duration_seconds, platform, app_version) ",
    );

    builder.push_values(sessions, |mut separated, session| {
        separated.push_bind(session.project_id);
        separated.push_bind(&session.install_id);
        separated.push_bind(&session.user_id);
        separated.push_bind(&session.session_id);
        separated.push_bind(session.session_start);
        separated.push_bind(session.session_end);
        separated.push_bind(session.event_count);
        separated.push_bind(session.duration_seconds);
        separated.push_bind(platform_as_str(&session.platform));
        separated.push_bind(&session.app_version);
    });

    builder.push(
        r#"
        ON CONFLICT (project_id, session_id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            session_start = EXCLUDED.session_start,
            session_end = EXCLUDED.session_end,
            event_count = EXCLUDED.event_count,
            duration_seconds = EXCLUDED.duration_seconds,
            platform = EXCLUDED.platform,
            app_version = EXCLUDED.app_version
        "#,
    );

    let result = builder.build().execute(executor).await?;

    Ok(result.rows_affected())
}

pub async fn delete_sessions_for_install_between(
    pool: &PgPool,
    project_id: Uuid,
    install_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<u64, StoreError> {
    delete_sessions_for_install_between_in_executor(pool, project_id, install_id, start, end).await
}

pub async fn delete_sessions_for_install_between_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<u64, StoreError> {
    delete_sessions_for_install_between_in_executor(&mut **tx, project_id, install_id, start, end)
        .await
}

async fn delete_sessions_for_install_between_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    install_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<u64, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let result = sqlx::query(
        r#"
        DELETE FROM sessions
        WHERE project_id = $1
          AND install_id = $2
          AND session_start BETWEEN $3 AND $4
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .bind(start)
    .bind(end)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

pub async fn load_worker_offset(pool: &PgPool, worker_name: &str) -> Result<i64, StoreError> {
    let offset = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT last_processed_event_id
        FROM worker_offsets
        WHERE worker_name = $1
        "#,
    )
    .bind(worker_name)
    .fetch_optional(pool)
    .await?
    .unwrap_or(0);

    Ok(offset)
}

pub async fn lock_worker_offset(
    tx: &mut Transaction<'_, Postgres>,
    worker_name: &str,
) -> Result<i64, StoreError> {
    sqlx::query(
        r#"
        INSERT INTO worker_offsets (worker_name, last_processed_event_id)
        VALUES ($1, 0)
        ON CONFLICT (worker_name) DO NOTHING
        "#,
    )
    .bind(worker_name)
    .execute(&mut **tx)
    .await?;

    let offset = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT last_processed_event_id
        FROM worker_offsets
        WHERE worker_name = $1
        FOR UPDATE
        "#,
    )
    .bind(worker_name)
    .fetch_one(&mut **tx)
    .await?;

    Ok(offset)
}

pub async fn save_worker_offset(
    pool: &PgPool,
    worker_name: &str,
    last_processed_event_id: i64,
) -> Result<(), StoreError> {
    sqlx::query(
        r#"
        INSERT INTO worker_offsets (worker_name, last_processed_event_id)
        VALUES ($1, $2)
        ON CONFLICT (worker_name) DO UPDATE SET
            last_processed_event_id = EXCLUDED.last_processed_event_id,
            updated_at = now()
        "#,
    )
    .bind(worker_name)
    .bind(last_processed_event_id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn save_worker_offset_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    worker_name: &str,
    last_processed_event_id: i64,
) -> Result<(), StoreError> {
    sqlx::query(
        r#"
        UPDATE worker_offsets
        SET last_processed_event_id = $2,
            updated_at = now()
        WHERE worker_name = $1
        "#,
    )
    .bind(worker_name)
    .bind(last_processed_event_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn count_sessions(
    pool: &PgPool,
    project_id: Uuid,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<u64, StoreError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT count(*)
        FROM sessions
        WHERE project_id = $1
          AND session_start BETWEEN $2 AND $3
        "#,
    )
    .bind(project_id)
    .bind(start)
    .bind(end)
    .fetch_one(pool)
    .await?;

    Ok(count as u64)
}

pub async fn average_session_duration_seconds(
    pool: &PgPool,
    project_id: Uuid,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<u64, StoreError> {
    let average = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE(FLOOR(AVG(duration_seconds)), 0)::BIGINT
        FROM sessions
        WHERE project_id = $1
          AND session_start BETWEEN $2 AND $3
        "#,
    )
    .bind(project_id)
    .bind(start)
    .bind(end)
    .fetch_one(pool)
    .await?;

    Ok(average as u64)
}

pub async fn count_active_installs(
    pool: &PgPool,
    project_id: Uuid,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<u64, StoreError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT count(DISTINCT install_id)
        FROM sessions
        WHERE project_id = $1
          AND session_end >= $2
          AND session_start <= $3
        "#,
    )
    .bind(project_id)
    .bind(start)
    .bind(end)
    .fetch_one(pool)
    .await?;

    Ok(count as u64)
}

fn raw_event_from_row(row: sqlx::postgres::PgRow) -> Result<RawEventRecord, StoreError> {
    Ok(RawEventRecord {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        timestamp: row.try_get("timestamp")?,
        install_id: row.try_get("install_id")?,
        user_id: row.try_get("user_id")?,
        platform: platform_from_str(&row.try_get::<String, _>("platform")?)?,
        app_version: row.try_get("app_version")?,
    })
}

fn session_from_row(row: sqlx::postgres::PgRow) -> Result<SessionRecord, StoreError> {
    Ok(SessionRecord {
        project_id: row.try_get("project_id")?,
        install_id: row.try_get("install_id")?,
        user_id: row.try_get("user_id")?,
        session_id: row.try_get("session_id")?,
        session_start: row.try_get("session_start")?,
        session_end: row.try_get("session_end")?,
        event_count: row.try_get("event_count")?,
        duration_seconds: row.try_get("duration_seconds")?,
        platform: platform_from_str(&row.try_get::<String, _>("platform")?)?,
        app_version: row.try_get("app_version")?,
    })
}

fn platform_as_str(platform: &Platform) -> &'static str {
    match platform {
        Platform::Ios => "ios",
        Platform::Android => "android",
    }
}

fn platform_from_str(value: &str) -> Result<Platform, StoreError> {
    match value {
        "ios" => Ok(Platform::Ios),
        "android" => Ok(Platform::Android),
        other => Err(StoreError::InvalidPlatform(other.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use fantasma_core::EventPayload;
    use serde_json::Map;

    fn project_id() -> Uuid {
        Uuid::from_u128(0x9bad8b88_5e7a_44ed_98ce_4cf9ddde713a)
    }

    fn bootstrap_config() -> BootstrapConfig {
        BootstrapConfig {
            project_id: project_id(),
            project_name: "Test Project".to_owned(),
            ingest_key: Some("fg_ing_test".to_owned()),
        }
    }

    fn timestamp(minutes: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("valid timestamp")
            + Duration::minutes(minutes)
    }

    fn event(install_id: &str, minutes: i64) -> EventPayload {
        EventPayload {
            event: "app_open".to_owned(),
            timestamp: timestamp(minutes),
            install_id: install_id.to_owned(),
            user_id: None,
            session_id: None,
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
            properties: Map::new(),
        }
    }

    fn session(session_id: &str, end_minutes: i64, event_count: i32) -> SessionRecord {
        SessionRecord {
            project_id: project_id(),
            install_id: "install-1".to_owned(),
            user_id: Some("user-1".to_owned()),
            session_id: session_id.to_owned(),
            session_start: timestamp(0),
            session_end: timestamp(end_minutes),
            event_count,
            duration_seconds: end_minutes as i32 * 60,
            platform: Platform::Ios,
            app_version: Some("1.0.1".to_owned()),
        }
    }

    #[sqlx::test]
    async fn bootstrap_creates_session_tables_and_indexes(pool: PgPool) {
        bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");

        let event_index_names = sqlx::query_scalar::<_, String>(
            r#"
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'events_raw'
              AND indexname = 'idx_events_project_install_timestamp'
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch event index names");

        let session_index_names = sqlx::query_scalar::<_, String>(
            r#"
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'sessions'
              AND indexname IN ('idx_sessions_project_install_end', 'idx_sessions_project_install_start')
            ORDER BY indexname
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch session index names");

        let session_table_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'sessions'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .expect("fetch session table existence");

        assert_eq!(
            event_index_names,
            vec!["idx_events_project_install_timestamp".to_owned()]
        );
        assert_eq!(
            session_index_names,
            vec![
                "idx_sessions_project_install_end".to_owned(),
                "idx_sessions_project_install_start".to_owned()
            ]
        );
        assert!(session_table_exists);
    }

    #[sqlx::test]
    async fn upsert_sessions_updates_existing_rows(pool: PgPool) {
        bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");

        upsert_sessions(
            &pool,
            &[session("install-1:2026-01-01T00:00:00+00:00", 10, 2)],
        )
        .await
        .expect("insert session");
        upsert_sessions(
            &pool,
            &[session("install-1:2026-01-01T00:00:00+00:00", 25, 3)],
        )
        .await
        .expect("update session");

        let stored = fetch_latest_session_for_install(&pool, project_id(), "install-1")
            .await
            .expect("fetch latest session")
            .expect("session exists");

        assert_eq!(stored.session_end, timestamp(25));
        assert_eq!(stored.event_count, 3);
        assert_eq!(stored.duration_seconds, 25 * 60);
    }

    #[sqlx::test]
    async fn worker_offsets_round_trip(pool: PgPool) {
        bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");

        assert_eq!(
            load_worker_offset(&pool, "sessions")
                .await
                .expect("load default offset"),
            0
        );

        save_worker_offset(&pool, "sessions", 42)
            .await
            .expect("save worker offset");

        assert_eq!(
            load_worker_offset(&pool, "sessions")
                .await
                .expect("load saved offset"),
            42
        );
    }

    #[sqlx::test]
    async fn lock_worker_offset_serializes_worker_claims(pool: PgPool) {
        bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");

        let mut tx_one = pool.begin().await.expect("begin first transaction");
        let mut tx_two = pool.begin().await.expect("begin second transaction");

        assert_eq!(
            lock_worker_offset(&mut tx_one, "sessions")
                .await
                .expect("lock first worker offset"),
            0
        );

        sqlx::query("SET LOCAL lock_timeout = '100ms'")
            .execute(&mut *tx_two)
            .await
            .expect("set local lock timeout");

        let error = lock_worker_offset(&mut tx_two, "sessions")
            .await
            .expect_err("second lock should time out");

        assert!(matches!(error, StoreError::Database(_)));

        tx_one.rollback().await.expect("rollback first transaction");
        tx_two
            .rollback()
            .await
            .expect("rollback second transaction");
    }

    #[sqlx::test]
    async fn fetch_events_for_install_between_orders_by_timestamp_then_id(pool: PgPool) {
        bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");

        insert_events(
            &pool,
            project_id(),
            &[
                event("install-1", 10),
                event("install-2", 5),
                event("install-1", 0),
            ],
        )
        .await
        .expect("insert events");

        let stored = fetch_events_for_install_between(
            &pool,
            project_id(),
            "install-1",
            timestamp(0),
            timestamp(10),
        )
        .await
        .expect("fetch install events");

        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0].timestamp, timestamp(0));
        assert_eq!(stored[1].timestamp, timestamp(10));
    }
}
