use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use fantasma_auth::{derive_key_prefix, hash_ingest_key};
use fantasma_core::{EventCountQuery, EventPayload, Platform};
pub use sqlx::PgPool;
use sqlx::{Postgres, QueryBuilder, Row, Transaction, migrate::Migrator, postgres::PgPoolOptions};
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 5;
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

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
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("invalid platform value: {0}")]
    InvalidPlatform(String),
    #[error("invariant violation: {0}")]
    InvariantViolation(String),
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

#[derive(Debug, Clone, Copy)]
pub struct SessionTailUpdate<'a> {
    pub project_id: Uuid,
    pub session_id: &'a str,
    pub session_end: DateTime<Utc>,
    pub event_count: i32,
    pub duration_seconds: i32,
    pub user_id: Option<&'a str>,
    pub app_version: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSessionStateRecord {
    pub project_id: Uuid,
    pub install_id: String,
    pub tail_session_id: String,
    pub tail_session_start: DateTime<Utc>,
    pub tail_session_end: DateTime<Utc>,
    pub tail_event_count: i32,
    pub tail_duration_seconds: i32,
    pub tail_day: NaiveDate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDailyRecord {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub sessions_count: i64,
    pub active_installs: i64,
    pub total_duration_seconds: i64,
}

pub async fn connect(config: &DatabaseConfig) -> Result<PgPool, StoreError> {
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(config.connect_timeout)
        .connect(&config.url)
        .await?;

    Ok(pool)
}

pub async fn run_migrations(pool: &PgPool) -> Result<(), StoreError> {
    MIGRATOR.run(pool).await?;

    Ok(())
}

pub async fn ensure_local_project(
    pool: &PgPool,
    config: Option<&BootstrapConfig>,
) -> Result<(), StoreError> {
    let Some(config) = config else {
        return Ok(());
    };

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

pub async fn bootstrap(pool: &PgPool, config: &BootstrapConfig) -> Result<(), StoreError> {
    run_migrations(pool).await?;
    ensure_local_project(pool, Some(config)).await?;

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

pub async fn load_install_session_state(
    pool: &PgPool,
    project_id: Uuid,
    install_id: &str,
) -> Result<Option<InstallSessionStateRecord>, StoreError> {
    load_install_session_state_in_executor(pool, project_id, install_id).await
}

pub async fn load_install_session_state_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
) -> Result<Option<InstallSessionStateRecord>, StoreError> {
    load_install_session_state_in_executor(&mut **tx, project_id, install_id).await
}

async fn load_install_session_state_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    install_id: &str,
) -> Result<Option<InstallSessionStateRecord>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            project_id,
            install_id,
            tail_session_id,
            tail_session_start,
            tail_session_end,
            tail_event_count,
            tail_duration_seconds,
            tail_day
        FROM install_session_state
        WHERE project_id = $1
          AND install_id = $2
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .fetch_optional(executor)
    .await?;

    row.map(install_session_state_from_row).transpose()
}

pub async fn upsert_install_session_state(
    pool: &PgPool,
    state: &InstallSessionStateRecord,
) -> Result<u64, StoreError> {
    upsert_install_session_state_in_executor(pool, state).await
}

pub async fn upsert_install_session_state_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    state: &InstallSessionStateRecord,
) -> Result<u64, StoreError> {
    upsert_install_session_state_in_executor(&mut **tx, state).await
}

async fn upsert_install_session_state_in_executor<'a, E>(
    executor: E,
    state: &InstallSessionStateRecord,
) -> Result<u64, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let result = sqlx::query(
        r#"
        INSERT INTO install_session_state (
            project_id,
            install_id,
            tail_session_id,
            tail_session_start,
            tail_session_end,
            tail_event_count,
            tail_duration_seconds,
            tail_day
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (project_id, install_id) DO UPDATE SET
            tail_session_id = EXCLUDED.tail_session_id,
            tail_session_start = EXCLUDED.tail_session_start,
            tail_session_end = EXCLUDED.tail_session_end,
            tail_event_count = EXCLUDED.tail_event_count,
            tail_duration_seconds = EXCLUDED.tail_duration_seconds,
            tail_day = EXCLUDED.tail_day,
            updated_at = now()
        "#,
    )
    .bind(state.project_id)
    .bind(&state.install_id)
    .bind(&state.tail_session_id)
    .bind(state.tail_session_start)
    .bind(state.tail_session_end)
    .bind(state.tail_event_count)
    .bind(state.tail_duration_seconds)
    .bind(state.tail_day)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

pub async fn insert_session_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionRecord,
) -> Result<u64, StoreError> {
    let result = sqlx::query(
        r#"
        INSERT INTO sessions (
            project_id,
            install_id,
            user_id,
            session_id,
            session_start,
            session_end,
            event_count,
            duration_seconds,
            platform,
            app_version
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(session.project_id)
    .bind(&session.install_id)
    .bind(&session.user_id)
    .bind(&session.session_id)
    .bind(session.session_start)
    .bind(session.session_end)
    .bind(session.event_count)
    .bind(session.duration_seconds)
    .bind(platform_as_str(&session.platform))
    .bind(&session.app_version)
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected())
}

pub async fn update_session_tail_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    update: SessionTailUpdate<'_>,
) -> Result<u64, StoreError> {
    let result = sqlx::query(
        r#"
        UPDATE sessions
        SET session_end = $3,
            event_count = $4,
            duration_seconds = $5,
            user_id = COALESCE($6, user_id),
            app_version = COALESCE($7, app_version)
        WHERE project_id = $1
          AND session_id = $2
        "#,
    )
    .bind(update.project_id)
    .bind(update.session_id)
    .bind(update.session_end)
    .bind(update.event_count)
    .bind(update.duration_seconds)
    .bind(update.user_id)
    .bind(update.app_version)
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected())
}

pub async fn increment_session_daily_for_new_session(
    pool: &PgPool,
    project_id: Uuid,
    day: NaiveDate,
    install_id: &str,
    duration_seconds: i64,
) -> Result<u64, StoreError> {
    increment_session_daily_for_new_session_in_executor(
        pool,
        project_id,
        day,
        install_id,
        duration_seconds,
    )
    .await
}

pub async fn increment_session_daily_for_new_session_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    day: NaiveDate,
    install_id: &str,
    duration_seconds: i64,
) -> Result<u64, StoreError> {
    increment_session_daily_for_new_session_in_executor(
        &mut **tx,
        project_id,
        day,
        install_id,
        duration_seconds,
    )
    .await
}

async fn increment_session_daily_for_new_session_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    day: NaiveDate,
    install_id: &str,
    duration_seconds: i64,
) -> Result<u64, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let result = sqlx::query(
        r#"
        WITH membership AS (
            INSERT INTO session_daily_installs (
                project_id,
                day,
                install_id,
                session_count
            )
            VALUES ($1, $2, $3, 1)
            ON CONFLICT (project_id, day, install_id) DO UPDATE SET
                session_count = session_daily_installs.session_count + 1,
                updated_at = now()
            RETURNING CASE WHEN session_count = 1 THEN 1 ELSE 0 END AS active_install_delta
        )
        INSERT INTO session_daily (
            project_id,
            day,
            sessions_count,
            active_installs,
            total_duration_seconds
        )
        SELECT $1, $2, 1, active_install_delta, $4
        FROM membership
        ON CONFLICT (project_id, day) DO UPDATE SET
            sessions_count = session_daily.sessions_count + 1,
            active_installs = session_daily.active_installs + EXCLUDED.active_installs,
            total_duration_seconds = session_daily.total_duration_seconds + EXCLUDED.total_duration_seconds,
            updated_at = now()
        "#,
    )
    .bind(project_id)
    .bind(day)
    .bind(install_id)
    .bind(duration_seconds)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

pub async fn add_session_daily_duration_delta(
    pool: &PgPool,
    project_id: Uuid,
    day: NaiveDate,
    duration_delta: i64,
) -> Result<u64, StoreError> {
    add_session_daily_duration_delta_in_executor(pool, project_id, day, duration_delta).await
}

pub async fn add_session_daily_duration_delta_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    day: NaiveDate,
    duration_delta: i64,
) -> Result<u64, StoreError> {
    add_session_daily_duration_delta_in_executor(&mut **tx, project_id, day, duration_delta).await
}

async fn add_session_daily_duration_delta_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    day: NaiveDate,
    duration_delta: i64,
) -> Result<u64, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let result = sqlx::query(
        r#"
        UPDATE session_daily
        SET total_duration_seconds = total_duration_seconds + $3,
            updated_at = now()
        WHERE project_id = $1
          AND day = $2
        "#,
    )
    .bind(project_id)
    .bind(day)
    .bind(duration_delta)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

pub async fn fetch_session_daily_range(
    pool: &PgPool,
    project_id: Uuid,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Vec<SessionDailyRecord>, StoreError> {
    let rows = sqlx::query(
        r#"
        SELECT
            project_id,
            day,
            sessions_count,
            active_installs,
            total_duration_seconds
        FROM session_daily
        WHERE project_id = $1
          AND day BETWEEN $2 AND $3
        ORDER BY day ASC
        "#,
    )
    .bind(project_id)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(session_daily_from_row).collect()
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

fn install_session_state_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<InstallSessionStateRecord, StoreError> {
    Ok(InstallSessionStateRecord {
        project_id: row.try_get("project_id")?,
        install_id: row.try_get("install_id")?,
        tail_session_id: row.try_get("tail_session_id")?,
        tail_session_start: row.try_get("tail_session_start")?,
        tail_session_end: row.try_get("tail_session_end")?,
        tail_event_count: row.try_get("tail_event_count")?,
        tail_duration_seconds: row.try_get("tail_duration_seconds")?,
        tail_day: row.try_get("tail_day")?,
    })
}

fn session_daily_from_row(row: sqlx::postgres::PgRow) -> Result<SessionDailyRecord, StoreError> {
    Ok(SessionDailyRecord {
        project_id: row.try_get("project_id")?,
        day: row.try_get("day")?,
        sessions_count: row.try_get("sessions_count")?,
        active_installs: row.try_get("active_installs")?,
        total_duration_seconds: row.try_get("total_duration_seconds")?,
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
    use chrono::{Duration, NaiveDate, TimeZone};
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

    fn tail_state(end_minutes: i64, event_count: i32) -> InstallSessionStateRecord {
        InstallSessionStateRecord {
            project_id: project_id(),
            install_id: "install-1".to_owned(),
            tail_session_id: "install-1:2026-01-01T00:00:00+00:00".to_owned(),
            tail_session_start: timestamp(0),
            tail_session_end: timestamp(end_minutes),
            tail_event_count: event_count,
            tail_duration_seconds: (end_minutes as i32) * 60,
            tail_day: NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"),
        }
    }

    #[sqlx::test]
    async fn run_migrations_creates_session_daily_table_and_indexes(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

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

        let session_daily_index_names = sqlx::query_scalar::<_, String>(
            r#"
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'session_daily'
              AND indexname = 'idx_session_daily_project_day'
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch session_daily index names");

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

        let session_daily_table_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'session_daily'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .expect("fetch session_daily table existence");

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
        assert_eq!(
            session_daily_index_names,
            vec!["idx_session_daily_project_day".to_owned()]
        );
        assert!(session_table_exists);
        assert!(session_daily_table_exists);
    }

    #[sqlx::test]
    async fn ensure_local_project_seeds_project_and_ingest_key_when_config_present(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let project_name =
            sqlx::query_scalar::<_, String>("SELECT name FROM projects WHERE id = $1")
                .bind(project_id())
                .fetch_one(&pool)
                .await
                .expect("fetch project");

        let key_count =
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM api_keys WHERE project_id = $1")
                .bind(project_id())
                .fetch_one(&pool)
                .await
                .expect("count api keys");

        assert_eq!(project_name, "Test Project");
        assert_eq!(key_count, 1);
    }

    #[sqlx::test]
    async fn ensure_local_project_skips_seeding_when_config_absent(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        ensure_local_project(&pool, None)
            .await
            .expect("ensure local project succeeds");

        let project_count = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM projects")
            .fetch_one(&pool)
            .await
            .expect("count projects");

        assert_eq!(project_count, 0);
    }

    #[sqlx::test]
    async fn run_migrations_creates_install_session_state_table(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let table_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'install_session_state'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .expect("fetch tail state table existence");

        assert!(table_exists);
    }

    #[sqlx::test]
    async fn run_migrations_creates_session_daily_installs_table(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let table_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'session_daily_installs'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .expect("fetch membership table existence");

        assert!(table_exists);
    }

    #[sqlx::test]
    async fn install_session_state_round_trips(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        upsert_install_session_state(&pool, &tail_state(10, 2))
            .await
            .expect("save tail state");

        let stored = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load tail state")
            .expect("tail state exists");

        assert_eq!(stored.tail_session_end, timestamp(10));
        assert_eq!(stored.tail_event_count, 2);
        assert_eq!(stored.tail_duration_seconds, 600);
    }

    #[sqlx::test]
    async fn session_daily_updates_incrementally_for_insert_and_extension(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        increment_session_daily_for_new_session(
            &pool,
            project_id(),
            NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"),
            "install-1",
            600,
        )
        .await
        .expect("track new session");
        increment_session_daily_for_new_session(
            &pool,
            project_id(),
            NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"),
            "install-1",
            0,
        )
        .await
        .expect("track second session same install");
        increment_session_daily_for_new_session(
            &pool,
            project_id(),
            NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"),
            "install-2",
            120,
        )
        .await
        .expect("track new install");
        add_session_daily_duration_delta(
            &pool,
            project_id(),
            NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"),
            300,
        )
        .await
        .expect("track extension");

        let row = sqlx::query(
            r#"
            SELECT sessions_count, active_installs, total_duration_seconds
            FROM session_daily
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"))
        .fetch_one(&pool)
        .await
        .expect("fetch session_daily row");

        assert_eq!(row.try_get::<i64, _>("sessions_count").expect("count"), 3);
        assert_eq!(
            row.try_get::<i64, _>("active_installs")
                .expect("active installs"),
            2
        );
        assert_eq!(
            row.try_get::<i64, _>("total_duration_seconds")
                .expect("duration"),
            1020
        );

        let membership_counts = sqlx::query(
            r#"
            SELECT install_id, session_count
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
            ORDER BY install_id ASC
            "#,
        )
        .bind(project_id())
        .bind(NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"))
        .fetch_all(&pool)
        .await
        .expect("fetch membership rows");

        assert_eq!(membership_counts.len(), 2);
        assert_eq!(
            membership_counts[0]
                .try_get::<String, _>("install_id")
                .expect("install id"),
            "install-1"
        );
        assert_eq!(
            membership_counts[0]
                .try_get::<i32, _>("session_count")
                .expect("session count"),
            2
        );
    }

    #[sqlx::test]
    async fn worker_offsets_round_trip(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

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
        run_migrations(&pool).await.expect("migrations succeed");

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
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

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
