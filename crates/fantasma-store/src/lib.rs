use std::{collections::BTreeMap, time::Duration};

use chrono::{DateTime, NaiveDate, Utc};
use fantasma_auth::{derive_key_prefix, hash_ingest_key};
use fantasma_core::{EventPayload, Platform};
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
    pub event_name: String,
    pub timestamp: DateTime<Utc>,
    pub install_id: String,
    pub platform: Platform,
    pub app_version: Option<String>,
    pub os_version: Option<String>,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub project_id: Uuid,
    pub install_id: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCountDailyTotalDelta {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub event_name: String,
    pub event_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCountDailyDim1Delta {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub event_name: String,
    pub dim1_key: String,
    pub dim1_value: String,
    pub event_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCountDailyDim2Delta {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub event_name: String,
    pub dim1_key: String,
    pub dim1_value: String,
    pub dim2_key: String,
    pub dim2_value: String,
    pub event_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCountDailyDim3Delta {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub event_name: String,
    pub dim1_key: String,
    pub dim1_value: String,
    pub dim2_key: String,
    pub dim2_value: String,
    pub dim3_key: String,
    pub dim3_value: String,
    pub event_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventMetricsCubeRow {
    pub day: NaiveDate,
    pub dimensions: BTreeMap<String, String>,
    pub event_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventMetricsAggregateCubeRow {
    pub dimensions: BTreeMap<String, String>,
    pub event_count: u64,
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
        "INSERT INTO events_raw (project_id, event_name, timestamp, install_id, platform, app_version, os_version, properties) ",
    );

    builder.push_values(events, |mut separated, event| {
        separated.push_bind(project_id);
        separated.push_bind(&event.event);
        separated.push_bind(event.timestamp);
        separated.push_bind(&event.install_id);
        separated.push_bind(match event.platform {
            fantasma_core::Platform::Ios => "ios",
            fantasma_core::Platform::Android => "android",
        });
        separated.push_bind(&event.app_version);
        separated.push_bind(&event.os_version);
        separated.push_bind(sqlx::types::Json(
            serde_json::to_value(&event.properties).expect("serialize event properties"),
        ));
    });

    let result = builder.build().execute(pool).await?;

    Ok(result.rows_affected())
}

pub async fn upsert_event_count_daily_totals_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[EventCountDailyTotalDelta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO event_count_daily_total (project_id, day, event_name, event_count) ",
    );
    builder.push_values(deltas, |mut separated, delta| {
        separated.push_bind(delta.project_id);
        separated.push_bind(delta.day);
        separated.push_bind(&delta.event_name);
        separated.push_bind(delta.event_count);
    });
    builder.push(
        " ON CONFLICT (project_id, day, event_name) DO UPDATE SET \
          event_count = event_count_daily_total.event_count + EXCLUDED.event_count, \
          updated_at = now()",
    );

    builder.build().execute(&mut **tx).await?;

    Ok(())
}

pub async fn upsert_event_count_daily_dim1_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[EventCountDailyDim1Delta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO event_count_daily_dim1 \
         (project_id, day, event_name, dim1_key, dim1_value, event_count) ",
    );
    builder.push_values(deltas, |mut separated, delta| {
        separated.push_bind(delta.project_id);
        separated.push_bind(delta.day);
        separated.push_bind(&delta.event_name);
        separated.push_bind(&delta.dim1_key);
        separated.push_bind(&delta.dim1_value);
        separated.push_bind(delta.event_count);
    });
    builder.push(
        " ON CONFLICT (project_id, day, event_name, dim1_key, dim1_value) DO UPDATE SET \
          event_count = event_count_daily_dim1.event_count + EXCLUDED.event_count, \
          updated_at = now()",
    );

    builder.build().execute(&mut **tx).await?;

    Ok(())
}

pub async fn upsert_event_count_daily_dim2_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[EventCountDailyDim2Delta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO event_count_daily_dim2 \
         (project_id, day, event_name, dim1_key, dim1_value, dim2_key, dim2_value, event_count) ",
    );
    builder.push_values(deltas, |mut separated, delta| {
        separated.push_bind(delta.project_id);
        separated.push_bind(delta.day);
        separated.push_bind(&delta.event_name);
        separated.push_bind(&delta.dim1_key);
        separated.push_bind(&delta.dim1_value);
        separated.push_bind(&delta.dim2_key);
        separated.push_bind(&delta.dim2_value);
        separated.push_bind(delta.event_count);
    });
    builder.push(
        " ON CONFLICT \
          (project_id, day, event_name, dim1_key, dim1_value, dim2_key, dim2_value) \
          DO UPDATE SET \
          event_count = event_count_daily_dim2.event_count + EXCLUDED.event_count, \
          updated_at = now()",
    );

    builder.build().execute(&mut **tx).await?;

    Ok(())
}

pub async fn upsert_event_count_daily_dim3_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[EventCountDailyDim3Delta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO event_count_daily_dim3 \
         (project_id, day, event_name, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, event_count) ",
    );
    builder.push_values(deltas, |mut separated, delta| {
        separated.push_bind(delta.project_id);
        separated.push_bind(delta.day);
        separated.push_bind(&delta.event_name);
        separated.push_bind(&delta.dim1_key);
        separated.push_bind(&delta.dim1_value);
        separated.push_bind(&delta.dim2_key);
        separated.push_bind(&delta.dim2_value);
        separated.push_bind(&delta.dim3_key);
        separated.push_bind(&delta.dim3_value);
        separated.push_bind(delta.event_count);
    });
    builder.push(
        " ON CONFLICT \
          (project_id, day, event_name, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value) \
          DO UPDATE SET \
          event_count = event_count_daily_dim3.event_count + EXCLUDED.event_count, \
          updated_at = now()",
    );

    builder.build().execute(&mut **tx).await?;

    Ok(())
}

pub async fn fetch_event_metrics_cube_rows(
    pool: &PgPool,
    project_id: Uuid,
    event_name: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError> {
    fetch_event_metrics_cube_rows_in_executor(
        pool, project_id, event_name, start_date, end_date, cube_keys, filters,
    )
    .await
}

pub async fn fetch_event_metrics_cube_rows_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    event_name: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError> {
    fetch_event_metrics_cube_rows_in_executor(
        &mut **tx, project_id, event_name, start_date, end_date, cube_keys, filters,
    )
    .await
}

async fn fetch_event_metrics_cube_rows_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    event_name: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    ensure_canonical_cube_keys(cube_keys)?;
    ensure_filters_belong_to_cube(cube_keys, filters)?;

    match cube_keys.len() {
        0 => {
            fetch_event_metrics_total_rows(executor, project_id, event_name, start_date, end_date)
                .await
        }
        1 => {
            fetch_event_metrics_dim1_rows(
                executor, project_id, event_name, start_date, end_date, cube_keys, filters,
            )
            .await
        }
        2 => {
            fetch_event_metrics_dim2_rows(
                executor, project_id, event_name, start_date, end_date, cube_keys, filters,
            )
            .await
        }
        3 => {
            fetch_event_metrics_dim3_rows(
                executor, project_id, event_name, start_date, end_date, cube_keys, filters,
            )
            .await
        }
        other => Err(StoreError::InvariantViolation(format!(
            "event metrics cube arity must be between 0 and 3, got {other}"
        ))),
    }
}

pub async fn fetch_event_metrics_aggregate_cube_rows(
    pool: &PgPool,
    project_id: Uuid,
    event_name: &str,
    window: (NaiveDate, NaiveDate),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError> {
    fetch_event_metrics_aggregate_cube_rows_in_executor(
        pool, project_id, event_name, window, cube_keys, filters, limit,
    )
    .await
}

pub async fn fetch_event_metrics_aggregate_cube_rows_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    event_name: &str,
    window: (NaiveDate, NaiveDate),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError> {
    fetch_event_metrics_aggregate_cube_rows_in_executor(
        &mut **tx, project_id, event_name, window, cube_keys, filters, limit,
    )
    .await
}

async fn fetch_event_metrics_aggregate_cube_rows_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    event_name: &str,
    window: (NaiveDate, NaiveDate),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start_date, end_date) = window;
    ensure_canonical_cube_keys(cube_keys)?;
    ensure_filters_belong_to_cube(cube_keys, filters)?;

    match cube_keys.len() {
        0 => {
            fetch_event_metrics_total_aggregate_rows(
                executor,
                project_id,
                event_name,
                (start_date, end_date),
            )
            .await
        }
        1 => {
            fetch_event_metrics_dim1_aggregate_rows(
                executor,
                project_id,
                event_name,
                (start_date, end_date),
                cube_keys,
                filters,
                limit,
            )
            .await
        }
        2 => {
            fetch_event_metrics_dim2_aggregate_rows(
                executor,
                project_id,
                event_name,
                (start_date, end_date),
                cube_keys,
                filters,
                limit,
            )
            .await
        }
        3 => {
            fetch_event_metrics_dim3_aggregate_rows(
                executor,
                project_id,
                event_name,
                (start_date, end_date),
                cube_keys,
                filters,
                limit,
            )
            .await
        }
        other => Err(StoreError::InvariantViolation(format!(
            "event metrics cube arity must be between 0 and 3, got {other}"
        ))),
    }
}

pub async fn fetch_events_after(
    pool: &PgPool,
    last_processed_event_id: i64,
    limit: i64,
) -> Result<Vec<RawEventRecord>, StoreError> {
    let rows = sqlx::query(
        r#"
        SELECT id, project_id, event_name, timestamp, install_id, platform, app_version, os_version, properties
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
    fetch_events_for_install_between_in_executor(pool, project_id, install_id, start, end).await
}

pub async fn fetch_events_for_install_between_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<RawEventRecord>, StoreError> {
    fetch_events_for_install_between_in_executor(&mut **tx, project_id, install_id, start, end)
        .await
}

async fn fetch_events_for_install_between_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    install_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<RawEventRecord>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT id, project_id, event_name, timestamp, install_id, platform, app_version, os_version, properties
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
    .fetch_all(executor)
    .await?;

    rows.into_iter().map(raw_event_from_row).collect()
}

pub async fn fetch_latest_session_for_install(
    pool: &PgPool,
    project_id: Uuid,
    install_id: &str,
) -> Result<Option<SessionRecord>, StoreError> {
    fetch_latest_session_for_install_in_executor(pool, project_id, install_id).await
}

pub async fn fetch_latest_session_for_install_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
) -> Result<Option<SessionRecord>, StoreError> {
    fetch_latest_session_for_install_in_executor(&mut **tx, project_id, install_id).await
}

async fn fetch_latest_session_for_install_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    install_id: &str,
) -> Result<Option<SessionRecord>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT project_id, install_id, session_id, session_start, session_end,
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
    .fetch_optional(executor)
    .await?;

    row.map(session_from_row).transpose()
}

pub async fn fetch_sessions_overlapping_window_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Result<Vec<SessionRecord>, StoreError> {
    let rows = sqlx::query(
        r#"
        SELECT project_id, install_id, session_id, session_start, session_end,
               event_count, duration_seconds, platform, app_version
        FROM sessions
        WHERE project_id = $1
          AND install_id = $2
          AND session_start <= $4
          AND session_end >= $3
        ORDER BY session_start ASC, id ASC
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .bind(window_start)
    .bind(window_end)
    .fetch_all(&mut **tx)
    .await?;

    rows.into_iter().map(session_from_row).collect()
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
            session_id,
            session_start,
            session_end,
            event_count,
            duration_seconds,
            platform,
            app_version
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(session.project_id)
    .bind(&session.install_id)
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
            app_version = COALESCE($6, app_version)
        WHERE project_id = $1
          AND session_id = $2
        "#,
    )
    .bind(update.project_id)
    .bind(update.session_id)
    .bind(update.session_end)
    .bind(update.event_count)
    .bind(update.duration_seconds)
    .bind(update.app_version)
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected())
}

pub async fn delete_sessions_overlapping_window_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Result<u64, StoreError> {
    let result = sqlx::query(
        r#"
        DELETE FROM sessions
        WHERE project_id = $1
          AND install_id = $2
          AND session_start <= $4
          AND session_end >= $3
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .bind(window_start)
    .bind(window_end)
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

pub async fn rebuild_session_daily_days_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    days: &[NaiveDate],
) -> Result<(), StoreError> {
    let days = days
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    if days.is_empty() {
        return Ok(());
    }

    rebuild_session_daily_installs_days_in_tx(tx, project_id, &days).await?;

    sqlx::query(
        r#"
        DELETE FROM session_daily
        WHERE project_id = $1
          AND day = ANY($2)
        "#,
    )
    .bind(project_id)
    .bind(&days)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
        WITH session_rollups AS (
            SELECT
                project_id,
                DATE(session_start AT TIME ZONE 'UTC') AS day,
                COUNT(*)::BIGINT AS sessions_count,
                COALESCE(SUM(duration_seconds), 0)::BIGINT AS total_duration_seconds
            FROM sessions
            WHERE project_id = $1
              AND DATE(session_start AT TIME ZONE 'UTC') = ANY($2)
            GROUP BY project_id, DATE(session_start AT TIME ZONE 'UTC')
        ),
        install_rollups AS (
            SELECT
                project_id,
                day,
                COUNT(*)::BIGINT AS active_installs
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = ANY($2)
            GROUP BY project_id, day
        )
        INSERT INTO session_daily (
            project_id,
            day,
            sessions_count,
            active_installs,
            total_duration_seconds
        )
        SELECT
            session_rollups.project_id,
            session_rollups.day,
            session_rollups.sessions_count,
            COALESCE(install_rollups.active_installs, 0),
            session_rollups.total_duration_seconds
        FROM session_rollups
        LEFT JOIN install_rollups
          ON install_rollups.project_id = session_rollups.project_id
         AND install_rollups.day = session_rollups.day
        "#,
    )
    .bind(project_id)
    .bind(&days)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn rebuild_session_daily_installs_days_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    days: &[NaiveDate],
) -> Result<(), StoreError> {
    sqlx::query(
        r#"
        DELETE FROM session_daily_installs
        WHERE project_id = $1
          AND day = ANY($2)
        "#,
    )
    .bind(project_id)
    .bind(days)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO session_daily_installs (
            project_id,
            day,
            install_id,
            session_count
        )
        SELECT
            project_id,
            DATE(session_start AT TIME ZONE 'UTC') AS day,
            install_id,
            COUNT(*)::INTEGER AS session_count
        FROM sessions
        WHERE project_id = $1
          AND DATE(session_start AT TIME ZONE 'UTC') = ANY($2)
        GROUP BY project_id, DATE(session_start AT TIME ZONE 'UTC'), install_id
        "#,
    )
    .bind(project_id)
    .bind(days)
    .execute(&mut **tx)
    .await?;

    Ok(())
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

async fn fetch_event_metrics_total_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    event_name: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Vec<EventMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT day, event_count
        FROM event_count_daily_total
        WHERE project_id = $1
          AND event_name = $2
          AND day BETWEEN $3 AND $4
        ORDER BY day ASC
        "#,
    )
    .bind(project_id)
    .bind(event_name)
    .bind(start_date)
    .bind(end_date)
    .fetch_all(executor)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| EventMetricsCubeRow {
            day: row.try_get("day").expect("day column exists"),
            dimensions: BTreeMap::new(),
            event_count: row
                .try_get::<i64, _>("event_count")
                .expect("event_count column exists") as u64,
        })
        .collect())
}

async fn fetch_event_metrics_total_aggregate_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    event_name: &str,
    window: (NaiveDate, NaiveDate),
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start_date, end_date) = window;
    let row = sqlx::query(
        r#"
        SELECT SUM(event_count)::BIGINT AS event_count
        FROM event_count_daily_total
        WHERE project_id = $1
          AND event_name = $2
          AND day BETWEEN $3 AND $4
        "#,
    )
    .bind(project_id)
    .bind(event_name)
    .bind(start_date)
    .bind(end_date)
    .fetch_one(executor)
    .await?;

    let event_count = row.try_get::<Option<i64>, _>("event_count")?;

    Ok(event_count
        .map(|event_count| {
            vec![EventMetricsAggregateCubeRow {
                dimensions: BTreeMap::new(),
                event_count: event_count as u64,
            }]
        })
        .unwrap_or_default())
}

async fn fetch_event_metrics_dim1_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    event_name: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT day, dim1_key, dim1_value, event_count \
         FROM event_count_daily_dim1 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND event_name = ");
    builder.push_bind(event_name);
    builder.push(" AND day BETWEEN ");
    builder.push_bind(start_date);
    builder.push(" AND ");
    builder.push_bind(end_date);
    builder.push(" AND dim1_key = ");
    builder.push_bind(&cube_keys[0]);

    if let Some(value) = filters.get(&cube_keys[0]) {
        builder.push(" AND dim1_value = ");
        builder.push_bind(value);
    }

    builder.push(" ORDER BY day ASC, dim1_value ASC");

    let rows = builder.build().fetch_all(executor).await?;

    Ok(rows
        .into_iter()
        .map(|row| EventMetricsCubeRow {
            day: row.try_get("day").expect("day column exists"),
            dimensions: BTreeMap::from([(
                row.try_get::<String, _>("dim1_key")
                    .expect("dim1_key column exists"),
                row.try_get::<String, _>("dim1_value")
                    .expect("dim1_value column exists"),
            )]),
            event_count: row
                .try_get::<i64, _>("event_count")
                .expect("event_count column exists") as u64,
        })
        .collect())
}

async fn fetch_event_metrics_dim1_aggregate_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    event_name: &str,
    window: (NaiveDate, NaiveDate),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start_date, end_date) = window;
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT dim1_key, dim1_value, SUM(event_count)::BIGINT AS event_count \
         FROM event_count_daily_dim1 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND event_name = ");
    builder.push_bind(event_name);
    builder.push(" AND day BETWEEN ");
    builder.push_bind(start_date);
    builder.push(" AND ");
    builder.push_bind(end_date);
    builder.push(" AND dim1_key = ");
    builder.push_bind(&cube_keys[0]);

    if let Some(value) = filters.get(&cube_keys[0]) {
        builder.push(" AND dim1_value = ");
        builder.push_bind(value);
    }

    builder.push(" GROUP BY dim1_key, dim1_value ORDER BY dim1_value ASC");

    if let Some(limit) = limit {
        builder.push(" LIMIT ");
        builder.push_bind(limit as i64);
    }

    let rows = builder.build().fetch_all(executor).await?;

    Ok(rows
        .into_iter()
        .map(|row| EventMetricsAggregateCubeRow {
            dimensions: BTreeMap::from([(
                row.try_get::<String, _>("dim1_key")
                    .expect("dim1_key column exists"),
                row.try_get::<String, _>("dim1_value")
                    .expect("dim1_value column exists"),
            )]),
            event_count: row
                .try_get::<i64, _>("event_count")
                .expect("event_count column exists") as u64,
        })
        .collect())
}

async fn fetch_event_metrics_dim2_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    event_name: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT day, dim1_key, dim1_value, dim2_key, dim2_value, event_count \
         FROM event_count_daily_dim2 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND event_name = ");
    builder.push_bind(event_name);
    builder.push(" AND day BETWEEN ");
    builder.push_bind(start_date);
    builder.push(" AND ");
    builder.push_bind(end_date);
    builder.push(" AND dim1_key = ");
    builder.push_bind(&cube_keys[0]);
    builder.push(" AND dim2_key = ");
    builder.push_bind(&cube_keys[1]);

    if let Some(value) = filters.get(&cube_keys[0]) {
        builder.push(" AND dim1_value = ");
        builder.push_bind(value);
    }

    if let Some(value) = filters.get(&cube_keys[1]) {
        builder.push(" AND dim2_value = ");
        builder.push_bind(value);
    }

    builder.push(" ORDER BY day ASC, dim1_value ASC, dim2_value ASC");

    let rows = builder.build().fetch_all(executor).await?;

    Ok(rows
        .into_iter()
        .map(|row| EventMetricsCubeRow {
            day: row.try_get("day").expect("day column exists"),
            dimensions: BTreeMap::from([
                (
                    row.try_get::<String, _>("dim1_key")
                        .expect("dim1_key column exists"),
                    row.try_get::<String, _>("dim1_value")
                        .expect("dim1_value column exists"),
                ),
                (
                    row.try_get::<String, _>("dim2_key")
                        .expect("dim2_key column exists"),
                    row.try_get::<String, _>("dim2_value")
                        .expect("dim2_value column exists"),
                ),
            ]),
            event_count: row
                .try_get::<i64, _>("event_count")
                .expect("event_count column exists") as u64,
        })
        .collect())
}

async fn fetch_event_metrics_dim2_aggregate_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    event_name: &str,
    window: (NaiveDate, NaiveDate),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start_date, end_date) = window;
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT dim1_key, dim1_value, dim2_key, dim2_value, SUM(event_count)::BIGINT AS event_count \
         FROM event_count_daily_dim2 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND event_name = ");
    builder.push_bind(event_name);
    builder.push(" AND day BETWEEN ");
    builder.push_bind(start_date);
    builder.push(" AND ");
    builder.push_bind(end_date);
    builder.push(" AND dim1_key = ");
    builder.push_bind(&cube_keys[0]);
    builder.push(" AND dim2_key = ");
    builder.push_bind(&cube_keys[1]);

    if let Some(value) = filters.get(&cube_keys[0]) {
        builder.push(" AND dim1_value = ");
        builder.push_bind(value);
    }

    if let Some(value) = filters.get(&cube_keys[1]) {
        builder.push(" AND dim2_value = ");
        builder.push_bind(value);
    }

    builder.push(
        " GROUP BY dim1_key, dim1_value, dim2_key, dim2_value \
          ORDER BY dim1_value ASC, dim2_value ASC",
    );

    if let Some(limit) = limit {
        builder.push(" LIMIT ");
        builder.push_bind(limit as i64);
    }

    let rows = builder.build().fetch_all(executor).await?;

    Ok(rows
        .into_iter()
        .map(|row| EventMetricsAggregateCubeRow {
            dimensions: BTreeMap::from([
                (
                    row.try_get::<String, _>("dim1_key")
                        .expect("dim1_key column exists"),
                    row.try_get::<String, _>("dim1_value")
                        .expect("dim1_value column exists"),
                ),
                (
                    row.try_get::<String, _>("dim2_key")
                        .expect("dim2_key column exists"),
                    row.try_get::<String, _>("dim2_value")
                        .expect("dim2_value column exists"),
                ),
            ]),
            event_count: row
                .try_get::<i64, _>("event_count")
                .expect("event_count column exists") as u64,
        })
        .collect())
}

async fn fetch_event_metrics_dim3_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    event_name: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT day, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, event_count \
         FROM event_count_daily_dim3 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND event_name = ");
    builder.push_bind(event_name);
    builder.push(" AND day BETWEEN ");
    builder.push_bind(start_date);
    builder.push(" AND ");
    builder.push_bind(end_date);
    builder.push(" AND dim1_key = ");
    builder.push_bind(&cube_keys[0]);
    builder.push(" AND dim2_key = ");
    builder.push_bind(&cube_keys[1]);
    builder.push(" AND dim3_key = ");
    builder.push_bind(&cube_keys[2]);

    if let Some(value) = filters.get(&cube_keys[0]) {
        builder.push(" AND dim1_value = ");
        builder.push_bind(value);
    }

    if let Some(value) = filters.get(&cube_keys[1]) {
        builder.push(" AND dim2_value = ");
        builder.push_bind(value);
    }

    if let Some(value) = filters.get(&cube_keys[2]) {
        builder.push(" AND dim3_value = ");
        builder.push_bind(value);
    }

    builder.push(" ORDER BY day ASC, dim1_value ASC, dim2_value ASC, dim3_value ASC");

    let rows = builder.build().fetch_all(executor).await?;

    Ok(rows
        .into_iter()
        .map(|row| EventMetricsCubeRow {
            day: row.try_get("day").expect("day column exists"),
            dimensions: BTreeMap::from([
                (
                    row.try_get::<String, _>("dim1_key")
                        .expect("dim1_key column exists"),
                    row.try_get::<String, _>("dim1_value")
                        .expect("dim1_value column exists"),
                ),
                (
                    row.try_get::<String, _>("dim2_key")
                        .expect("dim2_key column exists"),
                    row.try_get::<String, _>("dim2_value")
                        .expect("dim2_value column exists"),
                ),
                (
                    row.try_get::<String, _>("dim3_key")
                        .expect("dim3_key column exists"),
                    row.try_get::<String, _>("dim3_value")
                        .expect("dim3_value column exists"),
                ),
            ]),
            event_count: row
                .try_get::<i64, _>("event_count")
                .expect("event_count column exists") as u64,
        })
        .collect())
}

async fn fetch_event_metrics_dim3_aggregate_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    event_name: &str,
    window: (NaiveDate, NaiveDate),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start_date, end_date) = window;
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, \
                SUM(event_count)::BIGINT AS event_count \
         FROM event_count_daily_dim3 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND event_name = ");
    builder.push_bind(event_name);
    builder.push(" AND day BETWEEN ");
    builder.push_bind(start_date);
    builder.push(" AND ");
    builder.push_bind(end_date);
    builder.push(" AND dim1_key = ");
    builder.push_bind(&cube_keys[0]);
    builder.push(" AND dim2_key = ");
    builder.push_bind(&cube_keys[1]);
    builder.push(" AND dim3_key = ");
    builder.push_bind(&cube_keys[2]);

    if let Some(value) = filters.get(&cube_keys[0]) {
        builder.push(" AND dim1_value = ");
        builder.push_bind(value);
    }

    if let Some(value) = filters.get(&cube_keys[1]) {
        builder.push(" AND dim2_value = ");
        builder.push_bind(value);
    }

    if let Some(value) = filters.get(&cube_keys[2]) {
        builder.push(" AND dim3_value = ");
        builder.push_bind(value);
    }

    builder.push(
        " GROUP BY dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value \
          ORDER BY dim1_value ASC, dim2_value ASC, dim3_value ASC",
    );

    if let Some(limit) = limit {
        builder.push(" LIMIT ");
        builder.push_bind(limit as i64);
    }

    let rows = builder.build().fetch_all(executor).await?;

    Ok(rows
        .into_iter()
        .map(|row| EventMetricsAggregateCubeRow {
            dimensions: BTreeMap::from([
                (
                    row.try_get::<String, _>("dim1_key")
                        .expect("dim1_key column exists"),
                    row.try_get::<String, _>("dim1_value")
                        .expect("dim1_value column exists"),
                ),
                (
                    row.try_get::<String, _>("dim2_key")
                        .expect("dim2_key column exists"),
                    row.try_get::<String, _>("dim2_value")
                        .expect("dim2_value column exists"),
                ),
                (
                    row.try_get::<String, _>("dim3_key")
                        .expect("dim3_key column exists"),
                    row.try_get::<String, _>("dim3_value")
                        .expect("dim3_value column exists"),
                ),
            ]),
            event_count: row
                .try_get::<i64, _>("event_count")
                .expect("event_count column exists") as u64,
        })
        .collect())
}

fn ensure_canonical_cube_keys(cube_keys: &[String]) -> Result<(), StoreError> {
    if cube_keys.windows(2).any(|window| window[0] >= window[1]) {
        return Err(StoreError::InvariantViolation(
            "event metrics cube keys must be strictly increasing".to_owned(),
        ));
    }

    Ok(())
}

fn ensure_filters_belong_to_cube(
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<(), StoreError> {
    if filters
        .keys()
        .all(|key| cube_keys.iter().any(|cube_key| cube_key == key))
    {
        return Ok(());
    }

    Err(StoreError::InvariantViolation(
        "event metrics filters must be a subset of the requested cube".to_owned(),
    ))
}

fn raw_event_from_row(row: sqlx::postgres::PgRow) -> Result<RawEventRecord, StoreError> {
    Ok(RawEventRecord {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        event_name: row.try_get("event_name")?,
        timestamp: row.try_get("timestamp")?,
        install_id: row.try_get("install_id")?,
        platform: platform_from_str(&row.try_get::<String, _>("platform")?)?,
        app_version: row.try_get("app_version")?,
        os_version: row.try_get("os_version")?,
        properties: row
            .try_get::<Option<sqlx::types::Json<BTreeMap<String, String>>>, _>("properties")?
            .map(|json| json.0)
            .unwrap_or_default(),
    })
}

fn session_from_row(row: sqlx::postgres::PgRow) -> Result<SessionRecord, StoreError> {
    Ok(SessionRecord {
        project_id: row.try_get("project_id")?,
        install_id: row.try_get("install_id")?,
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
    use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
    use fantasma_core::EventPayload;
    use serde_json::Value;

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
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
            os_version: Some("18.3".to_owned()),
            properties: BTreeMap::new(),
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

    fn session(install_id: &str, start_minutes: i64, end_minutes: i64) -> SessionRecord {
        let session_start = timestamp(start_minutes);
        let session_end = timestamp(end_minutes);

        SessionRecord {
            project_id: project_id(),
            install_id: install_id.to_owned(),
            session_id: format!("{install_id}:{}", session_start.to_rfc3339()),
            session_start,
            session_end,
            event_count: if end_minutes > start_minutes { 2 } else { 1 },
            duration_seconds: session_end
                .signed_duration_since(session_start)
                .num_seconds() as i32,
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
        }
    }

    fn explain_plan_root(plan: &Value) -> &Value {
        &plan.as_array().expect("EXPLAIN JSON is an array")[0]["Plan"]
    }

    fn plan_contains_node_type(plan: &Value, node_type: &str) -> bool {
        let Some(object) = plan.as_object() else {
            return false;
        };

        if object
            .get("Node Type")
            .and_then(Value::as_str)
            .is_some_and(|value| value == node_type)
        {
            return true;
        }

        object
            .get("Plans")
            .and_then(Value::as_array)
            .is_some_and(|plans| {
                plans
                    .iter()
                    .any(|plan| plan_contains_node_type(plan, node_type))
            })
    }

    fn collect_plan_index_names(plan: &Value, names: &mut Vec<String>) {
        let Some(object) = plan.as_object() else {
            return;
        };

        if let Some(index_name) = object.get("Index Name").and_then(Value::as_str) {
            names.push(index_name.to_owned());
        }

        if let Some(plans) = object.get("Plans").and_then(Value::as_array) {
            for child in plans {
                collect_plan_index_names(child, names);
            }
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

        let event_metrics_index_names = sqlx::query_scalar::<_, String>(
            r#"
            SELECT indexname
            FROM pg_indexes
            WHERE tablename IN (
                'event_count_daily_total',
                'event_count_daily_dim1',
                'event_count_daily_dim2',
                'event_count_daily_dim3'
            )
              AND indexname IN (
                'idx_event_count_daily_total_project_event_day',
                'idx_event_count_daily_dim1_project_event_dims_day',
                'idx_event_count_daily_dim2_project_event_keys_day',
                'idx_event_count_daily_dim2_project_event_dim1_value_day',
                'idx_event_count_daily_dim2_project_event_dim2_value_day',
                'idx_event_count_daily_dim3_project_event_keys_day',
                'idx_event_count_daily_dim3_project_event_dim1_value_day',
                'idx_event_count_daily_dim3_project_event_dim2_value_day',
                'idx_event_count_daily_dim3_project_event_dim3_value_day'
              )
            ORDER BY indexname
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch event metric index names");

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

        let event_metrics_table_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_name IN (
                'event_count_daily_total',
                'event_count_daily_dim1',
                'event_count_daily_dim2',
                'event_count_daily_dim3'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .expect("fetch event metrics table count");

        let os_version_column_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'events_raw'
                  AND column_name = 'os_version'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .expect("fetch os_version column existence");

        let removed_identity_columns = sqlx::query_scalar::<_, String>(
            r#"
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name IN ('events_raw', 'sessions')
              AND column_name IN ('user_id', 'session_id')
            ORDER BY table_name, column_name
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch removed identity columns");

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
        assert_eq!(
            event_metrics_index_names,
            vec![
                "idx_event_count_daily_dim1_project_event_dims_day".to_owned(),
                "idx_event_count_daily_dim2_project_event_dim1_value_day".to_owned(),
                "idx_event_count_daily_dim2_project_event_dim2_value_day".to_owned(),
                "idx_event_count_daily_dim2_project_event_keys_day".to_owned(),
                "idx_event_count_daily_dim3_project_event_dim1_value_day".to_owned(),
                "idx_event_count_daily_dim3_project_event_dim2_value_day".to_owned(),
                "idx_event_count_daily_dim3_project_event_dim3_value_day".to_owned(),
                "idx_event_count_daily_dim3_project_event_keys_day".to_owned(),
                "idx_event_count_daily_total_project_event_day".to_owned(),
            ]
        );
        assert!(session_table_exists);
        assert!(session_daily_table_exists);
        assert_eq!(event_metrics_table_count, 4);
        assert!(os_version_column_exists);
        assert_eq!(removed_identity_columns, vec!["session_id".to_owned()]);
    }

    #[sqlx::test]
    async fn event_metrics_dim_tables_enforce_canonical_key_order(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let error = sqlx::query(
            r#"
            INSERT INTO event_count_daily_dim2 (
                project_id,
                day,
                event_name,
                dim1_key,
                dim1_value,
                dim2_key,
                dim2_value,
                event_count
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(project_id())
        .bind(NaiveDate::from_ymd_opt(2026, 3, 1).expect("date"))
        .bind("app_open")
        .bind("provider")
        .bind("strava")
        .bind("app_version")
        .bind("1.4.0")
        .bind(1_i64)
        .execute(&pool)
        .await
        .expect_err("out-of-order dimension keys should fail");

        assert!(matches!(error, sqlx::Error::Database(_)));
    }

    #[sqlx::test]
    async fn event_metrics_dim2_reads_use_bounded_read_indexes(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        sqlx::query(
            r#"
            INSERT INTO event_count_daily_dim2 (
                project_id,
                day,
                event_name,
                dim1_key,
                dim1_value,
                dim2_key,
                dim2_value,
                event_count
            )
            SELECT
                $1,
                DATE '2026-01-01' + day_offset,
                'app_open',
                'app_version',
                '1.' || version_offset || '.0',
                'provider',
                'provider-' || provider_offset,
                1
            FROM generate_series(0, 29) AS day_offset
            CROSS JOIN generate_series(0, 5) AS version_offset
            CROSS JOIN generate_series(0, 79) AS provider_offset
            "#,
        )
        .bind(project_id())
        .execute(&pool)
        .await
        .expect("insert dim2 rows");
        sqlx::query("ANALYZE event_count_daily_dim2")
            .execute(&pool)
            .await
            .expect("analyze dim2 table");

        let row = sqlx::query(
            r#"
            EXPLAIN (FORMAT JSON)
            SELECT day, dim1_key, dim1_value, dim2_key, dim2_value, event_count
            FROM event_count_daily_dim2
            WHERE project_id = $1
              AND event_name = $2
              AND day BETWEEN $3 AND $4
              AND dim1_key = $5
              AND dim2_key = $6
              AND dim2_value = $7
            ORDER BY day ASC, dim1_value ASC, dim2_value ASC
            "#,
        )
        .bind(project_id())
        .bind("app_open")
        .bind(NaiveDate::from_ymd_opt(2026, 1, 10).expect("valid date"))
        .bind(NaiveDate::from_ymd_opt(2026, 1, 20).expect("valid date"))
        .bind("app_version")
        .bind("provider")
        .bind("provider-11")
        .fetch_one(&pool)
        .await
        .expect("explain dim2 query");
        let plan = row
            .try_get::<Value, _>(0)
            .expect("EXPLAIN returns json plan");
        let root = explain_plan_root(&plan);
        let mut index_names = Vec::new();
        collect_plan_index_names(root, &mut index_names);

        assert!(
            !plan_contains_node_type(root, "Seq Scan"),
            "dim2 read path should avoid sequential scans: {plan:#}"
        );
        assert!(
            index_names.iter().any(|name| {
                name == "idx_event_count_daily_dim2_project_event_keys_day"
                    || name == "idx_event_count_daily_dim2_project_event_dim1_value_day"
                    || name == "idx_event_count_daily_dim2_project_event_dim2_value_day"
            }),
            "dim2 read path should use a dedicated read index, got {index_names:?} from {plan:#}"
        );
    }

    #[sqlx::test]
    async fn event_metrics_dim3_reads_use_bounded_read_indexes(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        sqlx::query(
            r#"
            INSERT INTO event_count_daily_dim3 (
                project_id,
                day,
                event_name,
                dim1_key,
                dim1_value,
                dim2_key,
                dim2_value,
                dim3_key,
                dim3_value,
                event_count
            )
            SELECT
                $1,
                DATE '2026-01-01' + day_offset,
                'app_open',
                'app_version',
                '1.' || version_offset || '.0',
                'provider',
                'provider-' || provider_offset,
                'region',
                'region-' || region_offset,
                1
            FROM generate_series(0, 19) AS day_offset
            CROSS JOIN generate_series(0, 4) AS version_offset
            CROSS JOIN generate_series(0, 49) AS provider_offset
            CROSS JOIN generate_series(0, 7) AS region_offset
            "#,
        )
        .bind(project_id())
        .execute(&pool)
        .await
        .expect("insert dim3 rows");
        sqlx::query("ANALYZE event_count_daily_dim3")
            .execute(&pool)
            .await
            .expect("analyze dim3 table");

        let row = sqlx::query(
            r#"
            EXPLAIN (FORMAT JSON)
            SELECT day, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, event_count
            FROM event_count_daily_dim3
            WHERE project_id = $1
              AND event_name = $2
              AND day BETWEEN $3 AND $4
              AND dim1_key = $5
              AND dim2_key = $6
              AND dim3_key = $7
              AND dim3_value = $8
            ORDER BY day ASC, dim1_value ASC, dim2_value ASC, dim3_value ASC
            "#,
        )
        .bind(project_id())
        .bind("app_open")
        .bind(NaiveDate::from_ymd_opt(2026, 1, 5).expect("valid date"))
        .bind(NaiveDate::from_ymd_opt(2026, 1, 12).expect("valid date"))
        .bind("app_version")
        .bind("provider")
        .bind("region")
        .bind("region-3")
        .fetch_one(&pool)
        .await
        .expect("explain dim3 query");
        let plan = row
            .try_get::<Value, _>(0)
            .expect("EXPLAIN returns json plan");
        let root = explain_plan_root(&plan);
        let mut index_names = Vec::new();
        collect_plan_index_names(root, &mut index_names);

        assert!(
            !plan_contains_node_type(root, "Seq Scan"),
            "dim3 read path should avoid sequential scans: {plan:#}"
        );
        assert!(
            index_names.iter().any(|name| {
                name == "idx_event_count_daily_dim3_project_event_keys_day"
                    || name == "idx_event_count_daily_dim3_project_event_dim1_value_day"
                    || name == "idx_event_count_daily_dim3_project_event_dim2_value_day"
                    || name == "idx_event_count_daily_dim3_project_event_dim3_value_day"
            }),
            "dim3 read path should use a dedicated read index, got {index_names:?} from {plan:#}"
        );
    }

    #[test]
    fn baseline_migration_retains_legacy_identity_columns_until_forward_migrations_remove_them() {
        let migration = include_str!("../migrations/0001_initial.sql");

        assert!(
            migration.contains("user_id TEXT"),
            "0001_initial.sql must retain the original events_raw/sessions identity columns"
        );
        assert!(
            migration.contains("session_id TEXT"),
            "0001_initial.sql must retain the original raw session_id column"
        );
    }

    #[test]
    fn forward_migration_removes_legacy_identity_columns() {
        let migration = include_str!("../migrations/0006_remove_legacy_identity_columns.sql");

        assert!(
            migration.contains("ALTER TABLE events_raw")
                && migration.contains("DROP COLUMN IF EXISTS user_id")
                && migration.contains("DROP COLUMN IF EXISTS session_id"),
            "0006 must remove legacy identity columns from events_raw"
        );
        assert!(
            migration.contains("ALTER TABLE sessions")
                && migration.contains("DROP COLUMN IF EXISTS user_id"),
            "0006 must remove legacy user_id from sessions"
        );
    }

    #[test]
    fn forward_migration_replaces_event_metrics_read_indexes() {
        let migration = include_str!("../migrations/0007_event_metrics_read_indexes.sql");

        assert!(
            migration
                .contains("DROP INDEX IF EXISTS idx_event_count_daily_dim2_project_event_dims_day")
                && migration.contains(
                    "DROP INDEX IF EXISTS idx_event_count_daily_dim3_project_event_dims_day"
                ),
            "0007 must remove the older slot-ordered dim2/dim3 read indexes"
        );
        assert!(
            migration.contains("idx_event_count_daily_dim2_project_event_keys_day")
                && migration.contains("idx_event_count_daily_dim2_project_event_dim1_value_day")
                && migration.contains("idx_event_count_daily_dim2_project_event_dim2_value_day")
                && migration.contains("idx_event_count_daily_dim3_project_event_keys_day")
                && migration.contains("idx_event_count_daily_dim3_project_event_dim1_value_day")
                && migration.contains("idx_event_count_daily_dim3_project_event_dim2_value_day")
                && migration.contains("idx_event_count_daily_dim3_project_event_dim3_value_day"),
            "0007 must add the expanded dim2/dim3 read-path indexes"
        );
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

    #[sqlx::test]
    async fn fetch_sessions_overlapping_window_in_tx_returns_only_overlaps(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let mut tx = pool.begin().await.expect("begin transaction");
        insert_session_in_tx(&mut tx, &session("install-1", 0, 10))
            .await
            .expect("insert first session");
        insert_session_in_tx(&mut tx, &session("install-1", 45, 60))
            .await
            .expect("insert second session");
        insert_session_in_tx(&mut tx, &session("install-2", 5, 15))
            .await
            .expect("insert other install session");

        let overlapping = fetch_sessions_overlapping_window_in_tx(
            &mut tx,
            project_id(),
            "install-1",
            timestamp(5),
            timestamp(50),
        )
        .await
        .expect("fetch overlapping sessions");
        let non_overlapping = fetch_sessions_overlapping_window_in_tx(
            &mut tx,
            project_id(),
            "install-1",
            timestamp(11),
            timestamp(44),
        )
        .await
        .expect("fetch non-overlapping sessions");

        assert_eq!(overlapping.len(), 2);
        assert_eq!(overlapping[0].session_start, timestamp(0));
        assert_eq!(overlapping[1].session_start, timestamp(45));
        assert!(non_overlapping.is_empty());
    }

    #[sqlx::test]
    async fn rebuild_session_daily_days_in_tx_leaves_untouched_days_alone(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");
        let day_2 = NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid date");
        let day_3 = NaiveDate::from_ymd_opt(2026, 1, 3).expect("valid date");

        let mut seed_tx = pool.begin().await.expect("begin seed transaction");
        insert_session_in_tx(&mut seed_tx, &session("install-1", 0, 10))
            .await
            .expect("insert day 1 session");
        insert_session_in_tx(&mut seed_tx, &session("install-2", 24 * 60, 24 * 60))
            .await
            .expect("insert day 2 session");
        insert_session_in_tx(&mut seed_tx, &session("install-3", 48 * 60, 48 * 60 + 10))
            .await
            .expect("insert day 3 session");
        rebuild_session_daily_days_in_tx(&mut seed_tx, project_id(), &[day_1, day_2, day_3])
            .await
            .expect("seed session_daily rows");
        seed_tx.commit().await.expect("commit seed transaction");

        let middle_day_before = sqlx::query(
            r#"
            SELECT sessions_count, active_installs, total_duration_seconds, updated_at
            FROM session_daily
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(day_2)
        .fetch_one(&pool)
        .await
        .expect("fetch middle day row");

        sqlx::query("SELECT pg_sleep(0.01)")
            .execute(&pool)
            .await
            .expect("sleep for updated_at delta");

        sqlx::query(
            r#"
            UPDATE sessions
            SET session_end = $3,
                duration_seconds = $4
            WHERE project_id = $1
              AND install_id = $2
            "#,
        )
        .bind(project_id())
        .bind("install-1")
        .bind(timestamp(45))
        .bind(45 * 60)
        .execute(&pool)
        .await
        .expect("update day 1 session");
        sqlx::query(
            r#"
            UPDATE sessions
            SET session_end = $3,
                duration_seconds = $4
            WHERE project_id = $1
              AND install_id = $2
            "#,
        )
        .bind(project_id())
        .bind("install-3")
        .bind(timestamp(48 * 60 + 45))
        .bind(45 * 60)
        .execute(&pool)
        .await
        .expect("update day 3 session");

        let mut rebuild_tx = pool.begin().await.expect("begin rebuild transaction");
        rebuild_session_daily_days_in_tx(&mut rebuild_tx, project_id(), &[day_1, day_3])
            .await
            .expect("rebuild touched days");
        rebuild_tx
            .commit()
            .await
            .expect("commit rebuild transaction");

        let day_1_row = sqlx::query(
            r#"
            SELECT sessions_count, active_installs, total_duration_seconds
            FROM session_daily
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(day_1)
        .fetch_one(&pool)
        .await
        .expect("fetch day 1 row");
        let middle_day_after = sqlx::query(
            r#"
            SELECT sessions_count, active_installs, total_duration_seconds, updated_at
            FROM session_daily
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(day_2)
        .fetch_one(&pool)
        .await
        .expect("fetch middle day row after rebuild");
        let day_3_row = sqlx::query(
            r#"
            SELECT sessions_count, active_installs, total_duration_seconds
            FROM session_daily
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(day_3)
        .fetch_one(&pool)
        .await
        .expect("fetch day 3 row");

        assert_eq!(day_1_row.try_get::<i64, _>("sessions_count").unwrap(), 1);
        assert_eq!(day_1_row.try_get::<i64, _>("active_installs").unwrap(), 1);
        assert_eq!(
            day_1_row
                .try_get::<i64, _>("total_duration_seconds")
                .unwrap(),
            45 * 60
        );
        assert_eq!(
            middle_day_after
                .try_get::<i64, _>("sessions_count")
                .unwrap(),
            middle_day_before
                .try_get::<i64, _>("sessions_count")
                .unwrap()
        );
        assert_eq!(
            middle_day_after
                .try_get::<i64, _>("active_installs")
                .unwrap(),
            middle_day_before
                .try_get::<i64, _>("active_installs")
                .unwrap()
        );
        assert_eq!(
            middle_day_after
                .try_get::<i64, _>("total_duration_seconds")
                .unwrap(),
            middle_day_before
                .try_get::<i64, _>("total_duration_seconds")
                .unwrap()
        );
        assert_eq!(
            middle_day_after
                .try_get::<DateTime<Utc>, _>("updated_at")
                .unwrap(),
            middle_day_before
                .try_get::<DateTime<Utc>, _>("updated_at")
                .unwrap()
        );
        assert_eq!(day_3_row.try_get::<i64, _>("sessions_count").unwrap(), 1);
        assert_eq!(day_3_row.try_get::<i64, _>("active_installs").unwrap(), 1);
        assert_eq!(
            day_3_row
                .try_get::<i64, _>("total_duration_seconds")
                .unwrap(),
            45 * 60
        );
    }
}
