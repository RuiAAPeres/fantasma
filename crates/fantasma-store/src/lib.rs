#![allow(clippy::too_many_arguments)]

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use chrono::{DateTime, NaiveDate, Utc};
use fantasma_auth::{derive_key_prefix, hash_ingest_key};
use fantasma_core::{EventPayload, MetricGranularity, Platform, SessionMetric};
pub use sqlx::PgPool;
use sqlx::{Postgres, QueryBuilder, Row, Transaction, migrate::Migrator, postgres::PgPoolOptions};
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 5;
const POSTGRES_MAX_BIND_PARAMS: usize = 65_535;
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

fn max_rows_per_batched_statement(bind_params_per_row: usize) -> usize {
    debug_assert!(bind_params_per_row > 0);
    (POSTGRES_MAX_BIND_PARAMS / bind_params_per_row).max(1)
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectRecord {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCatalogRecord {
    pub event_name: String,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopEventRecord {
    pub event_name: String,
    pub event_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBucketCountRecord {
    pub bucket_start: DateTime<Utc>,
    pub event_count: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyKind {
    Ingest,
    Read,
}

impl ApiKeyKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ingest => "ingest",
            Self::Read => "read",
        }
    }

    pub fn secret_prefix(self) -> &'static str {
        match self {
            Self::Ingest => "fg_ing_",
            Self::Read => "fg_rd_",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyRecord {
    pub id: Uuid,
    pub project_id: Uuid,
    pub name: String,
    pub kind: ApiKeyKind,
    pub key_prefix: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedApiKey {
    pub key_id: Uuid,
    pub project_id: Uuid,
    pub kind: ApiKeyKind,
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
    pub os_version: Option<String>,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy)]
pub struct SessionTailUpdate<'a> {
    pub project_id: Uuid,
    pub session_id: &'a str,
    pub session_end: DateTime<Utc>,
    pub event_count: i32,
    pub duration_seconds: i32,
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
    pub last_processed_event_id: i64,
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
pub struct SessionDailyInstallDelta {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub install_id: String,
    pub session_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionActiveInstallSliceDelta {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub install_id: String,
    pub platform: String,
    pub app_version: Option<String>,
    pub app_version_is_null: bool,
    pub os_version: Option<String>,
    pub os_version_is_null: bool,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDailyActiveInstallDelta {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub active_installs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDailyDelta {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub sessions_count: i64,
    pub active_installs: i64,
    pub total_duration_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDailyDurationDelta {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub duration_delta: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventMetricBucketTotalDelta {
    pub project_id: Uuid,
    pub granularity: MetricGranularity,
    pub bucket_start: DateTime<Utc>,
    pub event_name: String,
    pub event_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventMetricBucketDim1Delta {
    pub project_id: Uuid,
    pub granularity: MetricGranularity,
    pub bucket_start: DateTime<Utc>,
    pub event_name: String,
    pub dim1_key: String,
    pub dim1_value: String,
    pub event_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventMetricBucketDim2Delta {
    pub project_id: Uuid,
    pub granularity: MetricGranularity,
    pub bucket_start: DateTime<Utc>,
    pub event_name: String,
    pub dim1_key: String,
    pub dim1_value: String,
    pub dim2_key: String,
    pub dim2_value: String,
    pub event_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventMetricsCubeRow {
    pub bucket_start: DateTime<Utc>,
    pub dimensions: BTreeMap<String, String>,
    pub event_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventMetricsAggregateCubeRow {
    pub dimensions: BTreeMap<String, String>,
    pub event_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMetricTotalDelta {
    pub project_id: Uuid,
    pub granularity: MetricGranularity,
    pub bucket_start: DateTime<Utc>,
    pub session_count: i64,
    pub duration_total_seconds: i64,
    pub new_installs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMetricDim1Delta {
    pub project_id: Uuid,
    pub granularity: MetricGranularity,
    pub bucket_start: DateTime<Utc>,
    pub dim1_key: String,
    pub dim1_value: String,
    pub session_count: i64,
    pub duration_total_seconds: i64,
    pub new_installs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMetricDim2Delta {
    pub project_id: Uuid,
    pub granularity: MetricGranularity,
    pub bucket_start: DateTime<Utc>,
    pub dim1_key: String,
    pub dim1_value: String,
    pub dim2_key: String,
    pub dim2_value: String,
    pub session_count: i64,
    pub duration_total_seconds: i64,
    pub new_installs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMetricsCubeRow {
    pub bucket_start: DateTime<Utc>,
    pub dimensions: BTreeMap<String, String>,
    pub value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMetricsAggregateCubeRow {
    pub dimensions: BTreeMap<String, String>,
    pub value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallFirstSeenRecord {
    pub project_id: Uuid,
    pub install_id: String,
    pub first_seen_event_id: i64,
    pub first_seen_at: DateTime<Utc>,
    pub platform: Platform,
    pub app_version: Option<String>,
    pub os_version: Option<String>,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSessionRebuildBucketRecord {
    pub project_id: Uuid,
    pub granularity: MetricGranularity,
    pub bucket_start: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSessionDailyInstallRebuildRecord {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub install_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRepairJobRecord {
    pub project_id: Uuid,
    pub install_id: String,
    pub target_event_id: i64,
    pub claimed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRepairJobUpsert {
    pub project_id: Uuid,
    pub install_id: String,
    pub target_event_id: i64,
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
            INSERT INTO api_keys (id, project_id, name, kind, key_prefix, key_hash)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (key_hash) DO UPDATE
            SET project_id = EXCLUDED.project_id,
                name = EXCLUDED.name,
                kind = EXCLUDED.kind,
                key_prefix = EXCLUDED.key_prefix,
                revoked_at = NULL
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(config.project_id)
        .bind("Bootstrap key")
        .bind(ApiKeyKind::Ingest.as_str())
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

pub async fn create_project(pool: &PgPool, name: &str) -> Result<ProjectRecord, StoreError> {
    let row = sqlx::query(
        r#"
        INSERT INTO projects (id, name)
        VALUES ($1, $2)
        RETURNING id, name, created_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(name)
    .fetch_one(pool)
    .await?;

    project_record_from_row(&row)
}

pub async fn create_project_with_api_key(
    pool: &PgPool,
    project_name: &str,
    key_name: &str,
    kind: ApiKeyKind,
    secret: &str,
) -> Result<(ProjectRecord, ApiKeyRecord), StoreError> {
    let mut tx = pool.begin().await?;

    let project_row = sqlx::query(
        r#"
        INSERT INTO projects (id, name)
        VALUES ($1, $2)
        RETURNING id, name, created_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(project_name)
    .fetch_one(&mut *tx)
    .await?;
    let project = project_record_from_row(&project_row)?;

    let key_row = sqlx::query(
        r#"
        INSERT INTO api_keys (id, project_id, name, kind, key_prefix, key_hash)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, project_id, name, kind, key_prefix, created_at, revoked_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(project.id)
    .bind(key_name)
    .bind(kind.as_str())
    .bind(derive_key_prefix(secret))
    .bind(hash_ingest_key(secret))
    .fetch_one(&mut *tx)
    .await?;
    let key = api_key_record_from_row(&key_row)?;

    tx.commit().await?;

    Ok((project, key))
}

pub async fn list_projects(pool: &PgPool) -> Result<Vec<ProjectRecord>, StoreError> {
    let rows = sqlx::query(
        r#"
        SELECT id, name, created_at
        FROM projects
        ORDER BY created_at ASC, id ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    rows.iter().map(project_record_from_row).collect()
}

pub async fn load_project(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<Option<ProjectRecord>, StoreError> {
    let row = sqlx::query(
        r#"
        SELECT id, name, created_at
        FROM projects
        WHERE id = $1
        "#,
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await?;

    row.as_ref().map(project_record_from_row).transpose()
}

pub async fn rename_project(
    pool: &PgPool,
    project_id: Uuid,
    name: &str,
) -> Result<Option<ProjectRecord>, StoreError> {
    let row = sqlx::query(
        r#"
        UPDATE projects
        SET name = $2
        WHERE id = $1
        RETURNING id, name, created_at
        "#,
    )
    .bind(project_id)
    .bind(name)
    .fetch_optional(pool)
    .await?;

    row.as_ref().map(project_record_from_row).transpose()
}

pub async fn create_api_key(
    pool: &PgPool,
    project_id: Uuid,
    name: &str,
    kind: ApiKeyKind,
    secret: &str,
) -> Result<Option<ApiKeyRecord>, StoreError> {
    let row = sqlx::query(
        r#"
        INSERT INTO api_keys (id, project_id, name, kind, key_prefix, key_hash)
        SELECT $1, projects.id, $2, $3, $4, $5
        FROM projects
        WHERE projects.id = $6
        RETURNING id, project_id, name, kind, key_prefix, created_at, revoked_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(name)
    .bind(kind.as_str())
    .bind(derive_key_prefix(secret))
    .bind(hash_ingest_key(secret))
    .bind(project_id)
    .fetch_optional(pool)
    .await?;

    row.as_ref().map(api_key_record_from_row).transpose()
}

pub fn generate_api_key_secret(kind: ApiKeyKind) -> String {
    format!("{}{}", kind.secret_prefix(), Uuid::new_v4().simple())
}

pub async fn list_api_keys(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<Option<Vec<ApiKeyRecord>>, StoreError> {
    if load_project(pool, project_id).await?.is_none() {
        return Ok(None);
    }

    let rows = sqlx::query(
        r#"
        SELECT id, project_id, name, kind, key_prefix, created_at, revoked_at
        FROM api_keys
        WHERE project_id = $1
        ORDER BY created_at ASC, id ASC
        "#,
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;

    rows.iter()
        .map(api_key_record_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub async fn revoke_api_key(
    pool: &PgPool,
    project_id: Uuid,
    key_id: Uuid,
) -> Result<bool, StoreError> {
    let revoked = sqlx::query_scalar::<_, Uuid>(
        r#"
        UPDATE api_keys
        SET revoked_at = COALESCE(revoked_at, now())
        WHERE project_id = $1
          AND id = $2
        RETURNING id
        "#,
    )
    .bind(project_id)
    .bind(key_id)
    .fetch_optional(pool)
    .await?;

    Ok(revoked.is_some())
}

pub async fn resolve_api_key(
    pool: &PgPool,
    secret: &str,
) -> Result<Option<ResolvedApiKey>, StoreError> {
    let row = sqlx::query(
        r#"
        SELECT id, project_id, kind
        FROM api_keys
        WHERE key_hash = $1
          AND revoked_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(hash_ingest_key(secret))
    .fetch_optional(pool)
    .await?;

    row.as_ref().map(resolved_api_key_from_row).transpose()
}

pub async fn resolve_project_id_for_ingest_key(
    pool: &PgPool,
    ingest_key: &str,
) -> Result<Option<Uuid>, StoreError> {
    let resolved = resolve_api_key(pool, ingest_key).await?;

    Ok(resolved
        .filter(|record| record.kind == ApiKeyKind::Ingest)
        .map(|record| record.project_id))
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

pub async fn upsert_event_metric_buckets_total_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[EventMetricBucketTotalDelta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    for chunk in deltas.chunks(max_rows_per_batched_statement(5)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO event_metric_buckets_total \
             (project_id, granularity, bucket_start, event_name, event_count) ",
        );
        builder.push_values(chunk, |mut separated, delta| {
            separated.push_bind(delta.project_id);
            separated.push_bind(delta.granularity.as_str());
            separated.push_bind(delta.bucket_start);
            separated.push_bind(&delta.event_name);
            separated.push_bind(delta.event_count);
        });
        builder.push(
            " ON CONFLICT (project_id, granularity, bucket_start, event_name) DO UPDATE SET \
              event_count = event_metric_buckets_total.event_count + EXCLUDED.event_count, \
              updated_at = now()",
        );

        builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub async fn upsert_event_metric_buckets_dim1_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[EventMetricBucketDim1Delta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    for chunk in deltas.chunks(max_rows_per_batched_statement(7)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO event_metric_buckets_dim1 \
             (project_id, granularity, bucket_start, event_name, dim1_key, dim1_value, event_count) ",
        );
        builder.push_values(chunk, |mut separated, delta| {
            separated.push_bind(delta.project_id);
            separated.push_bind(delta.granularity.as_str());
            separated.push_bind(delta.bucket_start);
            separated.push_bind(&delta.event_name);
            separated.push_bind(&delta.dim1_key);
            separated.push_bind(&delta.dim1_value);
            separated.push_bind(delta.event_count);
        });
        builder.push(
            " ON CONFLICT (project_id, granularity, bucket_start, event_name, dim1_key, dim1_value) DO UPDATE SET \
              event_count = event_metric_buckets_dim1.event_count + EXCLUDED.event_count, \
              updated_at = now()",
        );

        builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub async fn upsert_event_metric_buckets_dim2_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[EventMetricBucketDim2Delta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    for chunk in deltas.chunks(max_rows_per_batched_statement(9)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO event_metric_buckets_dim2 \
             (project_id, granularity, bucket_start, event_name, dim1_key, dim1_value, dim2_key, dim2_value, event_count) ",
        );
        builder.push_values(chunk, |mut separated, delta| {
            separated.push_bind(delta.project_id);
            separated.push_bind(delta.granularity.as_str());
            separated.push_bind(delta.bucket_start);
            separated.push_bind(&delta.event_name);
            separated.push_bind(&delta.dim1_key);
            separated.push_bind(&delta.dim1_value);
            separated.push_bind(&delta.dim2_key);
            separated.push_bind(&delta.dim2_value);
            separated.push_bind(delta.event_count);
        });
        builder.push(
            " ON CONFLICT \
              (project_id, granularity, bucket_start, event_name, dim1_key, dim1_value, dim2_key, dim2_value) \
              DO UPDATE SET \
              event_count = event_metric_buckets_dim2.event_count + EXCLUDED.event_count, \
              updated_at = now()",
        );

        builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub async fn upsert_session_metric_totals_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[SessionMetricTotalDelta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    for chunk in deltas.chunks(max_rows_per_batched_statement(6)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO session_metric_buckets_total \
             (project_id, granularity, bucket_start, session_count, duration_total_seconds, new_installs) ",
        );
        builder.push_values(chunk, |mut separated, delta| {
            separated.push_bind(delta.project_id);
            separated.push_bind(delta.granularity.as_str());
            separated.push_bind(delta.bucket_start);
            separated.push_bind(delta.session_count);
            separated.push_bind(delta.duration_total_seconds);
            separated.push_bind(delta.new_installs);
        });
        builder.push(
            " ON CONFLICT (project_id, granularity, bucket_start) DO UPDATE SET \
              session_count = session_metric_buckets_total.session_count + EXCLUDED.session_count, \
              duration_total_seconds = session_metric_buckets_total.duration_total_seconds + EXCLUDED.duration_total_seconds, \
              new_installs = session_metric_buckets_total.new_installs + EXCLUDED.new_installs, \
              updated_at = now()",
        );

        builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub async fn upsert_session_metric_dim1_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[SessionMetricDim1Delta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    for chunk in deltas.chunks(max_rows_per_batched_statement(8)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO session_metric_buckets_dim1 \
             (project_id, granularity, bucket_start, dim1_key, dim1_value, session_count, duration_total_seconds, new_installs) ",
        );
        builder.push_values(chunk, |mut separated, delta| {
            separated.push_bind(delta.project_id);
            separated.push_bind(delta.granularity.as_str());
            separated.push_bind(delta.bucket_start);
            separated.push_bind(&delta.dim1_key);
            separated.push_bind(&delta.dim1_value);
            separated.push_bind(delta.session_count);
            separated.push_bind(delta.duration_total_seconds);
            separated.push_bind(delta.new_installs);
        });
        builder.push(
            " ON CONFLICT (project_id, granularity, bucket_start, dim1_key, dim1_value) DO UPDATE SET \
              session_count = session_metric_buckets_dim1.session_count + EXCLUDED.session_count, \
              duration_total_seconds = session_metric_buckets_dim1.duration_total_seconds + EXCLUDED.duration_total_seconds, \
              new_installs = session_metric_buckets_dim1.new_installs + EXCLUDED.new_installs, \
              updated_at = now()",
        );

        builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub async fn upsert_session_metric_dim2_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[SessionMetricDim2Delta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    for chunk in deltas.chunks(max_rows_per_batched_statement(10)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO session_metric_buckets_dim2 \
             (project_id, granularity, bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, session_count, duration_total_seconds, new_installs) ",
        );
        builder.push_values(chunk, |mut separated, delta| {
            separated.push_bind(delta.project_id);
            separated.push_bind(delta.granularity.as_str());
            separated.push_bind(delta.bucket_start);
            separated.push_bind(&delta.dim1_key);
            separated.push_bind(&delta.dim1_value);
            separated.push_bind(&delta.dim2_key);
            separated.push_bind(&delta.dim2_value);
            separated.push_bind(delta.session_count);
            separated.push_bind(delta.duration_total_seconds);
            separated.push_bind(delta.new_installs);
        });
        builder.push(
            " ON CONFLICT \
              (project_id, granularity, bucket_start, dim1_key, dim1_value, dim2_key, dim2_value) \
              DO UPDATE SET \
              session_count = session_metric_buckets_dim2.session_count + EXCLUDED.session_count, \
              duration_total_seconds = session_metric_buckets_dim2.duration_total_seconds + EXCLUDED.duration_total_seconds, \
              new_installs = session_metric_buckets_dim2.new_installs + EXCLUDED.new_installs, \
              updated_at = now()",
        );

        builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub async fn insert_install_first_seen_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    record: &InstallFirstSeenRecord,
) -> Result<bool, StoreError> {
    let result = sqlx::query(
        r#"
        INSERT INTO install_first_seen (
            project_id,
            install_id,
            first_seen_event_id,
            first_seen_at,
            platform,
            app_version,
            os_version,
            properties
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (project_id, install_id) DO NOTHING
        "#,
    )
    .bind(record.project_id)
    .bind(&record.install_id)
    .bind(record.first_seen_event_id)
    .bind(record.first_seen_at)
    .bind(platform_as_str(&record.platform))
    .bind(&record.app_version)
    .bind(&record.os_version)
    .bind(sqlx::types::Json(record.properties.clone()))
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected() == 1)
}

pub async fn fetch_event_metrics_cube_rows_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    granularity: MetricGranularity,
    event_name: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError> {
    fetch_event_metrics_cube_rows_in_executor(
        &mut **tx,
        project_id,
        granularity,
        event_name,
        start,
        end,
        cube_keys,
        filters,
    )
    .await
}

async fn fetch_event_metrics_cube_rows_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    event_name: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
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
            fetch_event_metrics_total_rows(
                executor,
                project_id,
                granularity,
                event_name,
                start,
                end,
            )
            .await
        }
        1 => {
            fetch_event_metrics_dim1_rows(
                executor,
                project_id,
                granularity,
                event_name,
                start,
                end,
                cube_keys,
                filters,
            )
            .await
        }
        2 => {
            fetch_event_metrics_dim2_rows(
                executor,
                project_id,
                granularity,
                event_name,
                start,
                end,
                cube_keys,
                filters,
            )
            .await
        }
        other => Err(StoreError::InvariantViolation(format!(
            "event metrics cube arity must be between 0 and 2, got {other}"
        ))),
    }
}

pub async fn fetch_event_metrics_aggregate_cube_rows_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    granularity: MetricGranularity,
    event_name: &str,
    window: (DateTime<Utc>, DateTime<Utc>),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError> {
    fetch_event_metrics_aggregate_cube_rows_in_executor(
        &mut **tx,
        project_id,
        granularity,
        event_name,
        window,
        cube_keys,
        filters,
        limit,
    )
    .await
}

async fn fetch_event_metrics_aggregate_cube_rows_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    event_name: &str,
    window: (DateTime<Utc>, DateTime<Utc>),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start, end) = window;
    ensure_canonical_cube_keys(cube_keys)?;
    ensure_filters_belong_to_cube(cube_keys, filters)?;

    match cube_keys.len() {
        0 => {
            fetch_event_metrics_total_aggregate_rows(
                executor,
                project_id,
                granularity,
                event_name,
                (start, end),
            )
            .await
        }
        1 => {
            fetch_event_metrics_dim1_aggregate_rows(
                executor,
                project_id,
                granularity,
                event_name,
                (start, end),
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
                granularity,
                event_name,
                (start, end),
                cube_keys,
                filters,
                limit,
            )
            .await
        }
        other => Err(StoreError::InvariantViolation(format!(
            "event metrics cube arity must be between 0 and 2, got {other}"
        ))),
    }
}

pub async fn fetch_session_metrics_cube_rows_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    granularity: MetricGranularity,
    metric: SessionMetric,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<SessionMetricsCubeRow>, StoreError> {
    fetch_session_metrics_cube_rows_in_executor(
        &mut **tx,
        project_id,
        granularity,
        metric,
        start,
        end,
        cube_keys,
        filters,
    )
    .await
}

async fn fetch_session_metrics_cube_rows_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    metric: SessionMetric,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<SessionMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    ensure_canonical_cube_keys(cube_keys)?;
    ensure_filters_belong_to_cube(cube_keys, filters)?;

    match cube_keys.len() {
        0 => {
            fetch_session_metrics_total_rows(executor, project_id, granularity, metric, start, end)
                .await
        }
        1 => {
            fetch_session_metrics_dim1_rows(
                executor,
                project_id,
                granularity,
                metric,
                start,
                end,
                cube_keys,
                filters,
            )
            .await
        }
        2 => {
            fetch_session_metrics_dim2_rows(
                executor,
                project_id,
                granularity,
                metric,
                start,
                end,
                cube_keys,
                filters,
            )
            .await
        }
        other => Err(StoreError::InvariantViolation(format!(
            "session metrics cube arity must be between 0 and 2, got {other}"
        ))),
    }
}

pub async fn fetch_session_metrics_aggregate_cube_rows_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    granularity: MetricGranularity,
    metric: SessionMetric,
    window: (DateTime<Utc>, DateTime<Utc>),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<SessionMetricsAggregateCubeRow>, StoreError> {
    fetch_session_metrics_aggregate_cube_rows_in_executor(
        &mut **tx,
        project_id,
        granularity,
        metric,
        window,
        cube_keys,
        filters,
        limit,
    )
    .await
}

async fn fetch_session_metrics_aggregate_cube_rows_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    metric: SessionMetric,
    window: (DateTime<Utc>, DateTime<Utc>),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<SessionMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start, end) = window;
    ensure_canonical_cube_keys(cube_keys)?;
    ensure_filters_belong_to_cube(cube_keys, filters)?;

    match cube_keys.len() {
        0 => {
            fetch_session_metrics_total_aggregate_rows(
                executor,
                project_id,
                granularity,
                metric,
                (start, end),
            )
            .await
        }
        1 => {
            fetch_session_metrics_dim1_aggregate_rows(
                executor,
                project_id,
                granularity,
                metric,
                (start, end),
                cube_keys,
                filters,
                limit,
            )
            .await
        }
        2 => {
            fetch_session_metrics_dim2_aggregate_rows(
                executor,
                project_id,
                granularity,
                metric,
                (start, end),
                cube_keys,
                filters,
                limit,
            )
            .await
        }
        other => Err(StoreError::InvariantViolation(format!(
            "session metrics cube arity must be between 0 and 2, got {other}"
        ))),
    }
}

pub async fn fetch_events_after_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    last_processed_event_id: i64,
    limit: i64,
) -> Result<Vec<RawEventRecord>, StoreError> {
    fetch_events_after_in_executor(&mut **tx, last_processed_event_id, limit).await
}

async fn fetch_events_after_in_executor<'a, E>(
    executor: E,
    last_processed_event_id: i64,
    limit: i64,
) -> Result<Vec<RawEventRecord>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
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
    .fetch_all(executor)
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

pub async fn list_event_catalog(
    pool: &PgPool,
    project_id: Uuid,
    start: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventCatalogRecord>, StoreError> {
    let mut query = QueryBuilder::<Postgres>::new(
        "SELECT event_name, MAX(timestamp) AS last_seen_at \
         FROM events_raw \
         WHERE project_id = ",
    );
    query.push_bind(project_id);
    query.push(" AND timestamp >= ");
    query.push_bind(start);
    query.push(" AND timestamp < ");
    query.push_bind(end_exclusive);
    append_raw_event_filters(&mut query, filters);
    query.push(" GROUP BY event_name ORDER BY MAX(timestamp) DESC, event_name ASC");

    let rows = query.build().fetch_all(pool).await?;

    rows.into_iter()
        .map(|row| {
            Ok(EventCatalogRecord {
                event_name: row.try_get("event_name")?,
                last_seen_at: row.try_get("last_seen_at")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(StoreError::Database)
}

pub async fn list_top_events(
    pool: &PgPool,
    project_id: Uuid,
    start: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
    filters: &BTreeMap<String, String>,
    limit: i64,
) -> Result<Vec<TopEventRecord>, StoreError> {
    let mut query = QueryBuilder::<Postgres>::new(
        "SELECT event_name, COUNT(*)::bigint AS event_count \
         FROM events_raw \
         WHERE project_id = ",
    );
    query.push_bind(project_id);
    query.push(" AND timestamp >= ");
    query.push_bind(start);
    query.push(" AND timestamp < ");
    query.push_bind(end_exclusive);
    append_raw_event_filters(&mut query, filters);
    query.push(" GROUP BY event_name ORDER BY COUNT(*) DESC, event_name ASC LIMIT ");
    query.push_bind(limit);

    let rows = query.build().fetch_all(pool).await?;

    rows.into_iter()
        .map(|row| {
            Ok(TopEventRecord {
                event_name: row.try_get("event_name")?,
                event_count: row.try_get("event_count")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(StoreError::Database)
}

pub async fn fetch_total_event_counts_by_bucket(
    pool: &PgPool,
    project_id: Uuid,
    granularity: MetricGranularity,
    start: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventBucketCountRecord>, StoreError> {
    let bucket_expression = match granularity {
        MetricGranularity::Hour => "date_trunc('hour', timestamp)",
        MetricGranularity::Day => "date_trunc('day', timestamp)",
    };

    let mut query = QueryBuilder::<Postgres>::new("SELECT ");
    query.push(bucket_expression);
    query.push(
        " AS bucket_start, COUNT(*)::bigint AS event_count FROM events_raw WHERE project_id = ",
    );
    query.push_bind(project_id);
    query.push(" AND timestamp >= ");
    query.push_bind(start);
    query.push(" AND timestamp < ");
    query.push_bind(end_exclusive);
    append_raw_event_filters(&mut query, filters);
    query.push(" GROUP BY 1 ORDER BY 1 ASC");

    let rows = query.build().fetch_all(pool).await?;

    rows.into_iter()
        .map(|row| {
            Ok(EventBucketCountRecord {
                bucket_start: row.try_get("bucket_start")?,
                event_count: row.try_get("event_count")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(StoreError::Database)
}

pub async fn fetch_events_for_install_after_id_through_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
    last_processed_event_id: i64,
    target_event_id: i64,
) -> Result<Vec<RawEventRecord>, StoreError> {
    let rows = sqlx::query(
        r#"
        SELECT id, project_id, event_name, timestamp, install_id, platform, app_version, os_version, properties
        FROM events_raw
        WHERE project_id = $1
          AND install_id = $2
          AND id > $3
          AND id <= $4
        ORDER BY id ASC
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .bind(last_processed_event_id)
    .bind(target_event_id)
    .fetch_all(&mut **tx)
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
               event_count, duration_seconds, platform, app_version, os_version, properties
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
               event_count, duration_seconds, platform, app_version, os_version, properties
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

pub async fn load_install_session_states_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    installs: &[(Uuid, String)],
) -> Result<BTreeMap<(Uuid, String), InstallSessionStateRecord>, StoreError> {
    if installs.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new("WITH requested(project_id, install_id) AS (");
    builder.push_values(installs, |mut row, (project_id, install_id)| {
        row.push_bind(project_id).push_bind(install_id);
    });
    builder.push(
        ") SELECT \
            s.project_id, \
            s.install_id, \
            s.tail_session_id, \
            s.tail_session_start, \
            s.tail_session_end, \
            s.tail_event_count, \
            s.tail_duration_seconds, \
            s.tail_day, \
            s.last_processed_event_id \
          FROM install_session_state s \
          INNER JOIN requested r \
            ON s.project_id = r.project_id \
           AND s.install_id = r.install_id",
    );

    let rows = builder.build().fetch_all(&mut **tx).await?;
    let mut states = BTreeMap::new();
    for row in rows {
        let state = install_session_state_from_row(row)?;
        states.insert((state.project_id, state.install_id.clone()), state);
    }

    Ok(states)
}

pub async fn load_tail_sessions_for_installs_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    installs: &[(Uuid, String)],
) -> Result<BTreeMap<(Uuid, String), SessionRecord>, StoreError> {
    if installs.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new("WITH requested(project_id, install_id) AS (");
    builder.push_values(installs, |mut row, (project_id, install_id)| {
        row.push_bind(project_id).push_bind(install_id);
    });
    builder.push(
        ") SELECT \
            s.project_id, \
            s.install_id, \
            s.session_id, \
            s.session_start, \
            s.session_end, \
            s.event_count, \
            s.duration_seconds, \
            s.platform, \
            s.app_version, \
            s.os_version, \
            s.properties \
          FROM install_session_state state \
          INNER JOIN requested r \
            ON state.project_id = r.project_id \
           AND state.install_id = r.install_id \
          INNER JOIN sessions s \
            ON s.project_id = state.project_id \
           AND s.session_id = state.tail_session_id",
    );

    let rows = builder.build().fetch_all(&mut **tx).await?;
    let mut sessions = BTreeMap::new();
    for row in rows {
        let session = session_from_row(row)?;
        sessions.insert((session.project_id, session.install_id.clone()), session);
    }

    Ok(sessions)
}

pub async fn load_session_repair_jobs_for_installs_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    installs: &[(Uuid, String)],
) -> Result<BTreeMap<(Uuid, String), SessionRepairJobRecord>, StoreError> {
    if installs.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new("WITH requested(project_id, install_id) AS (");
    builder.push_values(installs, |mut row, (project_id, install_id)| {
        row.push_bind(project_id).push_bind(install_id);
    });
    builder.push(
        ") SELECT \
            j.project_id, \
            j.install_id, \
            j.target_event_id, \
            j.claimed_at, \
            j.created_at, \
            j.updated_at \
          FROM session_repair_jobs j \
          INNER JOIN requested r \
            ON j.project_id = r.project_id \
           AND j.install_id = r.install_id",
    );

    let rows = builder.build().fetch_all(&mut **tx).await?;
    let mut jobs = BTreeMap::new();
    for row in rows {
        let job = session_repair_job_from_row(row)?;
        jobs.insert((job.project_id, job.install_id.clone()), job);
    }

    Ok(jobs)
}

pub async fn upsert_session_repair_job_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
    target_event_id: i64,
) -> Result<u64, StoreError> {
    let result = sqlx::query(
        r#"
        INSERT INTO session_repair_jobs (project_id, install_id, target_event_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (project_id, install_id) DO UPDATE SET
            target_event_id = GREATEST(
                session_repair_jobs.target_event_id,
                EXCLUDED.target_event_id
            ),
            updated_at = now()
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .bind(target_event_id)
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected())
}

pub async fn upsert_session_repair_jobs_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    jobs: &[SessionRepairJobUpsert],
) -> Result<(), StoreError> {
    if jobs.is_empty() {
        return Ok(());
    }

    for chunk in jobs.chunks(max_rows_per_batched_statement(3)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO session_repair_jobs (project_id, install_id, target_event_id) ",
        );
        builder.push_values(chunk, |mut separated, job| {
            separated
                .push_bind(job.project_id)
                .push_bind(&job.install_id)
                .push_bind(job.target_event_id);
        });
        builder.push(
            " ON CONFLICT (project_id, install_id) DO UPDATE SET \
              target_event_id = GREATEST(session_repair_jobs.target_event_id, EXCLUDED.target_event_id), \
              updated_at = now()",
        );
        builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub async fn claim_pending_session_repair_jobs_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    limit: i64,
) -> Result<Vec<SessionRepairJobRecord>, StoreError> {
    let rows = sqlx::query(
        r#"
        WITH claimable AS (
            SELECT project_id, install_id
            FROM session_repair_jobs
            WHERE claimed_at IS NULL
            ORDER BY updated_at ASC, project_id ASC, install_id ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE session_repair_jobs AS jobs
        SET claimed_at = now()
        FROM claimable
        WHERE jobs.project_id = claimable.project_id
          AND jobs.install_id = claimable.install_id
        RETURNING jobs.project_id, jobs.install_id, jobs.target_event_id, jobs.claimed_at, jobs.created_at, jobs.updated_at
        "#,
    )
    .bind(limit.max(1))
    .fetch_all(&mut **tx)
    .await?;

    rows.into_iter().map(session_repair_job_from_row).collect()
}

pub async fn complete_session_repair_job_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
    expected_target_event_id: i64,
) -> Result<(), StoreError> {
    let deleted = sqlx::query(
        r#"
        DELETE FROM session_repair_jobs
        WHERE project_id = $1
          AND install_id = $2
          AND target_event_id = $3
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .bind(expected_target_event_id)
    .execute(&mut **tx)
    .await?;

    if deleted.rows_affected() == 0 {
        sqlx::query(
            r#"
            UPDATE session_repair_jobs
            SET claimed_at = NULL,
                updated_at = now()
            WHERE project_id = $1
              AND install_id = $2
              AND claimed_at IS NOT NULL
            "#,
        )
        .bind(project_id)
        .bind(install_id)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

pub async fn release_session_repair_job_claim_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
) -> Result<(), StoreError> {
    sqlx::query(
        r#"
        UPDATE session_repair_jobs
        SET claimed_at = NULL,
            updated_at = now()
        WHERE project_id = $1
          AND install_id = $2
          AND claimed_at IS NOT NULL
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
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
            tail_day,
            last_processed_event_id
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
            tail_day,
            last_processed_event_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (project_id, install_id) DO UPDATE SET
            tail_session_id = EXCLUDED.tail_session_id,
            tail_session_start = EXCLUDED.tail_session_start,
            tail_session_end = EXCLUDED.tail_session_end,
            tail_event_count = EXCLUDED.tail_event_count,
            tail_duration_seconds = EXCLUDED.tail_duration_seconds,
            tail_day = EXCLUDED.tail_day,
            last_processed_event_id = EXCLUDED.last_processed_event_id,
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
    .bind(state.last_processed_event_id)
    .execute(executor)
    .await?;

    Ok(result.rows_affected())
}

pub async fn enqueue_session_rebuild_buckets_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    day_bucket_starts: &[DateTime<Utc>],
    hour_bucket_starts: &[DateTime<Utc>],
) -> Result<(), StoreError> {
    let mut pending_rows = Vec::with_capacity(
        day_bucket_starts
            .len()
            .saturating_add(hour_bucket_starts.len()),
    );
    pending_rows.extend(
        day_bucket_starts
            .iter()
            .copied()
            .map(|bucket_start| (MetricGranularity::Day, bucket_start)),
    );
    pending_rows.extend(
        hour_bucket_starts
            .iter()
            .copied()
            .map(|bucket_start| (MetricGranularity::Hour, bucket_start)),
    );

    if pending_rows.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO session_rebuild_queue (project_id, granularity, bucket_start) ",
    );
    builder.push_values(
        pending_rows,
        |mut separated, (granularity, bucket_start)| {
            separated
                .push_bind(project_id)
                .push_bind(granularity.as_str())
                .push_bind(bucket_start);
        },
    );
    builder.push(" ON CONFLICT (project_id, granularity, bucket_start) DO NOTHING");
    builder.build().execute(&mut **tx).await?;

    Ok(())
}

pub async fn enqueue_session_daily_installs_rebuilds_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    rebuilds: &[PendingSessionDailyInstallRebuildRecord],
) -> Result<(), StoreError> {
    if rebuilds.is_empty() {
        return Ok(());
    }

    for chunk in rebuilds.chunks(max_rows_per_batched_statement(3)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO session_daily_installs_rebuild_queue (project_id, day, install_id) ",
        );
        builder.push_values(chunk, |mut separated, rebuild| {
            separated
                .push_bind(rebuild.project_id)
                .push_bind(rebuild.day)
                .push_bind(&rebuild.install_id);
        });
        builder.push(" ON CONFLICT (project_id, day, install_id) DO NOTHING");
        builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub async fn load_pending_session_rebuild_buckets_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_ids: &[Uuid],
) -> Result<Vec<PendingSessionRebuildBucketRecord>, StoreError> {
    if project_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new("WITH requested(project_id) AS (");
    builder.push_values(project_ids, |mut row, project_id| {
        row.push_bind(project_id);
    });
    builder.push(
        ") SELECT \
            q.project_id, \
            q.granularity, \
            q.bucket_start \
          FROM session_rebuild_queue q \
          INNER JOIN requested r \
            ON q.project_id = r.project_id \
          ORDER BY q.project_id ASC, q.granularity ASC, q.bucket_start ASC",
    );

    let rows = builder.build().fetch_all(&mut **tx).await?;
    rows.into_iter()
        .map(|row| {
            Ok(PendingSessionRebuildBucketRecord {
                project_id: row.try_get("project_id")?,
                granularity: metric_granularity_from_str(row.try_get::<&str, _>("granularity")?)?,
                bucket_start: row.try_get("bucket_start")?,
            })
        })
        .collect()
}

pub async fn load_pending_session_daily_install_rebuilds_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_ids: &[Uuid],
) -> Result<Vec<PendingSessionDailyInstallRebuildRecord>, StoreError> {
    if project_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new("WITH requested(project_id) AS (");
    builder.push_values(project_ids, |mut row, project_id| {
        row.push_bind(project_id);
    });
    builder.push(
        ") SELECT \
            q.project_id, \
            q.day, \
            q.install_id \
          FROM session_daily_installs_rebuild_queue q \
          INNER JOIN requested r \
            ON q.project_id = r.project_id \
          ORDER BY q.project_id ASC, q.day ASC, q.install_id ASC",
    );

    let rows = builder.build().fetch_all(&mut **tx).await?;
    rows.into_iter()
        .map(|row| {
            Ok(PendingSessionDailyInstallRebuildRecord {
                project_id: row.try_get("project_id")?,
                day: row.try_get("day")?,
                install_id: row.try_get("install_id")?,
            })
        })
        .collect()
}

pub async fn claim_pending_session_rebuild_buckets_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    limit: i64,
) -> Result<Vec<PendingSessionRebuildBucketRecord>, StoreError> {
    let rows = sqlx::query(
        r#"
        WITH claimable AS (
            SELECT project_id, granularity, bucket_start
            FROM session_rebuild_queue
            ORDER BY project_id ASC, granularity ASC, bucket_start ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM session_rebuild_queue AS queue
        USING claimable
        WHERE queue.project_id = claimable.project_id
          AND queue.granularity = claimable.granularity
          AND queue.bucket_start = claimable.bucket_start
        RETURNING queue.project_id, queue.granularity, queue.bucket_start
        "#,
    )
    .bind(limit.max(1))
    .fetch_all(&mut **tx)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(PendingSessionRebuildBucketRecord {
                project_id: row.try_get("project_id")?,
                granularity: metric_granularity_from_str(row.try_get::<&str, _>("granularity")?)?,
                bucket_start: row.try_get("bucket_start")?,
            })
        })
        .collect()
}

pub async fn claim_and_sweep_pending_session_rebuild_buckets_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    limit: i64,
) -> Result<Vec<PendingSessionRebuildBucketRecord>, StoreError> {
    let mut swept = claim_pending_session_rebuild_buckets_in_tx(tx, limit).await?;
    if swept.is_empty() {
        return Ok(swept);
    }

    let project_ids = swept
        .iter()
        .map(|bucket| bucket.project_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut builder = QueryBuilder::<Postgres>::new("WITH requested(project_id) AS (");
    builder.push_values(&project_ids, |mut row, project_id| {
        row.push_bind(project_id);
    });
    builder.push(
        ") , sweepable AS ( \
            SELECT q.project_id, q.granularity, q.bucket_start \
            FROM session_rebuild_queue q \
            INNER JOIN requested r \
              ON q.project_id = r.project_id \
            ORDER BY q.project_id ASC, q.granularity ASC, q.bucket_start ASC \
            FOR UPDATE SKIP LOCKED \
        ) DELETE FROM session_rebuild_queue q \
          USING sweepable s \
          WHERE q.project_id = s.project_id \
            AND q.granularity = s.granularity \
            AND q.bucket_start = s.bucket_start \
          RETURNING q.project_id, q.granularity, q.bucket_start",
    );

    let rows = builder.build().fetch_all(&mut **tx).await?;
    swept.extend(
        rows.into_iter()
            .map(|row| {
                Ok(PendingSessionRebuildBucketRecord {
                    project_id: row.try_get("project_id")?,
                    granularity: metric_granularity_from_str(
                        row.try_get::<&str, _>("granularity")?,
                    )?,
                    bucket_start: row.try_get("bucket_start")?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?,
    );
    swept.sort_by(|left, right| {
        left.project_id
            .cmp(&right.project_id)
            .then_with(|| left.granularity.as_str().cmp(right.granularity.as_str()))
            .then_with(|| left.bucket_start.cmp(&right.bucket_start))
    });

    Ok(swept)
}

pub async fn delete_pending_session_rebuild_buckets_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_ids: &[Uuid],
) -> Result<u64, StoreError> {
    if project_ids.is_empty() {
        return Ok(0);
    }

    let mut builder = QueryBuilder::<Postgres>::new("WITH requested(project_id) AS (");
    builder.push_values(project_ids, |mut row, project_id| {
        row.push_bind(project_id);
    });
    builder.push(
        ") DELETE FROM session_rebuild_queue q \
         USING requested r \
         WHERE q.project_id = r.project_id",
    );

    let result = builder.build().execute(&mut **tx).await?;
    Ok(result.rows_affected())
}

pub async fn delete_pending_session_daily_install_rebuilds_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_ids: &[Uuid],
) -> Result<u64, StoreError> {
    if project_ids.is_empty() {
        return Ok(0);
    }

    let mut builder = QueryBuilder::<Postgres>::new("WITH requested(project_id) AS (");
    builder.push_values(project_ids, |mut row, project_id| {
        row.push_bind(project_id);
    });
    builder.push(
        ") DELETE FROM session_daily_installs_rebuild_queue q \
         USING requested r \
         WHERE q.project_id = r.project_id",
    );

    let result = builder.build().execute(&mut **tx).await?;
    Ok(result.rows_affected())
}

fn metric_granularity_from_str(value: &str) -> Result<MetricGranularity, StoreError> {
    match value {
        "day" => Ok(MetricGranularity::Day),
        "hour" => Ok(MetricGranularity::Hour),
        other => Err(StoreError::InvariantViolation(format!(
            "unknown metric granularity {other}"
        ))),
    }
}

fn session_repair_job_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<SessionRepairJobRecord, StoreError> {
    Ok(SessionRepairJobRecord {
        project_id: row.try_get("project_id")?,
        install_id: row.try_get("install_id")?,
        target_event_id: row.try_get("target_event_id")?,
        claimed_at: row.try_get("claimed_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
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
            app_version,
            os_version,
            properties
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
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
    .bind(&session.os_version)
    .bind(sqlx::types::Json(session.properties.clone()))
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected())
}

pub async fn insert_sessions_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    sessions: &[SessionRecord],
) -> Result<u64, StoreError> {
    if sessions.is_empty() {
        return Ok(0);
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO sessions \
         (project_id, install_id, session_id, session_start, session_end, event_count, duration_seconds, platform, app_version, os_version, properties) ",
    );
    builder.push_values(sessions, |mut separated, session| {
        separated
            .push_bind(session.project_id)
            .push_bind(&session.install_id)
            .push_bind(&session.session_id)
            .push_bind(session.session_start)
            .push_bind(session.session_end)
            .push_bind(session.event_count)
            .push_bind(session.duration_seconds)
            .push_bind(platform_as_str(&session.platform))
            .push_bind(&session.app_version)
            .push_bind(&session.os_version)
            .push_bind(sqlx::types::Json(session.properties.clone()));
    });
    let result = builder.build().execute(&mut **tx).await?;

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
            duration_seconds = $5
        WHERE project_id = $1
          AND session_id = $2
        "#,
    )
    .bind(update.project_id)
    .bind(update.session_id)
    .bind(update.session_end)
    .bind(update.event_count)
    .bind(update.duration_seconds)
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

pub async fn upsert_session_daily_install_deltas_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[SessionDailyInstallDelta],
) -> Result<Vec<SessionDailyActiveInstallDelta>, StoreError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO session_daily_installs (project_id, day, install_id, session_count) ",
    );
    builder.push_values(deltas, |mut separated, delta| {
        separated
            .push_bind(delta.project_id)
            .push_bind(delta.day)
            .push_bind(&delta.install_id)
            .push_bind(delta.session_count);
    });
    builder.push(
        " ON CONFLICT (project_id, day, install_id) DO UPDATE SET \
          session_count = session_daily_installs.session_count + EXCLUDED.session_count, \
          updated_at = now() \
          RETURNING project_id, day, (CASE WHEN xmax = 0 THEN 1 ELSE 0 END)::BIGINT AS active_install_delta",
    );

    let rows = builder.build().fetch_all(&mut **tx).await?;
    let mut active_by_day = BTreeMap::<(Uuid, NaiveDate), i64>::new();
    for row in rows {
        let project_id: Uuid = row.try_get("project_id")?;
        let day: NaiveDate = row.try_get("day")?;
        let active_install_delta: i64 = row.try_get("active_install_delta")?;
        *active_by_day.entry((project_id, day)).or_default() += active_install_delta;
    }

    Ok(active_by_day
        .into_iter()
        .filter_map(|((project_id, day), active_installs)| {
            (active_installs > 0).then_some(SessionDailyActiveInstallDelta {
                project_id,
                day,
                active_installs,
            })
        })
        .collect())
}

pub async fn upsert_session_active_install_slices_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[SessionActiveInstallSliceDelta],
) -> Result<u64, StoreError> {
    if deltas.is_empty() {
        return Ok(0);
    }

    let mut inserted_rows = 0;
    for chunk in deltas.chunks(max_rows_per_batched_statement(9)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO session_active_install_slices \
             (project_id, day, install_id, platform, app_version, app_version_is_null, os_version, os_version_is_null, properties) ",
        );
        builder.push_values(chunk, |mut separated, delta| {
            separated
                .push_bind(delta.project_id)
                .push_bind(delta.day)
                .push_bind(&delta.install_id)
                .push_bind(&delta.platform)
                .push_bind(delta.app_version.as_deref().unwrap_or(""))
                .push_bind(delta.app_version_is_null)
                .push_bind(delta.os_version.as_deref().unwrap_or(""))
                .push_bind(delta.os_version_is_null)
                .push_bind(sqlx::types::Json(delta.properties.clone()));
        });
        builder.push(" ON CONFLICT DO NOTHING");
        inserted_rows += builder.build().execute(&mut **tx).await?.rows_affected();
    }

    Ok(inserted_rows)
}

pub async fn rebuild_session_active_install_slices_days_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    days: &[NaiveDate],
) -> Result<(), StoreError> {
    let days = days
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if days.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        DELETE FROM session_active_install_slices
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
        WITH scoped_sessions AS MATERIALIZED (
            SELECT
                project_id,
                DATE(session_start AT TIME ZONE 'UTC') AS day,
                install_id,
                platform,
                app_version,
                os_version,
                properties
            FROM sessions
            WHERE project_id = $1
              AND DATE(session_start AT TIME ZONE 'UTC') = ANY($2)
        )
        INSERT INTO session_active_install_slices (
            project_id,
            day,
            install_id,
            platform,
            app_version,
            app_version_is_null,
            os_version,
            os_version_is_null,
            properties
        )
        SELECT DISTINCT
            project_id,
            day,
            install_id,
            platform::TEXT,
            COALESCE(app_version, ''),
            app_version IS NULL,
            COALESCE(os_version, ''),
            os_version IS NULL,
            COALESCE(properties, '{}'::jsonb)
        FROM scoped_sessions
        "#,
    )
    .bind(project_id)
    .bind(&days)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn upsert_session_daily_deltas_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[SessionDailyDelta],
) -> Result<u64, StoreError> {
    if deltas.is_empty() {
        return Ok(0);
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO session_daily \
         (project_id, day, sessions_count, active_installs, total_duration_seconds) ",
    );
    builder.push_values(deltas, |mut separated, delta| {
        separated
            .push_bind(delta.project_id)
            .push_bind(delta.day)
            .push_bind(delta.sessions_count)
            .push_bind(delta.active_installs)
            .push_bind(delta.total_duration_seconds);
    });
    builder.push(
        " ON CONFLICT (project_id, day) DO UPDATE SET \
          sessions_count = session_daily.sessions_count + EXCLUDED.sessions_count, \
          active_installs = session_daily.active_installs + EXCLUDED.active_installs, \
          total_duration_seconds = session_daily.total_duration_seconds + EXCLUDED.total_duration_seconds, \
          updated_at = now()",
    );

    let result = builder.build().execute(&mut **tx).await?;

    Ok(result.rows_affected())
}

pub async fn add_session_daily_duration_deltas_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[SessionDailyDurationDelta],
) -> Result<u64, StoreError> {
    if deltas.is_empty() {
        return Ok(0);
    }

    let mut builder =
        QueryBuilder::<Postgres>::new("WITH input(project_id, day, duration_delta) AS (");
    builder.push_values(deltas, |mut row, delta| {
        row.push_bind(delta.project_id)
            .push_bind(delta.day)
            .push_bind(delta.duration_delta);
    });
    builder.push(
        ") UPDATE session_daily AS daily \
         SET total_duration_seconds = daily.total_duration_seconds + input.duration_delta, \
             updated_at = now() \
         FROM input \
         WHERE daily.project_id = input.project_id \
           AND daily.day = input.day",
    );

    let result = builder.build().execute(&mut **tx).await?;

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

pub async fn rebuild_session_daily_days_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    days: &[NaiveDate],
) -> Result<(), StoreError> {
    rebuild_session_daily_days_with_telemetry_for_mode_in_tx(
        tx,
        project_id,
        days,
        SessionDailyInstallRebuildMode::FullDay,
    )
    .await
    .map(|_| ())
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SessionDailyRebuildTelemetry {
    pub installs_write_ms: u64,
    pub aggregate_write_ms: u64,
    pub cleanup_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionDailyInstallRebuildMode {
    FullDay,
    PendingQueue,
}

pub async fn rebuild_session_daily_days_from_pending_install_rebuilds_with_telemetry_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    days: &[NaiveDate],
) -> Result<SessionDailyRebuildTelemetry, StoreError> {
    rebuild_session_daily_days_with_telemetry_for_mode_in_tx(
        tx,
        project_id,
        days,
        SessionDailyInstallRebuildMode::PendingQueue,
    )
    .await
}

async fn rebuild_session_daily_days_with_telemetry_for_mode_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    days: &[NaiveDate],
    install_rebuild_mode: SessionDailyInstallRebuildMode,
) -> Result<SessionDailyRebuildTelemetry, StoreError> {
    let days = days
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    if days.is_empty() {
        return Ok(SessionDailyRebuildTelemetry::default());
    }

    let mut telemetry = SessionDailyRebuildTelemetry::default();

    let installs_write_started_at = std::time::Instant::now();
    match install_rebuild_mode {
        SessionDailyInstallRebuildMode::FullDay => {
            rebuild_session_daily_installs_days_in_tx(tx, project_id, &days).await?;
        }
        SessionDailyInstallRebuildMode::PendingQueue => {
            rebuild_session_daily_installs_for_pending_rebuilds_in_tx(tx, project_id, &days)
                .await?;
        }
    }
    telemetry.installs_write_ms = installs_write_started_at.elapsed().as_millis() as u64;

    let cleanup_started_at = std::time::Instant::now();
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

    let cleanup_elapsed_ms = cleanup_started_at.elapsed().as_millis() as u64;
    if cleanup_elapsed_ms > 0 {
        telemetry.cleanup_ms = Some(cleanup_elapsed_ms);
    }

    let aggregate_write_started_at = std::time::Instant::now();
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
    telemetry.aggregate_write_ms = aggregate_write_started_at.elapsed().as_millis() as u64;

    Ok(telemetry)
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

async fn rebuild_session_daily_installs_for_pending_rebuilds_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    days: &[NaiveDate],
) -> Result<(), StoreError> {
    let claimed_days =
        stage_pending_session_daily_install_rebuild_scope_in_tx(tx, project_id, days).await?;
    let requested_days = days.iter().copied().collect::<BTreeSet<_>>();
    if claimed_days != requested_days {
        return Err(StoreError::InvariantViolation(format!(
            "missing session_daily_installs rebuild queue rows for project {project_id}; requested {requested_days:?}, claimed {claimed_days:?}"
        )));
    }

    sqlx::query(
        r#"
        DELETE FROM session_daily_installs installs
        USING pg_temp.session_daily_install_rebuild_scope scope
        WHERE installs.project_id = scope.project_id
          AND installs.day = scope.day
          AND installs.install_id = scope.install_id
        "#,
    )
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
            scope.project_id,
            scope.day,
            scope.install_id,
            COUNT(*)::INTEGER AS session_count
        FROM pg_temp.session_daily_install_rebuild_scope scope
        INNER JOIN sessions
          ON sessions.project_id = scope.project_id
         AND DATE(sessions.session_start AT TIME ZONE 'UTC') = scope.day
         AND sessions.install_id = scope.install_id
        GROUP BY scope.project_id, scope.day, scope.install_id
        ON CONFLICT (project_id, day, install_id) DO UPDATE SET
            session_count = EXCLUDED.session_count,
            updated_at = now()
        "#,
    )
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn stage_pending_session_daily_install_rebuild_scope_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    days: &[NaiveDate],
) -> Result<BTreeSet<NaiveDate>, StoreError> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE IF NOT EXISTS pg_temp.session_daily_install_rebuild_scope (
            project_id UUID NOT NULL,
            day DATE NOT NULL,
            install_id TEXT NOT NULL,
            PRIMARY KEY (project_id, day, install_id)
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut **tx)
    .await?;

    sqlx::query("TRUNCATE pg_temp.session_daily_install_rebuild_scope")
        .execute(&mut **tx)
        .await?;

    sqlx::query_scalar::<_, i64>(
        r#"
        WITH claimed AS (
            DELETE FROM session_daily_installs_rebuild_queue
            WHERE project_id = $1
              AND day = ANY($2)
            RETURNING project_id, day, install_id
        ),
        inserted AS (
            INSERT INTO pg_temp.session_daily_install_rebuild_scope (
                project_id,
                day,
                install_id
            )
            SELECT project_id, day, install_id
            FROM claimed
            ON CONFLICT (project_id, day, install_id) DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*)::BIGINT
        FROM inserted
        "#,
    )
    .bind(project_id)
    .bind(days)
    .fetch_one(&mut **tx)
    .await?;

    let claimed_days = sqlx::query_scalar::<_, NaiveDate>(
        r#"
        SELECT DISTINCT day
        FROM pg_temp.session_daily_install_rebuild_scope
        ORDER BY day ASC
        "#,
    )
    .fetch_all(&mut **tx)
    .await?;

    Ok(claimed_days.into_iter().collect())
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

pub async fn load_raw_event_tail_id(pool: &PgPool) -> Result<i64, StoreError> {
    let tail_id = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT MAX(id)
        FROM events_raw
        "#,
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    Ok(tail_id)
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

pub async fn fetch_active_install_metrics_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<SessionMetricsCubeRow>, StoreError> {
    let rows = sqlx::query(
        r#"
        SELECT day, active_installs
        FROM session_daily
        WHERE project_id = $1
          AND day BETWEEN $2::date AND $3::date
        ORDER BY day ASC
        "#,
    )
    .bind(project_id)
    .bind(start.date_naive())
    .bind(end.date_naive())
    .fetch_all(&mut **tx)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| SessionMetricsCubeRow {
            bucket_start: DateTime::from_naive_utc_and_offset(
                row.try_get::<NaiveDate, _>("day")
                    .expect("day column exists")
                    .and_hms_opt(0, 0, 0)
                    .expect("midnight is a valid UTC datetime"),
                Utc,
            ),
            dimensions: BTreeMap::new(),
            value: row
                .try_get::<i64, _>("active_installs")
                .expect("active_installs column exists") as u64,
        })
        .collect())
}

async fn fetch_event_metrics_total_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    event_name: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT bucket_start, event_count
        FROM event_metric_buckets_total
        WHERE project_id = $1
          AND granularity = $2
          AND event_name = $3
          AND bucket_start BETWEEN $4 AND $5
        ORDER BY bucket_start ASC
        "#,
    )
    .bind(project_id)
    .bind(granularity.as_str())
    .bind(event_name)
    .bind(start)
    .bind(end)
    .fetch_all(executor)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| EventMetricsCubeRow {
            bucket_start: row
                .try_get("bucket_start")
                .expect("bucket_start column exists"),
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
    granularity: MetricGranularity,
    event_name: &str,
    window: (DateTime<Utc>, DateTime<Utc>),
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start, end) = window;
    let row = sqlx::query(
        r#"
        SELECT SUM(event_count)::BIGINT AS event_count
        FROM event_metric_buckets_total
        WHERE project_id = $1
          AND granularity = $2
          AND event_name = $3
          AND bucket_start BETWEEN $4 AND $5
        "#,
    )
    .bind(project_id)
    .bind(granularity.as_str())
    .bind(event_name)
    .bind(start)
    .bind(end)
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
    granularity: MetricGranularity,
    event_name: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT bucket_start, dim1_key, dim1_value, event_count \
         FROM event_metric_buckets_dim1 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND event_name = ");
    builder.push_bind(event_name);
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
    builder.push(" AND dim1_key = ");
    builder.push_bind(&cube_keys[0]);

    if let Some(value) = filters.get(&cube_keys[0]) {
        builder.push(" AND dim1_value = ");
        builder.push_bind(value);
    }

    builder.push(" ORDER BY bucket_start ASC, dim1_value ASC");

    let rows = builder.build().fetch_all(executor).await?;

    Ok(rows
        .into_iter()
        .map(|row| EventMetricsCubeRow {
            bucket_start: row
                .try_get("bucket_start")
                .expect("bucket_start column exists"),
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
    granularity: MetricGranularity,
    event_name: &str,
    window: (DateTime<Utc>, DateTime<Utc>),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start, end) = window;
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT dim1_key, dim1_value, SUM(event_count)::BIGINT AS event_count \
         FROM event_metric_buckets_dim1 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND event_name = ");
    builder.push_bind(event_name);
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
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
    granularity: MetricGranularity,
    event_name: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, event_count \
         FROM event_metric_buckets_dim2 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND event_name = ");
    builder.push_bind(event_name);
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
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

    builder.push(" ORDER BY bucket_start ASC, dim1_value ASC, dim2_value ASC");

    let rows = builder.build().fetch_all(executor).await?;

    Ok(rows
        .into_iter()
        .map(|row| EventMetricsCubeRow {
            bucket_start: row
                .try_get("bucket_start")
                .expect("bucket_start column exists"),
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
    granularity: MetricGranularity,
    event_name: &str,
    window: (DateTime<Utc>, DateTime<Utc>),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start, end) = window;
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT dim1_key, dim1_value, dim2_key, dim2_value, SUM(event_count)::BIGINT AS event_count \
         FROM event_metric_buckets_dim2 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND event_name = ");
    builder.push_bind(event_name);
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
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

async fn fetch_session_metrics_total_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    metric: SessionMetric,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<SessionMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let mut builder = QueryBuilder::<Postgres>::new("SELECT bucket_start, ");
    builder.push(session_metric_value_column(metric));
    builder.push(
        " AS value \
         FROM session_metric_buckets_total \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
    builder.push(" ORDER BY bucket_start ASC");

    let rows = builder.build().fetch_all(executor).await?;

    Ok(rows
        .into_iter()
        .map(|row| SessionMetricsCubeRow {
            bucket_start: row
                .try_get("bucket_start")
                .expect("bucket_start column exists"),
            dimensions: BTreeMap::new(),
            value: row.try_get::<i64, _>("value").expect("value column exists") as u64,
        })
        .collect())
}

async fn fetch_session_metrics_total_aggregate_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    metric: SessionMetric,
    window: (DateTime<Utc>, DateTime<Utc>),
) -> Result<Vec<SessionMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start, end) = window;
    let mut builder = QueryBuilder::<Postgres>::new("SELECT SUM(");
    builder.push(session_metric_value_column(metric));
    builder.push(
        ")::BIGINT AS value \
         FROM session_metric_buckets_total \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);

    let row = builder.build().fetch_one(executor).await?;
    let value = row.try_get::<Option<i64>, _>("value")?;

    Ok(value
        .map(|value| {
            vec![SessionMetricsAggregateCubeRow {
                dimensions: BTreeMap::new(),
                value: value as u64,
            }]
        })
        .unwrap_or_default())
}

async fn fetch_session_metrics_dim1_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    metric: SessionMetric,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<SessionMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let mut builder = QueryBuilder::<Postgres>::new("SELECT bucket_start, dim1_key, dim1_value, ");
    builder.push(session_metric_value_column(metric));
    builder.push(
        " AS value \
         FROM session_metric_buckets_dim1 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
    builder.push(" AND dim1_key = ");
    builder.push_bind(&cube_keys[0]);

    if let Some(value) = filters.get(&cube_keys[0]) {
        builder.push(" AND dim1_value = ");
        builder.push_bind(value);
    }

    builder.push(" ORDER BY bucket_start ASC, dim1_value ASC");

    let rows = builder.build().fetch_all(executor).await?;

    Ok(rows
        .into_iter()
        .map(|row| SessionMetricsCubeRow {
            bucket_start: row
                .try_get("bucket_start")
                .expect("bucket_start column exists"),
            dimensions: BTreeMap::from([(
                row.try_get::<String, _>("dim1_key")
                    .expect("dim1_key column exists"),
                row.try_get::<String, _>("dim1_value")
                    .expect("dim1_value column exists"),
            )]),
            value: row.try_get::<i64, _>("value").expect("value column exists") as u64,
        })
        .collect())
}

async fn fetch_session_metrics_dim1_aggregate_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    metric: SessionMetric,
    window: (DateTime<Utc>, DateTime<Utc>),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<SessionMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start, end) = window;
    let mut builder = QueryBuilder::<Postgres>::new("SELECT dim1_key, dim1_value, SUM(");
    builder.push(session_metric_value_column(metric));
    builder.push(
        ")::BIGINT AS value \
         FROM session_metric_buckets_dim1 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
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
        .map(|row| SessionMetricsAggregateCubeRow {
            dimensions: BTreeMap::from([(
                row.try_get::<String, _>("dim1_key")
                    .expect("dim1_key column exists"),
                row.try_get::<String, _>("dim1_value")
                    .expect("dim1_value column exists"),
            )]),
            value: row.try_get::<i64, _>("value").expect("value column exists") as u64,
        })
        .collect())
}

async fn fetch_session_metrics_dim2_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    metric: SessionMetric,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<SessionMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, ",
    );
    builder.push(session_metric_value_column(metric));
    builder.push(
        " AS value \
         FROM session_metric_buckets_dim2 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
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

    builder.push(" ORDER BY bucket_start ASC, dim1_value ASC, dim2_value ASC");

    let rows = builder.build().fetch_all(executor).await?;

    Ok(rows
        .into_iter()
        .map(|row| SessionMetricsCubeRow {
            bucket_start: row
                .try_get("bucket_start")
                .expect("bucket_start column exists"),
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
            value: row.try_get::<i64, _>("value").expect("value column exists") as u64,
        })
        .collect())
}

async fn fetch_session_metrics_dim2_aggregate_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    metric: SessionMetric,
    window: (DateTime<Utc>, DateTime<Utc>),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<SessionMetricsAggregateCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let (start, end) = window;
    let mut builder =
        QueryBuilder::<Postgres>::new("SELECT dim1_key, dim1_value, dim2_key, dim2_value, SUM(");
    builder.push(session_metric_value_column(metric));
    builder.push(
        ")::BIGINT AS value \
         FROM session_metric_buckets_dim2 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
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
        .map(|row| SessionMetricsAggregateCubeRow {
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
            value: row.try_get::<i64, _>("value").expect("value column exists") as u64,
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
        "metric filters must be a subset of the requested cube".to_owned(),
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
        os_version: row.try_get("os_version")?,
        properties: row
            .try_get::<Option<sqlx::types::Json<BTreeMap<String, String>>>, _>("properties")?
            .map(|json| json.0)
            .unwrap_or_default(),
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
        last_processed_event_id: row.try_get("last_processed_event_id")?,
    })
}

fn session_metric_value_column(metric: SessionMetric) -> &'static str {
    match metric {
        SessionMetric::Count => "session_count",
        SessionMetric::DurationTotal => "duration_total_seconds",
        SessionMetric::NewInstalls => "new_installs",
        SessionMetric::ActiveInstalls => unreachable!("active_installs uses session_daily reads"),
    }
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

fn append_raw_event_filters(
    query: &mut QueryBuilder<'_, Postgres>,
    filters: &BTreeMap<String, String>,
) {
    for (key, value) in filters {
        match key.as_str() {
            "platform" | "app_version" | "os_version" => {
                query.push(" AND ");
                query.push(key.as_str());
                query.push(" = ");
                query.push_bind(value.clone());
            }
            _ => {
                query.push(" AND properties ->> ");
                query.push_bind(key.clone());
                query.push(" = ");
                query.push_bind(value.clone());
            }
        }
    }
}

fn api_key_kind_from_str(value: &str) -> Result<ApiKeyKind, StoreError> {
    match value {
        "ingest" => Ok(ApiKeyKind::Ingest),
        "read" => Ok(ApiKeyKind::Read),
        other => Err(StoreError::InvariantViolation(format!(
            "invalid api key kind: {other}"
        ))),
    }
}

fn project_record_from_row(row: &sqlx::postgres::PgRow) -> Result<ProjectRecord, StoreError> {
    Ok(ProjectRecord {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        created_at: row.try_get("created_at")?,
    })
}

fn api_key_record_from_row(row: &sqlx::postgres::PgRow) -> Result<ApiKeyRecord, StoreError> {
    Ok(ApiKeyRecord {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        name: row.try_get("name")?,
        kind: api_key_kind_from_str(row.try_get("kind")?)?,
        key_prefix: row.try_get("key_prefix")?,
        created_at: row.try_get("created_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

fn resolved_api_key_from_row(row: &sqlx::postgres::PgRow) -> Result<ResolvedApiKey, StoreError> {
    Ok(ResolvedApiKey {
        key_id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        kind: api_key_kind_from_str(row.try_get("kind")?)?,
    })
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
            last_processed_event_id: 0,
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
            os_version: Some("18.3".to_owned()),
            properties: BTreeMap::new(),
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
                'event_metric_buckets_total',
                'event_metric_buckets_dim1',
                'event_metric_buckets_dim2'
            )
              AND indexname IN (
                'idx_event_metric_buckets_total_project_event_bucket',
                'idx_event_metric_buckets_dim1_project_event_bucket',
                'idx_event_metric_buckets_dim2_project_event_bucket',
                'idx_event_metric_buckets_dim2_project_event_dim1_value_bucket',
                'idx_event_metric_buckets_dim2_project_event_dim2_value_bucket'
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
                'event_metric_buckets_total',
                'event_metric_buckets_dim1',
                'event_metric_buckets_dim2'
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
                "idx_event_metric_buckets_dim1_project_event_bucket".to_owned(),
                "idx_event_metric_buckets_dim2_project_event_bucket".to_owned(),
                "idx_event_metric_buckets_dim2_project_event_dim1_value_bucket".to_owned(),
                "idx_event_metric_buckets_dim2_project_event_dim2_value_bucket".to_owned(),
                "idx_event_metric_buckets_total_project_event_bucket".to_owned(),
            ]
        );
        assert!(session_table_exists);
        assert!(session_daily_table_exists);
        assert_eq!(event_metrics_table_count, 3);
        assert!(os_version_column_exists);
        assert_eq!(removed_identity_columns, vec!["session_id".to_owned()]);
    }

    #[sqlx::test]
    async fn event_metric_bucket_tables_enforce_canonical_key_order(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let error = sqlx::query(
            r#"
            INSERT INTO event_metric_buckets_dim2 (
                project_id,
                granularity,
                bucket_start,
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
        .bind(MetricGranularity::Day.as_str())
        .bind(timestamp(24 * 60))
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
            INSERT INTO event_metric_buckets_dim2 (
                project_id,
                granularity,
                bucket_start,
                event_name,
                dim1_key,
                dim1_value,
                dim2_key,
                dim2_value,
                event_count
            )
            SELECT
                $1,
                'day',
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
        sqlx::query("ANALYZE event_metric_buckets_dim2")
            .execute(&pool)
            .await
            .expect("analyze dim2 table");

        let row = sqlx::query(
            r#"
            EXPLAIN (FORMAT JSON)
            SELECT bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, event_count
            FROM event_metric_buckets_dim2
            WHERE project_id = $1
              AND granularity = $2
              AND event_name = $3
              AND bucket_start BETWEEN $4 AND $5
              AND dim1_key = $6
              AND dim2_key = $7
              AND dim2_value = $8
            ORDER BY bucket_start ASC, dim1_value ASC, dim2_value ASC
            "#,
        )
        .bind(project_id())
        .bind(MetricGranularity::Day.as_str())
        .bind("app_open")
        .bind(timestamp(24 * 60 * 9))
        .bind(timestamp(24 * 60 * 19))
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
            index_names
                .iter()
                .any(|name| name == "idx_event_metric_buckets_dim2_project_event_dim2_value_bucket"),
            "dim2 read path should use the dim2 value index, got {index_names:?} from {plan:#}"
        );
    }

    #[sqlx::test]
    async fn event_metric_total_upserts_handle_batches_above_postgres_bind_limit(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let deltas = (0..13_108)
            .map(|index| EventMetricBucketTotalDelta {
                project_id: project_id(),
                granularity: MetricGranularity::Hour,
                bucket_start: timestamp(index as i64 * 60),
                event_name: "app_open".to_owned(),
                event_count: 1,
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin tx");
        upsert_event_metric_buckets_total_in_tx(&mut tx, &deltas)
            .await
            .expect("large total upsert should succeed");
        tx.commit().await.expect("commit tx");

        let row = sqlx::query(
            r#"
            SELECT COUNT(*)::BIGINT AS row_count, COALESCE(SUM(event_count), 0)::BIGINT AS total_count
            FROM event_metric_buckets_total
            WHERE project_id = $1
              AND granularity = $2
              AND event_name = $3
            "#,
        )
        .bind(project_id())
        .bind(MetricGranularity::Hour.as_str())
        .bind("app_open")
        .fetch_one(&pool)
        .await
        .expect("fetch total counts");

        assert_eq!(
            row.try_get::<i64, _>("row_count").expect("row_count"),
            13_108
        );
        assert_eq!(
            row.try_get::<i64, _>("total_count").expect("total_count"),
            13_108
        );
    }

    #[sqlx::test]
    async fn event_metric_dim1_upserts_handle_batches_above_postgres_bind_limit(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let deltas = (0..9_363)
            .map(|index| EventMetricBucketDim1Delta {
                project_id: project_id(),
                granularity: MetricGranularity::Hour,
                bucket_start: timestamp(60),
                event_name: "app_open".to_owned(),
                dim1_key: "provider".to_owned(),
                dim1_value: format!("provider-{index:04}"),
                event_count: 1,
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin tx");
        upsert_event_metric_buckets_dim1_in_tx(&mut tx, &deltas)
            .await
            .expect("large dim1 upsert should succeed");
        tx.commit().await.expect("commit tx");

        let row = sqlx::query(
            r#"
            SELECT COUNT(*)::BIGINT AS row_count, COALESCE(SUM(event_count), 0)::BIGINT AS total_count
            FROM event_metric_buckets_dim1
            WHERE project_id = $1
              AND granularity = $2
              AND bucket_start = $3
              AND event_name = $4
            "#,
        )
        .bind(project_id())
        .bind(MetricGranularity::Hour.as_str())
        .bind(timestamp(60))
        .bind("app_open")
        .fetch_one(&pool)
        .await
        .expect("fetch dim1 counts");

        assert_eq!(
            row.try_get::<i64, _>("row_count").expect("row_count"),
            9_363
        );
        assert_eq!(
            row.try_get::<i64, _>("total_count").expect("total_count"),
            9_363
        );
    }

    #[sqlx::test]
    async fn event_metric_dim2_upserts_handle_batches_above_postgres_bind_limit(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let deltas = (0..7_282)
            .map(|index| EventMetricBucketDim2Delta {
                project_id: project_id(),
                granularity: MetricGranularity::Hour,
                bucket_start: timestamp(60),
                event_name: "app_open".to_owned(),
                dim1_key: "app_version".to_owned(),
                dim1_value: "1.0.0".to_owned(),
                dim2_key: "provider".to_owned(),
                dim2_value: format!("provider-{index:04}"),
                event_count: 1,
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin tx");
        upsert_event_metric_buckets_dim2_in_tx(&mut tx, &deltas)
            .await
            .expect("large dim2 upsert should succeed");
        tx.commit().await.expect("commit tx");

        let row = sqlx::query(
            r#"
            SELECT COUNT(*)::BIGINT AS row_count, COALESCE(SUM(event_count), 0)::BIGINT AS total_count
            FROM event_metric_buckets_dim2
            WHERE project_id = $1
              AND granularity = $2
              AND bucket_start = $3
              AND event_name = $4
            "#,
        )
        .bind(project_id())
        .bind(MetricGranularity::Hour.as_str())
        .bind(timestamp(60))
        .bind("app_open")
        .fetch_one(&pool)
        .await
        .expect("fetch dim2 counts");

        assert_eq!(
            row.try_get::<i64, _>("row_count").expect("row_count"),
            7_282
        );
        assert_eq!(
            row.try_get::<i64, _>("total_count").expect("total_count"),
            7_282
        );
    }

    #[sqlx::test]
    async fn session_metric_total_upserts_handle_batches_above_postgres_bind_limit(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let deltas = (0..10_923)
            .map(|index| SessionMetricTotalDelta {
                project_id: project_id(),
                granularity: MetricGranularity::Hour,
                bucket_start: timestamp(index as i64 * 60),
                session_count: 1,
                duration_total_seconds: 60,
                new_installs: 1,
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin tx");
        upsert_session_metric_totals_in_tx(&mut tx, &deltas)
            .await
            .expect("large session total upsert should succeed");
        tx.commit().await.expect("commit tx");

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS row_count,
                COALESCE(SUM(session_count), 0)::BIGINT AS total_sessions,
                COALESCE(SUM(duration_total_seconds), 0)::BIGINT AS total_duration,
                COALESCE(SUM(new_installs), 0)::BIGINT AS total_new_installs
            FROM session_metric_buckets_total
            WHERE project_id = $1
              AND granularity = $2
            "#,
        )
        .bind(project_id())
        .bind(MetricGranularity::Hour.as_str())
        .fetch_one(&pool)
        .await
        .expect("fetch session total counts");

        assert_eq!(
            row.try_get::<i64, _>("row_count").expect("row_count"),
            10_923
        );
        assert_eq!(
            row.try_get::<i64, _>("total_sessions")
                .expect("total_sessions"),
            10_923
        );
        assert_eq!(
            row.try_get::<i64, _>("total_duration")
                .expect("total_duration"),
            655_380
        );
        assert_eq!(
            row.try_get::<i64, _>("total_new_installs")
                .expect("total_new_installs"),
            10_923
        );
    }

    #[sqlx::test]
    async fn session_metric_dim1_upserts_handle_batches_above_postgres_bind_limit(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let deltas = (0..8_192)
            .map(|index| SessionMetricDim1Delta {
                project_id: project_id(),
                granularity: MetricGranularity::Hour,
                bucket_start: timestamp(60),
                dim1_key: "app_version".to_owned(),
                dim1_value: format!("1.{index}.0"),
                session_count: 1,
                duration_total_seconds: 60,
                new_installs: 1,
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin tx");
        upsert_session_metric_dim1_in_tx(&mut tx, &deltas)
            .await
            .expect("large session dim1 upsert should succeed");
        tx.commit().await.expect("commit tx");

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS row_count,
                COALESCE(SUM(session_count), 0)::BIGINT AS total_sessions,
                COALESCE(SUM(duration_total_seconds), 0)::BIGINT AS total_duration,
                COALESCE(SUM(new_installs), 0)::BIGINT AS total_new_installs
            FROM session_metric_buckets_dim1
            WHERE project_id = $1
              AND granularity = $2
              AND bucket_start = $3
            "#,
        )
        .bind(project_id())
        .bind(MetricGranularity::Hour.as_str())
        .bind(timestamp(60))
        .fetch_one(&pool)
        .await
        .expect("fetch session dim1 counts");

        assert_eq!(
            row.try_get::<i64, _>("row_count").expect("row_count"),
            8_192
        );
        assert_eq!(
            row.try_get::<i64, _>("total_sessions")
                .expect("total_sessions"),
            8_192
        );
        assert_eq!(
            row.try_get::<i64, _>("total_duration")
                .expect("total_duration"),
            491_520
        );
        assert_eq!(
            row.try_get::<i64, _>("total_new_installs")
                .expect("total_new_installs"),
            8_192
        );
    }

    #[sqlx::test]
    async fn session_metric_dim2_upserts_handle_batches_above_postgres_bind_limit(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let deltas = (0..6_554)
            .map(|index| SessionMetricDim2Delta {
                project_id: project_id(),
                granularity: MetricGranularity::Hour,
                bucket_start: timestamp(60),
                dim1_key: "app_version".to_owned(),
                dim1_value: "1.0.0".to_owned(),
                dim2_key: "platform".to_owned(),
                dim2_value: format!("platform-{index:04}"),
                session_count: 1,
                duration_total_seconds: 60,
                new_installs: 1,
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin tx");
        upsert_session_metric_dim2_in_tx(&mut tx, &deltas)
            .await
            .expect("large session dim2 upsert should succeed");
        tx.commit().await.expect("commit tx");

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS row_count,
                COALESCE(SUM(session_count), 0)::BIGINT AS total_sessions,
                COALESCE(SUM(duration_total_seconds), 0)::BIGINT AS total_duration,
                COALESCE(SUM(new_installs), 0)::BIGINT AS total_new_installs
            FROM session_metric_buckets_dim2
            WHERE project_id = $1
              AND granularity = $2
              AND bucket_start = $3
            "#,
        )
        .bind(project_id())
        .bind(MetricGranularity::Hour.as_str())
        .bind(timestamp(60))
        .fetch_one(&pool)
        .await
        .expect("fetch session dim2 counts");

        assert_eq!(
            row.try_get::<i64, _>("row_count").expect("row_count"),
            6_554
        );
        assert_eq!(
            row.try_get::<i64, _>("total_sessions")
                .expect("total_sessions"),
            6_554
        );
        assert_eq!(
            row.try_get::<i64, _>("total_duration")
                .expect("total_duration"),
            393_240
        );
        assert_eq!(
            row.try_get::<i64, _>("total_new_installs")
                .expect("total_new_installs"),
            6_554
        );
    }

    #[sqlx::test]
    async fn session_metric_dim2_conflicts_accumulate_across_chunk_boundaries(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let chunk_rows = max_rows_per_batched_statement(10);
        let deltas = (0..(chunk_rows + 1))
            .map(|index| {
                let app_version = if index == 0 || index == chunk_rows {
                    "1.0.0".to_owned()
                } else {
                    format!("1.{index}.0")
                };

                SessionMetricDim2Delta {
                    project_id: project_id(),
                    granularity: MetricGranularity::Hour,
                    bucket_start: timestamp(60),
                    dim1_key: "app_version".to_owned(),
                    dim1_value: app_version,
                    dim2_key: "platform".to_owned(),
                    dim2_value: "ios".to_owned(),
                    session_count: 1,
                    duration_total_seconds: 60,
                    new_installs: 1,
                }
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin tx");
        upsert_session_metric_dim2_in_tx(&mut tx, &deltas)
            .await
            .expect("cross-chunk session dim2 conflict upsert should succeed");
        tx.commit().await.expect("commit tx");

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS row_count,
                COALESCE(SUM(session_count), 0)::BIGINT AS total_sessions,
                COALESCE(SUM(duration_total_seconds), 0)::BIGINT AS total_duration,
                COALESCE(SUM(new_installs), 0)::BIGINT AS total_new_installs
            FROM session_metric_buckets_dim2
            WHERE project_id = $1
              AND granularity = $2
              AND bucket_start = $3
              AND dim1_key = $4
              AND dim1_value = $5
              AND dim2_key = $6
              AND dim2_value = $7
            "#,
        )
        .bind(project_id())
        .bind(MetricGranularity::Hour.as_str())
        .bind(timestamp(60))
        .bind("app_version")
        .bind("1.0.0")
        .bind("platform")
        .bind("ios")
        .fetch_one(&pool)
        .await
        .expect("fetch session dim2 conflict counts");

        assert_eq!(row.try_get::<i64, _>("row_count").expect("row_count"), 1);
        assert_eq!(
            row.try_get::<i64, _>("total_sessions")
                .expect("total_sessions"),
            2
        );
        assert_eq!(
            row.try_get::<i64, _>("total_duration")
                .expect("total_duration"),
            120
        );
        assert_eq!(
            row.try_get::<i64, _>("total_new_installs")
                .expect("total_new_installs"),
            2
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

    #[test]
    fn forward_migration_keeps_event_bucket_storage_at_dim2_and_cleans_legacy_daily_tables() {
        let migration = include_str!("../migrations/0012_event_metric_dim4_cleanup.sql");

        assert!(
            migration.contains(
                "DROP INDEX IF EXISTS idx_event_metric_buckets_dim2_project_event_bucket"
            ) && migration
                .contains("idx_event_metric_buckets_dim2_project_event_dim1_value_bucket")
                && migration
                    .contains("idx_event_metric_buckets_dim2_project_event_dim2_value_bucket"),
            "0012 must keep the bounded dim2 event bucket indexes"
        );
        assert!(
            migration.contains("DROP TABLE IF EXISTS event_metric_buckets_dim3")
                && migration.contains("DROP TABLE IF EXISTS event_metric_buckets_dim4"),
            "0012 must drop dim3/dim4 event bucket storage in the D2 contract"
        );
        assert!(
            migration.contains("DROP TABLE IF EXISTS event_count_daily_total")
                && migration.contains("DROP TABLE IF EXISTS event_count_daily_dim1")
                && migration.contains("DROP TABLE IF EXISTS event_count_daily_dim2")
                && migration.contains("DROP TABLE IF EXISTS event_count_daily_dim3"),
            "0012 must remove the obsolete event_count_daily_* tables"
        );
    }

    #[test]
    fn forward_migration_extends_api_keys_for_scoped_project_keys() {
        let migration = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("migrations/0008_scoped_project_api_keys.sql"),
        )
        .expect("read migration");

        assert!(
            migration.contains("ALTER TABLE api_keys")
                && migration.contains("ADD COLUMN name TEXT NOT NULL")
                && migration.contains("ADD COLUMN kind TEXT NOT NULL")
                && migration.contains("ADD COLUMN revoked_at TIMESTAMPTZ"),
            "0008 must add name, kind, and revoked_at to api_keys"
        );
        assert!(
            !migration.contains("UPDATE api_keys"),
            "0008 should not carry legacy api_keys backfill logic in a brand-new stack"
        );
    }

    #[test]
    fn forward_migration_keeps_session_storage_at_dim2_and_adds_snapshot_columns() {
        let migration = include_str!("../migrations/0015_session_metric_dim4.sql");

        assert!(
            migration.contains("ALTER TABLE sessions")
                && migration.contains("ADD COLUMN IF NOT EXISTS os_version TEXT")
                && migration.contains("ADD COLUMN IF NOT EXISTS properties JSONB"),
            "0015 must widen sessions with session snapshot dimensions"
        );
        assert!(
            migration.contains("ALTER TABLE install_first_seen")
                && migration.contains("ADD COLUMN IF NOT EXISTS os_version TEXT")
                && migration.contains("ADD COLUMN IF NOT EXISTS properties JSONB"),
            "0015 must widen install_first_seen with first-seen snapshot dimensions"
        );
        assert!(
            !migration.contains("CREATE TABLE IF NOT EXISTS session_metric_buckets_dim3")
                && !migration.contains("CREATE TABLE IF NOT EXISTS session_metric_buckets_dim4"),
            "0015 must not retain session dim3/dim4 bucket storage in the D2 contract"
        );
    }

    #[test]
    fn forward_migration_keeps_active_install_slice_membership_storage() {
        let migration = include_str!("../migrations/0016_session_active_install_membership.sql");

        assert!(
            migration.contains("CREATE TABLE IF NOT EXISTS session_active_install_slices")
                && migration.contains("idx_session_active_install_slices_project_day")
                && migration.contains("idx_session_active_install_slices_project_day_platform"),
            "0016 must keep the active-install membership table and its bounded day/platform indexes"
        );
    }

    #[sqlx::test]
    async fn run_migrations_extends_api_keys_for_scoped_project_keys(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let columns = sqlx::query_scalar::<_, String>(
            r#"
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'api_keys'
              AND column_name IN ('name', 'kind', 'revoked_at')
            ORDER BY column_name ASC
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch api_keys columns");

        assert_eq!(
            columns,
            vec![
                "kind".to_owned(),
                "name".to_owned(),
                "revoked_at".to_owned(),
            ]
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
    async fn create_project_lists_projects_in_creation_order(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let alpha = create_project(&pool, "Alpha")
            .await
            .expect("create alpha project");
        let beta = create_project(&pool, "Beta")
            .await
            .expect("create beta project");
        let projects = list_projects(&pool).await.expect("list projects");

        assert_eq!(projects.len(), 2);
        assert_eq!(projects[0].id, alpha.id);
        assert_eq!(projects[0].name, "Alpha");
        assert_eq!(projects[1].id, beta.id);
        assert_eq!(projects[1].name, "Beta");
    }

    #[sqlx::test]
    async fn create_project_with_api_key_returns_both_records(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let secret = generate_api_key_secret(ApiKeyKind::Ingest);
        let (project, key) = create_project_with_api_key(
            &pool,
            "Provisioned Project",
            "bootstrap",
            ApiKeyKind::Ingest,
            &secret,
        )
        .await
        .expect("create project with api key");

        let projects = list_projects(&pool).await.expect("list projects");
        let keys = list_api_keys(&pool, project.id)
            .await
            .expect("list keys")
            .expect("project exists");

        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].id, project.id);
        assert_eq!(projects[0].name, "Provisioned Project");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].id, key.id);
        assert_eq!(keys[0].name, "bootstrap");
        assert_eq!(keys[0].kind, ApiKeyKind::Ingest);
    }

    #[sqlx::test]
    async fn api_key_lifecycle_round_trips_kinds_and_revocation(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let project = create_project(&pool, "Metrics Project")
            .await
            .expect("create project");
        let ingest_secret = "fg_ing_test_secret";
        let read_secret = "fg_rd_test_secret";
        let ingest_key = create_api_key(
            &pool,
            project.id,
            "ios-prod",
            ApiKeyKind::Ingest,
            ingest_secret,
        )
        .await
        .expect("create ingest key")
        .expect("project exists");
        let read_key = create_api_key(
            &pool,
            project.id,
            "local-read",
            ApiKeyKind::Read,
            read_secret,
        )
        .await
        .expect("create read key")
        .expect("project exists");

        let listed = list_api_keys(&pool, project.id)
            .await
            .expect("list project keys")
            .expect("project exists");
        let resolved_ingest = resolve_api_key(&pool, ingest_secret)
            .await
            .expect("resolve ingest key")
            .expect("ingest key is active");
        let resolved_read = resolve_api_key(&pool, read_secret)
            .await
            .expect("resolve read key")
            .expect("read key is active");

        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].id, ingest_key.id);
        assert_eq!(listed[0].name, "ios-prod");
        assert_eq!(listed[0].kind, ApiKeyKind::Ingest);
        assert_eq!(listed[0].revoked_at, None);
        assert_eq!(listed[1].id, read_key.id);
        assert_eq!(listed[1].kind, ApiKeyKind::Read);
        assert_eq!(resolved_ingest.project_id, project.id);
        assert_eq!(resolved_ingest.key_id, ingest_key.id);
        assert_eq!(resolved_ingest.kind, ApiKeyKind::Ingest);
        assert_eq!(resolved_read.kind, ApiKeyKind::Read);

        assert!(
            revoke_api_key(&pool, project.id, ingest_key.id)
                .await
                .expect("revoke ingest key"),
            "existing key should be revocable"
        );
        assert!(
            revoke_api_key(&pool, project.id, ingest_key.id)
                .await
                .expect("repeat revoke stays idempotent"),
            "repeat revoke should still report the resource exists"
        );
        assert!(
            !revoke_api_key(&pool, project.id, Uuid::new_v4())
                .await
                .expect("unknown key is ignored"),
            "unknown project/key pair should report no resource"
        );
        assert_eq!(
            resolve_api_key(&pool, ingest_secret)
                .await
                .expect("resolve revoked ingest key"),
            None
        );
        assert!(
            list_api_keys(&pool, Uuid::new_v4())
                .await
                .expect("list missing project")
                .is_none()
        );
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
    async fn run_migrations_adds_install_session_progress_column(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let column_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'install_session_state'
                  AND column_name = 'last_processed_event_id'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .expect("fetch install progress column existence");

        assert!(column_exists);
    }

    #[sqlx::test]
    async fn run_migrations_creates_session_rebuild_queue_table(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let table_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'session_rebuild_queue'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .expect("fetch session rebuild queue existence");

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
    async fn run_migrations_creates_session_daily_installs_rebuild_queue_table(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let table_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'session_daily_installs_rebuild_queue'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .expect("fetch membership rebuild queue existence");

        assert!(table_exists);
    }

    #[sqlx::test]
    async fn run_migrations_creates_session_active_install_slices_table(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let table_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'session_active_install_slices'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .expect("fetch active install slices table existence");

        assert!(table_exists);
    }

    #[sqlx::test]
    async fn run_migrations_create_bucketed_metrics_and_first_seen_tables(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let tables = sqlx::query_scalar::<_, String>(
            r#"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name IN (
                'event_metric_buckets_total',
                'event_metric_buckets_dim1',
                'event_metric_buckets_dim2',
                'session_metric_buckets_total',
                'session_metric_buckets_dim1',
                'session_metric_buckets_dim2',
                'install_first_seen',
                'session_active_install_slices'
            )
            ORDER BY table_name ASC
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch metrics bucket tables");

        assert_eq!(
            tables,
            vec![
                "event_metric_buckets_dim1".to_owned(),
                "event_metric_buckets_dim2".to_owned(),
                "event_metric_buckets_total".to_owned(),
                "install_first_seen".to_owned(),
                "session_active_install_slices".to_owned(),
                "session_metric_buckets_dim1".to_owned(),
                "session_metric_buckets_dim2".to_owned(),
                "session_metric_buckets_total".to_owned(),
            ]
        );

        let legacy_event_metric_tables = sqlx::query_scalar::<_, String>(
            r#"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name IN (
                'event_count_daily_total',
                'event_count_daily_dim1',
                'event_count_daily_dim2',
                'event_count_daily_dim3'
            )
            ORDER BY table_name ASC
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch legacy event metrics tables");

        assert!(
            legacy_event_metric_tables.is_empty(),
            "legacy event_count_daily_* tables should be removed once bucketed storage is canonical"
        );
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
        assert_eq!(stored.last_processed_event_id, 0);
    }

    #[sqlx::test]
    async fn install_session_state_persists_last_processed_event_id(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        upsert_install_session_state(&pool, &tail_state(10, 2))
            .await
            .expect("save tail state");

        let initial = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT last_processed_event_id
            FROM install_session_state
            WHERE project_id = $1
              AND install_id = $2
            "#,
        )
        .bind(project_id())
        .bind("install-1")
        .fetch_one(&pool)
        .await
        .expect("fetch initial last processed event id");
        assert_eq!(initial, 0);

        sqlx::query(
            r#"
            UPDATE install_session_state
            SET last_processed_event_id = $3
            WHERE project_id = $1
              AND install_id = $2
            "#,
        )
        .bind(project_id())
        .bind("install-1")
        .bind(42_i64)
        .execute(&pool)
        .await
        .expect("update last processed event id");

        let updated = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT last_processed_event_id
            FROM install_session_state
            WHERE project_id = $1
              AND install_id = $2
            "#,
        )
        .bind(project_id())
        .bind("install-1")
        .fetch_one(&pool)
        .await
        .expect("fetch updated last processed event id");
        assert_eq!(updated, 42);
    }

    #[sqlx::test]
    async fn load_install_session_states_returns_requested_rows(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        upsert_install_session_state(&pool, &tail_state(10, 2))
            .await
            .expect("save first tail state");
        upsert_install_session_state(
            &pool,
            &InstallSessionStateRecord {
                project_id: project_id(),
                install_id: "install-2".to_owned(),
                tail_session_id: "install-2:2026-01-01T00:00:00+00:00".to_owned(),
                tail_session_start: timestamp(0),
                tail_session_end: timestamp(20),
                tail_event_count: 3,
                tail_duration_seconds: 1_200,
                tail_day: NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"),
                last_processed_event_id: 77,
            },
        )
        .await
        .expect("save second tail state");

        let mut tx = pool.begin().await.expect("begin transaction");
        let states = load_install_session_states_in_tx(
            &mut tx,
            &[
                (project_id(), "install-1".to_owned()),
                (project_id(), "install-2".to_owned()),
                (project_id(), "missing".to_owned()),
            ],
        )
        .await
        .expect("load install states");

        assert_eq!(states.len(), 2);
        assert_eq!(
            states
                .get(&(project_id(), "install-1".to_owned()))
                .expect("install-1 state")
                .tail_event_count,
            2
        );
        assert_eq!(
            states
                .get(&(project_id(), "install-2".to_owned()))
                .expect("install-2 state")
                .last_processed_event_id,
            77
        );
    }

    #[sqlx::test]
    async fn session_rebuild_queue_round_trips_pending_buckets_by_project(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");
        let other_project = Uuid::from_u128(u128::MAX);
        sqlx::query("INSERT INTO projects (id, name) VALUES ($1, $2)")
            .bind(other_project)
            .bind("Other")
            .execute(&pool)
            .await
            .expect("insert other project");

        let mut tx = pool.begin().await.expect("begin queue seed tx");
        enqueue_session_rebuild_buckets_in_tx(
            &mut tx,
            project_id(),
            &[timestamp(0)],
            &[timestamp(0), timestamp(60)],
        )
        .await
        .expect("enqueue primary project buckets");
        enqueue_session_rebuild_buckets_in_tx(
            &mut tx,
            other_project,
            &[timestamp(24 * 60)],
            &[timestamp(24 * 60)],
        )
        .await
        .expect("enqueue other project buckets");
        tx.commit().await.expect("commit queue seed tx");

        let mut load_tx = pool.begin().await.expect("begin queue load tx");
        let pending = load_pending_session_rebuild_buckets_in_tx(&mut load_tx, &[project_id()])
            .await
            .expect("load pending buckets");
        assert_eq!(
            pending,
            vec![
                PendingSessionRebuildBucketRecord {
                    project_id: project_id(),
                    granularity: MetricGranularity::Day,
                    bucket_start: timestamp(0),
                },
                PendingSessionRebuildBucketRecord {
                    project_id: project_id(),
                    granularity: MetricGranularity::Hour,
                    bucket_start: timestamp(0),
                },
                PendingSessionRebuildBucketRecord {
                    project_id: project_id(),
                    granularity: MetricGranularity::Hour,
                    bucket_start: timestamp(60),
                },
            ]
        );
        let deleted = delete_pending_session_rebuild_buckets_in_tx(&mut load_tx, &[project_id()])
            .await
            .expect("delete pending buckets");
        load_tx.commit().await.expect("commit queue delete tx");

        assert_eq!(deleted, 3);

        let mut verify_tx = pool.begin().await.expect("begin queue verify tx");
        let primary_pending =
            load_pending_session_rebuild_buckets_in_tx(&mut verify_tx, &[project_id()])
                .await
                .expect("load deleted project buckets");
        let other_pending =
            load_pending_session_rebuild_buckets_in_tx(&mut verify_tx, &[other_project])
                .await
                .expect("load untouched project buckets");
        verify_tx.commit().await.expect("commit queue verify tx");

        assert!(primary_pending.is_empty());
        assert_eq!(
            other_pending,
            vec![
                PendingSessionRebuildBucketRecord {
                    project_id: other_project,
                    granularity: MetricGranularity::Day,
                    bucket_start: timestamp(24 * 60),
                },
                PendingSessionRebuildBucketRecord {
                    project_id: other_project,
                    granularity: MetricGranularity::Hour,
                    bucket_start: timestamp(24 * 60),
                },
            ]
        );
    }

    #[sqlx::test]
    async fn claim_and_sweep_pending_session_rebuild_buckets_sweeps_visible_project_scope(
        pool: PgPool,
    ) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");
        let other_project = Uuid::from_u128(u128::MAX);
        sqlx::query("INSERT INTO projects (id, name) VALUES ($1, $2)")
            .bind(other_project)
            .bind("Other")
            .execute(&pool)
            .await
            .expect("insert other project");

        let mut seed_tx = pool.begin().await.expect("begin queue seed tx");
        enqueue_session_rebuild_buckets_in_tx(
            &mut seed_tx,
            project_id(),
            &[timestamp(0)],
            &[timestamp(0), timestamp(60)],
        )
        .await
        .expect("enqueue primary project buckets");
        enqueue_session_rebuild_buckets_in_tx(
            &mut seed_tx,
            other_project,
            &[timestamp(24 * 60)],
            &[timestamp(24 * 60)],
        )
        .await
        .expect("enqueue other project buckets");
        seed_tx.commit().await.expect("commit queue seed tx");

        let mut sweep_tx = pool.begin().await.expect("begin queue sweep tx");
        let swept = claim_and_sweep_pending_session_rebuild_buckets_in_tx(&mut sweep_tx, 1)
            .await
            .expect("claim and sweep pending buckets");
        let remaining_primary =
            load_pending_session_rebuild_buckets_in_tx(&mut sweep_tx, &[project_id()])
                .await
                .expect("load remaining primary buckets");
        let remaining_other =
            load_pending_session_rebuild_buckets_in_tx(&mut sweep_tx, &[other_project])
                .await
                .expect("load remaining other buckets");
        sweep_tx.commit().await.expect("commit queue sweep tx");

        assert_eq!(
            swept,
            vec![
                PendingSessionRebuildBucketRecord {
                    project_id: project_id(),
                    granularity: MetricGranularity::Day,
                    bucket_start: timestamp(0),
                },
                PendingSessionRebuildBucketRecord {
                    project_id: project_id(),
                    granularity: MetricGranularity::Hour,
                    bucket_start: timestamp(0),
                },
                PendingSessionRebuildBucketRecord {
                    project_id: project_id(),
                    granularity: MetricGranularity::Hour,
                    bucket_start: timestamp(60),
                },
            ]
        );
        assert!(remaining_primary.is_empty());
        assert_eq!(
            remaining_other,
            vec![
                PendingSessionRebuildBucketRecord {
                    project_id: other_project,
                    granularity: MetricGranularity::Day,
                    bucket_start: timestamp(24 * 60),
                },
                PendingSessionRebuildBucketRecord {
                    project_id: other_project,
                    granularity: MetricGranularity::Hour,
                    bucket_start: timestamp(24 * 60),
                },
            ]
        );
    }

    #[sqlx::test]
    async fn session_daily_installs_rebuild_queue_round_trips_pending_rows_by_project(
        pool: PgPool,
    ) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");
        let other_project = Uuid::new_v4();
        sqlx::query("INSERT INTO projects (id, name) VALUES ($1, $2)")
            .bind(other_project)
            .bind("Other")
            .execute(&pool)
            .await
            .expect("insert other project");

        let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");
        let day_2 = NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid date");

        let mut seed_tx = pool.begin().await.expect("begin queue seed tx");
        enqueue_session_daily_installs_rebuilds_in_tx(
            &mut seed_tx,
            &[
                PendingSessionDailyInstallRebuildRecord {
                    project_id: project_id(),
                    day: day_1,
                    install_id: "install-1".to_owned(),
                },
                PendingSessionDailyInstallRebuildRecord {
                    project_id: project_id(),
                    day: day_1,
                    install_id: "install-2".to_owned(),
                },
                PendingSessionDailyInstallRebuildRecord {
                    project_id: project_id(),
                    day: day_1,
                    install_id: "install-1".to_owned(),
                },
                PendingSessionDailyInstallRebuildRecord {
                    project_id: other_project,
                    day: day_2,
                    install_id: "other-install".to_owned(),
                },
            ],
        )
        .await
        .expect("enqueue install rebuild rows");
        seed_tx.commit().await.expect("commit queue seed tx");

        let mut load_tx = pool.begin().await.expect("begin queue load tx");
        let pending =
            load_pending_session_daily_install_rebuilds_in_tx(&mut load_tx, &[project_id()])
                .await
                .expect("load pending install rebuild rows");
        let deleted =
            delete_pending_session_daily_install_rebuilds_in_tx(&mut load_tx, &[project_id()])
                .await
                .expect("delete pending install rebuild rows");
        load_tx.commit().await.expect("commit queue load tx");

        assert_eq!(
            pending,
            vec![
                PendingSessionDailyInstallRebuildRecord {
                    project_id: project_id(),
                    day: day_1,
                    install_id: "install-1".to_owned(),
                },
                PendingSessionDailyInstallRebuildRecord {
                    project_id: project_id(),
                    day: day_1,
                    install_id: "install-2".to_owned(),
                },
            ]
        );
        assert_eq!(deleted, 2);

        let mut verify_tx = pool.begin().await.expect("begin queue verify tx");
        let primary_pending =
            load_pending_session_daily_install_rebuilds_in_tx(&mut verify_tx, &[project_id()])
                .await
                .expect("load deleted install rebuild rows");
        let other_pending =
            load_pending_session_daily_install_rebuilds_in_tx(&mut verify_tx, &[other_project])
                .await
                .expect("load untouched install rebuild rows");
        verify_tx.commit().await.expect("commit queue verify tx");

        assert!(primary_pending.is_empty());
        assert_eq!(
            other_pending,
            vec![PendingSessionDailyInstallRebuildRecord {
                project_id: other_project,
                day: day_2,
                install_id: "other-install".to_owned(),
            }]
        );
    }

    #[sqlx::test]
    async fn run_migrations_creates_session_repair_jobs_table(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let repair_queue_tables = sqlx::query_scalar::<_, String>(
            r#"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'session_repair_jobs'
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch repair queue tables");

        assert_eq!(repair_queue_tables, vec!["session_repair_jobs".to_owned()]);
    }

    #[sqlx::test]
    async fn session_repair_jobs_widen_install_frontier(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        for frontier in [10_i64, 12_i64, 11_i64] {
            sqlx::query(
                r#"
                INSERT INTO session_repair_jobs (project_id, install_id, target_event_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (project_id, install_id) DO UPDATE SET
                    target_event_id = GREATEST(
                        session_repair_jobs.target_event_id,
                        EXCLUDED.target_event_id
                    ),
                    updated_at = now()
                "#,
            )
            .bind(project_id())
            .bind("install-1")
            .bind(frontier)
            .execute(&pool)
            .await
            .expect("upsert repair frontier");
        }

        let widened = sqlx::query(
            r#"
            SELECT target_event_id
            FROM session_repair_jobs
            WHERE project_id = $1
              AND install_id = $2
            "#,
        )
        .bind(project_id())
        .bind("install-1")
        .fetch_one(&pool)
        .await
        .expect("load widened repair frontier");

        assert_eq!(widened.try_get::<i64, _>("target_event_id").unwrap(), 12);

        let row_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM session_repair_jobs
            WHERE project_id = $1
              AND install_id = $2
            "#,
        )
        .bind(project_id())
        .bind("install-1")
        .fetch_one(&pool)
        .await
        .expect("count repair frontier rows");

        assert_eq!(row_count, 1);
    }

    #[sqlx::test]
    async fn load_session_repair_jobs_for_installs_returns_requested_rows(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let mut seed_tx = pool.begin().await.expect("begin repair frontier seed tx");
        upsert_session_repair_job_in_tx(&mut seed_tx, project_id(), "install-1", 12)
            .await
            .expect("save first repair frontier");
        upsert_session_repair_job_in_tx(&mut seed_tx, project_id(), "install-2", 77)
            .await
            .expect("save second repair frontier");
        seed_tx
            .commit()
            .await
            .expect("commit repair frontier seed tx");

        let mut load_tx = pool.begin().await.expect("begin repair frontier load tx");
        let jobs = load_session_repair_jobs_for_installs_in_tx(
            &mut load_tx,
            &[
                (project_id(), "install-1".to_owned()),
                (project_id(), "install-2".to_owned()),
                (project_id(), "missing".to_owned()),
            ],
        )
        .await
        .expect("load repair frontiers");
        load_tx
            .commit()
            .await
            .expect("commit repair frontier load tx");

        assert_eq!(jobs.len(), 2);
        assert_eq!(
            jobs.get(&(project_id(), "install-1".to_owned()))
                .expect("install-1 frontier")
                .target_event_id,
            12
        );
        assert_eq!(
            jobs.get(&(project_id(), "install-2".to_owned()))
                .expect("install-2 frontier")
                .target_event_id,
            77
        );
    }

    #[sqlx::test]
    async fn claimed_session_repair_jobs_round_trip_and_complete_by_target(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let other_project = Uuid::new_v4();
        sqlx::query("INSERT INTO projects (id, name) VALUES ($1, $2)")
            .bind(other_project)
            .bind("Other")
            .execute(&pool)
            .await
            .expect("insert other project");

        let mut seed_tx = pool.begin().await.expect("begin repair frontier seed tx");
        upsert_session_repair_job_in_tx(&mut seed_tx, project_id(), "install-1", 12)
            .await
            .expect("save first repair frontier");
        upsert_session_repair_job_in_tx(&mut seed_tx, project_id(), "install-2", 77)
            .await
            .expect("save second repair frontier");
        upsert_session_repair_job_in_tx(&mut seed_tx, other_project, "install-3", 99)
            .await
            .expect("save other project repair frontier");
        seed_tx
            .commit()
            .await
            .expect("commit repair frontier seed tx");

        let mut load_tx = pool.begin().await.expect("begin repair frontier load tx");
        let pending = claim_pending_session_repair_jobs_in_tx(&mut load_tx, 10)
            .await
            .expect("claim pending repair frontiers");
        assert_eq!(pending.len(), 3);
        assert!(pending.iter().all(|job| job.claimed_at.is_some()));
        assert!(pending.iter().any(|job| {
            job.project_id == project_id()
                && job.install_id == "install-1"
                && job.target_event_id == 12
        }));
        assert!(pending.iter().any(|job| {
            job.project_id == project_id()
                && job.install_id == "install-2"
                && job.target_event_id == 77
        }));
        assert!(pending.iter().any(|job| {
            job.project_id == other_project
                && job.install_id == "install-3"
                && job.target_event_id == 99
        }));

        complete_session_repair_job_in_tx(&mut load_tx, project_id(), "install-1", 12)
            .await
            .expect("complete matching repair frontier");
        complete_session_repair_job_in_tx(&mut load_tx, project_id(), "install-2", 12)
            .await
            .expect("release non-matching repair frontier");
        complete_session_repair_job_in_tx(&mut load_tx, other_project, "install-3", 99)
            .await
            .expect("complete other project repair frontier");
        load_tx
            .commit()
            .await
            .expect("commit repair frontier load tx");

        let mut verify_tx = pool.begin().await.expect("begin repair frontier verify tx");
        let remaining = load_session_repair_jobs_for_installs_in_tx(
            &mut verify_tx,
            &[
                (project_id(), "install-2".to_owned()),
                (other_project, "install-3".to_owned()),
            ],
        )
        .await
        .expect("load remaining repair frontiers");
        let reclaimed = claim_pending_session_repair_jobs_in_tx(&mut verify_tx, 10)
            .await
            .expect("claim remaining repair frontiers");
        verify_tx
            .commit()
            .await
            .expect("commit repair frontier verify tx");

        assert_eq!(remaining.len(), 1);
        let install_2 = remaining
            .get(&(project_id(), "install-2".to_owned()))
            .expect("install-2 remains queued");
        assert_eq!(install_2.target_event_id, 77);
        assert_eq!(install_2.claimed_at, None);
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].project_id, project_id());
        assert_eq!(reclaimed[0].install_id, "install-2");
        assert_eq!(reclaimed[0].target_event_id, 77);
        assert!(reclaimed[0].claimed_at.is_some());
    }

    #[sqlx::test]
    async fn claimed_session_repair_job_releases_newer_frontier_for_later_repair(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let mut seed_tx = pool.begin().await.expect("begin repair frontier seed tx");
        upsert_session_repair_job_in_tx(&mut seed_tx, project_id(), "install-1", 12)
            .await
            .expect("save repair frontier");
        seed_tx
            .commit()
            .await
            .expect("commit repair frontier seed tx");

        let mut claim_tx = pool.begin().await.expect("begin claim tx");
        let claimed = claim_pending_session_repair_jobs_in_tx(&mut claim_tx, 1)
            .await
            .expect("claim repair frontier");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].target_event_id, 12);
        claim_tx.commit().await.expect("commit claim tx");

        let mut widen_tx = pool.begin().await.expect("begin widen tx");
        upsert_session_repair_job_in_tx(&mut widen_tx, project_id(), "install-1", 15)
            .await
            .expect("widen claimed repair frontier");
        widen_tx.commit().await.expect("commit widen tx");

        let mut complete_tx = pool.begin().await.expect("begin complete tx");
        complete_session_repair_job_in_tx(&mut complete_tx, project_id(), "install-1", 12)
            .await
            .expect("release claimed frontier after widen");
        let widened = load_session_repair_jobs_for_installs_in_tx(
            &mut complete_tx,
            &[(project_id(), "install-1".to_owned())],
        )
        .await
        .expect("load widened frontier");
        let reclaimed = claim_pending_session_repair_jobs_in_tx(&mut complete_tx, 1)
            .await
            .expect("reclaim widened frontier");
        complete_tx.commit().await.expect("commit complete tx");

        let widened = widened
            .get(&(project_id(), "install-1".to_owned()))
            .expect("widened frontier exists");
        assert_eq!(widened.target_event_id, 15);
        assert_eq!(widened.claimed_at, None);
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].target_event_id, 15);
        assert!(reclaimed[0].claimed_at.is_some());
    }

    #[sqlx::test]
    async fn release_session_repair_job_claim_keeps_frontier_claimable(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let mut seed_tx = pool.begin().await.expect("begin repair frontier seed tx");
        upsert_session_repair_job_in_tx(&mut seed_tx, project_id(), "install-1", 12)
            .await
            .expect("save repair frontier");
        seed_tx
            .commit()
            .await
            .expect("commit repair frontier seed tx");

        let mut claim_tx = pool.begin().await.expect("begin claim tx");
        let claimed = claim_pending_session_repair_jobs_in_tx(&mut claim_tx, 1)
            .await
            .expect("claim repair frontier");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].target_event_id, 12);
        claim_tx.commit().await.expect("commit claim tx");

        let mut release_tx = pool.begin().await.expect("begin release tx");
        release_session_repair_job_claim_in_tx(&mut release_tx, project_id(), "install-1")
            .await
            .expect("release claimed repair frontier");
        let released = load_session_repair_jobs_for_installs_in_tx(
            &mut release_tx,
            &[(project_id(), "install-1".to_owned())],
        )
        .await
        .expect("load released frontier");
        let reclaimed = claim_pending_session_repair_jobs_in_tx(&mut release_tx, 1)
            .await
            .expect("reclaim released frontier");
        release_tx.commit().await.expect("commit release tx");

        let released = released
            .get(&(project_id(), "install-1".to_owned()))
            .expect("released frontier exists");
        assert_eq!(released.target_event_id, 12);
        assert_eq!(released.claimed_at, None);
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].target_event_id, 12);
        assert!(reclaimed[0].claimed_at.is_some());
    }

    #[sqlx::test]
    async fn fetch_events_for_install_after_id_through_orders_by_id(pool: PgPool) {
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
                event("install-1", 20),
            ],
        )
        .await
        .expect("insert events");

        let mut tx = pool.begin().await.expect("begin install frontier fetch tx");
        let stored = fetch_events_for_install_after_id_through_in_tx(
            &mut tx,
            project_id(),
            "install-1",
            0,
            4,
        )
        .await
        .expect("fetch install frontier events");
        tx.commit().await.expect("commit install frontier fetch tx");

        assert_eq!(stored.len(), 3);
        assert_eq!(stored[0].timestamp, timestamp(10));
        assert_eq!(stored[1].timestamp, timestamp(0));
        assert_eq!(stored[2].timestamp, timestamp(20));
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
    async fn session_daily_install_deltas_only_count_new_active_installs_once_per_install_day(
        pool: PgPool,
    ) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let day = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");
        let mut first_tx = pool.begin().await.expect("begin first tx");
        let first_active_deltas = upsert_session_daily_install_deltas_in_tx(
            &mut first_tx,
            &[
                SessionDailyInstallDelta {
                    project_id: project_id(),
                    day,
                    install_id: "install-1".to_owned(),
                    session_count: 2,
                },
                SessionDailyInstallDelta {
                    project_id: project_id(),
                    day,
                    install_id: "install-2".to_owned(),
                    session_count: 1,
                },
            ],
        )
        .await
        .expect("apply first membership deltas");
        first_tx.commit().await.expect("commit first tx");

        assert_eq!(
            first_active_deltas,
            vec![SessionDailyActiveInstallDelta {
                project_id: project_id(),
                day,
                active_installs: 2,
            }]
        );

        let mut second_tx = pool.begin().await.expect("begin second tx");
        let second_active_deltas = upsert_session_daily_install_deltas_in_tx(
            &mut second_tx,
            &[SessionDailyInstallDelta {
                project_id: project_id(),
                day,
                install_id: "install-1".to_owned(),
                session_count: 1,
            }],
        )
        .await
        .expect("apply second membership delta");
        second_tx.commit().await.expect("commit second tx");

        assert!(second_active_deltas.is_empty());

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
        .bind(day)
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
            3
        );
        assert_eq!(
            membership_counts[1]
                .try_get::<String, _>("install_id")
                .expect("install id"),
            "install-2"
        );
        assert_eq!(
            membership_counts[1]
                .try_get::<i32, _>("session_count")
                .expect("session count"),
            1
        );
    }

    #[sqlx::test]
    async fn session_daily_batch_helpers_upsert_new_days_and_update_duration_only_days(
        pool: PgPool,
    ) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");
        let day_2 = NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid date");

        sqlx::query(
            r#"
            INSERT INTO session_daily (
                project_id,
                day,
                sessions_count,
                active_installs,
                total_duration_seconds
            )
            VALUES ($1, $2, 1, 1, 600)
            "#,
        )
        .bind(project_id())
        .bind(day_1)
        .execute(&pool)
        .await
        .expect("seed day 1 session_daily row");

        let mut tx = pool.begin().await.expect("begin tx");
        upsert_session_daily_deltas_in_tx(
            &mut tx,
            &[SessionDailyDelta {
                project_id: project_id(),
                day: day_2,
                sessions_count: 1,
                active_installs: 1,
                total_duration_seconds: 0,
            }],
        )
        .await
        .expect("upsert daily deltas");
        add_session_daily_duration_deltas_in_tx(
            &mut tx,
            &[SessionDailyDurationDelta {
                project_id: project_id(),
                day: day_1,
                duration_delta: 25 * 60,
            }],
        )
        .await
        .expect("update duration-only deltas");
        tx.commit().await.expect("commit tx");

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
        let day_2_row = sqlx::query(
            r#"
            SELECT sessions_count, active_installs, total_duration_seconds
            FROM session_daily
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(day_2)
        .fetch_one(&pool)
        .await
        .expect("fetch day 2 row");

        assert_eq!(day_1_row.try_get::<i64, _>("sessions_count").unwrap(), 1);
        assert_eq!(day_1_row.try_get::<i64, _>("active_installs").unwrap(), 1);
        assert_eq!(
            day_1_row
                .try_get::<i64, _>("total_duration_seconds")
                .unwrap(),
            35 * 60
        );
        assert_eq!(day_2_row.try_get::<i64, _>("sessions_count").unwrap(), 1);
        assert_eq!(day_2_row.try_get::<i64, _>("active_installs").unwrap(), 1);
        assert_eq!(
            day_2_row
                .try_get::<i64, _>("total_duration_seconds")
                .unwrap(),
            0
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
        let middle_day_installs_before = sqlx::query(
            r#"
            SELECT install_id, session_count, updated_at
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
            ORDER BY install_id ASC
            "#,
        )
        .bind(project_id())
        .bind(day_2)
        .fetch_all(&pool)
        .await
        .expect("fetch middle day install rows");

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
        let day_1_installs_after = sqlx::query(
            r#"
            SELECT install_id, session_count
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
            ORDER BY install_id ASC
            "#,
        )
        .bind(project_id())
        .bind(day_1)
        .fetch_all(&pool)
        .await
        .expect("fetch day 1 install rows after rebuild");
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
        let middle_day_installs_after = sqlx::query(
            r#"
            SELECT install_id, session_count, updated_at
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
            ORDER BY install_id ASC
            "#,
        )
        .bind(project_id())
        .bind(day_2)
        .fetch_all(&pool)
        .await
        .expect("fetch middle day install rows after rebuild");

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
        assert_eq!(
            day_1_installs_after
                .iter()
                .map(|row| (
                    row.try_get::<String, _>("install_id").unwrap(),
                    row.try_get::<i32, _>("session_count").unwrap()
                ))
                .collect::<Vec<_>>(),
            vec![("install-1".to_owned(), 1)]
        );
        assert_eq!(
            middle_day_installs_after.len(),
            middle_day_installs_before.len()
        );
        for (after, before) in middle_day_installs_after
            .iter()
            .zip(middle_day_installs_before.iter())
        {
            assert_eq!(
                after.try_get::<String, _>("install_id").unwrap(),
                before.try_get::<String, _>("install_id").unwrap()
            );
            assert_eq!(
                after.try_get::<i32, _>("session_count").unwrap(),
                before.try_get::<i32, _>("session_count").unwrap()
            );
            assert_eq!(
                after.try_get::<DateTime<Utc>, _>("updated_at").unwrap(),
                before.try_get::<DateTime<Utc>, _>("updated_at").unwrap()
            );
        }
        assert_eq!(day_3_row.try_get::<i64, _>("sessions_count").unwrap(), 1);
        assert_eq!(day_3_row.try_get::<i64, _>("active_installs").unwrap(), 1);
        assert_eq!(
            day_3_row
                .try_get::<i64, _>("total_duration_seconds")
                .unwrap(),
            45 * 60
        );
    }

    #[sqlx::test]
    async fn rebuild_session_daily_days_in_tx_rollback_preserves_existing_rows(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");
        let day_2 = NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid date");

        let mut seed_tx = pool.begin().await.expect("begin seed transaction");
        insert_session_in_tx(&mut seed_tx, &session("install-1", 0, 10))
            .await
            .expect("insert install-1 day 1 session");
        insert_session_in_tx(&mut seed_tx, &session("install-2", 24 * 60, 24 * 60 + 10))
            .await
            .expect("insert install-2 day 2 session");
        rebuild_session_daily_days_in_tx(&mut seed_tx, project_id(), &[day_1, day_2])
            .await
            .expect("seed session_daily rows");
        seed_tx.commit().await.expect("commit seed transaction");

        let session_daily_before = sqlx::query(
            r#"
            SELECT day, sessions_count, active_installs, total_duration_seconds
            FROM session_daily
            WHERE project_id = $1
            ORDER BY day ASC
            "#,
        )
        .bind(project_id())
        .fetch_all(&pool)
        .await
        .expect("fetch session_daily rows before rollback");
        let session_daily_installs_before = sqlx::query(
            r#"
            SELECT day, install_id, session_count
            FROM session_daily_installs
            WHERE project_id = $1
            ORDER BY day ASC, install_id ASC
            "#,
        )
        .bind(project_id())
        .fetch_all(&pool)
        .await
        .expect("fetch session_daily_installs rows before rollback");

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

        let mut rebuild_tx = pool.begin().await.expect("begin rebuild transaction");
        rebuild_session_daily_days_in_tx(&mut rebuild_tx, project_id(), &[day_1])
            .await
            .expect("rebuild day 1 inside rollback transaction");
        rebuild_tx
            .rollback()
            .await
            .expect("rollback rebuild transaction");

        let session_daily_after = sqlx::query(
            r#"
            SELECT day, sessions_count, active_installs, total_duration_seconds
            FROM session_daily
            WHERE project_id = $1
            ORDER BY day ASC
            "#,
        )
        .bind(project_id())
        .fetch_all(&pool)
        .await
        .expect("fetch session_daily rows after rollback");
        let session_daily_installs_after = sqlx::query(
            r#"
            SELECT day, install_id, session_count
            FROM session_daily_installs
            WHERE project_id = $1
            ORDER BY day ASC, install_id ASC
            "#,
        )
        .bind(project_id())
        .fetch_all(&pool)
        .await
        .expect("fetch session_daily_installs rows after rollback");

        assert_eq!(session_daily_after.len(), session_daily_before.len());
        for (after, before) in session_daily_after.iter().zip(session_daily_before.iter()) {
            assert_eq!(
                after.try_get::<NaiveDate, _>("day").unwrap(),
                before.try_get::<NaiveDate, _>("day").unwrap()
            );
            assert_eq!(
                after.try_get::<i64, _>("sessions_count").unwrap(),
                before.try_get::<i64, _>("sessions_count").unwrap()
            );
            assert_eq!(
                after.try_get::<i64, _>("active_installs").unwrap(),
                before.try_get::<i64, _>("active_installs").unwrap()
            );
            assert_eq!(
                after.try_get::<i64, _>("total_duration_seconds").unwrap(),
                before.try_get::<i64, _>("total_duration_seconds").unwrap()
            );
        }

        assert_eq!(
            session_daily_installs_after.len(),
            session_daily_installs_before.len()
        );
        for (after, before) in session_daily_installs_after
            .iter()
            .zip(session_daily_installs_before.iter())
        {
            assert_eq!(
                after.try_get::<NaiveDate, _>("day").unwrap(),
                before.try_get::<NaiveDate, _>("day").unwrap()
            );
            assert_eq!(
                after.try_get::<String, _>("install_id").unwrap(),
                before.try_get::<String, _>("install_id").unwrap()
            );
            assert_eq!(
                after.try_get::<i32, _>("session_count").unwrap(),
                before.try_get::<i32, _>("session_count").unwrap()
            );
        }
    }

    #[test]
    fn rebuild_session_daily_days_helper_uses_full_rebuild_for_direct_callers_and_queue_scope_for_stage_b()
     {
        let source = include_str!("lib.rs");
        let start = source
            .find("pub async fn rebuild_session_daily_days_in_tx")
            .expect("find rebuild_session_daily_days_in_tx");
        let end = source
            .find("pub async fn load_worker_offset")
            .expect("find load_worker_offset");
        let helper = &source[start..end];

        assert!(
            helper.contains("rebuild_session_daily_installs_days_in_tx"),
            "direct callers should keep the existing full-day session_daily_installs rebuild helper"
        );
        assert_eq!(
            helper.matches("FROM sessions").count(),
            2,
            "direct callers should still scan sessions once for installs and once for the aggregate rollup"
        );
        assert!(
            helper.contains("install_rollups AS"),
            "the session_daily aggregate rebuild should keep the install_rollups CTE"
        );
        assert!(
            helper.contains("FROM session_daily_installs"),
            "session_daily aggregate rebuild should still derive active installs from session_daily_installs"
        );
        assert!(
            source.contains("rebuild_session_daily_installs_for_pending_rebuilds_in_tx"),
            "Stage B should gain a queue-driven install-day session_daily_installs rewrite helper"
        );
    }

    #[sqlx::test]
    async fn queued_session_daily_install_rebuild_rewrites_only_targeted_install_rows_within_touched_day(
        pool: PgPool,
    ) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");
        let day_2 = NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid date");

        let mut seed_tx = pool.begin().await.expect("begin seed transaction");
        insert_session_in_tx(&mut seed_tx, &session("install-1", 0, 10))
            .await
            .expect("insert install-1 day 1 session");
        insert_session_in_tx(&mut seed_tx, &session("install-2", 15, 30))
            .await
            .expect("insert install-2 day 1 session");
        insert_session_in_tx(&mut seed_tx, &session("install-3", 24 * 60, 24 * 60 + 10))
            .await
            .expect("insert install-3 day 2 session");
        rebuild_session_daily_days_in_tx(&mut seed_tx, project_id(), &[day_1, day_2])
            .await
            .expect("seed session_daily rows");
        seed_tx.commit().await.expect("commit seed transaction");

        let untouched_install_before = sqlx::query(
            r#"
            SELECT session_count, updated_at
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
              AND install_id = $3
            "#,
        )
        .bind(project_id())
        .bind(day_1)
        .bind("install-2")
        .fetch_one(&pool)
        .await
        .expect("fetch untouched install before rewrite");

        sqlx::query("SELECT pg_sleep(0.01)")
            .execute(&pool)
            .await
            .expect("sleep for updated_at delta");

        sqlx::query(
            r#"
            UPDATE sessions
            SET session_end = $4,
                duration_seconds = $5
            WHERE project_id = $1
              AND install_id = $2
              AND DATE(session_start AT TIME ZONE 'UTC') = $3
            "#,
        )
        .bind(project_id())
        .bind("install-1")
        .bind(day_1)
        .bind(timestamp(45))
        .bind(45 * 60)
        .execute(&pool)
        .await
        .expect("update targeted install session");

        let mut rebuild_tx = pool.begin().await.expect("begin queued rebuild tx");
        enqueue_session_daily_installs_rebuilds_in_tx(
            &mut rebuild_tx,
            &[PendingSessionDailyInstallRebuildRecord {
                project_id: project_id(),
                day: day_1,
                install_id: "install-1".to_owned(),
            }],
        )
        .await
        .expect("enqueue targeted install rebuild");
        rebuild_session_daily_installs_for_pending_rebuilds_in_tx(
            &mut rebuild_tx,
            project_id(),
            &[day_1],
        )
        .await
        .expect("rebuild queued install rows");
        rebuild_tx.commit().await.expect("commit queued rebuild tx");

        let targeted_install_after = sqlx::query(
            r#"
            SELECT session_count
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
              AND install_id = $3
            "#,
        )
        .bind(project_id())
        .bind(day_1)
        .bind("install-1")
        .fetch_one(&pool)
        .await
        .expect("fetch targeted install after rewrite");
        let untouched_install_after = sqlx::query(
            r#"
            SELECT session_count, updated_at
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
              AND install_id = $3
            "#,
        )
        .bind(project_id())
        .bind(day_1)
        .bind("install-2")
        .fetch_one(&pool)
        .await
        .expect("fetch untouched install after rewrite");

        assert_eq!(
            targeted_install_after
                .try_get::<i32, _>("session_count")
                .unwrap(),
            1
        );
        assert_eq!(
            untouched_install_after
                .try_get::<i32, _>("session_count")
                .unwrap(),
            untouched_install_before
                .try_get::<i32, _>("session_count")
                .unwrap()
        );
        assert_eq!(
            untouched_install_after
                .try_get::<DateTime<Utc>, _>("updated_at")
                .unwrap(),
            untouched_install_before
                .try_get::<DateTime<Utc>, _>("updated_at")
                .unwrap()
        );
    }

    #[sqlx::test]
    async fn queued_session_daily_install_rebuild_rollback_preserves_existing_rows_and_pending_queue(
        pool: PgPool,
    ) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");

        let mut seed_tx = pool.begin().await.expect("begin seed transaction");
        insert_session_in_tx(&mut seed_tx, &session("install-1", 0, 10))
            .await
            .expect("insert install-1 session");
        insert_session_in_tx(&mut seed_tx, &session("install-2", 15, 30))
            .await
            .expect("insert install-2 session");
        rebuild_session_daily_days_in_tx(&mut seed_tx, project_id(), &[day_1])
            .await
            .expect("seed session_daily rows");
        seed_tx.commit().await.expect("commit seed transaction");

        let installs_before = sqlx::query(
            r#"
            SELECT install_id, session_count
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
            ORDER BY install_id ASC
            "#,
        )
        .bind(project_id())
        .bind(day_1)
        .fetch_all(&pool)
        .await
        .expect("fetch installs before rollback");

        sqlx::query(
            r#"
            UPDATE sessions
            SET session_end = $4,
                duration_seconds = $5
            WHERE project_id = $1
              AND install_id = $2
              AND DATE(session_start AT TIME ZONE 'UTC') = $3
            "#,
        )
        .bind(project_id())
        .bind("install-1")
        .bind(day_1)
        .bind(timestamp(45))
        .bind(45 * 60)
        .execute(&pool)
        .await
        .expect("update targeted install session");

        let queued_row = PendingSessionDailyInstallRebuildRecord {
            project_id: project_id(),
            day: day_1,
            install_id: "install-1".to_owned(),
        };
        let mut queue_tx = pool.begin().await.expect("begin queue seed tx");
        enqueue_session_daily_installs_rebuilds_in_tx(
            &mut queue_tx,
            std::slice::from_ref(&queued_row),
        )
        .await
        .expect("enqueue install rebuild row");
        queue_tx.commit().await.expect("commit queue seed tx");

        let mut rebuild_tx = pool.begin().await.expect("begin queued rebuild tx");
        rebuild_session_daily_installs_for_pending_rebuilds_in_tx(
            &mut rebuild_tx,
            project_id(),
            &[day_1],
        )
        .await
        .expect("rebuild queued install rows");
        rebuild_tx
            .rollback()
            .await
            .expect("rollback queued rebuild tx");

        let installs_after = sqlx::query(
            r#"
            SELECT install_id, session_count
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
            ORDER BY install_id ASC
            "#,
        )
        .bind(project_id())
        .bind(day_1)
        .fetch_all(&pool)
        .await
        .expect("fetch installs after rollback");
        let queued_after = sqlx::query(
            r#"
            SELECT project_id, day, install_id
            FROM session_daily_installs_rebuild_queue
            WHERE project_id = $1
            ORDER BY day ASC, install_id ASC
            "#,
        )
        .bind(project_id())
        .fetch_all(&pool)
        .await
        .expect("fetch queued install rebuild rows after rollback");

        assert_eq!(installs_after.len(), installs_before.len());
        for (after, before) in installs_after.iter().zip(installs_before.iter()) {
            assert_eq!(
                after.try_get::<String, _>("install_id").unwrap(),
                before.try_get::<String, _>("install_id").unwrap()
            );
            assert_eq!(
                after.try_get::<i32, _>("session_count").unwrap(),
                before.try_get::<i32, _>("session_count").unwrap()
            );
        }
        assert_eq!(
            queued_after
                .into_iter()
                .map(|row| PendingSessionDailyInstallRebuildRecord {
                    project_id: row.try_get("project_id").unwrap(),
                    day: row.try_get("day").unwrap(),
                    install_id: row.try_get("install_id").unwrap(),
                })
                .collect::<Vec<_>>(),
            vec![queued_row]
        );
    }

    #[sqlx::test]
    async fn queued_session_daily_install_rebuild_requires_pending_queue_rows_for_stage_b_scope(
        pool: PgPool,
    ) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");
        let mut seed_tx = pool.begin().await.expect("begin seed transaction");
        insert_session_in_tx(&mut seed_tx, &session("install-1", 0, 10))
            .await
            .expect("insert install-1 session");
        rebuild_session_daily_days_in_tx(&mut seed_tx, project_id(), &[day_1])
            .await
            .expect("seed session_daily rows");
        seed_tx.commit().await.expect("commit seed transaction");

        let mut rebuild_tx = pool.begin().await.expect("begin rebuild transaction");
        let error = rebuild_session_daily_installs_for_pending_rebuilds_in_tx(
            &mut rebuild_tx,
            project_id(),
            &[day_1],
        )
        .await
        .expect_err("Stage B scope without queue rows should fail loudly");
        rebuild_tx
            .rollback()
            .await
            .expect("rollback failed rebuild transaction");

        assert!(
            matches!(error, StoreError::InvariantViolation(message) if message.contains("missing session_daily_installs rebuild queue rows")),
            "missing queue rows should raise an invariant violation instead of silently broadening the rebuild"
        );
    }

    #[sqlx::test]
    async fn queued_session_daily_install_rebuild_requires_queue_coverage_for_every_requested_day(
        pool: PgPool,
    ) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");
        let day_2 = NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid date");

        let mut seed_tx = pool.begin().await.expect("begin seed transaction");
        insert_session_in_tx(&mut seed_tx, &session("install-1", 0, 10))
            .await
            .expect("insert day 1 session");
        insert_session_in_tx(&mut seed_tx, &session("install-2", 24 * 60, 24 * 60 + 10))
            .await
            .expect("insert day 2 session");
        rebuild_session_daily_days_in_tx(&mut seed_tx, project_id(), &[day_1, day_2])
            .await
            .expect("seed session_daily rows");
        seed_tx.commit().await.expect("commit seed transaction");

        let mut queue_tx = pool.begin().await.expect("begin queue seed tx");
        enqueue_session_daily_installs_rebuilds_in_tx(
            &mut queue_tx,
            &[PendingSessionDailyInstallRebuildRecord {
                project_id: project_id(),
                day: day_1,
                install_id: "install-1".to_owned(),
            }],
        )
        .await
        .expect("enqueue partial queue coverage");
        queue_tx.commit().await.expect("commit queue seed tx");

        let mut rebuild_tx = pool.begin().await.expect("begin rebuild transaction");
        let error = rebuild_session_daily_installs_for_pending_rebuilds_in_tx(
            &mut rebuild_tx,
            project_id(),
            &[day_1, day_2],
        )
        .await
        .expect_err("partial queue coverage should fail loudly");
        rebuild_tx
            .rollback()
            .await
            .expect("rollback partial coverage rebuild transaction");

        assert!(
            matches!(error, StoreError::InvariantViolation(message) if message.contains("requested") && message.contains("claimed")),
            "Stage B should reject partial queue coverage instead of rebuilding only some requested days"
        );
    }

    #[sqlx::test]
    async fn later_same_day_refill_waits_until_current_sweep_commits_and_keeps_matching_queue_rows(
        pool: PgPool,
    ) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");
        let day_bucket_start = timestamp(0);

        let mut seed_tx = pool.begin().await.expect("begin seed transaction");
        insert_session_in_tx(&mut seed_tx, &session("install-1", 0, 10))
            .await
            .expect("insert install-1 session");
        insert_session_in_tx(&mut seed_tx, &session("install-2", 15, 30))
            .await
            .expect("insert install-2 session");
        rebuild_session_daily_days_in_tx(&mut seed_tx, project_id(), &[day_1])
            .await
            .expect("seed session_daily rows");
        enqueue_session_rebuild_buckets_in_tx(&mut seed_tx, project_id(), &[day_bucket_start], &[])
            .await
            .expect("enqueue original day bucket");
        enqueue_session_daily_installs_rebuilds_in_tx(
            &mut seed_tx,
            &[PendingSessionDailyInstallRebuildRecord {
                project_id: project_id(),
                day: day_1,
                install_id: "install-1".to_owned(),
            }],
        )
        .await
        .expect("enqueue original install rebuild row");
        seed_tx.commit().await.expect("commit seed transaction");

        let mut finalizer_tx = pool.begin().await.expect("begin finalizer transaction");
        let swept = claim_and_sweep_pending_session_rebuild_buckets_in_tx(&mut finalizer_tx, 1)
            .await
            .expect("claim current day bucket");
        assert_eq!(
            swept,
            vec![PendingSessionRebuildBucketRecord {
                project_id: project_id(),
                granularity: MetricGranularity::Day,
                bucket_start: day_bucket_start,
            }]
        );

        let refill_started = std::sync::Arc::new(tokio::sync::Notify::new());
        let refill_bucket_enqueued = std::sync::Arc::new(tokio::sync::Notify::new());
        let refill_pool = pool.clone();
        let refill_started_clone = refill_started.clone();
        let refill_bucket_enqueued_clone = refill_bucket_enqueued.clone();
        let refill_task = tokio::spawn(async move {
            let mut refill_tx = refill_pool.begin().await.expect("begin refill transaction");
            enqueue_session_daily_installs_rebuilds_in_tx(
                &mut refill_tx,
                &[PendingSessionDailyInstallRebuildRecord {
                    project_id: project_id(),
                    day: day_1,
                    install_id: "install-2".to_owned(),
                }],
            )
            .await
            .expect("enqueue refill install row");
            refill_started_clone.notify_waiters();
            enqueue_session_rebuild_buckets_in_tx(
                &mut refill_tx,
                project_id(),
                &[day_bucket_start],
                &[],
            )
            .await
            .expect("enqueue refill day bucket");
            refill_bucket_enqueued_clone.notify_waiters();
            refill_tx.commit().await.expect("commit refill transaction");
        });

        refill_started.notified().await;

        rebuild_session_daily_installs_for_pending_rebuilds_in_tx(
            &mut finalizer_tx,
            project_id(),
            &[day_1],
        )
        .await
        .expect("rebuild current sweep install rows");

        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                refill_bucket_enqueued.notified()
            )
            .await
            .is_err(),
            "matching day-bucket refill should stay blocked until the current sweep commits"
        );

        finalizer_tx
            .commit()
            .await
            .expect("commit finalizer transaction");
        refill_task.await.expect("join refill task");

        let mut verify_tx = pool.begin().await.expect("begin verify transaction");
        let pending_buckets =
            load_pending_session_rebuild_buckets_in_tx(&mut verify_tx, &[project_id()])
                .await
                .expect("load refilled day bucket");
        let pending_installs =
            load_pending_session_daily_install_rebuilds_in_tx(&mut verify_tx, &[project_id()])
                .await
                .expect("load refilled install rows");

        assert_eq!(
            pending_buckets,
            vec![PendingSessionRebuildBucketRecord {
                project_id: project_id(),
                granularity: MetricGranularity::Day,
                bucket_start: day_bucket_start,
            }]
        );
        assert_eq!(
            pending_installs,
            vec![PendingSessionDailyInstallRebuildRecord {
                project_id: project_id(),
                day: day_1,
                install_id: "install-2".to_owned(),
            }]
        );

        let next_swept = claim_and_sweep_pending_session_rebuild_buckets_in_tx(&mut verify_tx, 1)
            .await
            .expect("claim refilled day bucket");
        assert_eq!(
            next_swept,
            vec![PendingSessionRebuildBucketRecord {
                project_id: project_id(),
                granularity: MetricGranularity::Day,
                bucket_start: day_bucket_start,
            }]
        );
        rebuild_session_daily_installs_for_pending_rebuilds_in_tx(
            &mut verify_tx,
            project_id(),
            &[day_1],
        )
        .await
        .expect("rebuild refilled install rows");
        verify_tx.commit().await.expect("commit verify transaction");
    }
}
