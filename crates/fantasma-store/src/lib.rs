#![allow(clippy::too_many_arguments)]

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    time::Duration,
};

use chrono::{DateTime, NaiveDate, Utc};
use fantasma_auth::{derive_key_prefix, hash_ingest_key};
use fantasma_core::{
    EventPayload, MetricGranularity, Platform, ProjectSummary, SessionMetric, UsageProjectEvents,
};
use roaring::RoaringTreemap;
use serde_json::Value;
pub use sqlx::PgPool;
use sqlx::{
    Executor, Postgres, QueryBuilder, Row, Transaction, migrate::Migrator, postgres::PgPoolOptions,
};
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 5;
const POSTGRES_MAX_BIND_PARAMS: usize = 65_535;
pub const PROJECT_PROCESSING_LEASE_STALE_AFTER_SECS: i64 = 60 * 60;
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
    pub state: ProjectState,
    pub fence_epoch: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectState {
    Active,
    RangeDeleting,
    PendingDeletion,
}

impl ProjectState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::RangeDeleting => "range_deleting",
            Self::PendingDeletion => "pending_deletion",
        }
    }
}

impl std::fmt::Display for ProjectState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectProcessingActorKind {
    Ingest,
    SessionApply,
    SessionRepair,
    EventMetrics,
    ProjectPatch,
    ApiKeyCreate,
    ApiKeyRevoke,
}

impl ProjectProcessingActorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ingest => "ingest",
            Self::SessionApply => "session_apply",
            Self::SessionRepair => "session_repair",
            Self::EventMetrics => "event_metrics",
            Self::ProjectPatch => "project_patch",
            Self::ApiKeyCreate => "api_key_create",
            Self::ApiKeyRevoke => "api_key_revoke",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectProcessingLease {
    pub project_id: Uuid,
    pub actor_kind: ProjectProcessingActorKind,
    pub actor_id: String,
    pub fence_epoch: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectDeletionKind {
    ProjectPurge,
    RangeDelete,
}

impl ProjectDeletionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProjectPurge => "project_purge",
            Self::RangeDelete => "range_delete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectDeletionStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

impl ProjectDeletionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectDeletionRecord {
    pub id: Uuid,
    pub project_id: Uuid,
    pub project_name: String,
    pub kind: ProjectDeletionKind,
    pub status: ProjectDeletionStatus,
    pub scope: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub deleted_raw_events: i64,
    pub deleted_sessions: i64,
    pub rebuilt_installs: i64,
    pub rebuilt_days: i64,
    pub rebuilt_hours: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProjectDeletionStats {
    pub deleted_raw_events: i64,
    pub deleted_sessions: i64,
    pub rebuilt_installs: i64,
    pub rebuilt_days: i64,
    pub rebuilt_hours: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartProjectDeletionResult {
    Enqueued(Box<ProjectDeletionRecord>),
    ProjectBusy,
    ProjectPendingDeletion,
    NotFound,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedIngestProject {
    pub project_id: Uuid,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("invalid platform value: {0}")]
    InvalidPlatform(String),
    #[error("project not found")]
    ProjectNotFound,
    #[error("project is not active: {0}")]
    ProjectNotActive(ProjectState),
    #[error("project fence changed during mutation")]
    ProjectFenceChanged,
    #[error("invariant violation: {0}")]
    InvariantViolation(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEventRecord {
    pub id: i64,
    pub project_id: Uuid,
    pub event_name: String,
    pub timestamp: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub install_id: String,
    pub platform: Platform,
    pub app_version: Option<String>,
    pub os_version: Option<String>,
    pub locale: Option<String>,
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
    pub locale: Option<String>,
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
    pub locale: Option<String>,
    pub locale_is_null: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDailyActiveInstallDelta {
    pub project_id: Uuid,
    pub day: NaiveDate,
    pub active_installs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveInstallStateUpsert {
    pub project_id: Uuid,
    pub install_id: String,
    pub last_received_at: DateTime<Utc>,
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
pub struct EventMetricBucketDim3Delta {
    pub project_id: Uuid,
    pub granularity: MetricGranularity,
    pub bucket_start: DateTime<Utc>,
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
pub struct EventMetricBucketDim4Delta {
    pub project_id: Uuid,
    pub granularity: MetricGranularity,
    pub bucket_start: DateTime<Utc>,
    pub event_name: String,
    pub dim1_key: String,
    pub dim1_value: String,
    pub dim2_key: String,
    pub dim2_value: String,
    pub dim3_key: String,
    pub dim3_value: String,
    pub dim4_key: String,
    pub dim4_value: String,
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
pub struct SessionMetricDim3Delta {
    pub project_id: Uuid,
    pub granularity: MetricGranularity,
    pub bucket_start: DateTime<Utc>,
    pub dim1_key: String,
    pub dim1_value: String,
    pub dim2_key: String,
    pub dim2_value: String,
    pub dim3_key: String,
    pub dim3_value: String,
    pub session_count: i64,
    pub duration_total_seconds: i64,
    pub new_installs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMetricDim4Delta {
    pub project_id: Uuid,
    pub granularity: MetricGranularity,
    pub bucket_start: DateTime<Utc>,
    pub dim1_key: String,
    pub dim1_value: String,
    pub dim2_key: String,
    pub dim2_value: String,
    pub dim3_key: String,
    pub dim3_value: String,
    pub dim4_key: String,
    pub dim4_value: String,
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
    pub locale: Option<String>,
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
        RETURNING id, name, state, fence_epoch, created_at
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
        RETURNING id, name, state, fence_epoch, created_at
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
        SELECT id, name, state, fence_epoch, created_at
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
        SELECT id, name, state, fence_epoch, created_at
        FROM projects
        WHERE id = $1
        "#,
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await?;

    row.as_ref().map(project_record_from_row).transpose()
}

pub async fn claim_project_processing_lease_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    actor_kind: ProjectProcessingActorKind,
    actor_id: &str,
) -> Result<ProjectProcessingLease, StoreError> {
    let project = sqlx::query(
        r#"
        SELECT state, fence_epoch
        FROM projects
        WHERE id = $1
        "#,
    )
    .bind(project_id)
    .fetch_optional(&mut **tx)
    .await?;

    let Some(project) = project else {
        return Err(StoreError::ProjectNotFound);
    };

    let state = project_state_from_str(&project.try_get::<String, _>("state")?)?;
    if state != ProjectState::Active {
        return Err(StoreError::ProjectNotActive(state));
    }

    let fence_epoch: i64 = project.try_get("fence_epoch")?;
    sqlx::query(
        r#"
        INSERT INTO project_processing_leases (
            project_id,
            actor_kind,
            actor_id,
            fence_epoch,
            claimed_at,
            heartbeat_at
        )
        VALUES ($1, $2, $3, $4, now(), now())
        ON CONFLICT (project_id, actor_kind, actor_id)
        DO UPDATE SET
            fence_epoch = EXCLUDED.fence_epoch,
            claimed_at = now(),
            heartbeat_at = now()
        "#,
    )
    .bind(project_id)
    .bind(actor_kind.as_str())
    .bind(actor_id)
    .bind(fence_epoch)
    .execute(&mut **tx)
    .await?;

    Ok(ProjectProcessingLease {
        project_id,
        actor_kind,
        actor_id: actor_id.to_owned(),
        fence_epoch,
    })
}

pub async fn claim_project_processing_lease(
    pool: &PgPool,
    project_id: Uuid,
    actor_kind: ProjectProcessingActorKind,
    actor_id: &str,
) -> Result<ProjectProcessingLease, StoreError> {
    let mut tx = pool.begin().await?;
    let lease =
        claim_project_processing_lease_in_tx(&mut tx, project_id, actor_kind, actor_id).await?;
    tx.commit().await?;

    Ok(lease)
}

pub async fn verify_project_processing_lease_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ProjectProcessingLease,
) -> Result<(), StoreError> {
    let fence_epoch = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT fence_epoch
        FROM projects
        WHERE id = $1
        "#,
    )
    .bind(lease.project_id)
    .fetch_optional(&mut **tx)
    .await?;

    match fence_epoch {
        Some(current_epoch) if current_epoch == lease.fence_epoch => Ok(()),
        Some(_) | None => Err(StoreError::ProjectFenceChanged),
    }
}

pub async fn release_project_processing_lease_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ProjectProcessingLease,
) -> Result<(), StoreError> {
    sqlx::query(
        r#"
        DELETE FROM project_processing_leases
        WHERE project_id = $1
          AND actor_kind = $2
          AND actor_id = $3
        "#,
    )
    .bind(lease.project_id)
    .bind(lease.actor_kind.as_str())
    .bind(&lease.actor_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn release_project_processing_lease(
    pool: &PgPool,
    lease: &ProjectProcessingLease,
) -> Result<(), StoreError> {
    sqlx::query(
        r#"
        DELETE FROM project_processing_leases
        WHERE project_id = $1
          AND actor_kind = $2
          AND actor_id = $3
        "#,
    )
    .bind(lease.project_id)
    .bind(lease.actor_kind.as_str())
    .bind(&lease.actor_id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn refresh_project_processing_lease(
    pool: &PgPool,
    lease: &ProjectProcessingLease,
) -> Result<(), StoreError> {
    sqlx::query(
        r#"
        UPDATE project_processing_leases
        SET heartbeat_at = now()
        WHERE project_id = $1
          AND actor_kind = $2
          AND actor_id = $3
        "#,
    )
    .bind(lease.project_id)
    .bind(lease.actor_kind.as_str())
    .bind(&lease.actor_id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn count_project_processing_leases_before_epoch(
    pool: &PgPool,
    project_id: Uuid,
    fence_epoch: i64,
) -> Result<i64, StoreError> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM project_processing_leases
        WHERE project_id = $1
          AND fence_epoch < $2
          AND heartbeat_at >= now() - ($3 * interval '1 second')
        "#,
    )
    .bind(project_id)
    .bind(fence_epoch)
    .bind(PROJECT_PROCESSING_LEASE_STALE_AFTER_SECS)
    .fetch_one(pool)
    .await
    .map_err(StoreError::from)
}

pub async fn rename_project(
    pool: &PgPool,
    project_id: Uuid,
    name: &str,
) -> Result<Option<ProjectRecord>, StoreError> {
    let actor_id = Uuid::new_v4().to_string();
    let lease = claim_project_processing_lease(
        pool,
        project_id,
        ProjectProcessingActorKind::ProjectPatch,
        &actor_id,
    )
    .await?;

    let result = async {
        let mut tx = pool.begin().await?;
        let row = sqlx::query(
            r#"
            UPDATE projects
            SET name = $2
            WHERE id = $1
            RETURNING id, name, state, fence_epoch, created_at
            "#,
        )
        .bind(project_id)
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?;
        verify_project_processing_lease_in_tx(&mut tx, &lease).await?;
        tx.commit().await?;

        row.as_ref().map(project_record_from_row).transpose()
    }
    .await;
    let release_result = release_project_processing_lease(pool, &lease).await;

    match (result, release_result) {
        (Ok(project), Ok(())) => Ok(project),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), _) => Err(error),
    }
}

pub async fn create_api_key(
    pool: &PgPool,
    project_id: Uuid,
    name: &str,
    kind: ApiKeyKind,
    secret: &str,
) -> Result<Option<ApiKeyRecord>, StoreError> {
    let actor_id = Uuid::new_v4().to_string();
    let lease = claim_project_processing_lease(
        pool,
        project_id,
        ProjectProcessingActorKind::ApiKeyCreate,
        &actor_id,
    )
    .await?;

    let result = async {
        let mut tx = pool.begin().await?;
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
        .fetch_optional(&mut *tx)
        .await?;
        verify_project_processing_lease_in_tx(&mut tx, &lease).await?;
        tx.commit().await?;

        row.as_ref().map(api_key_record_from_row).transpose()
    }
    .await;
    let release_result = release_project_processing_lease(pool, &lease).await;

    match (result, release_result) {
        (Ok(key), Ok(())) => Ok(key),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), _) => Err(error),
    }
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
    let actor_id = Uuid::new_v4().to_string();
    let lease = claim_project_processing_lease(
        pool,
        project_id,
        ProjectProcessingActorKind::ApiKeyRevoke,
        &actor_id,
    )
    .await?;

    let result = async {
        let mut tx = pool.begin().await?;
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
        .fetch_optional(&mut *tx)
        .await?;
        verify_project_processing_lease_in_tx(&mut tx, &lease).await?;
        tx.commit().await?;

        Ok(revoked.is_some())
    }
    .await;
    let release_result = release_project_processing_lease(pool, &lease).await;

    match (result, release_result) {
        (Ok(revoked), Ok(())) => Ok(revoked),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), _) => Err(error),
    }
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

pub async fn authenticate_ingest_request(
    pool: &PgPool,
    ingest_key: &str,
) -> Result<AuthenticatedIngestProject, StoreError> {
    let row = sqlx::query(
        r#"
        SELECT api_keys.project_id, api_keys.kind, projects.state
        FROM api_keys
        INNER JOIN projects
          ON projects.id = api_keys.project_id
        WHERE api_keys.key_hash = $1
          AND api_keys.revoked_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(hash_ingest_key(ingest_key))
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Err(StoreError::ProjectNotFound);
    };

    let kind = api_key_kind_from_str(row.try_get::<&str, _>("kind")?)?;
    if kind != ApiKeyKind::Ingest {
        return Err(StoreError::ProjectNotFound);
    }

    let state = project_state_from_str(row.try_get::<&str, _>("state")?)?;
    if state != ProjectState::Active {
        return Err(StoreError::ProjectNotActive(state));
    }

    Ok(AuthenticatedIngestProject {
        project_id: row.try_get("project_id")?,
    })
}

pub async fn insert_events_with_authenticated_ingest(
    pool: &PgPool,
    authenticated: &AuthenticatedIngestProject,
    events: &[EventPayload],
) -> Result<u64, StoreError> {
    let actor_id = Uuid::new_v4().to_string();
    let lease = claim_project_processing_lease(
        pool,
        authenticated.project_id,
        ProjectProcessingActorKind::Ingest,
        &actor_id,
    )
    .await?;

    let result = async {
        let mut tx = pool.begin().await?;

        let mut builder = QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO events_raw (project_id, event_name, timestamp, install_id, platform, app_version, os_version, locale) ",
        );

        builder.push_values(events, |mut separated, event| {
            separated.push_bind(authenticated.project_id);
            separated.push_bind(&event.event);
            separated.push_bind(event.timestamp);
            separated.push_bind(&event.install_id);
            separated.push_bind(match event.platform {
                fantasma_core::Platform::Ios => "ios",
                fantasma_core::Platform::Android => "android",
            });
            separated.push_bind(&event.app_version);
            separated.push_bind(&event.os_version);
            separated.push_bind(&event.locale);
        });

        let inserted = builder.build().execute(&mut *tx).await?;
        verify_project_processing_lease_in_tx(&mut tx, &lease).await?;
        tx.commit().await?;

        Ok::<_, StoreError>(inserted.rows_affected())
    }
    .await;
    let release_result = release_project_processing_lease(pool, &lease).await;

    match (result, release_result) {
        (Ok(inserted), Ok(())) => Ok(inserted),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), _) => Err(error),
    }
}

pub async fn insert_events(
    pool: &PgPool,
    project_id: Uuid,
    events: &[EventPayload],
) -> Result<u64, StoreError> {
    insert_events_with_authenticated_ingest(
        pool,
        &AuthenticatedIngestProject { project_id },
        events,
    )
    .await
}

pub async fn create_project_deletion_job(
    pool: &PgPool,
    project_id: Uuid,
    kind: ProjectDeletionKind,
    scope: Option<&Value>,
) -> Result<Option<ProjectDeletionRecord>, StoreError> {
    let row = sqlx::query(
        r#"
        INSERT INTO project_deletions (
            id,
            project_id,
            project_name,
            kind,
            status,
            scope
        )
        SELECT
            $1,
            projects.id,
            projects.name,
            $2,
            'queued',
            $3
        FROM projects
        WHERE projects.id = $4
        RETURNING
            id,
            project_id,
            project_name,
            kind,
            status,
            scope,
            created_at,
            started_at,
            finished_at,
            error_code,
            error_message,
            deleted_raw_events,
            deleted_sessions,
            rebuilt_installs,
            rebuilt_days,
            rebuilt_hours
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(kind.as_str())
    .bind(scope.cloned().map(sqlx::types::Json))
    .bind(project_id)
    .fetch_optional(pool)
    .await?;

    row.as_ref()
        .map(project_deletion_record_from_row)
        .transpose()
}

pub async fn enqueue_project_purge(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<StartProjectDeletionResult, StoreError> {
    let mut tx = pool.begin().await?;

    let project = sqlx::query(
        r#"
        SELECT id, name, state, fence_epoch, created_at
        FROM projects
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(project_id)
    .fetch_optional(&mut *tx)
    .await?;

    let outcome = match project.as_ref().map(project_record_from_row).transpose()? {
        Some(project) => match project.state {
            ProjectState::Active => {
                let updated = sqlx::query(
                    r#"
                    UPDATE projects
                    SET state = 'pending_deletion',
                        fence_epoch = fence_epoch + 1
                    WHERE id = $1
                    RETURNING id, name, state, fence_epoch, created_at
                    "#,
                )
                .bind(project_id)
                .fetch_one(&mut *tx)
                .await?;
                let updated = project_record_from_row(&updated)?;
                let job = insert_project_deletion_job_in_tx(
                    &mut tx,
                    updated.id,
                    &updated.name,
                    ProjectDeletionKind::ProjectPurge,
                    None,
                )
                .await?;
                StartProjectDeletionResult::Enqueued(Box::new(job))
            }
            ProjectState::RangeDeleting => StartProjectDeletionResult::ProjectBusy,
            ProjectState::PendingDeletion => {
                let latest = load_latest_project_deletion_job_in_tx(
                    &mut tx,
                    project_id,
                    ProjectDeletionKind::ProjectPurge,
                )
                .await?;
                match latest {
                    Some(job)
                        if matches!(
                            job.status,
                            ProjectDeletionStatus::Queued | ProjectDeletionStatus::Running
                        ) =>
                    {
                        StartProjectDeletionResult::Enqueued(Box::new(job))
                    }
                    Some(job) if job.status == ProjectDeletionStatus::Failed => {
                        let retried = insert_project_deletion_job_in_tx(
                            &mut tx,
                            project.id,
                            &project.name,
                            ProjectDeletionKind::ProjectPurge,
                            None,
                        )
                        .await?;
                        StartProjectDeletionResult::Enqueued(Box::new(retried))
                    }
                    Some(job) => StartProjectDeletionResult::Enqueued(Box::new(job)),
                    None => {
                        let queued = insert_project_deletion_job_in_tx(
                            &mut tx,
                            project.id,
                            &project.name,
                            ProjectDeletionKind::ProjectPurge,
                            None,
                        )
                        .await?;
                        StartProjectDeletionResult::Enqueued(Box::new(queued))
                    }
                }
            }
        },
        None => {
            let latest = load_latest_successful_project_deletion(
                pool,
                project_id,
                ProjectDeletionKind::ProjectPurge,
            )
            .await?;
            match latest {
                Some(job) => StartProjectDeletionResult::Enqueued(Box::new(job)),
                None => StartProjectDeletionResult::NotFound,
            }
        }
    };

    tx.commit().await?;
    Ok(outcome)
}

pub async fn enqueue_range_deletion(
    pool: &PgPool,
    project_id: Uuid,
    scope: &Value,
) -> Result<StartProjectDeletionResult, StoreError> {
    let mut tx = pool.begin().await?;

    let project = sqlx::query(
        r#"
        SELECT id, name, state, fence_epoch, created_at
        FROM projects
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(project_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(project) = project.as_ref().map(project_record_from_row).transpose()? else {
        tx.commit().await?;
        return Ok(StartProjectDeletionResult::NotFound);
    };

    let outcome = match project.state {
        ProjectState::Active => {
            let updated = sqlx::query(
                r#"
                UPDATE projects
                SET state = 'range_deleting',
                    fence_epoch = fence_epoch + 1
                WHERE id = $1
                RETURNING id, name, state, fence_epoch, created_at
                "#,
            )
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await?;
            let updated = project_record_from_row(&updated)?;
            let job = insert_project_deletion_job_in_tx(
                &mut tx,
                updated.id,
                &updated.name,
                ProjectDeletionKind::RangeDelete,
                Some(scope),
            )
            .await?;
            StartProjectDeletionResult::Enqueued(Box::new(job))
        }
        ProjectState::RangeDeleting => StartProjectDeletionResult::ProjectBusy,
        ProjectState::PendingDeletion => StartProjectDeletionResult::ProjectPendingDeletion,
    };

    tx.commit().await?;
    Ok(outcome)
}

pub async fn list_project_deletion_jobs(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<Option<Vec<ProjectDeletionRecord>>, StoreError> {
    let project_exists = load_project(pool, project_id).await?.is_some();
    let rows = sqlx::query(
        r#"
        SELECT
            id,
            project_id,
            project_name,
            kind,
            status,
            scope,
            created_at,
            started_at,
            finished_at,
            error_code,
            error_message,
            deleted_raw_events,
            deleted_sessions,
            rebuilt_installs,
            rebuilt_days,
            rebuilt_hours
        FROM project_deletions
        WHERE project_id = $1
        ORDER BY created_at DESC, id DESC
        "#,
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;

    if !project_exists && rows.is_empty() {
        return Ok(None);
    }

    rows.iter()
        .map(project_deletion_record_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub async fn load_project_deletion_job(
    pool: &PgPool,
    project_id: Uuid,
    deletion_id: Uuid,
) -> Result<Option<ProjectDeletionRecord>, StoreError> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            project_id,
            project_name,
            kind,
            status,
            scope,
            created_at,
            started_at,
            finished_at,
            error_code,
            error_message,
            deleted_raw_events,
            deleted_sessions,
            rebuilt_installs,
            rebuilt_days,
            rebuilt_hours
        FROM project_deletions
        WHERE project_id = $1
          AND id = $2
        "#,
    )
    .bind(project_id)
    .bind(deletion_id)
    .fetch_optional(pool)
    .await?;

    row.as_ref()
        .map(project_deletion_record_from_row)
        .transpose()
}

async fn insert_project_deletion_job_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    project_name: &str,
    kind: ProjectDeletionKind,
    scope: Option<&Value>,
) -> Result<ProjectDeletionRecord, StoreError> {
    let row = sqlx::query(
        r#"
        INSERT INTO project_deletions (
            id,
            project_id,
            project_name,
            kind,
            status,
            scope
        )
        VALUES ($1, $2, $3, $4, 'queued', $5)
        RETURNING
            id,
            project_id,
            project_name,
            kind,
            status,
            scope,
            created_at,
            started_at,
            finished_at,
            error_code,
            error_message,
            deleted_raw_events,
            deleted_sessions,
            rebuilt_installs,
            rebuilt_days,
            rebuilt_hours
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(project_id)
    .bind(project_name)
    .bind(kind.as_str())
    .bind(scope.cloned().map(sqlx::types::Json))
    .fetch_one(&mut **tx)
    .await?;

    project_deletion_record_from_row(&row)
}

async fn load_latest_project_deletion_job_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    kind: ProjectDeletionKind,
) -> Result<Option<ProjectDeletionRecord>, StoreError> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            project_id,
            project_name,
            kind,
            status,
            scope,
            created_at,
            started_at,
            finished_at,
            error_code,
            error_message,
            deleted_raw_events,
            deleted_sessions,
            rebuilt_installs,
            rebuilt_days,
            rebuilt_hours
        FROM project_deletions
        WHERE project_id = $1
          AND kind = $2
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        "#,
    )
    .bind(project_id)
    .bind(kind.as_str())
    .fetch_optional(&mut **tx)
    .await?;

    row.as_ref()
        .map(project_deletion_record_from_row)
        .transpose()
}

pub async fn load_latest_successful_project_deletion(
    pool: &PgPool,
    project_id: Uuid,
    kind: ProjectDeletionKind,
) -> Result<Option<ProjectDeletionRecord>, StoreError> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            project_id,
            project_name,
            kind,
            status,
            scope,
            created_at,
            started_at,
            finished_at,
            error_code,
            error_message,
            deleted_raw_events,
            deleted_sessions,
            rebuilt_installs,
            rebuilt_days,
            rebuilt_hours
        FROM project_deletions
        WHERE project_id = $1
          AND kind = $2
          AND status = 'succeeded'
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        "#,
    )
    .bind(project_id)
    .bind(kind.as_str())
    .fetch_optional(pool)
    .await?;

    row.as_ref()
        .map(project_deletion_record_from_row)
        .transpose()
}

pub async fn claim_next_project_deletion_job_in_tx(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Option<ProjectDeletionRecord>, StoreError> {
    if let Some(row) = sqlx::query(
        r#"
        SELECT
            deletions.id,
            deletions.project_id,
            deletions.project_name,
            deletions.kind,
            deletions.status,
            deletions.scope,
            deletions.created_at,
            deletions.started_at,
            deletions.finished_at,
            deletions.error_code,
            deletions.error_message,
            deletions.deleted_raw_events,
            deletions.deleted_sessions,
            deletions.rebuilt_installs,
            deletions.rebuilt_days,
            deletions.rebuilt_hours
        FROM project_deletions AS deletions
        LEFT JOIN projects ON projects.id = deletions.project_id
        WHERE deletions.status = 'running'
          AND (
                projects.id IS NULL
                OR NOT EXISTS (
                    SELECT 1
                    FROM project_processing_leases AS leases
                    WHERE leases.project_id = deletions.project_id
                      AND leases.fence_epoch < projects.fence_epoch
                      AND leases.heartbeat_at >= now() - ($1 * interval '1 second')
                )
              )
        ORDER BY deletions.started_at ASC NULLS FIRST, deletions.created_at ASC, deletions.id ASC
        LIMIT 1
        FOR UPDATE OF deletions SKIP LOCKED
        "#,
    )
    .bind(PROJECT_PROCESSING_LEASE_STALE_AFTER_SECS)
    .fetch_optional(&mut **tx)
    .await?
    {
        return project_deletion_record_from_row(&row).map(Some);
    }

    let row = sqlx::query(
        r#"
        WITH claimable AS (
            SELECT deletions.id
            FROM project_deletions AS deletions
            LEFT JOIN projects ON projects.id = deletions.project_id
            WHERE deletions.status = 'queued'
              AND (
                    projects.id IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                        FROM project_processing_leases AS leases
                        WHERE leases.project_id = deletions.project_id
                          AND leases.fence_epoch < projects.fence_epoch
                          AND leases.heartbeat_at >= now() - ($1 * interval '1 second')
                    )
                  )
            ORDER BY deletions.created_at ASC, deletions.id ASC
            LIMIT 1
            FOR UPDATE OF deletions SKIP LOCKED
        )
        UPDATE project_deletions AS deletions
        SET status = 'running',
            started_at = COALESCE(deletions.started_at, now()),
            finished_at = NULL,
            error_code = NULL,
            error_message = NULL
        FROM claimable
        WHERE deletions.id = claimable.id
        RETURNING
            deletions.id,
            deletions.project_id,
            deletions.project_name,
            deletions.kind,
            deletions.status,
            deletions.scope,
            deletions.created_at,
            deletions.started_at,
            deletions.finished_at,
            deletions.error_code,
            deletions.error_message,
            deletions.deleted_raw_events,
            deletions.deleted_sessions,
            deletions.rebuilt_installs,
            deletions.rebuilt_days,
            deletions.rebuilt_hours
        "#,
    )
    .bind(PROJECT_PROCESSING_LEASE_STALE_AFTER_SECS)
    .fetch_optional(&mut **tx)
    .await?;

    row.as_ref()
        .map(project_deletion_record_from_row)
        .transpose()
}

pub async fn load_project_deletion_job_by_id(
    pool: &PgPool,
    deletion_id: Uuid,
) -> Result<Option<ProjectDeletionRecord>, StoreError> {
    let row = sqlx::query(
        r#"
        SELECT
            id,
            project_id,
            project_name,
            kind,
            status,
            scope,
            created_at,
            started_at,
            finished_at,
            error_code,
            error_message,
            deleted_raw_events,
            deleted_sessions,
            rebuilt_installs,
            rebuilt_days,
            rebuilt_hours
        FROM project_deletions
        WHERE id = $1
        "#,
    )
    .bind(deletion_id)
    .fetch_optional(pool)
    .await?;

    row.as_ref()
        .map(project_deletion_record_from_row)
        .transpose()
}

pub async fn mark_project_deletion_job_succeeded_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deletion_id: Uuid,
    stats: ProjectDeletionStats,
) -> Result<(), StoreError> {
    sqlx::query(
        r#"
        UPDATE project_deletions
        SET status = 'succeeded',
            finished_at = now(),
            error_code = NULL,
            error_message = NULL,
            deleted_raw_events = $2,
            deleted_sessions = $3,
            rebuilt_installs = $4,
            rebuilt_days = $5,
            rebuilt_hours = $6
        WHERE id = $1
        "#,
    )
    .bind(deletion_id)
    .bind(stats.deleted_raw_events)
    .bind(stats.deleted_sessions)
    .bind(stats.rebuilt_installs)
    .bind(stats.rebuilt_days)
    .bind(stats.rebuilt_hours)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn mark_project_deletion_job_failed(
    pool: &PgPool,
    deletion_id: Uuid,
    error_code: &str,
    error_message: &str,
) -> Result<(), StoreError> {
    let mut tx = pool.begin().await?;
    mark_project_deletion_job_failed_in_tx(&mut tx, deletion_id, error_code, error_message).await?;
    tx.commit().await?;

    Ok(())
}

pub async fn mark_project_deletion_job_failed_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deletion_id: Uuid,
    error_code: &str,
    error_message: &str,
) -> Result<(), StoreError> {
    sqlx::query(
        r#"
        UPDATE project_deletions
        SET status = 'failed',
            finished_at = now(),
            error_code = $2,
            error_message = $3
        WHERE id = $1
        "#,
    )
    .bind(deletion_id)
    .bind(error_code)
    .bind(error_message)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn set_project_state_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    state: ProjectState,
) -> Result<(), StoreError> {
    sqlx::query(
        r#"
        UPDATE projects
        SET state = $2
        WHERE id = $1
        "#,
    )
    .bind(project_id)
    .bind(state.as_str())
    .execute(&mut **tx)
    .await?;

    Ok(())
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

pub async fn upsert_event_metric_buckets_dim3_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[EventMetricBucketDim3Delta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    for chunk in deltas.chunks(max_rows_per_batched_statement(11)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO event_metric_buckets_dim3 \
             (project_id, granularity, bucket_start, event_name, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, event_count) ",
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
            separated.push_bind(&delta.dim3_key);
            separated.push_bind(&delta.dim3_value);
            separated.push_bind(delta.event_count);
        });
        builder.push(
            " ON CONFLICT \
              (project_id, granularity, bucket_start, event_name, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value) \
              DO UPDATE SET \
              event_count = event_metric_buckets_dim3.event_count + EXCLUDED.event_count, \
              updated_at = now()",
        );

        builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub async fn upsert_event_metric_buckets_dim4_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[EventMetricBucketDim4Delta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    for chunk in deltas.chunks(max_rows_per_batched_statement(13)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO event_metric_buckets_dim4 \
             (project_id, granularity, bucket_start, event_name, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, dim4_key, dim4_value, event_count) ",
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
            separated.push_bind(&delta.dim3_key);
            separated.push_bind(&delta.dim3_value);
            separated.push_bind(&delta.dim4_key);
            separated.push_bind(&delta.dim4_value);
            separated.push_bind(delta.event_count);
        });
        builder.push(
            " ON CONFLICT \
              (project_id, granularity, bucket_start, event_name, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, dim4_key, dim4_value) \
              DO UPDATE SET \
              event_count = event_metric_buckets_dim4.event_count + EXCLUDED.event_count, \
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

pub async fn upsert_session_metric_dim3_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[SessionMetricDim3Delta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    for chunk in deltas.chunks(max_rows_per_batched_statement(12)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO session_metric_buckets_dim3 \
             (project_id, granularity, bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, session_count, duration_total_seconds, new_installs) ",
        );
        builder.push_values(chunk, |mut separated, delta| {
            separated.push_bind(delta.project_id);
            separated.push_bind(delta.granularity.as_str());
            separated.push_bind(delta.bucket_start);
            separated.push_bind(&delta.dim1_key);
            separated.push_bind(&delta.dim1_value);
            separated.push_bind(&delta.dim2_key);
            separated.push_bind(&delta.dim2_value);
            separated.push_bind(&delta.dim3_key);
            separated.push_bind(&delta.dim3_value);
            separated.push_bind(delta.session_count);
            separated.push_bind(delta.duration_total_seconds);
            separated.push_bind(delta.new_installs);
        });
        builder.push(
            " ON CONFLICT \
              (project_id, granularity, bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value) \
              DO UPDATE SET \
              session_count = session_metric_buckets_dim3.session_count + EXCLUDED.session_count, \
              duration_total_seconds = session_metric_buckets_dim3.duration_total_seconds + EXCLUDED.duration_total_seconds, \
              new_installs = session_metric_buckets_dim3.new_installs + EXCLUDED.new_installs, \
              updated_at = now()",
        );

        builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub async fn upsert_session_metric_dim4_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[SessionMetricDim4Delta],
) -> Result<(), StoreError> {
    if deltas.is_empty() {
        return Ok(());
    }

    for chunk in deltas.chunks(max_rows_per_batched_statement(14)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO session_metric_buckets_dim4 \
             (project_id, granularity, bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, dim4_key, dim4_value, session_count, duration_total_seconds, new_installs) ",
        );
        builder.push_values(chunk, |mut separated, delta| {
            separated.push_bind(delta.project_id);
            separated.push_bind(delta.granularity.as_str());
            separated.push_bind(delta.bucket_start);
            separated.push_bind(&delta.dim1_key);
            separated.push_bind(&delta.dim1_value);
            separated.push_bind(&delta.dim2_key);
            separated.push_bind(&delta.dim2_value);
            separated.push_bind(&delta.dim3_key);
            separated.push_bind(&delta.dim3_value);
            separated.push_bind(&delta.dim4_key);
            separated.push_bind(&delta.dim4_value);
            separated.push_bind(delta.session_count);
            separated.push_bind(delta.duration_total_seconds);
            separated.push_bind(delta.new_installs);
        });
        builder.push(
            " ON CONFLICT \
              (project_id, granularity, bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, dim4_key, dim4_value) \
              DO UPDATE SET \
              session_count = session_metric_buckets_dim4.session_count + EXCLUDED.session_count, \
              duration_total_seconds = session_metric_buckets_dim4.duration_total_seconds + EXCLUDED.duration_total_seconds, \
              new_installs = session_metric_buckets_dim4.new_installs + EXCLUDED.new_installs, \
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
            locale
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
    .bind(&record.locale)
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected() == 1)
}

pub async fn insert_install_first_seen_many_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    records: &[InstallFirstSeenRecord],
) -> Result<Vec<String>, StoreError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let mut inserted_install_ids = Vec::new();
    let mut unique_records = Vec::with_capacity(records.len());
    let mut seen = BTreeSet::new();
    for record in records {
        if seen.insert((record.project_id, record.install_id.as_str())) {
            unique_records.push(record);
        }
    }

    for chunk in unique_records.chunks(max_rows_per_batched_statement(8)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO install_first_seen \
             (project_id, install_id, first_seen_event_id, first_seen_at, platform, app_version, os_version, locale) ",
        );
        builder.push_values(chunk, |mut separated, record| {
            separated
                .push_bind(record.project_id)
                .push_bind(&record.install_id)
                .push_bind(record.first_seen_event_id)
                .push_bind(record.first_seen_at)
                .push_bind(platform_as_str(&record.platform))
                .push_bind(&record.app_version)
                .push_bind(&record.os_version)
                .push_bind(&record.locale);
        });
        builder.push(
            " ON CONFLICT (project_id, install_id) DO NOTHING \
              RETURNING install_id",
        );

        let rows = builder.build().fetch_all(&mut **tx).await?;
        inserted_install_ids.extend(
            rows.into_iter()
                .map(|row| row.try_get("install_id"))
                .collect::<Result<Vec<String>, sqlx::Error>>()?,
        );
    }

    Ok(inserted_install_ids)
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
        3 => {
            fetch_event_metrics_dim3_rows(
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
        4 => {
            fetch_event_metrics_dim4_rows(
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
            "event metrics cube arity must be between 0 and 4, got {other}"
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
        3 => {
            fetch_event_metrics_dim3_aggregate_rows(
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
        4 => {
            fetch_event_metrics_dim4_aggregate_rows(
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
            "event metrics cube arity must be between 0 and 4, got {other}"
        ))),
    }
}

pub async fn fetch_total_event_metrics_cube_rows_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    granularity: MetricGranularity,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError> {
    fetch_total_event_metrics_cube_rows_in_executor(
        &mut **tx,
        project_id,
        granularity,
        start,
        end,
        cube_keys,
        filters,
    )
    .await
}

async fn fetch_total_event_metrics_cube_rows_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
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
            fetch_total_event_metric_total_rows(executor, project_id, granularity, start, end).await
        }
        1 => {
            fetch_total_event_metric_dim1_rows(
                executor,
                project_id,
                granularity,
                start,
                end,
                cube_keys,
                filters,
            )
            .await
        }
        2 => {
            fetch_total_event_metric_dim2_rows(
                executor,
                project_id,
                granularity,
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

pub async fn fetch_total_event_metrics_aggregate_cube_rows_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    granularity: MetricGranularity,
    window: (DateTime<Utc>, DateTime<Utc>),
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
    limit: Option<usize>,
) -> Result<Vec<EventMetricsAggregateCubeRow>, StoreError> {
    fetch_total_event_metrics_aggregate_cube_rows_in_executor(
        &mut **tx,
        project_id,
        granularity,
        window,
        cube_keys,
        filters,
        limit,
    )
    .await
}

async fn fetch_total_event_metrics_aggregate_cube_rows_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
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
            fetch_total_event_metric_total_aggregate_rows(
                executor,
                project_id,
                granularity,
                (start, end),
            )
            .await
        }
        1 => {
            fetch_total_event_metric_dim1_aggregate_rows(
                executor,
                project_id,
                granularity,
                (start, end),
                cube_keys,
                filters,
                limit,
            )
            .await
        }
        2 => {
            fetch_total_event_metric_dim2_aggregate_rows(
                executor,
                project_id,
                granularity,
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
        3 => {
            fetch_session_metrics_dim3_rows(
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
        4 => {
            fetch_session_metrics_dim4_rows(
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
            "session metrics cube arity must be between 0 and 4, got {other}"
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
        3 => {
            fetch_session_metrics_dim3_aggregate_rows(
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
        4 => {
            fetch_session_metrics_dim4_aggregate_rows(
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
            "session metrics cube arity must be between 0 and 4, got {other}"
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
        SELECT id, project_id, event_name, timestamp, received_at, install_id, platform, app_version, os_version, locale
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

pub async fn fetch_all_events_for_install_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
) -> Result<Vec<RawEventRecord>, StoreError> {
    let rows = sqlx::query(
        r#"
        SELECT id, project_id, event_name, timestamp, received_at, install_id, platform, app_version, os_version, locale
        FROM events_raw
        WHERE project_id = $1
          AND install_id = $2
        ORDER BY timestamp ASC, id ASC
        "#,
    )
    .bind(project_id)
    .bind(install_id)
    .fetch_all(&mut **tx)
    .await?;

    rows.into_iter().map(raw_event_from_row).collect()
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
        SELECT id, project_id, event_name, timestamp, received_at, install_id, platform, app_version, os_version, locale
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
    limit: i64,
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
    query.push(" GROUP BY event_name ORDER BY MAX(timestamp) DESC, event_name ASC LIMIT ");
    query.push_bind(limit);

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
        MetricGranularity::Week => "date_trunc('week', timestamp)",
        MetricGranularity::Month => "date_trunc('month', timestamp)",
        MetricGranularity::Year => "date_trunc('year', timestamp)",
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

pub async fn list_usage_events(
    pool: &PgPool,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<UsageProjectEvents>, StoreError> {
    let start: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
        start
            .and_hms_opt(0, 0, 0)
            .expect("midnight is always valid for a NaiveDate"),
        Utc,
    );
    let end_exclusive: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
        end.succ_opt()
            .ok_or_else(|| StoreError::InvariantViolation("usage end date overflow".to_owned()))?
            .and_hms_opt(0, 0, 0)
            .expect("midnight is always valid for a NaiveDate"),
        Utc,
    );

    let rows = sqlx::query(
        r#"
        SELECT
            projects.id,
            projects.name,
            projects.created_at,
            COUNT(events_raw.id)::bigint AS events_processed
        FROM projects
        INNER JOIN events_raw
            ON events_raw.project_id = projects.id
        WHERE events_raw.received_at >= $1
          AND events_raw.received_at < $2
        GROUP BY projects.id, projects.name, projects.created_at
        ORDER BY events_processed DESC, projects.created_at ASC, projects.id ASC
        "#,
    )
    .bind(start)
    .bind(end_exclusive)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let events_processed: i64 =
                row.try_get("events_processed").map_err(StoreError::from)?;
            if events_processed < 0 {
                return Err(StoreError::InvariantViolation(
                    "usage event counts must not be negative".to_owned(),
                ));
            }

            Ok(UsageProjectEvents {
                project: ProjectSummary {
                    id: row.try_get("id").map_err(StoreError::from)?,
                    name: row.try_get("name").map_err(StoreError::from)?,
                    created_at: row.try_get("created_at").map_err(StoreError::from)?,
                },
                events_processed: events_processed as u64,
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()
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
        SELECT id, project_id, event_name, timestamp, received_at, install_id, platform, app_version, os_version, locale
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
               event_count, duration_seconds, platform, app_version, os_version, locale
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
               event_count, duration_seconds, platform, app_version, os_version, locale
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
            s.locale \
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

pub async fn upsert_install_session_states_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    states: &[InstallSessionStateRecord],
) -> Result<u64, StoreError> {
    if states.is_empty() {
        return Ok(0);
    }

    let mut affected_rows = 0;
    for chunk in states.chunks(max_rows_per_batched_statement(9)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO install_session_state \
             (project_id, install_id, tail_session_id, tail_session_start, tail_session_end, tail_event_count, tail_duration_seconds, tail_day, last_processed_event_id) ",
        );
        builder.push_values(chunk, |mut separated, state| {
            separated
                .push_bind(state.project_id)
                .push_bind(&state.install_id)
                .push_bind(&state.tail_session_id)
                .push_bind(state.tail_session_start)
                .push_bind(state.tail_session_end)
                .push_bind(state.tail_event_count)
                .push_bind(state.tail_duration_seconds)
                .push_bind(state.tail_day)
                .push_bind(state.last_processed_event_id);
        });
        builder.push(
            " ON CONFLICT (project_id, install_id) DO UPDATE SET \
              tail_session_id = EXCLUDED.tail_session_id, \
              tail_session_start = EXCLUDED.tail_session_start, \
              tail_session_end = EXCLUDED.tail_session_end, \
              tail_event_count = EXCLUDED.tail_event_count, \
              tail_duration_seconds = EXCLUDED.tail_duration_seconds, \
              tail_day = EXCLUDED.tail_day, \
              last_processed_event_id = EXCLUDED.last_processed_event_id, \
              updated_at = now()",
        );

        affected_rows += builder.build().execute(&mut **tx).await?.rows_affected();
    }

    Ok(affected_rows)
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
        "week" => Ok(MetricGranularity::Week),
        "month" => Ok(MetricGranularity::Month),
        "year" => Ok(MetricGranularity::Year),
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
            locale
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
    .bind(&session.locale)
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

    let mut affected_rows = 0;
    for chunk in sessions.chunks(max_rows_per_batched_statement(11)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO sessions \
             (project_id, install_id, session_id, session_start, session_end, event_count, duration_seconds, platform, app_version, os_version, locale) ",
        );
        builder.push_values(chunk, |mut separated, session| {
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
                .push_bind(&session.locale);
        });
        affected_rows += builder.build().execute(&mut **tx).await?.rows_affected();
    }

    Ok(affected_rows)
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

pub async fn update_session_tails_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    updates: &[SessionTailUpdate<'_>],
) -> Result<u64, StoreError> {
    if updates.is_empty() {
        return Ok(0);
    }

    let mut affected_rows = 0;
    for chunk in updates.chunks(max_rows_per_batched_statement(5)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "WITH input(project_id, session_id, session_end, event_count, duration_seconds) AS (",
        );
        builder.push_values(chunk, |mut row, update| {
            row.push_bind(update.project_id)
                .push_bind(update.session_id)
                .push_bind(update.session_end)
                .push_bind(update.event_count)
                .push_bind(update.duration_seconds);
        });
        builder.push(
            ") UPDATE sessions AS s \
             SET session_end = input.session_end, \
                 event_count = input.event_count, \
                 duration_seconds = input.duration_seconds \
             FROM input \
             WHERE s.project_id = input.project_id \
               AND s.session_id = input.session_id",
        );

        affected_rows += builder.build().execute(&mut **tx).await?.rows_affected();
    }

    Ok(affected_rows)
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

    let mut active_by_day = BTreeMap::<(Uuid, NaiveDate), i64>::new();
    for chunk in deltas.chunks(max_rows_per_batched_statement(4)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO session_daily_installs (project_id, day, install_id, session_count) ",
        );
        builder.push_values(chunk, |mut separated, delta| {
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
        for row in rows {
            let project_id: Uuid = row.try_get("project_id")?;
            let day: NaiveDate = row.try_get("day")?;
            let active_install_delta: i64 = row.try_get("active_install_delta")?;
            *active_by_day.entry((project_id, day)).or_default() += active_install_delta;
        }
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
    for chunk in deltas.chunks(max_rows_per_batched_statement(10)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO session_active_install_slices \
             (project_id, day, install_id, platform, app_version, app_version_is_null, os_version, os_version_is_null, locale, locale_is_null) ",
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
                .push_bind(delta.locale.as_deref().unwrap_or(""))
                .push_bind(delta.locale_is_null);
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
                locale
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
            locale,
            locale_is_null
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
            COALESCE(locale, ''),
            locale IS NULL
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

    let mut affected_rows = 0;
    for chunk in deltas.chunks(max_rows_per_batched_statement(5)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO session_daily \
             (project_id, day, sessions_count, active_installs, total_duration_seconds) ",
        );
        builder.push_values(chunk, |mut separated, delta| {
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

        affected_rows += builder.build().execute(&mut **tx).await?.rows_affected();
    }

    Ok(affected_rows)
}

pub async fn add_session_daily_duration_deltas_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    deltas: &[SessionDailyDurationDelta],
) -> Result<u64, StoreError> {
    if deltas.is_empty() {
        return Ok(0);
    }

    let mut affected_rows = 0;
    for chunk in deltas.chunks(max_rows_per_batched_statement(3)) {
        let mut builder =
            QueryBuilder::<Postgres>::new("WITH input(project_id, day, duration_delta) AS (");
        builder.push_values(chunk, |mut row, delta| {
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

        affected_rows += builder.build().execute(&mut **tx).await?.rows_affected();
    }

    Ok(affected_rows)
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

pub async fn upsert_live_install_state_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    upserts: &[LiveInstallStateUpsert],
) -> Result<(), StoreError> {
    if upserts.is_empty() {
        return Ok(());
    }

    for chunk in upserts.chunks(max_rows_per_batched_statement(3)) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO live_install_state (project_id, install_id, last_received_at) ",
        );
        builder.push_values(chunk, |mut separated, upsert| {
            separated.push_bind(upsert.project_id);
            separated.push_bind(&upsert.install_id);
            separated.push_bind(upsert.last_received_at);
        });
        builder.push(
            " ON CONFLICT (project_id, install_id) DO UPDATE SET \
              last_received_at = GREATEST(live_install_state.last_received_at, EXCLUDED.last_received_at)",
        );

        builder.build().execute(&mut **tx).await?;
    }

    Ok(())
}

pub async fn load_live_install_count_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    window_seconds: i64,
) -> Result<(DateTime<Utc>, u64), StoreError> {
    let row = sqlx::query(
        r#"
        WITH snapshot AS (
            SELECT statement_timestamp() AS as_of
        )
        SELECT
            snapshot.as_of AS as_of,
            COUNT(live.install_id)::BIGINT AS live_installs
        FROM snapshot
        LEFT JOIN live_install_state live
          ON live.project_id = $1
         AND live.last_received_at >= snapshot.as_of - ($2::BIGINT * INTERVAL '1 second')
        GROUP BY snapshot.as_of
        "#,
    )
    .bind(project_id)
    .bind(window_seconds)
    .fetch_one(&mut **tx)
    .await?;

    Ok((
        row.try_get("as_of")?,
        row.try_get::<i64, _>("live_installs")? as u64,
    ))
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

async fn fetch_event_metrics_dim3_rows<'a, E>(
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
        "SELECT bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, event_count \
         FROM event_metric_buckets_dim3 \
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
    for (index, key) in cube_keys.iter().enumerate() {
        builder.push(format!(" AND dim{}_key = ", index + 1));
        builder.push_bind(key);
    }
    for (index, key) in cube_keys.iter().enumerate() {
        if let Some(value) = filters.get(key) {
            builder.push(format!(" AND dim{}_value = ", index + 1));
            builder.push_bind(value);
        }
    }
    builder.push(" ORDER BY bucket_start ASC, dim1_value ASC, dim2_value ASC, dim3_value ASC");

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

async fn fetch_event_metrics_dim4_rows<'a, E>(
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
        "SELECT bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, dim4_key, dim4_value, event_count \
         FROM event_metric_buckets_dim4 \
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
    for (index, key) in cube_keys.iter().enumerate() {
        builder.push(format!(" AND dim{}_key = ", index + 1));
        builder.push_bind(key);
    }
    for (index, key) in cube_keys.iter().enumerate() {
        if let Some(value) = filters.get(key) {
            builder.push(format!(" AND dim{}_value = ", index + 1));
            builder.push_bind(value);
        }
    }
    builder.push(
        " ORDER BY bucket_start ASC, dim1_value ASC, dim2_value ASC, dim3_value ASC, dim4_value ASC",
    );

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
                (
                    row.try_get::<String, _>("dim3_key")
                        .expect("dim3_key column exists"),
                    row.try_get::<String, _>("dim3_value")
                        .expect("dim3_value column exists"),
                ),
                (
                    row.try_get::<String, _>("dim4_key")
                        .expect("dim4_key column exists"),
                    row.try_get::<String, _>("dim4_value")
                        .expect("dim4_value column exists"),
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
        "SELECT dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, SUM(event_count)::BIGINT AS event_count \
         FROM event_metric_buckets_dim3 \
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
    for (index, key) in cube_keys.iter().enumerate() {
        builder.push(format!(" AND dim{}_key = ", index + 1));
        builder.push_bind(key);
    }
    for (index, key) in cube_keys.iter().enumerate() {
        if let Some(value) = filters.get(key) {
            builder.push(format!(" AND dim{}_value = ", index + 1));
            builder.push_bind(value);
        }
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

async fn fetch_event_metrics_dim4_aggregate_rows<'a, E>(
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
        "SELECT dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, dim4_key, dim4_value, SUM(event_count)::BIGINT AS event_count \
         FROM event_metric_buckets_dim4 \
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
    for (index, key) in cube_keys.iter().enumerate() {
        builder.push(format!(" AND dim{}_key = ", index + 1));
        builder.push_bind(key);
    }
    for (index, key) in cube_keys.iter().enumerate() {
        if let Some(value) = filters.get(key) {
            builder.push(format!(" AND dim{}_value = ", index + 1));
            builder.push_bind(value);
        }
    }
    builder.push(
        " GROUP BY dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, dim4_key, dim4_value \
          ORDER BY dim1_value ASC, dim2_value ASC, dim3_value ASC, dim4_value ASC",
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
                (
                    row.try_get::<String, _>("dim4_key")
                        .expect("dim4_key column exists"),
                    row.try_get::<String, _>("dim4_value")
                        .expect("dim4_value column exists"),
                ),
            ]),
            event_count: row
                .try_get::<i64, _>("event_count")
                .expect("event_count column exists") as u64,
        })
        .collect())
}

async fn fetch_total_event_metric_total_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT bucket_start, SUM(event_count)::BIGINT AS event_count
        FROM event_metric_buckets_total
        WHERE project_id = $1
          AND granularity = $2
          AND bucket_start BETWEEN $3 AND $4
        GROUP BY bucket_start
        ORDER BY bucket_start ASC
        "#,
    )
    .bind(project_id)
    .bind(granularity.as_str())
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

async fn fetch_total_event_metric_total_aggregate_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
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
          AND bucket_start BETWEEN $3 AND $4
        "#,
    )
    .bind(project_id)
    .bind(granularity.as_str())
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

async fn fetch_total_event_metric_dim1_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT bucket_start, dim1_key, dim1_value, SUM(event_count)::BIGINT AS event_count \
         FROM event_metric_buckets_dim1 \
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

    builder.push(" GROUP BY bucket_start, dim1_key, dim1_value");
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

async fn fetch_total_event_metric_dim1_aggregate_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
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

async fn fetch_total_event_metric_dim2_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    cube_keys: &[String],
    filters: &BTreeMap<String, String>,
) -> Result<Vec<EventMetricsCubeRow>, StoreError>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, SUM(event_count)::BIGINT AS event_count \
         FROM event_metric_buckets_dim2 \
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

    builder.push(" GROUP BY bucket_start, dim1_key, dim1_value, dim2_key, dim2_value");
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

async fn fetch_total_event_metric_dim2_aggregate_rows<'a, E>(
    executor: E,
    project_id: Uuid,
    granularity: MetricGranularity,
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

async fn fetch_session_metrics_dim3_rows<'a, E>(
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
        "SELECT bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, ",
    );
    builder.push(session_metric_value_column(metric));
    builder.push(
        " AS value \
         FROM session_metric_buckets_dim3 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
    for (index, key) in cube_keys.iter().enumerate() {
        builder.push(format!(" AND dim{}_key = ", index + 1));
        builder.push_bind(key);
    }
    for (index, key) in cube_keys.iter().enumerate() {
        if let Some(value) = filters.get(key) {
            builder.push(format!(" AND dim{}_value = ", index + 1));
            builder.push_bind(value);
        }
    }
    builder.push(" ORDER BY bucket_start ASC, dim1_value ASC, dim2_value ASC, dim3_value ASC");

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
                (
                    row.try_get::<String, _>("dim3_key")
                        .expect("dim3_key column exists"),
                    row.try_get::<String, _>("dim3_value")
                        .expect("dim3_value column exists"),
                ),
            ]),
            value: row.try_get::<i64, _>("value").expect("value column exists") as u64,
        })
        .collect())
}

async fn fetch_session_metrics_dim4_rows<'a, E>(
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
        "SELECT bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, dim4_key, dim4_value, ",
    );
    builder.push(session_metric_value_column(metric));
    builder.push(
        " AS value \
         FROM session_metric_buckets_dim4 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
    for (index, key) in cube_keys.iter().enumerate() {
        builder.push(format!(" AND dim{}_key = ", index + 1));
        builder.push_bind(key);
    }
    for (index, key) in cube_keys.iter().enumerate() {
        if let Some(value) = filters.get(key) {
            builder.push(format!(" AND dim{}_value = ", index + 1));
            builder.push_bind(value);
        }
    }
    builder.push(
        " ORDER BY bucket_start ASC, dim1_value ASC, dim2_value ASC, dim3_value ASC, dim4_value ASC",
    );

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
                (
                    row.try_get::<String, _>("dim3_key")
                        .expect("dim3_key column exists"),
                    row.try_get::<String, _>("dim3_value")
                        .expect("dim3_value column exists"),
                ),
                (
                    row.try_get::<String, _>("dim4_key")
                        .expect("dim4_key column exists"),
                    row.try_get::<String, _>("dim4_value")
                        .expect("dim4_value column exists"),
                ),
            ]),
            value: row.try_get::<i64, _>("value").expect("value column exists") as u64,
        })
        .collect())
}

async fn fetch_session_metrics_dim3_aggregate_rows<'a, E>(
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
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, SUM(",
    );
    builder.push(session_metric_value_column(metric));
    builder.push(
        ")::BIGINT AS value \
         FROM session_metric_buckets_dim3 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
    for (index, key) in cube_keys.iter().enumerate() {
        builder.push(format!(" AND dim{}_key = ", index + 1));
        builder.push_bind(key);
    }
    for (index, key) in cube_keys.iter().enumerate() {
        if let Some(value) = filters.get(key) {
            builder.push(format!(" AND dim{}_value = ", index + 1));
            builder.push_bind(value);
        }
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
                (
                    row.try_get::<String, _>("dim3_key")
                        .expect("dim3_key column exists"),
                    row.try_get::<String, _>("dim3_value")
                        .expect("dim3_value column exists"),
                ),
            ]),
            value: row.try_get::<i64, _>("value").expect("value column exists") as u64,
        })
        .collect())
}

async fn fetch_session_metrics_dim4_aggregate_rows<'a, E>(
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
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, dim4_key, dim4_value, SUM(",
    );
    builder.push(session_metric_value_column(metric));
    builder.push(
        ")::BIGINT AS value \
         FROM session_metric_buckets_dim4 \
         WHERE project_id = ",
    );
    builder.push_bind(project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(start);
    builder.push(" AND ");
    builder.push_bind(end);
    for (index, key) in cube_keys.iter().enumerate() {
        builder.push(format!(" AND dim{}_key = ", index + 1));
        builder.push_bind(key);
    }
    for (index, key) in cube_keys.iter().enumerate() {
        if let Some(value) = filters.get(key) {
            builder.push(format!(" AND dim{}_value = ", index + 1));
            builder.push_bind(value);
        }
    }
    builder.push(
        " GROUP BY dim1_key, dim1_value, dim2_key, dim2_value, dim3_key, dim3_value, dim4_key, dim4_value \
          ORDER BY dim1_value ASC, dim2_value ASC, dim3_value ASC, dim4_value ASC",
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
                (
                    row.try_get::<String, _>("dim3_key")
                        .expect("dim3_key column exists"),
                    row.try_get::<String, _>("dim3_value")
                        .expect("dim3_value column exists"),
                ),
                (
                    row.try_get::<String, _>("dim4_key")
                        .expect("dim4_key column exists"),
                    row.try_get::<String, _>("dim4_value")
                        .expect("dim4_value column exists"),
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
        received_at: row.try_get("received_at")?,
        install_id: row.try_get("install_id")?,
        platform: platform_from_str(&row.try_get::<String, _>("platform")?)?,
        app_version: row.try_get("app_version")?,
        os_version: row.try_get("os_version")?,
        locale: row.try_get("locale")?,
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
        locale: row.try_get("locale")?,
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
            "platform" | "app_version" | "os_version" | "locale" => {
                query.push(" AND ");
                query.push(key.as_str());
                query.push(" = ");
                query.push_bind(value.clone());
            }
            _ => unreachable!("filters are validated before store reads"),
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

fn project_state_from_str(value: &str) -> Result<ProjectState, StoreError> {
    match value {
        "active" => Ok(ProjectState::Active),
        "range_deleting" => Ok(ProjectState::RangeDeleting),
        "pending_deletion" => Ok(ProjectState::PendingDeletion),
        other => Err(StoreError::InvariantViolation(format!(
            "invalid project state: {other}"
        ))),
    }
}

fn project_deletion_kind_from_str(value: &str) -> Result<ProjectDeletionKind, StoreError> {
    match value {
        "project_purge" => Ok(ProjectDeletionKind::ProjectPurge),
        "range_delete" => Ok(ProjectDeletionKind::RangeDelete),
        other => Err(StoreError::InvariantViolation(format!(
            "invalid project deletion kind: {other}"
        ))),
    }
}

fn project_deletion_status_from_str(value: &str) -> Result<ProjectDeletionStatus, StoreError> {
    match value {
        "queued" => Ok(ProjectDeletionStatus::Queued),
        "running" => Ok(ProjectDeletionStatus::Running),
        "succeeded" => Ok(ProjectDeletionStatus::Succeeded),
        "failed" => Ok(ProjectDeletionStatus::Failed),
        other => Err(StoreError::InvariantViolation(format!(
            "invalid project deletion status: {other}"
        ))),
    }
}

fn project_record_from_row(row: &sqlx::postgres::PgRow) -> Result<ProjectRecord, StoreError> {
    Ok(ProjectRecord {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        state: project_state_from_str(row.try_get("state")?)?,
        fence_epoch: row.try_get("fence_epoch")?,
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

fn project_deletion_record_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<ProjectDeletionRecord, StoreError> {
    Ok(ProjectDeletionRecord {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        project_name: row.try_get("project_name")?,
        kind: project_deletion_kind_from_str(row.try_get("kind")?)?,
        status: project_deletion_status_from_str(row.try_get("status")?)?,
        scope: row
            .try_get::<Option<sqlx::types::Json<Value>>, _>("scope")?
            .map(|value| value.0),
        created_at: row.try_get("created_at")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        error_code: row.try_get("error_code")?,
        error_message: row.try_get("error_message")?,
        deleted_raw_events: row.try_get("deleted_raw_events")?,
        deleted_sessions: row.try_get("deleted_sessions")?,
        rebuilt_installs: row.try_get("rebuilt_installs")?,
        rebuilt_days: row.try_get("rebuilt_days")?,
        rebuilt_hours: row.try_get("rebuilt_hours")?,
    })
}

// Active install bitmap helpers

#[derive(Debug, Clone)]
pub struct ActiveInstallBitmapDim1Input {
    pub dim1_key: String,
    pub dim1_value: Option<String>,
    pub bitmap: RoaringTreemap,
}

#[derive(Debug, Clone)]
pub struct ActiveInstallBitmapDim2Input {
    pub dim1_key: String,
    pub dim1_value: Option<String>,
    pub dim2_key: String,
    pub dim2_value: Option<String>,
    pub bitmap: RoaringTreemap,
}

#[derive(Debug, Clone)]
pub struct ActiveInstallBitmapTotalRow {
    pub day: NaiveDate,
    pub bitmap: RoaringTreemap,
    pub cardinality: i64,
}

#[derive(Debug, Clone)]
pub struct ActiveInstallBitmapDim1Row {
    pub day: NaiveDate,
    pub dim1_key: String,
    pub dim1_value: Option<String>,
    pub bitmap: RoaringTreemap,
    pub cardinality: i64,
}

#[derive(Debug, Clone)]
pub struct ActiveInstallBitmapDim2Row {
    pub day: NaiveDate,
    pub dim1_key: String,
    pub dim1_value: Option<String>,
    pub dim2_key: String,
    pub dim2_value: Option<String>,
    pub bitmap: RoaringTreemap,
    pub cardinality: i64,
}

pub async fn allocate_project_install_ordinals_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_ids: &[String],
) -> Result<BTreeMap<String, i64>, StoreError> {
    if install_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let install_ids = install_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    sqlx::query(
        "INSERT INTO project_install_ordinal_state (project_id) VALUES ($1) ON CONFLICT DO NOTHING",
    )
    .bind(project_id)
    .execute(&mut **tx)
    .await?;

    let state_row = sqlx::query(
        "SELECT next_ordinal FROM project_install_ordinal_state WHERE project_id = $1 FOR UPDATE",
    )
    .bind(project_id)
    .fetch_one(&mut **tx)
    .await?;
    let mut next_ordinal = state_row.try_get::<i64, _>("next_ordinal")?;

    let existing = sqlx::query(
        "SELECT install_id, ordinal FROM project_install_ordinals WHERE project_id = $1 AND install_id = ANY($2)",
    )
    .bind(project_id)
    .bind(&install_ids)
    .fetch_all(&mut **tx)
    .await?;

    let mut ordinals = BTreeMap::new();
    for row in existing {
        ordinals.insert(row.try_get("install_id")?, row.try_get("ordinal")?);
    }

    let mut unique_installs = BTreeSet::new();
    for install_id in install_ids {
        unique_installs.insert(install_id.clone());
    }
    unique_installs.retain(|install_id| !ordinals.contains_key(install_id));

    if unique_installs.is_empty() {
        return Ok(ordinals);
    }

    let assigned_rows: Vec<(String, i64)> = unique_installs
        .into_iter()
        .map(|install_id| {
            let ordinal = next_ordinal;
            next_ordinal += 1;
            (install_id, ordinal)
        })
        .collect();

    if !assigned_rows.is_empty() {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO project_install_ordinals (project_id, install_id, ordinal) ",
        );
        builder.push_values(assigned_rows.iter(), |mut row, (install_id, ordinal)| {
            row.push_bind(project_id)
                .push_bind(install_id)
                .push_bind(*ordinal);
        });
        builder.build().execute(&mut **tx).await?;

        sqlx::query(
            "UPDATE project_install_ordinal_state SET next_ordinal = $1, updated_at = now() WHERE project_id = $2",
        )
        .bind(next_ordinal)
        .bind(project_id)
        .execute(&mut **tx)
        .await?;

        for (install_id, ordinal) in assigned_rows {
            ordinals.insert(install_id, ordinal);
        }
    }

    Ok(ordinals)
}

pub async fn enqueue_active_install_bitmap_rebuild(
    pool: &PgPool,
    project_id: Uuid,
    day: NaiveDate,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO active_install_bitmap_rebuild_queue (project_id, day) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(project_id)
    .bind(day)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn enqueue_active_install_bitmap_rebuild_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    day: NaiveDate,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO active_install_bitmap_rebuild_queue (project_id, day) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(project_id)
    .bind(day)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn load_active_install_bitmap_rebuild_queue(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<Vec<NaiveDate>, StoreError> {
    let rows = sqlx::query_scalar::<_, NaiveDate>(
        r#"
        SELECT day
        FROM active_install_bitmap_rebuild_queue
        WHERE project_id = $1
        ORDER BY day ASC
        "#,
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn delete_active_install_bitmap_rebuild_entry(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    day: NaiveDate,
) -> Result<bool, StoreError> {
    let result = sqlx::query(
        "DELETE FROM active_install_bitmap_rebuild_queue WHERE project_id = $1 AND day = $2",
    )
    .bind(project_id)
    .bind(day)
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected() > 0)
}

pub async fn replace_active_install_day_bitmaps_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    day: NaiveDate,
    total_bitmap: &RoaringTreemap,
    dim1_rows: &[ActiveInstallBitmapDim1Input],
    dim2_rows: &[ActiveInstallBitmapDim2Input],
) -> Result<(), StoreError> {
    sqlx::query(
        "DELETE FROM active_install_daily_bitmaps_total WHERE project_id = $1 AND day = $2",
    )
    .bind(project_id)
    .bind(day)
    .execute(&mut **tx)
    .await?;
    sqlx::query("DELETE FROM active_install_daily_bitmaps_dim1 WHERE project_id = $1 AND day = $2")
        .bind(project_id)
        .bind(day)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM active_install_daily_bitmaps_dim2 WHERE project_id = $1 AND day = $2")
        .bind(project_id)
        .bind(day)
        .execute(&mut **tx)
        .await?;

    if !total_bitmap.is_empty() {
        let total_bytes = serialize_active_install_bitmap(total_bitmap)?;
        sqlx::query(
            "INSERT INTO active_install_daily_bitmaps_total (project_id, day, bitmap, cardinality) VALUES ($1, $2, $3, $4)",
        )
        .bind(project_id)
        .bind(day)
        .bind(total_bytes)
        .bind(total_bitmap.len() as i64)
        .execute(&mut **tx)
        .await?;
    }

    for row in dim1_rows {
        let (dim1_value, dim1_value_is_null) = encode_dimension_value(row.dim1_value.as_deref());
        let bitmap_bytes = serialize_active_install_bitmap(&row.bitmap)?;
        sqlx::query(
            "INSERT INTO active_install_daily_bitmaps_dim1 (project_id, day, dim1_key, dim1_value, dim1_value_is_null, bitmap, cardinality) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(project_id)
        .bind(day)
        .bind(&row.dim1_key)
        .bind(dim1_value)
        .bind(dim1_value_is_null)
        .bind(bitmap_bytes)
        .bind(row.bitmap.len() as i64)
        .execute(&mut **tx)
        .await?;
    }

    for row in dim2_rows {
        let (dim1_value, dim1_value_is_null) = encode_dimension_value(row.dim1_value.as_deref());
        let (dim2_value, dim2_value_is_null) = encode_dimension_value(row.dim2_value.as_deref());
        let bitmap_bytes = serialize_active_install_bitmap(&row.bitmap)?;
        sqlx::query(
            "INSERT INTO active_install_daily_bitmaps_dim2 (project_id, day, dim1_key, dim1_value, dim1_value_is_null, dim2_key, dim2_value, dim2_value_is_null, bitmap, cardinality) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(project_id)
        .bind(day)
        .bind(&row.dim1_key)
        .bind(dim1_value)
        .bind(dim1_value_is_null)
        .bind(&row.dim2_key)
        .bind(dim2_value)
        .bind(dim2_value_is_null)
        .bind(bitmap_bytes)
        .bind(row.bitmap.len() as i64)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

pub async fn load_active_install_bitmap_totals_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    days: &[NaiveDate],
) -> Result<Vec<ActiveInstallBitmapTotalRow>, StoreError>
where
    E: Executor<'a, Database = Postgres>,
{
    if days.is_empty() {
        return Ok(vec![]);
    }

    let rows = sqlx::query(
        "SELECT day, bitmap, cardinality
        FROM active_install_daily_bitmaps_total
        WHERE project_id = $1
          AND day = ANY($2)
        ORDER BY day ASC",
    )
    .bind(project_id)
    .bind(days)
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(|row| {
            let bitmap = deserialize_active_install_bitmap(&row.try_get::<Vec<u8>, _>("bitmap")?)?;
            Ok(ActiveInstallBitmapTotalRow {
                day: row.try_get("day")?,
                bitmap,
                cardinality: row.try_get("cardinality")?,
            })
        })
        .collect()
}

pub async fn load_active_install_bitmap_dim1_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    dim1_key: &str,
    dim1_value: Option<&str>,
    days: &[NaiveDate],
) -> Result<Vec<ActiveInstallBitmapDim1Row>, StoreError>
where
    E: Executor<'a, Database = Postgres>,
{
    if days.is_empty() {
        return Ok(vec![]);
    }

    let (value, is_null) = encode_dimension_value(dim1_value);
    let rows = sqlx::query(
        "SELECT day, dim1_key, dim1_value, dim1_value_is_null, bitmap, cardinality
        FROM active_install_daily_bitmaps_dim1
        WHERE project_id = $1
          AND day = ANY($2)
          AND dim1_key = $3
          AND dim1_value = $4
          AND dim1_value_is_null = $5
        ORDER BY day ASC",
    )
    .bind(project_id)
    .bind(days)
    .bind(dim1_key)
    .bind(value)
    .bind(is_null)
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(|row| {
            let bitmap = deserialize_active_install_bitmap(&row.try_get::<Vec<u8>, _>("bitmap")?)?;
            let dim1_value = row.try_get::<String, _>("dim1_value")?;
            let dim1_value_is_null = row.try_get::<bool, _>("dim1_value_is_null")?;
            Ok(ActiveInstallBitmapDim1Row {
                day: row.try_get("day")?,
                dim1_key: row.try_get("dim1_key")?,
                dim1_value: decode_dimension_value(&dim1_value, dim1_value_is_null),
                bitmap,
                cardinality: row.try_get("cardinality")?,
            })
        })
        .collect()
}

pub async fn load_active_install_bitmap_dim2_in_executor<'a, E>(
    executor: E,
    project_id: Uuid,
    dim1_key: &str,
    dim1_value: Option<&str>,
    dim2_key: &str,
    dim2_value: Option<&str>,
    days: &[NaiveDate],
) -> Result<Vec<ActiveInstallBitmapDim2Row>, StoreError>
where
    E: Executor<'a, Database = Postgres>,
{
    if days.is_empty() {
        return Ok(vec![]);
    }

    let (dim1_encoded, dim1_is_null) = encode_dimension_value(dim1_value);
    let (dim2_encoded, dim2_is_null) = encode_dimension_value(dim2_value);
    let rows = sqlx::query(
        "SELECT day, dim1_key, dim1_value, dim1_value_is_null, dim2_key, dim2_value, dim2_value_is_null, bitmap, cardinality
        FROM active_install_daily_bitmaps_dim2
        WHERE project_id = $1
          AND day = ANY($2)
          AND dim1_key = $3
          AND dim1_value = $4
          AND dim1_value_is_null = $5
          AND dim2_key = $6
          AND dim2_value = $7
          AND dim2_value_is_null = $8
        ORDER BY day ASC",
    )
    .bind(project_id)
    .bind(days)
    .bind(dim1_key)
    .bind(dim1_encoded)
    .bind(dim1_is_null)
    .bind(dim2_key)
    .bind(dim2_encoded)
    .bind(dim2_is_null)
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(|row| {
            let bitmap = deserialize_active_install_bitmap(&row.try_get::<Vec<u8>, _>("bitmap")?)?;
            let dim1_value = row.try_get::<String, _>("dim1_value")?;
            let dim1_value_is_null = row.try_get::<bool, _>("dim1_value_is_null")?;
            let dim2_value = row.try_get::<String, _>("dim2_value")?;
            let dim2_value_is_null = row.try_get::<bool, _>("dim2_value_is_null")?;
            Ok(ActiveInstallBitmapDim2Row {
                day: row.try_get("day")?,
                dim1_key: row.try_get("dim1_key")?,
                dim1_value: decode_dimension_value(&dim1_value, dim1_value_is_null),
                dim2_key: row.try_get("dim2_key")?,
                dim2_value: decode_dimension_value(&dim2_value, dim2_value_is_null),
                bitmap,
                cardinality: row.try_get("cardinality")?,
            })
        })
        .collect()
}

pub fn serialize_active_install_bitmap(bitmap: &RoaringTreemap) -> Result<Vec<u8>, StoreError> {
    let mut buffer = Vec::new();
    bitmap
        .serialize_into(&mut buffer)
        .map_err(|err| StoreError::InvariantViolation(format!("serialize bitmap: {err}")))?;

    Ok(buffer)
}

pub fn deserialize_active_install_bitmap(bytes: &[u8]) -> Result<RoaringTreemap, StoreError> {
    let mut cursor = Cursor::new(bytes);
    RoaringTreemap::deserialize_from(&mut cursor)
        .map_err(|err| StoreError::InvariantViolation(format!("deserialize bitmap: {err}")))
}

fn encode_dimension_value(value: Option<&str>) -> (String, bool) {
    match value {
        Some(text) => (text.to_owned(), false),
        None => (String::new(), true),
    }
}

fn decode_dimension_value(value: &str, is_null: bool) -> Option<String> {
    if is_null {
        None
    } else {
        Some(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
    use fantasma_core::EventPayload;
    use roaring::RoaringTreemap;
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
            locale: Some("en-GB".to_owned()),
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
            locale: Some("en-GB".to_owned()),
        }
    }

    fn day(offset_days: i64) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date") + Duration::days(offset_days)
    }

    async fn insert_session_daily_rows_for_test(
        tx: &mut Transaction<'_, Postgres>,
        rows: &[SessionDailyRecord],
    ) -> Result<u64, StoreError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut affected_rows = 0;
        for chunk in rows.chunks(max_rows_per_batched_statement(5)) {
            let mut builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO session_daily \
                 (project_id, day, sessions_count, active_installs, total_duration_seconds) ",
            );
            builder.push_values(chunk, |mut separated, row| {
                separated
                    .push_bind(row.project_id)
                    .push_bind(row.day)
                    .push_bind(row.sessions_count)
                    .push_bind(row.active_installs)
                    .push_bind(row.total_duration_seconds);
            });
            affected_rows += builder.build().execute(&mut **tx).await?.rows_affected();
        }

        Ok(affected_rows)
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
    async fn active_install_bitmap_tables_and_indexes_exist(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        for table in [
            "project_install_ordinals",
            "project_install_ordinal_state",
            "active_install_bitmap_rebuild_queue",
            "active_install_daily_bitmaps_total",
            "active_install_daily_bitmaps_dim1",
            "active_install_daily_bitmaps_dim2",
        ] {
            let exists = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = $1
                )
                "#,
            )
            .bind(table)
            .fetch_one(&pool)
            .await
            .expect("fetch table existence");

            assert!(exists, "table {table} exists");
        }

        let ordinal_index_names = sqlx::query_scalar::<_, String>(
            r#"
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'project_install_ordinals'
              AND indexname = 'idx_project_install_ordinals_project_ordinal'
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch ordinal index");

        assert_eq!(
            ordinal_index_names,
            vec!["idx_project_install_ordinals_project_ordinal".to_owned()]
        );

        let dim1_index_names = sqlx::query_scalar::<_, String>(
            r#"
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'active_install_daily_bitmaps_dim1'
              AND indexname = 'idx_active_install_daily_bitmaps_dim1_filter'
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch dim1 index");

        assert_eq!(
            dim1_index_names,
            vec!["idx_active_install_daily_bitmaps_dim1_filter".to_owned()]
        );

        let dim2_index_names = sqlx::query_scalar::<_, String>(
            r#"
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'active_install_daily_bitmaps_dim2'
              AND indexname IN (
                'idx_active_install_daily_bitmaps_dim2_filter_dim1',
                'idx_active_install_daily_bitmaps_dim2_filter_dim2'
              )
            ORDER BY indexname
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch dim2 indexes");

        assert_eq!(
            dim2_index_names,
            vec![
                "idx_active_install_daily_bitmaps_dim2_filter_dim1".to_owned(),
                "idx_active_install_daily_bitmaps_dim2_filter_dim2".to_owned()
            ]
        );
    }

    #[sqlx::test]
    async fn active_install_bitmap_dim1_filter_index_exists(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let index_names = sqlx::query_scalar::<_, String>(
            r#"
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'active_install_daily_bitmaps_dim1'
            ORDER BY indexname
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch dim1 index names");

        assert!(
            index_names
                .iter()
                .any(|name| name == "idx_active_install_daily_bitmaps_dim1_filter"),
            "dim1 filter index should exist, got {index_names:?}"
        );
    }

    #[sqlx::test]
    async fn project_install_ordinals_allocate(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let mut tx = pool.begin().await.expect("begin tx");
        let install_ids = vec!["install-ios".to_owned(), "install-android".to_owned()];
        let ordinals = allocate_project_install_ordinals_in_tx(&mut tx, project_id(), &install_ids)
            .await
            .expect("allocate ordinals");
        assert_eq!(ordinals.len(), 2);

        let ios_ord = *ordinals.get("install-ios").expect("ios ordinal");
        let android_ord = *ordinals.get("install-android").expect("android ordinal");
        assert_ne!(ios_ord, android_ord);

        let additional_ids = vec!["install-android".to_owned(), "install-tablet".to_owned()];
        let ordinals =
            allocate_project_install_ordinals_in_tx(&mut tx, project_id(), &additional_ids)
                .await
                .expect("allocate additional ordinals");

        assert_eq!(ordinals.get("install-android"), Some(&android_ord));
        let tablet_ord = *ordinals
            .get("install-tablet")
            .expect("new ordinal assigned");
        assert!(tablet_ord > android_ord);

        tx.commit().await.expect("commit tx");
    }

    #[sqlx::test]
    async fn active_install_bitmap_rebuild_queue_helpers(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let day = NaiveDate::from_ymd_opt(2026, 3, 1).expect("valid day");
        enqueue_active_install_bitmap_rebuild(&pool, project_id(), day)
            .await
            .expect("enqueue day");
        enqueue_active_install_bitmap_rebuild(&pool, project_id(), day)
            .await
            .expect("enqueue dedup");

        let entries = load_active_install_bitmap_rebuild_queue(&pool, project_id())
            .await
            .expect("load queue");
        assert_eq!(entries, vec![day]);

        let mut tx = pool.begin().await.expect("begin tx");
        let deleted = delete_active_install_bitmap_rebuild_entry(&mut tx, project_id(), day)
            .await
            .expect("delete entry");
        assert!(deleted);
        tx.commit().await.expect("commit tx");

        let entries = load_active_install_bitmap_rebuild_queue(&pool, project_id())
            .await
            .expect("load queue after delete");
        assert!(entries.is_empty());
    }

    #[sqlx::test]
    async fn active_install_bitmaps_replace_and_load(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let day = NaiveDate::from_ymd_opt(2026, 3, 1).expect("valid day");
        let mut tx = pool.begin().await.expect("begin tx");

        let install_ids = vec!["install-ios".to_owned(), "install-android".to_owned()];
        let ordinals = allocate_project_install_ordinals_in_tx(&mut tx, project_id(), &install_ids)
            .await
            .expect("allocate ordinals");
        let ios_ord = *ordinals.get("install-ios").expect("ios ordinal");
        let android_ord = *ordinals.get("install-android").expect("android ordinal");

        let mut total_bitmap = RoaringTreemap::new();
        total_bitmap.insert(ios_ord as u64);
        total_bitmap.insert(android_ord as u64);

        replace_active_install_day_bitmaps_in_tx(
            &mut tx,
            project_id(),
            day,
            &total_bitmap,
            &[ActiveInstallBitmapDim1Input {
                dim1_key: "platform".to_owned(),
                dim1_value: Some("ios".to_owned()),
                bitmap: {
                    let mut bitmap = RoaringTreemap::new();
                    bitmap.insert(ios_ord as u64);
                    bitmap
                },
            }],
            &[ActiveInstallBitmapDim2Input {
                dim1_key: "platform".to_owned(),
                dim1_value: Some("ios".to_owned()),
                dim2_key: "os_version".to_owned(),
                dim2_value: None,
                bitmap: {
                    let mut bitmap = RoaringTreemap::new();
                    bitmap.insert(ios_ord as u64);
                    bitmap
                },
            }],
        )
        .await
        .expect("replace day bitmaps");

        tx.commit().await.expect("commit tx");

        let totals = load_active_install_bitmap_totals_in_executor(&pool, project_id(), &[day])
            .await
            .expect("load totals");
        assert_eq!(totals.len(), 1);
        assert_eq!(totals[0].cardinality, 2);
        assert!(totals[0].bitmap.contains(ios_ord as u64));
        assert!(totals[0].bitmap.contains(android_ord as u64));

        let dim1_rows = load_active_install_bitmap_dim1_in_executor(
            &pool,
            project_id(),
            "platform",
            Some("ios"),
            &[day],
        )
        .await
        .expect("load dim1 rows");
        assert_eq!(dim1_rows.len(), 1);
        assert_eq!(dim1_rows[0].cardinality, 1);
        assert!(dim1_rows[0].bitmap.contains(ios_ord as u64));

        let dim2_rows = load_active_install_bitmap_dim2_in_executor(
            &pool,
            project_id(),
            "platform",
            Some("ios"),
            "os_version",
            None,
            &[day],
        )
        .await
        .expect("load dim2 rows");
        assert_eq!(dim2_rows.len(), 1);
        assert_eq!(dim2_rows[0].cardinality, 1);
        assert!(dim2_rows[0].bitmap.contains(ios_ord as u64));
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
                && migration.contains("ADD COLUMN IF NOT EXISTS locale TEXT"),
            "0015 must widen sessions with session snapshot dimensions"
        );
        assert!(
            migration.contains("ALTER TABLE install_first_seen")
                && migration.contains("ADD COLUMN IF NOT EXISTS os_version TEXT")
                && migration.contains("ADD COLUMN IF NOT EXISTS locale TEXT"),
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

    #[test]
    fn forward_migration_adds_live_install_state_storage() {
        let migration = include_str!("../migrations/0018_live_installs.sql");

        assert!(
            migration.contains("CREATE TABLE IF NOT EXISTS live_install_state")
                && migration.contains("PRIMARY KEY (project_id, install_id)")
                && migration.contains("last_received_at TIMESTAMPTZ NOT NULL"),
            "0018 must add live_install_state keyed by project and install"
        );
        assert!(
            migration.contains("idx_live_install_state_project_received_at"),
            "0018 must add the bounded read index for live install counts"
        );
    }

    #[test]
    fn forward_migration_adds_active_install_daily_bitmap_storage() {
        let migration = include_str!("../migrations/0019_active_install_daily_bitmaps.sql");

        assert!(
            migration.contains("CREATE TABLE IF NOT EXISTS project_install_ordinals")
                && migration.contains("CREATE TABLE IF NOT EXISTS project_install_ordinal_state")
                && migration
                    .contains("CREATE TABLE IF NOT EXISTS active_install_bitmap_rebuild_queue")
                && migration
                    .contains("CREATE TABLE IF NOT EXISTS active_install_daily_bitmaps_total")
                && migration
                    .contains("CREATE TABLE IF NOT EXISTS active_install_daily_bitmaps_dim1")
                && migration
                    .contains("CREATE TABLE IF NOT EXISTS active_install_daily_bitmaps_dim2"),
            "0019 must add the active-install bitmap storage family"
        );
        assert!(
            migration.contains("idx_active_install_daily_bitmaps_dim1_filter")
                && migration.contains("idx_active_install_daily_bitmaps_dim1_value")
                && migration.contains("idx_active_install_daily_bitmaps_dim2_value")
                && migration.contains("idx_active_install_daily_bitmaps_dim2_filter_dim1")
                && migration.contains("idx_active_install_daily_bitmaps_dim2_filter_dim2"),
            "0019 must keep bounded read indexes for dim1 and dim2 active-install bitmap reads"
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

    async fn insert_usage_event_row(
        pool: &PgPool,
        project_id: Uuid,
        timestamp: &str,
        received_at: &str,
        install_id: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO events_raw (
                project_id,
                event_name,
                timestamp,
                received_at,
                install_id,
                platform,
                app_version,
                os_version,
                locale
            )
            VALUES ($1, 'app_open', $2, $3, $4, 'ios', NULL, NULL, $5)
            "#,
        )
        .bind(project_id)
        .bind(
            DateTime::parse_from_rfc3339(timestamp)
                .expect("timestamp")
                .with_timezone(&Utc),
        )
        .bind(
            DateTime::parse_from_rfc3339(received_at)
                .expect("received_at")
                .with_timezone(&Utc),
        )
        .bind(install_id)
        .bind("en-GB")
        .execute(pool)
        .await
        .expect("insert usage event");
    }

    #[sqlx::test]
    async fn list_usage_events_counts_received_at_and_omits_empty_projects(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let project_a = Uuid::new_v4();
        let project_b = Uuid::new_v4();
        let project_c = Uuid::new_v4();
        let created_at_a = DateTime::parse_from_rfc3339("2026-03-10T09:00:00Z")
            .expect("created_at_a")
            .with_timezone(&Utc);
        let created_at_b = DateTime::parse_from_rfc3339("2026-03-11T09:00:00Z")
            .expect("created_at_b")
            .with_timezone(&Utc);
        let created_at_c = DateTime::parse_from_rfc3339("2026-03-12T09:00:00Z")
            .expect("created_at_c")
            .with_timezone(&Utc);

        sqlx::query(
            r#"
            INSERT INTO projects (id, name, state, fence_epoch, created_at)
            VALUES ($1, $2, 'active', 0, $3), ($4, $5, 'active', 0, $6), ($7, $8, 'active', 0, $9)
            "#,
        )
        .bind(project_a)
        .bind("Usage Project A")
        .bind(created_at_a)
        .bind(project_b)
        .bind("Usage Project B")
        .bind(created_at_b)
        .bind(project_c)
        .bind("Usage Project C")
        .bind(created_at_c)
        .execute(&pool)
        .await
        .expect("insert projects");

        insert_usage_event_row(
            &pool,
            project_a,
            "2026-03-01T10:00:00Z",
            "2026-03-01T10:00:00Z",
            "install-a",
        )
        .await;
        insert_usage_event_row(
            &pool,
            project_a,
            "2026-03-02T10:00:00Z",
            "2026-03-01T11:00:00Z",
            "install-b",
        )
        .await;
        insert_usage_event_row(
            &pool,
            project_b,
            "2026-02-28T12:00:00Z",
            "2026-03-01T12:00:00Z",
            "install-c",
        )
        .await;
        insert_usage_event_row(
            &pool,
            project_b,
            "2026-03-01T13:00:00Z",
            "2026-03-04T13:00:00Z",
            "install-d",
        )
        .await;

        let rows = list_usage_events(
            &pool,
            NaiveDate::from_ymd_opt(2026, 3, 1).expect("start"),
            NaiveDate::from_ymd_opt(2026, 3, 1).expect("end"),
        )
        .await
        .expect("list usage events");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].project.id, project_a);
        assert_eq!(rows[0].project.name, "Usage Project A");
        assert_eq!(rows[0].events_processed, 2);
        assert_eq!(rows[1].project.id, project_b);
        assert_eq!(rows[1].project.name, "Usage Project B");
        assert_eq!(rows[1].events_processed, 1);
    }

    #[sqlx::test]
    async fn list_usage_events_uses_created_at_as_tiebreaker(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let project_early = Uuid::new_v4();
        let project_late = Uuid::new_v4();
        let early_created_at = DateTime::parse_from_rfc3339("2026-03-10T09:00:00Z")
            .expect("early created_at")
            .with_timezone(&Utc);
        let late_created_at = DateTime::parse_from_rfc3339("2026-03-11T09:00:00Z")
            .expect("late created_at")
            .with_timezone(&Utc);

        sqlx::query(
            r#"
            INSERT INTO projects (id, name, state, fence_epoch, created_at)
            VALUES ($1, $2, 'active', 0, $3), ($4, $5, 'active', 0, $6)
            "#,
        )
        .bind(project_early)
        .bind("Early Project")
        .bind(early_created_at)
        .bind(project_late)
        .bind("Late Project")
        .bind(late_created_at)
        .execute(&pool)
        .await
        .expect("insert projects");

        for project_id in [project_late, project_early] {
            insert_usage_event_row(
                &pool,
                project_id,
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:00:00Z",
                "install",
            )
            .await;
        }

        let rows = list_usage_events(
            &pool,
            NaiveDate::from_ymd_opt(2026, 3, 1).expect("start"),
            NaiveDate::from_ymd_opt(2026, 3, 1).expect("end"),
        )
        .await
        .expect("list usage events");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].project.id, project_early);
        assert_eq!(rows[1].project.id, project_late);
    }

    #[sqlx::test]
    async fn projects_default_to_active_state_with_zero_fence_epoch(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let project = create_project(&pool, "Lifecycle Project")
            .await
            .expect("create project");

        let row = sqlx::query(
            r#"
            SELECT state, fence_epoch
            FROM projects
            WHERE id = $1
            "#,
        )
        .bind(project.id)
        .fetch_one(&pool)
        .await
        .expect("fetch project lifecycle state");

        let state: String = row.try_get("state").expect("read project state");
        let fence_epoch: i64 = row.try_get("fence_epoch").expect("read fence epoch");

        assert_eq!(state, "active");
        assert_eq!(fence_epoch, 0);
    }

    #[sqlx::test]
    async fn project_processing_lease_detects_fence_epoch_advance(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let project = create_project(&pool, "Lease Project")
            .await
            .expect("create project");

        let lease = claim_project_processing_lease(
            &pool,
            project.id,
            ProjectProcessingActorKind::Ingest,
            "lease-1",
        )
        .await
        .expect("claim project lease");
        let mut mutation_tx = pool.begin().await.expect("begin mutation tx");

        sqlx::query(
            r#"
            UPDATE projects
            SET state = 'range_deleting',
                fence_epoch = fence_epoch + 1
            WHERE id = $1
            "#,
        )
        .bind(project.id)
        .execute(&pool)
        .await
        .expect("advance project fence");

        let error = verify_project_processing_lease_in_tx(&mut mutation_tx, &lease)
            .await
            .expect_err("fence advance should invalidate the lease");
        assert!(matches!(error, StoreError::ProjectFenceChanged));

        mutation_tx.rollback().await.expect("rollback mutation tx");
        release_project_processing_lease(&pool, &lease)
            .await
            .expect("release project lease");
    }

    #[sqlx::test]
    async fn count_project_processing_leases_ignores_stale_leases(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let project = create_project(&pool, "Lease Freshness Project")
            .await
            .expect("create project");

        let fresh_lease = claim_project_processing_lease(
            &pool,
            project.id,
            ProjectProcessingActorKind::Ingest,
            "fresh-lease",
        )
        .await
        .expect("claim fresh lease");

        sqlx::query(
            r#"
            INSERT INTO project_processing_leases (
                project_id,
                actor_kind,
                actor_id,
                fence_epoch,
                claimed_at,
                heartbeat_at
            )
            VALUES (
                $1,
                'session_apply',
                'stale-lease',
                0,
                now() - ($2 * interval '1 second'),
                now() - ($2 * interval '1 second')
            )
            "#,
        )
        .bind(project.id)
        .bind(PROJECT_PROCESSING_LEASE_STALE_AFTER_SECS + 1)
        .execute(&pool)
        .await
        .expect("insert stale lease");

        let count = count_project_processing_leases_before_epoch(&pool, project.id, 1)
            .await
            .expect("count active leases");

        assert_eq!(count, 1);

        release_project_processing_lease(&pool, &fresh_lease)
            .await
            .expect("release fresh lease");
    }

    #[sqlx::test]
    async fn claim_next_project_deletion_job_skips_blocked_projects(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let blocked_project = create_project(&pool, "Blocked Deletion Project")
            .await
            .expect("create blocked project");
        let runnable_project = create_project(&pool, "Runnable Deletion Project")
            .await
            .expect("create runnable project");

        let blocked_lease = claim_project_processing_lease(
            &pool,
            blocked_project.id,
            ProjectProcessingActorKind::Ingest,
            "blocked-lease",
        )
        .await
        .expect("claim blocked lease");

        sqlx::query(
            r#"
            UPDATE projects
            SET state = 'pending_deletion',
                fence_epoch = fence_epoch + 1
            WHERE id = $1
            "#,
        )
        .bind(blocked_project.id)
        .execute(&pool)
        .await
        .expect("advance blocked project fence");

        let blocked_job_id = Uuid::new_v4();
        let runnable_job_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO project_deletions (
                id,
                project_id,
                project_name,
                kind,
                status,
                scope,
                created_at
            )
            VALUES
                ($1, $2, $3, 'project_purge', 'queued', NULL, now() - interval '2 seconds'),
                ($4, $5, $6, 'project_purge', 'queued', NULL, now() - interval '1 second')
            "#,
        )
        .bind(blocked_job_id)
        .bind(blocked_project.id)
        .bind(blocked_project.name.clone())
        .bind(runnable_job_id)
        .bind(runnable_project.id)
        .bind(runnable_project.name.clone())
        .execute(&pool)
        .await
        .expect("insert deletion jobs");

        let mut tx = pool.begin().await.expect("begin tx");
        let claimed = claim_next_project_deletion_job_in_tx(&mut tx)
            .await
            .expect("claim next deletion job")
            .expect("runnable job");
        tx.rollback().await.expect("rollback tx");

        assert_eq!(claimed.id, runnable_job_id);
        assert_eq!(claimed.project_id, runnable_project.id);

        release_project_processing_lease(&pool, &blocked_lease)
            .await
            .expect("release blocked lease");
    }

    #[sqlx::test]
    async fn insert_events_rejects_non_active_projects(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        for (state, expected_state) in [
            ("range_deleting", ProjectState::RangeDeleting),
            ("pending_deletion", ProjectState::PendingDeletion),
        ] {
            let project = create_project(&pool, &format!("Event Gate {state}"))
                .await
                .expect("create project");
            sqlx::query("UPDATE projects SET state = $2 WHERE id = $1")
                .bind(project.id)
                .bind(state)
                .execute(&pool)
                .await
                .expect("update project state");

            let error = insert_events(&pool, project.id, &[event("install-1", 0)])
                .await
                .expect_err("non-active project should reject inserts");
            assert!(matches!(
                error,
                StoreError::ProjectNotActive(state) if state == expected_state
            ));
        }
    }

    #[sqlx::test]
    async fn rename_and_key_mutations_reject_non_active_projects(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let active_project = create_project(&pool, "Mutation Gate Active")
            .await
            .expect("create project");
        let key = create_api_key(
            &pool,
            active_project.id,
            "ios",
            ApiKeyKind::Read,
            "fg_rd_mutation_gate",
        )
        .await
        .expect("create key")
        .expect("key exists");

        for (state, expected_state) in [
            ("range_deleting", ProjectState::RangeDeleting),
            ("pending_deletion", ProjectState::PendingDeletion),
        ] {
            let project = create_project(&pool, &format!("Mutation Gate {state}"))
                .await
                .expect("create project");
            sqlx::query("UPDATE projects SET state = $2 WHERE id = $1")
                .bind(project.id)
                .bind(state)
                .execute(&pool)
                .await
                .expect("update project state");

            let rename_error = rename_project(&pool, project.id, "Renamed")
                .await
                .expect_err("rename should reject non-active project");
            assert!(matches!(
                rename_error,
                StoreError::ProjectNotActive(state) if state == expected_state
            ));

            let secret = format!("fg_rd_new_gate_{state}");
            let create_key_error =
                create_api_key(&pool, project.id, "ios", ApiKeyKind::Read, &secret)
                    .await
                    .expect_err("create key should reject non-active project");
            assert!(matches!(
                create_key_error,
                StoreError::ProjectNotActive(state) if state == expected_state
            ));
        }

        sqlx::query("UPDATE projects SET state = 'range_deleting' WHERE id = $1")
            .bind(active_project.id)
            .execute(&pool)
            .await
            .expect("update active project state");

        let revoke_error = revoke_api_key(&pool, active_project.id, key.id)
            .await
            .expect_err("revoke should reject non-active project");
        assert!(matches!(
            revoke_error,
            StoreError::ProjectNotActive(ProjectState::RangeDeleting)
        ));
    }

    #[sqlx::test]
    async fn deletion_history_survives_project_row_removal(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let project = create_project(&pool, "Historical Delete Project")
            .await
            .expect("create project");

        sqlx::query(
            r#"
            INSERT INTO project_deletions (
                id,
                project_id,
                project_name,
                kind,
                status,
                scope,
                created_at,
                finished_at
            )
            VALUES ($1, $2, $3, 'project_purge', 'succeeded', NULL, now(), now())
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(project.id)
        .bind(project.name.clone())
        .execute(&pool)
        .await
        .expect("insert deletion history");

        sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(project.id)
            .execute(&pool)
            .await
            .expect("delete project row");

        let remaining = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM project_deletions WHERE project_id = $1",
        )
        .bind(project.id)
        .fetch_one(&pool)
        .await
        .expect("count deletion history rows");

        assert_eq!(remaining, 1);
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
    async fn authenticate_ingest_request_accepts_active_ingest_keys(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let project = create_project(&pool, "Authenticated Ingest Project")
            .await
            .expect("create project");
        let ingest_secret = generate_api_key_secret(ApiKeyKind::Ingest);
        create_api_key(
            &pool,
            project.id,
            "ios-prod",
            ApiKeyKind::Ingest,
            &ingest_secret,
        )
        .await
        .expect("create ingest key")
        .expect("project exists");

        let authenticated = authenticate_ingest_request(&pool, &ingest_secret)
            .await
            .expect("authenticate ingest request");

        assert_eq!(authenticated.project_id, project.id);
    }

    #[sqlx::test]
    async fn authenticate_ingest_request_rejects_invalid_revoked_and_wrong_kind_keys(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let project = create_project(&pool, "Reject Bad Ingest Keys")
            .await
            .expect("create project");
        let revoked_secret = generate_api_key_secret(ApiKeyKind::Ingest);
        let revoked_key = create_api_key(
            &pool,
            project.id,
            "revoked",
            ApiKeyKind::Ingest,
            &revoked_secret,
        )
        .await
        .expect("create revoked ingest key")
        .expect("project exists");
        let read_secret = generate_api_key_secret(ApiKeyKind::Read);
        create_api_key(&pool, project.id, "read", ApiKeyKind::Read, &read_secret)
            .await
            .expect("create read key")
            .expect("project exists");
        assert!(
            revoke_api_key(&pool, project.id, revoked_key.id)
                .await
                .expect("revoke ingest key"),
            "revoked key should exist"
        );

        for secret in ["fg_ing_missing_key".to_owned(), revoked_secret, read_secret] {
            let error = authenticate_ingest_request(&pool, &secret)
                .await
                .expect_err("non-active ingest auth should fail");
            assert!(
                matches!(error, StoreError::ProjectNotFound),
                "expected unauthorized-like store error for {secret}, got {error:?}"
            );
        }
    }

    #[sqlx::test]
    async fn authenticate_ingest_request_rejects_non_active_projects(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        for (state, expected_state) in [
            ("range_deleting", ProjectState::RangeDeleting),
            ("pending_deletion", ProjectState::PendingDeletion),
        ] {
            let project = create_project(&pool, &format!("Auth Gate {state}"))
                .await
                .expect("create project");
            let ingest_secret = generate_api_key_secret(ApiKeyKind::Ingest);
            create_api_key(
                &pool,
                project.id,
                "ingest",
                ApiKeyKind::Ingest,
                &ingest_secret,
            )
            .await
            .expect("create ingest key")
            .expect("project exists");
            sqlx::query("UPDATE projects SET state = $2 WHERE id = $1")
                .bind(project.id)
                .bind(state)
                .execute(&pool)
                .await
                .expect("update project state");

            let error = authenticate_ingest_request(&pool, &ingest_secret)
                .await
                .expect_err("non-active project should fail auth gate");
            assert!(matches!(
                error,
                StoreError::ProjectNotActive(actual) if actual == expected_state
            ));
        }
    }

    #[sqlx::test]
    async fn insert_events_with_authenticated_ingest_inserts_rows(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let project = create_project(&pool, "Authenticated Insert Project")
            .await
            .expect("create project");
        let ingest_secret = generate_api_key_secret(ApiKeyKind::Ingest);
        create_api_key(
            &pool,
            project.id,
            "ingest",
            ApiKeyKind::Ingest,
            &ingest_secret,
        )
        .await
        .expect("create ingest key")
        .expect("project exists");
        let authenticated = authenticate_ingest_request(&pool, &ingest_secret)
            .await
            .expect("authenticate ingest request");

        let inserted = insert_events_with_authenticated_ingest(
            &pool,
            &authenticated,
            &[event("install-1", 0), event("install-2", 1)],
        )
        .await
        .expect("insert events with authenticated ingest");

        assert_eq!(inserted, 2);
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
    async fn run_migrations_creates_live_install_state_table_and_indexes(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let table_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'live_install_state'
            )
            "#,
        )
        .fetch_one(&pool)
        .await
        .expect("fetch live_install_state table existence");

        let index_names = sqlx::query_scalar::<_, String>(
            r#"
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'live_install_state'
              AND indexname IN (
                'live_install_state_pkey',
                'idx_live_install_state_project_received_at'
              )
            ORDER BY indexname ASC
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch live_install_state index names");

        assert!(table_exists);
        assert_eq!(
            index_names,
            vec![
                "idx_live_install_state_project_received_at".to_owned(),
                "live_install_state_pkey".to_owned(),
            ]
        );
    }

    #[sqlx::test]
    async fn run_migrations_creates_active_install_daily_bitmap_tables_and_indexes(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");

        let tables = sqlx::query_scalar::<_, String>(
            r#"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name IN (
                'project_install_ordinals',
                'project_install_ordinal_state',
                'active_install_bitmap_rebuild_queue',
                'active_install_daily_bitmaps_total',
                'active_install_daily_bitmaps_dim1',
                'active_install_daily_bitmaps_dim2'
            )
            ORDER BY table_name ASC
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch active install bitmap tables");

        let indexes = sqlx::query_scalar::<_, String>(
            r#"
            SELECT indexname
            FROM pg_indexes
            WHERE tablename IN (
                'project_install_ordinals',
                'active_install_daily_bitmaps_dim1',
                'active_install_daily_bitmaps_dim2'
            )
              AND indexname IN (
                'project_install_ordinals_pkey',
                'idx_project_install_ordinals_project_ordinal',
                'idx_active_install_daily_bitmaps_dim1_value',
                'idx_active_install_daily_bitmaps_dim1_filter',
                'idx_active_install_daily_bitmaps_dim2_value',
                'idx_active_install_daily_bitmaps_dim2_filter_dim1',
                'idx_active_install_daily_bitmaps_dim2_filter_dim2'
              )
            ORDER BY indexname ASC
            "#,
        )
        .fetch_all(&pool)
        .await
        .expect("fetch active install bitmap indexes");

        assert_eq!(
            tables,
            vec![
                "active_install_bitmap_rebuild_queue".to_owned(),
                "active_install_daily_bitmaps_dim1".to_owned(),
                "active_install_daily_bitmaps_dim2".to_owned(),
                "active_install_daily_bitmaps_total".to_owned(),
                "project_install_ordinal_state".to_owned(),
                "project_install_ordinals".to_owned(),
            ]
        );
        assert_eq!(
            indexes,
            vec![
                "idx_active_install_daily_bitmaps_dim1_filter".to_owned(),
                "idx_active_install_daily_bitmaps_dim1_value".to_owned(),
                "idx_active_install_daily_bitmaps_dim2_filter_dim1".to_owned(),
                "idx_active_install_daily_bitmaps_dim2_filter_dim2".to_owned(),
                "idx_active_install_daily_bitmaps_dim2_value".to_owned(),
                "idx_project_install_ordinals_project_ordinal".to_owned(),
                "project_install_ordinals_pkey".to_owned(),
            ]
        );
    }

    #[test]
    fn active_install_bitmap_serialization_round_trips() {
        let bitmap = RoaringTreemap::from_iter([1_u64, 17, 999_999]);

        let encoded = serialize_active_install_bitmap(&bitmap).expect("serialize bitmap");
        let decoded = deserialize_active_install_bitmap(&encoded).expect("deserialize bitmap");

        assert_eq!(decoded, bitmap);
    }

    #[sqlx::test]
    async fn allocate_project_install_ordinals_in_tx_assigns_stable_unique_ordinals(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let mut tx = pool.begin().await.expect("begin allocation tx");
        let first = allocate_project_install_ordinals_in_tx(
            &mut tx,
            project_id(),
            &[
                "install-b".to_owned(),
                "install-a".to_owned(),
                "install-b".to_owned(),
            ],
        )
        .await
        .expect("allocate first ordinals");
        let second = allocate_project_install_ordinals_in_tx(
            &mut tx,
            project_id(),
            &["install-a".to_owned(), "install-c".to_owned()],
        )
        .await
        .expect("allocate second ordinals");
        tx.commit().await.expect("commit allocation tx");

        assert_eq!(first.get("install-a").copied(), Some(0));
        assert_eq!(first.get("install-b").copied(), Some(1));
        assert_eq!(second.get("install-a").copied(), Some(0));
        assert_eq!(second.get("install-c").copied(), Some(2));
    }

    #[sqlx::test]
    async fn active_install_bitmap_rebuild_queue_round_trips(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid day");
        let day_2 = NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid day");

        enqueue_active_install_bitmap_rebuild(&pool, project_id(), day_2)
            .await
            .expect("enqueue later day");
        enqueue_active_install_bitmap_rebuild(&pool, project_id(), day_1)
            .await
            .expect("enqueue earlier day");
        enqueue_active_install_bitmap_rebuild(&pool, project_id(), day_1)
            .await
            .expect("dedupe earlier day");

        assert_eq!(
            load_active_install_bitmap_rebuild_queue(&pool, project_id())
                .await
                .expect("load queue"),
            vec![day_1, day_2]
        );

        let mut delete_tx = pool.begin().await.expect("begin delete tx");
        assert!(
            delete_active_install_bitmap_rebuild_entry(&mut delete_tx, project_id(), day_1)
                .await
                .expect("delete existing queue row")
        );
        assert!(
            !delete_active_install_bitmap_rebuild_entry(&mut delete_tx, project_id(), day_1)
                .await
                .expect("delete missing queue row")
        );
        delete_tx.commit().await.expect("commit delete tx");

        assert_eq!(
            load_active_install_bitmap_rebuild_queue(&pool, project_id())
                .await
                .expect("load queue after delete"),
            vec![day_2]
        );
    }

    #[sqlx::test]
    async fn replace_active_install_day_bitmaps_round_trips_through_bitmap_loaders(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let day = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid day");
        let total = RoaringTreemap::from_iter([0_u64, 1, 2]);
        let dim1_rows = vec![
            ActiveInstallBitmapDim1Input {
                dim1_key: "platform".to_owned(),
                dim1_value: Some("ios".to_owned()),
                bitmap: RoaringTreemap::from_iter([0_u64, 2]),
            },
            ActiveInstallBitmapDim1Input {
                dim1_key: "app_version".to_owned(),
                dim1_value: None,
                bitmap: RoaringTreemap::from_iter([1_u64]),
            },
        ];
        let dim2_rows = vec![ActiveInstallBitmapDim2Input {
            dim1_key: "app_version".to_owned(),
            dim1_value: None,
            dim2_key: "platform".to_owned(),
            dim2_value: Some("ios".to_owned()),
            bitmap: RoaringTreemap::from_iter([1_u64]),
        }];

        let mut tx = pool.begin().await.expect("begin bitmap write tx");
        replace_active_install_day_bitmaps_in_tx(
            &mut tx,
            project_id(),
            day,
            &total,
            &dim1_rows,
            &dim2_rows,
        )
        .await
        .expect("replace day bitmaps");
        tx.commit().await.expect("commit bitmap write tx");

        let totals = load_active_install_bitmap_totals_in_executor(&pool, project_id(), &[day])
            .await
            .expect("load total bitmaps");
        let platform_rows = load_active_install_bitmap_dim1_in_executor(
            &pool,
            project_id(),
            "platform",
            Some("ios"),
            &[day],
        )
        .await
        .expect("load platform dim1 bitmaps");
        let null_app_version_rows = load_active_install_bitmap_dim1_in_executor(
            &pool,
            project_id(),
            "app_version",
            None,
            &[day],
        )
        .await
        .expect("load null app_version dim1 bitmaps");
        let dim2 = load_active_install_bitmap_dim2_in_executor(
            &pool,
            project_id(),
            "app_version",
            None,
            "platform",
            Some("ios"),
            &[day],
        )
        .await
        .expect("load dim2 bitmaps");

        assert_eq!(totals.len(), 1);
        assert_eq!(totals[0].bitmap, total);
        assert_eq!(totals[0].cardinality, 3);
        assert_eq!(platform_rows.len(), 1);
        assert_eq!(platform_rows[0].bitmap, dim1_rows[0].bitmap);
        assert_eq!(platform_rows[0].cardinality, 2);
        assert_eq!(null_app_version_rows.len(), 1);
        assert_eq!(null_app_version_rows[0].bitmap, dim1_rows[1].bitmap);
        assert_eq!(null_app_version_rows[0].cardinality, 1);
        assert_eq!(dim2.len(), 1);
        assert_eq!(dim2[0].bitmap, dim2_rows[0].bitmap);
        assert_eq!(dim2[0].cardinality, 1);
    }

    #[sqlx::test]
    async fn active_install_bitmap_reads_use_bounded_indexes(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let day = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid day");
        let total = RoaringTreemap::from_iter([0_u64]);
        let dim1_rows = (0..20_000)
            .map(|offset| ActiveInstallBitmapDim1Input {
                dim1_key: "platform".to_owned(),
                dim1_value: Some(format!("platform-{offset}")),
                bitmap: RoaringTreemap::from_iter([offset as u64]),
            })
            .collect::<Vec<_>>();
        let dim2_rows = (0..20_000)
            .map(|offset| ActiveInstallBitmapDim2Input {
                dim1_key: "app_version".to_owned(),
                dim1_value: Some(format!("app-{offset}")),
                dim2_key: "platform".to_owned(),
                dim2_value: Some(format!("platform-{offset}")),
                bitmap: RoaringTreemap::from_iter([offset as u64]),
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin bitmap seed tx");
        replace_active_install_day_bitmaps_in_tx(
            &mut tx,
            project_id(),
            day,
            &total,
            &dim1_rows,
            &dim2_rows,
        )
        .await
        .expect("seed bitmap rows");
        tx.commit().await.expect("commit bitmap seed tx");

        sqlx::query("ANALYZE active_install_daily_bitmaps_dim1")
            .execute(&pool)
            .await
            .expect("analyze dim1");
        sqlx::query("ANALYZE active_install_daily_bitmaps_dim2")
            .execute(&pool)
            .await
            .expect("analyze dim2");

        let dim1_plan_row = sqlx::query(
            r#"
            EXPLAIN (FORMAT JSON)
            SELECT bitmap
            FROM active_install_daily_bitmaps_dim1
            WHERE project_id = $1
              AND dim1_key = 'platform'
              AND dim1_value_is_null = FALSE
              AND dim1_value = 'platform-19999'
              AND day = ANY($2)
            "#,
        )
        .bind(project_id())
        .bind(vec![day])
        .fetch_one(&pool)
        .await
        .expect("explain dim1 read");
        let dim1_plan = dim1_plan_row
            .try_get::<Value, _>(0)
            .expect("dim1 explain json");
        let dim1_root = explain_plan_root(&dim1_plan);
        let mut dim1_indexes = Vec::new();
        collect_plan_index_names(dim1_root, &mut dim1_indexes);

        assert!(
            !plan_contains_node_type(dim1_root, "Seq Scan"),
            "dim1 bitmap read should avoid sequential scans: {dim1_plan:#}"
        );
        assert!(
            dim1_indexes
                .iter()
                .any(|name| name == "idx_active_install_daily_bitmaps_dim1_value"),
            "dim1 bitmap read should use the value index, got {dim1_indexes:?} from {dim1_plan:#}"
        );

        let dim2_plan_row = sqlx::query(
            r#"
            EXPLAIN (FORMAT JSON)
            SELECT bitmap
            FROM active_install_daily_bitmaps_dim2
            WHERE project_id = $1
              AND dim1_key = 'app_version'
              AND dim2_key = 'platform'
              AND dim2_value_is_null = FALSE
              AND dim2_value = 'platform-19999'
              AND day = ANY($2)
            "#,
        )
        .bind(project_id())
        .bind(vec![day])
        .fetch_one(&pool)
        .await
        .expect("explain dim2 filtered read");
        let dim2_plan = dim2_plan_row
            .try_get::<Value, _>(0)
            .expect("dim2 explain json");
        let dim2_root = explain_plan_root(&dim2_plan);
        let mut dim2_indexes = Vec::new();
        collect_plan_index_names(dim2_root, &mut dim2_indexes);

        assert!(
            !plan_contains_node_type(dim2_root, "Seq Scan"),
            "dim2 bitmap read should avoid sequential scans: {dim2_plan:#}"
        );
        assert!(
            dim2_indexes
                .iter()
                .any(|name| name == "idx_active_install_daily_bitmaps_dim2_filter_dim2"),
            "dim2 bitmap read should use the later-dimension filter index, got {dim2_indexes:?} from {dim2_plan:#}"
        );
    }

    #[sqlx::test]
    async fn live_install_counts_use_bounded_read_indexes(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        sqlx::query(
            r#"
            INSERT INTO live_install_state (project_id, install_id, last_received_at)
            SELECT
                $1,
                'install-' || install_offset,
                TIMESTAMPTZ '2026-01-01T00:00:00Z'
                    + (install_offset % 60) * INTERVAL '1 minute'
            FROM generate_series(0, 19999) AS install_offset
            "#,
        )
        .bind(project_id())
        .execute(&pool)
        .await
        .expect("insert live_install_state rows");
        sqlx::query("ANALYZE live_install_state")
            .execute(&pool)
            .await
            .expect("analyze live_install_state");

        let row = sqlx::query(
            r#"
            EXPLAIN (FORMAT JSON)
            WITH snapshot AS (
                SELECT TIMESTAMPTZ '2026-01-01T00:59:00Z' AS as_of
            )
            SELECT snapshot.as_of, COUNT(live.install_id)::BIGINT
            FROM snapshot
            LEFT JOIN live_install_state live
              ON live.project_id = $1
             AND live.last_received_at >= snapshot.as_of - INTERVAL '120 seconds'
            GROUP BY snapshot.as_of
            "#,
        )
        .bind(project_id())
        .fetch_one(&pool)
        .await
        .expect("explain live installs query");
        let plan = row
            .try_get::<Value, _>(0)
            .expect("EXPLAIN returns json plan");
        let root = explain_plan_root(&plan);
        let mut index_names = Vec::new();
        collect_plan_index_names(root, &mut index_names);

        assert!(
            !plan_contains_node_type(root, "Seq Scan"),
            "live install count should avoid sequential scans: {plan:#}"
        );
        assert!(
            index_names
                .iter()
                .any(|name| name == "idx_live_install_state_project_received_at"),
            "live install count should use the received_at index, got {index_names:?} from {plan:#}"
        );
    }

    #[sqlx::test]
    async fn live_install_count_as_of_uses_statement_timestamp(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let mut tx = pool.begin().await.expect("begin transaction");
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .expect("set repeatable read");

        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let lower_bound = Utc::now() - chrono::Duration::milliseconds(200);
        let (as_of, value) = load_live_install_count_in_tx(&mut tx, project_id(), 120)
            .await
            .expect("load live installs");

        assert_eq!(value, 0);
        assert!(
            as_of >= lower_bound,
            "as_of should track the count statement time, got {as_of} with lower bound {lower_bound}"
        );
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
    async fn batch_insert_install_first_seen_inserts_only_new_rows(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let existing = InstallFirstSeenRecord {
            project_id: project_id(),
            install_id: "install-1".to_owned(),
            first_seen_event_id: 1,
            first_seen_at: timestamp(0),
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
            os_version: Some("18.3".to_owned()),
            locale: Some("en-GB".to_owned()),
        };

        let mut seed_tx = pool.begin().await.expect("begin seed tx");
        assert!(
            insert_install_first_seen_in_tx(&mut seed_tx, &existing)
                .await
                .expect("insert existing row"),
            "seed insert should create the first row"
        );
        seed_tx.commit().await.expect("commit seed tx");

        let new_record = InstallFirstSeenRecord {
            install_id: "install-2".to_owned(),
            first_seen_event_id: 2,
            first_seen_at: timestamp(5),
            ..existing.clone()
        };

        let mut tx = pool.begin().await.expect("begin batch tx");
        let inserted = insert_install_first_seen_many_in_tx(
            &mut tx,
            &[existing.clone(), new_record.clone(), new_record.clone()],
        )
        .await
        .expect("batch insert first-seen rows");
        tx.commit().await.expect("commit batch tx");

        assert_eq!(inserted, vec![new_record.install_id.clone()]);

        let stored_installs = sqlx::query_scalar::<_, String>(
            r#"
            SELECT install_id
            FROM install_first_seen
            WHERE project_id = $1
            ORDER BY install_id ASC
            "#,
        )
        .bind(project_id())
        .fetch_all(&pool)
        .await
        .expect("fetch stored installs");
        assert_eq!(
            stored_installs,
            vec!["install-1".to_owned(), "install-2".to_owned()]
        );
    }

    #[sqlx::test]
    async fn batch_update_session_tails_updates_all_requested_rows(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let mut seed_tx = pool.begin().await.expect("begin seed tx");
        insert_session_in_tx(&mut seed_tx, &session("install-1", 0, 10))
            .await
            .expect("insert install-1 session");
        insert_session_in_tx(&mut seed_tx, &session("install-2", 15, 30))
            .await
            .expect("insert install-2 session");
        seed_tx.commit().await.expect("commit seed tx");

        let mut tx = pool.begin().await.expect("begin update tx");
        let updated_rows = update_session_tails_in_tx(
            &mut tx,
            &[
                SessionTailUpdate {
                    project_id: project_id(),
                    session_id: "install-1:2026-01-01T00:00:00+00:00",
                    session_end: timestamp(20),
                    event_count: 3,
                    duration_seconds: 20 * 60,
                },
                SessionTailUpdate {
                    project_id: project_id(),
                    session_id: "install-2:2026-01-01T00:15:00+00:00",
                    session_end: timestamp(45),
                    event_count: 4,
                    duration_seconds: 30 * 60,
                },
            ],
        )
        .await
        .expect("batch update tails");
        tx.commit().await.expect("commit update tx");

        assert_eq!(updated_rows, 2);

        let rows = sqlx::query(
            r#"
            SELECT session_id, session_end, event_count, duration_seconds
            FROM sessions
            WHERE project_id = $1
            ORDER BY session_id ASC
            "#,
        )
        .bind(project_id())
        .fetch_all(&pool)
        .await
        .expect("fetch updated sessions");

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].try_get::<DateTime<Utc>, _>("session_end").unwrap(),
            timestamp(20)
        );
        assert_eq!(rows[0].try_get::<i32, _>("event_count").unwrap(), 3);
        assert_eq!(
            rows[0].try_get::<i32, _>("duration_seconds").unwrap(),
            20 * 60
        );
        assert_eq!(
            rows[1].try_get::<DateTime<Utc>, _>("session_end").unwrap(),
            timestamp(45)
        );
        assert_eq!(rows[1].try_get::<i32, _>("event_count").unwrap(), 4);
        assert_eq!(
            rows[1].try_get::<i32, _>("duration_seconds").unwrap(),
            30 * 60
        );
    }

    #[sqlx::test]
    async fn batch_upsert_install_session_states_updates_existing_rows(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("ensure local project succeeds");

        let mut tx = pool.begin().await.expect("begin first upsert tx");
        upsert_install_session_states_in_tx(
            &mut tx,
            &[
                tail_state(10, 2),
                InstallSessionStateRecord {
                    project_id: project_id(),
                    install_id: "install-2".to_owned(),
                    tail_session_id: "install-2:2026-01-01T00:00:00+00:00".to_owned(),
                    tail_session_start: timestamp(0),
                    tail_session_end: timestamp(15),
                    tail_event_count: 2,
                    tail_duration_seconds: 15 * 60,
                    tail_day: NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"),
                    last_processed_event_id: 2,
                },
            ],
        )
        .await
        .expect("initial batch upsert");
        tx.commit().await.expect("commit first upsert tx");

        let mut update_tx = pool.begin().await.expect("begin update upsert tx");
        upsert_install_session_states_in_tx(
            &mut update_tx,
            &[
                InstallSessionStateRecord {
                    tail_session_end: timestamp(20),
                    tail_event_count: 3,
                    tail_duration_seconds: 20 * 60,
                    last_processed_event_id: 3,
                    ..tail_state(10, 2)
                },
                InstallSessionStateRecord {
                    project_id: project_id(),
                    install_id: "install-2".to_owned(),
                    tail_session_id: "install-2:2026-01-01T00:00:00+00:00".to_owned(),
                    tail_session_start: timestamp(0),
                    tail_session_end: timestamp(25),
                    tail_event_count: 4,
                    tail_duration_seconds: 25 * 60,
                    tail_day: NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date"),
                    last_processed_event_id: 4,
                },
            ],
        )
        .await
        .expect("update batch upsert");
        update_tx.commit().await.expect("commit update upsert tx");

        let states = sqlx::query(
            r#"
            SELECT install_id, tail_event_count, tail_duration_seconds, last_processed_event_id
            FROM install_session_state
            WHERE project_id = $1
            ORDER BY install_id ASC
            "#,
        )
        .bind(project_id())
        .fetch_all(&pool)
        .await
        .expect("fetch stored states");

        assert_eq!(states.len(), 2);
        assert_eq!(
            states[0].try_get::<String, _>("install_id").unwrap(),
            "install-1"
        );
        assert_eq!(states[0].try_get::<i32, _>("tail_event_count").unwrap(), 3);
        assert_eq!(
            states[0]
                .try_get::<i32, _>("tail_duration_seconds")
                .unwrap(),
            20 * 60
        );
        assert_eq!(
            states[0]
                .try_get::<i64, _>("last_processed_event_id")
                .unwrap(),
            3
        );
        assert_eq!(
            states[1].try_get::<String, _>("install_id").unwrap(),
            "install-2"
        );
        assert_eq!(states[1].try_get::<i32, _>("tail_event_count").unwrap(), 4);
        assert_eq!(
            states[1]
                .try_get::<i32, _>("tail_duration_seconds")
                .unwrap(),
            25 * 60
        );
        assert_eq!(
            states[1]
                .try_get::<i64, _>("last_processed_event_id")
                .unwrap(),
            4
        );
    }

    #[sqlx::test]
    async fn insert_sessions_handle_batches_above_postgres_bind_limit(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let sessions = (0..5_958)
            .map(|index| {
                session(
                    &format!("install-{index}"),
                    index as i64 * 2,
                    index as i64 * 2 + 1,
                )
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin tx");
        let inserted_rows = insert_sessions_in_tx(&mut tx, &sessions)
            .await
            .expect("large session insert should succeed");
        tx.commit().await.expect("commit tx");

        assert_eq!(inserted_rows, sessions.len() as u64);

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS row_count,
                COALESCE(SUM(event_count), 0)::BIGINT AS total_events,
                COALESCE(SUM(duration_seconds), 0)::BIGINT AS total_duration
            FROM sessions
            WHERE project_id = $1
            "#,
        )
        .bind(project_id())
        .fetch_one(&pool)
        .await
        .expect("fetch session counts");

        assert_eq!(
            row.try_get::<i64, _>("row_count").expect("row_count"),
            5_958
        );
        assert_eq!(
            row.try_get::<i64, _>("total_events").expect("total_events"),
            11_916
        );
        assert_eq!(
            row.try_get::<i64, _>("total_duration")
                .expect("total_duration"),
            357_480
        );
    }

    #[sqlx::test]
    async fn session_daily_install_upserts_handle_batches_above_postgres_bind_limit(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let target_day = day(0);
        let deltas = (0..16_384)
            .map(|index| SessionDailyInstallDelta {
                project_id: project_id(),
                day: target_day,
                install_id: format!("install-{index}"),
                session_count: 1,
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin tx");
        let active_install_deltas = upsert_session_daily_install_deltas_in_tx(&mut tx, &deltas)
            .await
            .expect("large session daily install upsert should succeed");
        tx.commit().await.expect("commit tx");

        assert_eq!(
            active_install_deltas,
            vec![SessionDailyActiveInstallDelta {
                project_id: project_id(),
                day: target_day,
                active_installs: 16_384,
            }]
        );

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS row_count,
                COALESCE(SUM(session_count), 0)::BIGINT AS total_sessions
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(target_day)
        .fetch_one(&pool)
        .await
        .expect("fetch session daily install counts");

        assert_eq!(
            row.try_get::<i64, _>("row_count").expect("row_count"),
            16_384
        );
        assert_eq!(
            row.try_get::<i64, _>("total_sessions")
                .expect("total_sessions"),
            16_384
        );
    }

    #[sqlx::test]
    async fn session_daily_upserts_handle_batches_above_postgres_bind_limit(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let deltas = (0..13_108)
            .map(|index| SessionDailyDelta {
                project_id: project_id(),
                day: day(index as i64),
                sessions_count: 1,
                active_installs: 1,
                total_duration_seconds: 60,
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin tx");
        let affected_rows = upsert_session_daily_deltas_in_tx(&mut tx, &deltas)
            .await
            .expect("large session daily upsert should succeed");
        tx.commit().await.expect("commit tx");

        assert_eq!(affected_rows, deltas.len() as u64);

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS row_count,
                COALESCE(SUM(sessions_count), 0)::BIGINT AS total_sessions,
                COALESCE(SUM(active_installs), 0)::BIGINT AS total_active_installs,
                COALESCE(SUM(total_duration_seconds), 0)::BIGINT AS total_duration
            FROM session_daily
            WHERE project_id = $1
            "#,
        )
        .bind(project_id())
        .fetch_one(&pool)
        .await
        .expect("fetch session daily counts");

        assert_eq!(
            row.try_get::<i64, _>("row_count").expect("row_count"),
            13_108
        );
        assert_eq!(
            row.try_get::<i64, _>("total_sessions")
                .expect("total_sessions"),
            13_108
        );
        assert_eq!(
            row.try_get::<i64, _>("total_active_installs")
                .expect("total_active_installs"),
            13_108
        );
        assert_eq!(
            row.try_get::<i64, _>("total_duration")
                .expect("total_duration"),
            786_480
        );
    }

    #[sqlx::test]
    async fn session_daily_duration_updates_handle_batches_above_postgres_bind_limit(pool: PgPool) {
        run_migrations(&pool).await.expect("migrations succeed");
        ensure_local_project(&pool, Some(&bootstrap_config()))
            .await
            .expect("seed project");

        let existing_rows = (0..21_846)
            .map(|index| SessionDailyRecord {
                project_id: project_id(),
                day: day(index as i64),
                sessions_count: 1,
                active_installs: 1,
                total_duration_seconds: 10,
            })
            .collect::<Vec<_>>();
        let deltas = (0..21_846)
            .map(|index| SessionDailyDurationDelta {
                project_id: project_id(),
                day: day(index as i64),
                duration_delta: 60,
            })
            .collect::<Vec<_>>();

        let mut tx = pool.begin().await.expect("begin tx");
        let seeded_rows = insert_session_daily_rows_for_test(&mut tx, &existing_rows)
            .await
            .expect("seed session daily rows");
        assert_eq!(seeded_rows, existing_rows.len() as u64);
        let updated_rows = add_session_daily_duration_deltas_in_tx(&mut tx, &deltas)
            .await
            .expect("large session daily duration update should succeed");
        tx.commit().await.expect("commit tx");

        assert_eq!(updated_rows, deltas.len() as u64);

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS row_count,
                COALESCE(SUM(total_duration_seconds), 0)::BIGINT AS total_duration
            FROM session_daily
            WHERE project_id = $1
            "#,
        )
        .bind(project_id())
        .fetch_one(&pool)
        .await
        .expect("fetch session daily duration counts");

        assert_eq!(
            row.try_get::<i64, _>("row_count").expect("row_count"),
            21_846
        );
        assert_eq!(
            row.try_get::<i64, _>("total_duration")
                .expect("total_duration"),
            1_529_220
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

    #[test]
    fn insert_events_with_authenticated_ingest_keeps_lease_claim_outside_insert_transaction() {
        let source = include_str!("lib.rs");
        let start = source
            .find("pub async fn insert_events_with_authenticated_ingest")
            .expect("find insert_events_with_authenticated_ingest");
        let end = source
            .find("pub async fn insert_events(")
            .expect("find insert_events");
        let helper = &source[start..end];
        let lease_claim = helper
            .find("let lease = claim_project_processing_lease(")
            .expect("find out-of-transaction lease claim");
        let begin_tx = helper
            .find("let mut tx = pool.begin().await?")
            .expect("find insert transaction start");

        assert!(
            lease_claim < begin_tx,
            "authenticated ingest should claim the ingest lease before opening the insert transaction"
        );
        assert!(
            !helper.contains("claim_project_processing_lease_in_tx"),
            "authenticated ingest should not widen the hot insert transaction with an in-transaction lease claim"
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
