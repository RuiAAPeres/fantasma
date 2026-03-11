use std::time::Duration;

use fantasma_auth::{derive_key_prefix, hash_ingest_key};
use fantasma_core::{EventCountQuery, EventPayload};
pub use sqlx::PgPool;
use sqlx::{QueryBuilder, Row, postgres::PgPoolOptions};
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
        "CREATE INDEX IF NOT EXISTS idx_events_event_name ON events_raw(event_name)",
        "CREATE INDEX IF NOT EXISTS idx_events_received_at ON events_raw(received_at)",
        "CREATE INDEX IF NOT EXISTS idx_events_platform ON events_raw(platform)",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }

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
