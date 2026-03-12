use std::{collections::BTreeMap, env, time::Duration};

use chrono::Duration as ChronoDuration;
use fantasma_store::{
    BootstrapConfig, DatabaseConfig, PgPool, RawEventRecord, SessionRecord, StoreError, bootstrap,
    connect, delete_sessions_for_install_between_in_tx, fetch_events_after,
    fetch_events_for_install_between, fetch_sessions_overlapping_window, lock_worker_offset,
    save_worker_offset_in_tx, upsert_sessions_in_tx,
};
use thiserror::Error;
use tracing::{error, info};
use uuid::Uuid;

mod sessionization;

const DEFAULT_BATCH_SIZE: i64 = 500;
const SESSION_TIMEOUT_MINS: i64 = 30;
const WORKER_NAME: &str = "sessions";

#[derive(Debug, Error)]
enum WorkerError {
    #[error(transparent)]
    Store(#[from] StoreError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecomputeWindow {
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug)]
struct DerivedInstallSessions {
    sessions: Vec<SessionRecord>,
    delete_window: Option<RecomputeWindow>,
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            env::var("FANTASMA_LOG_LEVEL").unwrap_or_else(|_| "fantasma_worker=info".to_owned()),
        )
        .try_init();

    let database_url = env::var("FANTASMA_DATABASE_URL").expect("FANTASMA_DATABASE_URL");
    let project_id = env::var("FANTASMA_PROJECT_ID")
        .ok()
        .and_then(|value| Uuid::parse_str(&value).ok())
        .unwrap_or_else(default_project_id);
    let project_name = env::var("FANTASMA_PROJECT_NAME")
        .unwrap_or_else(|_| "Local Development Project".to_owned());
    let ingest_key = env::var("FANTASMA_INGEST_KEY").ok();
    let poll_interval = env::var("FANTASMA_WORKER_POLL_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5_000);
    let batch_size = env::var("FANTASMA_WORKER_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(DEFAULT_BATCH_SIZE);
    let pool = connect(&DatabaseConfig::new(database_url))
        .await
        .expect("connect database");
    bootstrap(
        &pool,
        &BootstrapConfig {
            project_id,
            project_name,
            ingest_key,
        },
    )
    .await
    .expect("bootstrap database");

    info!("fantasma-worker started with {poll_interval}ms poll interval");

    loop {
        match process_session_batch(&pool, batch_size).await {
            Ok(processed) if processed > 0 => {
                info!("processed {processed} raw events into sessions");
            }
            Ok(_) => {
                info!("worker tick: no new raw events");
            }
            Err(err) => {
                error!(?err, "failed to process session batch");
            }
        }
        tokio::time::sleep(Duration::from_millis(poll_interval)).await;
    }
}

fn default_project_id() -> Uuid {
    Uuid::from_u128(0x9bad8b88_5e7a_44ed_98ce_4cf9ddde713a)
}

async fn process_session_batch(pool: &PgPool, batch_size: i64) -> Result<usize, WorkerError> {
    let mut tx = pool.begin().await.map_err(StoreError::from)?;
    let last_processed_event_id = lock_worker_offset(&mut tx, WORKER_NAME).await?;
    let batch = fetch_events_after(pool, last_processed_event_id, batch_size).await?;
    let processed_events = batch.len();

    if batch.is_empty() {
        tx.commit().await.map_err(StoreError::from)?;
        return Ok(0);
    }

    let next_offset = batch
        .last()
        .map(|event| event.id)
        .expect("non-empty batch has last event");

    for ((project_id, install_id), events) in group_events(batch) {
        let derived = derive_sessions_for_install(pool, project_id, &install_id, events).await?;

        if let Some(window) = derived.delete_window {
            delete_sessions_for_install_between_in_tx(
                &mut tx,
                project_id,
                &install_id,
                window.start,
                window.end,
            )
            .await?;
        }

        upsert_sessions_in_tx(&mut tx, &derived.sessions).await?;
    }

    save_worker_offset_in_tx(&mut tx, WORKER_NAME, next_offset).await?;
    tx.commit().await.map_err(StoreError::from)?;

    Ok(processed_events)
}

fn group_events(batch: Vec<RawEventRecord>) -> BTreeMap<(Uuid, String), Vec<RawEventRecord>> {
    let mut grouped = BTreeMap::new();

    for event in batch {
        grouped
            .entry((event.project_id, event.install_id.clone()))
            .or_insert_with(Vec::new)
            .push(event);
    }

    for events in grouped.values_mut() {
        events.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then(left.id.cmp(&right.id))
        });
    }

    grouped
}

async fn derive_sessions_for_install(
    pool: &PgPool,
    project_id: Uuid,
    install_id: &str,
    batch_events: Vec<RawEventRecord>,
) -> Result<DerivedInstallSessions, WorkerError> {
    let batch_min_ts = batch_events
        .first()
        .map(|event| event.timestamp)
        .expect("install batch contains at least one event");
    let batch_max_ts = batch_events
        .last()
        .map(|event| event.timestamp)
        .expect("install batch contains at least one event");

    let recompute_window =
        load_recompute_window(pool, project_id, install_id, batch_min_ts, batch_max_ts).await?;
    let raw_events = match recompute_window {
        Some(window) => {
            fetch_events_for_install_between(pool, project_id, install_id, window.start, window.end)
                .await?
        }
        None => batch_events,
    };

    let session_events = raw_events
        .iter()
        .map(|event| sessionization::SessionEvent {
            project_id: event.project_id,
            event_id: event.id,
            timestamp: event.timestamp,
            install_id: event.install_id.clone(),
            user_id: event.user_id.clone(),
            platform: event.platform.clone(),
            app_version: event.app_version.clone(),
        })
        .collect::<Vec<_>>();

    Ok(DerivedInstallSessions {
        sessions: sessionization::derive_sessions(&session_events, None)
            .into_iter()
            .map(|session| SessionRecord {
                project_id: session.project_id,
                install_id: session.install_id,
                user_id: session.user_id,
                session_id: session.session_id,
                session_start: session.session_start,
                session_end: session.session_end,
                event_count: session.event_count,
                duration_seconds: session.duration_seconds,
                platform: session.platform,
                app_version: session.app_version,
            })
            .collect(),
        delete_window: recompute_window,
    })
}

async fn load_recompute_window(
    pool: &PgPool,
    project_id: Uuid,
    install_id: &str,
    batch_min_ts: chrono::DateTime<chrono::Utc>,
    batch_max_ts: chrono::DateTime<chrono::Utc>,
) -> Result<Option<RecomputeWindow>, WorkerError> {
    let mut search_start = batch_min_ts - ChronoDuration::minutes(SESSION_TIMEOUT_MINS);
    let mut search_end = batch_max_ts + ChronoDuration::minutes(SESSION_TIMEOUT_MINS);
    let mut resolved = None;

    loop {
        let sessions = fetch_sessions_overlapping_window(
            pool,
            project_id,
            install_id,
            search_start,
            search_end,
        )
        .await?;

        if sessions.is_empty() {
            return Ok(resolved);
        }

        let next = recompute_window_from_sessions(&sessions, batch_max_ts);
        let next_search_start = next.start - ChronoDuration::minutes(SESSION_TIMEOUT_MINS);
        let next_search_end = next.end + ChronoDuration::minutes(SESSION_TIMEOUT_MINS);

        if resolved == Some(next)
            || (next_search_start == search_start && next_search_end == search_end)
        {
            return Ok(Some(next));
        }

        resolved = Some(next);
        search_start = next_search_start;
        search_end = next_search_end;
    }
}

fn recompute_window_from_sessions(
    sessions: &[SessionRecord],
    batch_max_ts: chrono::DateTime<chrono::Utc>,
) -> RecomputeWindow {
    let start = sessions
        .first()
        .map(|session| session.session_start)
        .expect("sessions window is non-empty");
    let end = sessions
        .iter()
        .map(|session| session.session_end)
        .max()
        .expect("sessions window is non-empty")
        .max(batch_max_ts);

    RecomputeWindow { start, end }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use fantasma_core::{EventPayload, Platform};
    use fantasma_store::{
        BootstrapConfig, average_session_duration_seconds, count_active_installs, count_sessions,
        fetch_latest_session_for_install, insert_events, load_worker_offset,
    };
    use serde_json::Map;

    fn project_id() -> Uuid {
        Uuid::from_u128(0x9bad8b88_5e7a_44ed_98ce_4cf9ddde713a)
    }

    fn bootstrap_config() -> BootstrapConfig {
        BootstrapConfig {
            project_id: project_id(),
            project_name: "Worker Test Project".to_owned(),
            ingest_key: Some("fg_ing_test".to_owned()),
        }
    }

    fn timestamp(hour: u32, minute: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, hour, minute, 0)
            .single()
            .expect("valid timestamp")
    }

    fn event(install_id: &str, hour: u32, minute: u32) -> EventPayload {
        EventPayload {
            event: "app_open".to_owned(),
            timestamp: timestamp(hour, minute),
            install_id: install_id.to_owned(),
            user_id: Some("user-1".to_owned()),
            session_id: None,
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
            properties: Map::new(),
        }
    }

    fn stored_session(
        start_hour: u32,
        start_minute: u32,
        end_hour: u32,
        end_minute: u32,
    ) -> SessionRecord {
        SessionRecord {
            project_id: project_id(),
            install_id: "install-1".to_owned(),
            user_id: Some("user-1".to_owned()),
            session_id: format!(
                "install-1:{}",
                timestamp(start_hour, start_minute).to_rfc3339()
            ),
            session_start: timestamp(start_hour, start_minute),
            session_end: timestamp(end_hour, end_minute),
            event_count: 2,
            duration_seconds: (timestamp(end_hour, end_minute)
                .signed_duration_since(timestamp(start_hour, start_minute))
                .num_seconds()) as i32,
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
        }
    }

    #[test]
    fn recompute_window_from_sessions_spans_late_event_bridge() {
        let window = recompute_window_from_sessions(
            &[stored_session(0, 0, 0, 10), stored_session(0, 50, 1, 0)],
            timestamp(0, 35),
        );

        assert_eq!(
            window,
            RecomputeWindow {
                start: timestamp(0, 0),
                end: timestamp(1, 0),
            }
        );
    }

    #[sqlx::test]
    async fn process_session_batch_derives_sessions_and_updates_tail(pool: PgPool) {
        bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");

        insert_events(
            &pool,
            project_id(),
            &[
                event("install-1", 0, 0),
                event("install-1", 0, 10),
                event("install-2", 0, 45),
            ],
        )
        .await
        .expect("insert initial events");

        let processed = process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        assert_eq!(processed, 3);
        assert_eq!(
            count_sessions(&pool, project_id(), timestamp(0, 0), timestamp(23, 59))
                .await
                .expect("count sessions"),
            2
        );
        assert_eq!(
            count_active_installs(&pool, project_id(), timestamp(0, 0), timestamp(23, 59))
                .await
                .expect("count active installs"),
            2
        );
        assert_eq!(
            average_session_duration_seconds(
                &pool,
                project_id(),
                timestamp(0, 0),
                timestamp(23, 59)
            )
            .await
            .expect("average duration"),
            300
        );
        assert_eq!(
            load_worker_offset(&pool, WORKER_NAME)
                .await
                .expect("load worker offset"),
            3
        );

        insert_events(&pool, project_id(), &[event("install-1", 0, 25)])
            .await
            .expect("insert late tail event");

        let processed = process_session_batch(&pool, 100)
            .await
            .expect("process tail update batch");

        assert_eq!(processed, 1);

        let install_one = fetch_latest_session_for_install(&pool, project_id(), "install-1")
            .await
            .expect("fetch install one session")
            .expect("session exists");

        assert_eq!(install_one.session_start, timestamp(0, 0));
        assert_eq!(install_one.session_end, timestamp(0, 25));
        assert_eq!(install_one.event_count, 3);
        assert_eq!(install_one.duration_seconds, 25 * 60);
        assert_eq!(
            load_worker_offset(&pool, WORKER_NAME)
                .await
                .expect("load worker offset"),
            4
        );
    }

    #[sqlx::test]
    async fn process_session_batch_repairs_late_event_before_current_tail(pool: PgPool) {
        bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");

        insert_events(
            &pool,
            project_id(),
            &[
                event("install-1", 0, 0),
                event("install-1", 0, 10),
                event("install-1", 0, 50),
                event("install-1", 1, 0),
            ],
        )
        .await
        .expect("insert initial events");

        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        assert_eq!(
            count_sessions(&pool, project_id(), timestamp(0, 0), timestamp(23, 59))
                .await
                .expect("count initial sessions"),
            2
        );

        insert_events(&pool, project_id(), &[event("install-1", 0, 35)])
            .await
            .expect("insert late bridging event");

        process_session_batch(&pool, 100)
            .await
            .expect("process repair batch");

        assert_eq!(
            count_sessions(&pool, project_id(), timestamp(0, 0), timestamp(23, 59))
                .await
                .expect("count repaired sessions"),
            1
        );

        let repaired = fetch_latest_session_for_install(&pool, project_id(), "install-1")
            .await
            .expect("fetch repaired session")
            .expect("repaired session exists");

        assert_eq!(repaired.session_start, timestamp(0, 0));
        assert_eq!(repaired.session_end, timestamp(1, 0));
        assert_eq!(repaired.event_count, 5);
        assert_eq!(repaired.duration_seconds, 60 * 60);
    }
}
