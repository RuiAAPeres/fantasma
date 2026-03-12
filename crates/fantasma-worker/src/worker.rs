use std::collections::BTreeMap;

use chrono::Duration as ChronoDuration;
use fantasma_store::{
    InstallSessionStateRecord, PgPool, RawEventRecord, SessionRecord, SessionTailUpdate,
    StoreError, add_session_daily_duration_delta_in_tx, fetch_events_after,
    increment_session_daily_for_new_session_in_tx, insert_session_in_tx,
    load_install_session_state_in_tx, lock_worker_offset, save_worker_offset_in_tx,
    update_session_tail_in_tx, upsert_install_session_state_in_tx,
};
use uuid::Uuid;

const SESSION_TIMEOUT_MINS: i64 = 30;
const WORKER_NAME: &str = "sessions";

pub async fn process_session_batch(pool: &PgPool, batch_size: i64) -> Result<usize, StoreError> {
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
        process_install_batch(&mut tx, project_id, &install_id, events).await?;
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

async fn process_install_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    project_id: Uuid,
    install_id: &str,
    events: Vec<RawEventRecord>,
) -> Result<(), StoreError> {
    let mut tail_state = load_install_session_state_in_tx(tx, project_id, install_id).await?;

    for event in events {
        match tail_state.as_mut() {
            None => {
                let session = session_from_event(&event);
                insert_session_in_tx(tx, &session).await?;
                increment_session_daily_for_new_session_in_tx(
                    tx,
                    session.project_id,
                    session.session_start.date_naive(),
                    &session.install_id,
                    session.duration_seconds as i64,
                )
                .await?;

                let state = install_state_from_session(&session);
                upsert_install_session_state_in_tx(tx, &state).await?;
                tail_state = Some(state);
            }
            Some(state) if event.timestamp < state.tail_session_end => {
                // Older-than-tail raw events remain stored in events_raw but do not mutate derived state.
                continue;
            }
            Some(state) => {
                let gap = event
                    .timestamp
                    .signed_duration_since(state.tail_session_end);

                if gap <= ChronoDuration::minutes(SESSION_TIMEOUT_MINS) {
                    extend_tail_session(tx, state, &event).await?;
                } else {
                    let session = session_from_event(&event);
                    insert_session_in_tx(tx, &session).await?;
                    increment_session_daily_for_new_session_in_tx(
                        tx,
                        session.project_id,
                        session.session_start.date_naive(),
                        &session.install_id,
                        session.duration_seconds as i64,
                    )
                    .await?;

                    let next_state = install_state_from_session(&session);
                    upsert_install_session_state_in_tx(tx, &next_state).await?;
                    *state = next_state;
                }
            }
        }
    }

    Ok(())
}

async fn extend_tail_session(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &mut InstallSessionStateRecord,
    event: &RawEventRecord,
) -> Result<(), StoreError> {
    let new_end = state.tail_session_end.max(event.timestamp);
    let new_duration = new_end
        .signed_duration_since(state.tail_session_start)
        .num_seconds() as i32;
    let duration_delta = i64::from(new_duration - state.tail_duration_seconds);
    let new_event_count = state.tail_event_count + 1;

    let updated_session_rows = update_session_tail_in_tx(
        tx,
        SessionTailUpdate {
            project_id: state.project_id,
            session_id: &state.tail_session_id,
            session_end: new_end,
            event_count: new_event_count,
            duration_seconds: new_duration,
            user_id: event.user_id.as_deref(),
            app_version: event.app_version.as_deref(),
        },
    )
    .await?;
    ensure_single_row_update(
        updated_session_rows,
        format!(
            "tail session {} missing for project {} install {}",
            state.tail_session_id, state.project_id, state.install_id
        ),
    )?;

    if duration_delta > 0 {
        let updated_daily_rows = add_session_daily_duration_delta_in_tx(
            tx,
            state.project_id,
            state.tail_day,
            duration_delta,
        )
        .await?;
        ensure_single_row_update(
            updated_daily_rows,
            format!(
                "session_daily missing for project {} day {}",
                state.project_id, state.tail_day
            ),
        )?;
    }

    state.tail_session_end = new_end;
    state.tail_event_count = new_event_count;
    state.tail_duration_seconds = new_duration;
    upsert_install_session_state_in_tx(tx, state).await?;

    Ok(())
}

fn ensure_single_row_update(rows_affected: u64, message: String) -> Result<(), StoreError> {
    if rows_affected == 1 {
        return Ok(());
    }

    Err(StoreError::InvariantViolation(message))
}

fn session_from_event(event: &RawEventRecord) -> SessionRecord {
    SessionRecord {
        project_id: event.project_id,
        install_id: event.install_id.clone(),
        user_id: event.user_id.clone(),
        session_id: format!("{}:{}", event.install_id, event.timestamp.to_rfc3339()),
        session_start: event.timestamp,
        session_end: event.timestamp,
        event_count: 1,
        duration_seconds: 0,
        platform: event.platform.clone(),
        app_version: event.app_version.clone(),
    }
}

fn install_state_from_session(session: &SessionRecord) -> InstallSessionStateRecord {
    InstallSessionStateRecord {
        project_id: session.project_id,
        install_id: session.install_id.clone(),
        tail_session_id: session.session_id.clone(),
        tail_session_start: session.session_start,
        tail_session_end: session.session_end,
        tail_event_count: session.event_count,
        tail_duration_seconds: session.duration_seconds,
        tail_day: session.session_start.date_naive(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone, Utc};
    use fantasma_core::{EventPayload, Platform};
    use fantasma_store::{
        BootstrapConfig, average_session_duration_seconds, count_active_installs, count_sessions,
        fetch_latest_session_for_install, insert_events, load_install_session_state,
        load_worker_offset,
    };
    use serde_json::Map;
    use sqlx::Row;

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

    fn timestamp(day: u32, hour: u32, minute: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, day, hour, minute, 0)
            .single()
            .expect("valid timestamp")
    }

    fn event(install_id: &str, day: u32, hour: u32, minute: u32) -> EventPayload {
        EventPayload {
            event: "app_open".to_owned(),
            timestamp: timestamp(day, hour, minute),
            install_id: install_id.to_owned(),
            user_id: Some("user-1".to_owned()),
            session_id: None,
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
            properties: Map::new(),
        }
    }

    async fn session_daily_row(pool: &PgPool, day: NaiveDate) -> Option<(i64, i64, i64)> {
        sqlx::query(
            r#"
            SELECT sessions_count, active_installs, total_duration_seconds
            FROM session_daily
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(day)
        .fetch_optional(pool)
        .await
        .expect("fetch session_daily row")
        .map(|row| {
            (
                row.try_get::<i64, _>("sessions_count")
                    .expect("sessions count"),
                row.try_get::<i64, _>("active_installs")
                    .expect("active installs"),
                row.try_get::<i64, _>("total_duration_seconds")
                    .expect("duration"),
            )
        })
    }

    #[sqlx::test]
    async fn first_event_creates_session_and_tail_state(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 0)])
            .await
            .expect("insert event");

        let processed = process_session_batch(&pool, 100)
            .await
            .expect("process batch");

        assert_eq!(processed, 1);
        assert_eq!(
            count_sessions(
                &pool,
                project_id(),
                timestamp(1, 0, 0),
                timestamp(1, 23, 59)
            )
            .await
            .expect("count sessions"),
            1
        );
        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load tail")
            .expect("tail exists");
        assert_eq!(tail.tail_session_start, timestamp(1, 0, 0));
        assert_eq!(tail.tail_session_end, timestamp(1, 0, 0));
        assert_eq!(tail.tail_event_count, 1);
        let memberships = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())
        .fetch_one(&pool)
        .await
        .expect("count memberships");
        assert_eq!(memberships, 1);
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 0))
        );
    }

    #[sqlx::test]
    async fn later_event_within_thirty_minutes_extends_tail_in_place(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 0, 0), event("install-1", 1, 0, 10)],
        )
        .await
        .expect("insert events");

        process_session_batch(&pool, 100)
            .await
            .expect("process batch");

        assert_eq!(
            count_sessions(
                &pool,
                project_id(),
                timestamp(1, 0, 0),
                timestamp(1, 23, 59)
            )
            .await
            .expect("count sessions"),
            1
        );
        let session = fetch_latest_session_for_install(&pool, project_id(), "install-1")
            .await
            .expect("fetch session")
            .expect("session exists");
        assert_eq!(session.session_end, timestamp(1, 0, 10));
        assert_eq!(session.event_count, 2);
        assert_eq!(session.duration_seconds, 600);
        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load tail")
            .expect("tail exists");
        assert_eq!(tail.tail_session_end, timestamp(1, 0, 10));
        assert_eq!(tail.tail_event_count, 2);
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 600))
        );
    }

    #[sqlx::test]
    async fn later_event_beyond_thirty_minutes_starts_new_session(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 0, 0), event("install-1", 1, 0, 45)],
        )
        .await
        .expect("insert events");

        process_session_batch(&pool, 100)
            .await
            .expect("process batch");

        assert_eq!(
            count_sessions(
                &pool,
                project_id(),
                timestamp(1, 0, 0),
                timestamp(1, 23, 59)
            )
            .await
            .expect("count sessions"),
            2
        );
        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load tail")
            .expect("tail exists");
        assert_eq!(tail.tail_session_start, timestamp(1, 0, 45));
        assert_eq!(tail.tail_event_count, 1);
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((2, 1, 0))
        );
    }

    #[sqlx::test]
    async fn older_than_tail_event_does_not_modify_historical_sessions(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 0, 0), event("install-1", 1, 0, 45)],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 10)])
            .await
            .expect("insert older event");
        process_session_batch(&pool, 100)
            .await
            .expect("process older event batch");

        assert_eq!(
            count_sessions(
                &pool,
                project_id(),
                timestamp(1, 0, 0),
                timestamp(1, 23, 59)
            )
            .await
            .expect("count sessions"),
            2
        );
        let first_session = sqlx::query(
            r#"
            SELECT event_count, duration_seconds
            FROM sessions
            WHERE project_id = $1
              AND session_start = $2
            "#,
        )
        .bind(project_id())
        .bind(timestamp(1, 0, 0))
        .fetch_one(&pool)
        .await
        .expect("fetch first session");
        assert_eq!(first_session.try_get::<i32, _>("event_count").unwrap(), 1);
        assert_eq!(
            first_session.try_get::<i32, _>("duration_seconds").unwrap(),
            0
        );
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((2, 1, 0))
        );
    }

    #[sqlx::test]
    async fn cross_midnight_extension_stays_bucketed_on_session_start_day(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 23, 55), event("install-1", 2, 0, 10)],
        )
        .await
        .expect("insert events");

        process_session_batch(&pool, 100)
            .await
            .expect("process batch");

        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load tail")
            .expect("tail exists");
        assert_eq!(tail.tail_day, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap());
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 900))
        );
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()).await,
            None
        );
    }

    #[sqlx::test]
    async fn active_installs_daily_is_maintained_from_membership_state(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[
                event("install-1", 1, 0, 0),
                event("install-1", 1, 0, 45),
                event("install-2", 1, 1, 0),
            ],
        )
        .await
        .expect("insert events");

        process_session_batch(&pool, 100)
            .await
            .expect("process batch");

        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((3, 2, 0))
        );

        let memberships = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM session_daily_installs
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())
        .fetch_one(&pool)
        .await
        .expect("count memberships");

        assert_eq!(memberships, 2);
    }

    #[sqlx::test]
    async fn missing_tail_session_row_causes_hard_failure(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 0, 0), event("install-1", 1, 0, 10)],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        sqlx::query("DELETE FROM sessions WHERE project_id = $1 AND install_id = $2")
            .bind(project_id())
            .bind("install-1")
            .execute(&pool)
            .await
            .expect("delete session row");

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 20)])
            .await
            .expect("insert extension event");

        let error = process_session_batch(&pool, 100)
            .await
            .expect_err("drift should fail batch");

        assert!(matches!(error, StoreError::InvariantViolation(_)));
        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load tail state")
            .expect("tail state exists");
        assert_eq!(tail.tail_session_end, timestamp(1, 0, 10));
    }

    #[sqlx::test]
    async fn missing_session_daily_row_causes_hard_failure(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 0, 0), event("install-1", 1, 0, 10)],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        sqlx::query("DELETE FROM session_daily WHERE project_id = $1 AND day = $2")
            .bind(project_id())
            .bind(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())
            .execute(&pool)
            .await
            .expect("delete session_daily row");

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 20)])
            .await
            .expect("insert extension event");

        let error = process_session_batch(&pool, 100)
            .await
            .expect_err("missing session_daily should fail batch");

        assert!(matches!(error, StoreError::InvariantViolation(_)));
        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load tail state")
            .expect("tail state exists");
        assert_eq!(tail.tail_session_end, timestamp(1, 0, 10));
    }

    #[sqlx::test]
    async fn summary_metrics_remain_queryable_from_sessions(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 0, 0), event("install-2", 1, 0, 45)],
        )
        .await
        .expect("insert events");

        process_session_batch(&pool, 100)
            .await
            .expect("process batch");

        assert_eq!(
            count_active_installs(
                &pool,
                project_id(),
                timestamp(1, 0, 0),
                timestamp(1, 23, 59)
            )
            .await
            .expect("count installs"),
            2
        );
        assert_eq!(
            average_session_duration_seconds(
                &pool,
                project_id(),
                timestamp(1, 0, 0),
                timestamp(1, 23, 59)
            )
            .await
            .expect("average duration"),
            0
        );
        assert_eq!(
            load_worker_offset(&pool, WORKER_NAME)
                .await
                .expect("load offset"),
            2
        );
    }
}
