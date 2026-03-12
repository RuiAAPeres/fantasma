use std::collections::{BTreeMap, BTreeSet};

use chrono::{Duration as ChronoDuration, NaiveDate};
use fantasma_store::{
    EventCountDailyDim1Delta, EventCountDailyDim2Delta, EventCountDailyDim3Delta,
    EventCountDailyTotalDelta, InstallSessionStateRecord, PgPool, RawEventRecord, SessionRecord,
    SessionTailUpdate, StoreError, add_session_daily_duration_delta_in_tx,
    delete_sessions_overlapping_window_in_tx, fetch_events_after,
    fetch_events_for_install_between_in_tx, fetch_latest_session_for_install_in_tx,
    fetch_sessions_overlapping_window_in_tx, increment_session_daily_for_new_session_in_tx,
    insert_session_in_tx, load_install_session_state_in_tx, lock_worker_offset,
    rebuild_session_daily_days_in_tx, save_worker_offset_in_tx, update_session_tail_in_tx,
    upsert_event_count_daily_dim1_in_tx, upsert_event_count_daily_dim2_in_tx,
    upsert_event_count_daily_dim3_in_tx, upsert_event_count_daily_totals_in_tx,
    upsert_install_session_state_in_tx,
};
use uuid::Uuid;

use crate::sessionization;

const SESSION_TIMEOUT_MINS: i64 = 30;
const SESSION_WORKER_NAME: &str = "sessions";
const EVENT_METRICS_WORKER_NAME: &str = "event_metrics";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecomputeWindow {
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
}

pub async fn process_session_batch(pool: &PgPool, batch_size: i64) -> Result<usize, StoreError> {
    let mut tx = pool.begin().await.map_err(StoreError::from)?;
    let last_processed_event_id = lock_worker_offset(&mut tx, SESSION_WORKER_NAME).await?;
    let batch = fetch_events_after(pool, last_processed_event_id, batch_size).await?;
    let processed_events = batch.len();
    let mut touched_days_by_project = BTreeMap::<Uuid, BTreeSet<NaiveDate>>::new();

    if batch.is_empty() {
        tx.commit().await.map_err(StoreError::from)?;
        return Ok(0);
    }

    let next_offset = batch
        .last()
        .map(|event| event.id)
        .expect("non-empty batch has last event");

    for ((project_id, install_id), events) in group_events(batch) {
        let touched_days = process_install_batch(&mut tx, project_id, &install_id, events).await?;

        if !touched_days.is_empty() {
            touched_days_by_project
                .entry(project_id)
                .or_default()
                .extend(touched_days);
        }
    }

    for (project_id, touched_days) in touched_days_by_project {
        let days = touched_days.into_iter().collect::<Vec<_>>();
        rebuild_session_daily_days_in_tx(&mut tx, project_id, &days).await?;
    }

    save_worker_offset_in_tx(&mut tx, SESSION_WORKER_NAME, next_offset).await?;
    tx.commit().await.map_err(StoreError::from)?;

    Ok(processed_events)
}

pub async fn process_event_metrics_batch(
    pool: &PgPool,
    batch_size: i64,
) -> Result<usize, StoreError> {
    let mut tx = pool.begin().await.map_err(StoreError::from)?;
    let last_processed_event_id = lock_worker_offset(&mut tx, EVENT_METRICS_WORKER_NAME).await?;
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
    let rollups = build_event_metric_rollups(&batch);

    upsert_event_count_daily_totals_in_tx(&mut tx, &rollups.total_deltas).await?;
    upsert_event_count_daily_dim1_in_tx(&mut tx, &rollups.dim1_deltas).await?;
    upsert_event_count_daily_dim2_in_tx(&mut tx, &rollups.dim2_deltas).await?;
    upsert_event_count_daily_dim3_in_tx(&mut tx, &rollups.dim3_deltas).await?;
    save_worker_offset_in_tx(&mut tx, EVENT_METRICS_WORKER_NAME, next_offset).await?;
    tx.commit().await.map_err(StoreError::from)?;

    Ok(processed_events)
}

#[derive(Debug, Default)]
struct EventMetricRollups {
    total_deltas: Vec<EventCountDailyTotalDelta>,
    dim1_deltas: Vec<EventCountDailyDim1Delta>,
    dim2_deltas: Vec<EventCountDailyDim2Delta>,
    dim3_deltas: Vec<EventCountDailyDim3Delta>,
}

fn build_event_metric_rollups(batch: &[RawEventRecord]) -> EventMetricRollups {
    let mut totals = BTreeMap::<(Uuid, NaiveDate, String), i64>::new();
    let mut dim1 = BTreeMap::<(Uuid, NaiveDate, String, String, String), i64>::new();
    let mut dim2 =
        BTreeMap::<(Uuid, NaiveDate, String, String, String, String, String), i64>::new();
    let mut dim3 = BTreeMap::<
        (
            Uuid,
            NaiveDate,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ),
        i64,
    >::new();

    for event in batch {
        let day = event.timestamp.date_naive();
        let event_name = event.event_name.clone();
        *totals
            .entry((event.project_id, day, event_name.clone()))
            .or_default() += 1;

        let dimensions = event_metric_dimensions(event);

        for dimension in &dimensions {
            *dim1
                .entry((
                    event.project_id,
                    day,
                    event_name.clone(),
                    dimension.0.clone(),
                    dimension.1.clone(),
                ))
                .or_default() += 1;
        }

        for left_index in 0..dimensions.len() {
            for right_index in left_index + 1..dimensions.len() {
                let left = &dimensions[left_index];
                let right = &dimensions[right_index];
                *dim2
                    .entry((
                        event.project_id,
                        day,
                        event_name.clone(),
                        left.0.clone(),
                        left.1.clone(),
                        right.0.clone(),
                        right.1.clone(),
                    ))
                    .or_default() += 1;
            }
        }

        for first_index in 0..dimensions.len() {
            for second_index in first_index + 1..dimensions.len() {
                for third_index in second_index + 1..dimensions.len() {
                    let first = &dimensions[first_index];
                    let second = &dimensions[second_index];
                    let third = &dimensions[third_index];
                    *dim3
                        .entry((
                            event.project_id,
                            day,
                            event_name.clone(),
                            first.0.clone(),
                            first.1.clone(),
                            second.0.clone(),
                            second.1.clone(),
                            third.0.clone(),
                            third.1.clone(),
                        ))
                        .or_default() += 1;
                }
            }
        }
    }

    EventMetricRollups {
        total_deltas: totals
            .into_iter()
            .map(
                |((project_id, day, event_name), event_count)| EventCountDailyTotalDelta {
                    project_id,
                    day,
                    event_name,
                    event_count,
                },
            )
            .collect(),
        dim1_deltas: dim1
            .into_iter()
            .map(
                |((project_id, day, event_name, dim1_key, dim1_value), event_count)| {
                    EventCountDailyDim1Delta {
                        project_id,
                        day,
                        event_name,
                        dim1_key,
                        dim1_value,
                        event_count,
                    }
                },
            )
            .collect(),
        dim2_deltas: dim2
            .into_iter()
            .map(
                |(
                    (project_id, day, event_name, dim1_key, dim1_value, dim2_key, dim2_value),
                    event_count,
                )| EventCountDailyDim2Delta {
                    project_id,
                    day,
                    event_name,
                    dim1_key,
                    dim1_value,
                    dim2_key,
                    dim2_value,
                    event_count,
                },
            )
            .collect(),
        dim3_deltas: dim3
            .into_iter()
            .map(
                |(
                    (
                        project_id,
                        day,
                        event_name,
                        dim1_key,
                        dim1_value,
                        dim2_key,
                        dim2_value,
                        dim3_key,
                        dim3_value,
                    ),
                    event_count,
                )| EventCountDailyDim3Delta {
                    project_id,
                    day,
                    event_name,
                    dim1_key,
                    dim1_value,
                    dim2_key,
                    dim2_value,
                    dim3_key,
                    dim3_value,
                    event_count,
                },
            )
            .collect(),
    }
}

fn event_metric_dimensions(event: &RawEventRecord) -> Vec<(String, String)> {
    let mut dimensions = Vec::with_capacity(3 + event.properties.len());

    dimensions.push((
        "platform".to_owned(),
        platform_dimension_value(&event.platform),
    ));

    if let Some(app_version) = event.app_version.as_ref() {
        dimensions.push(("app_version".to_owned(), app_version.clone()));
    }

    if let Some(os_version) = event.os_version.as_ref() {
        dimensions.push(("os_version".to_owned(), os_version.clone()));
    }

    dimensions.extend(
        event
            .properties
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    dimensions.sort_by(|left, right| left.0.cmp(&right.0));

    dimensions
}

fn platform_dimension_value(platform: &fantasma_core::Platform) -> String {
    match platform {
        fantasma_core::Platform::Ios => "ios".to_owned(),
        fantasma_core::Platform::Android => "android".to_owned(),
    }
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
) -> Result<BTreeSet<NaiveDate>, StoreError> {
    let tail_state = load_install_session_state_in_tx(tx, project_id, install_id).await?;

    if needs_exact_day_repair(tail_state.as_ref(), &events) {
        return repair_install_batch(tx, project_id, install_id, events).await;
    }

    process_install_batch_incremental(tx, project_id, install_id, events, tail_state).await?;

    Ok(BTreeSet::new())
}

fn needs_exact_day_repair(
    tail_state: Option<&InstallSessionStateRecord>,
    events: &[RawEventRecord],
) -> bool {
    let Some(tail_state) = tail_state else {
        return false;
    };

    events
        .iter()
        .any(|event| event.timestamp < tail_state.tail_session_end)
}

async fn process_install_batch_incremental(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    project_id: Uuid,
    install_id: &str,
    events: Vec<RawEventRecord>,
    mut tail_state: Option<InstallSessionStateRecord>,
) -> Result<(), StoreError> {
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
                return Err(StoreError::InvariantViolation(format!(
                    "incremental path received older-than-tail event for project {project_id} install {install_id}"
                )));
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

async fn repair_install_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    project_id: Uuid,
    install_id: &str,
    batch_events: Vec<RawEventRecord>,
) -> Result<BTreeSet<NaiveDate>, StoreError> {
    let batch_min_ts = batch_events
        .first()
        .map(|event| event.timestamp)
        .expect("install batch contains at least one event");
    let batch_max_ts = batch_events
        .last()
        .map(|event| event.timestamp)
        .expect("install batch contains at least one event");

    let recompute_window =
        load_recompute_window_in_tx(tx, project_id, install_id, batch_min_ts, batch_max_ts).await?;
    let raw_events = fetch_events_for_install_between_in_tx(
        tx,
        project_id,
        install_id,
        recompute_window.start,
        recompute_window.end,
    )
    .await?;
    let overlapping_sessions = fetch_sessions_overlapping_window_in_tx(
        tx,
        project_id,
        install_id,
        recompute_window.start,
        recompute_window.end,
    )
    .await?;
    let repaired_sessions = derive_sessions_for_window(&raw_events);
    let touched_days = overlapping_sessions
        .iter()
        .map(session_daily_day)
        .chain(repaired_sessions.iter().map(session_daily_day))
        .collect::<BTreeSet<_>>();

    delete_sessions_overlapping_window_in_tx(
        tx,
        project_id,
        install_id,
        recompute_window.start,
        recompute_window.end,
    )
    .await?;

    for session in &repaired_sessions {
        insert_session_in_tx(tx, session).await?;
    }

    let latest_session = fetch_latest_session_for_install_in_tx(tx, project_id, install_id).await?;
    let Some(latest_session) = latest_session else {
        return Err(StoreError::InvariantViolation(format!(
            "repaired install {install_id} for project {project_id} has no latest session"
        )));
    };

    let next_state = install_state_from_session(&latest_session);
    upsert_install_session_state_in_tx(tx, &next_state).await?;

    Ok(touched_days)
}

async fn load_recompute_window_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    project_id: Uuid,
    install_id: &str,
    batch_min_ts: chrono::DateTime<chrono::Utc>,
    batch_max_ts: chrono::DateTime<chrono::Utc>,
) -> Result<RecomputeWindow, StoreError> {
    let mut search_start = batch_min_ts - ChronoDuration::minutes(SESSION_TIMEOUT_MINS);
    let mut search_end = batch_max_ts + ChronoDuration::minutes(SESSION_TIMEOUT_MINS);

    loop {
        let sessions = fetch_sessions_overlapping_window_in_tx(
            tx,
            project_id,
            install_id,
            search_start,
            search_end,
        )
        .await?;

        if sessions.is_empty() {
            return Ok(RecomputeWindow {
                start: batch_min_ts,
                end: batch_max_ts,
            });
        }

        let next = recompute_window_from_sessions(&sessions, batch_max_ts);
        let next_search_start = next.start - ChronoDuration::minutes(SESSION_TIMEOUT_MINS);
        let next_search_end = next.end + ChronoDuration::minutes(SESSION_TIMEOUT_MINS);

        if next_search_start == search_start && next_search_end == search_end {
            return Ok(next);
        }

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

fn derive_sessions_for_window(raw_events: &[RawEventRecord]) -> Vec<SessionRecord> {
    let session_events = raw_events
        .iter()
        .map(|event| sessionization::SessionEvent {
            project_id: event.project_id,
            timestamp: event.timestamp,
            install_id: event.install_id.clone(),
            platform: event.platform.clone(),
            app_version: event.app_version.clone(),
        })
        .collect::<Vec<_>>();

    sessionization::derive_sessions(&session_events)
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

fn session_daily_day(session: &SessionRecord) -> NaiveDate {
    session.session_start.date_naive()
}

fn session_from_event(event: &RawEventRecord) -> SessionRecord {
    SessionRecord {
        project_id: event.project_id,
        install_id: event.install_id.clone(),
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
    use std::collections::BTreeMap;

    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use fantasma_core::{EventPayload, Platform};
    use fantasma_store::{
        BootstrapConfig, RawEventRecord, average_session_duration_seconds, count_active_installs,
        count_sessions, fetch_latest_session_for_install, insert_events,
        load_install_session_state, load_worker_offset,
    };
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
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
            os_version: Some("18.3".to_owned()),
            properties: BTreeMap::new(),
        }
    }

    fn raw_event_with_max_dimensions() -> RawEventRecord {
        RawEventRecord {
            id: 1,
            project_id: project_id(),
            event_name: "app_open".to_owned(),
            timestamp: timestamp(1, 0, 0),
            install_id: "install-1".to_owned(),
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
            os_version: Some("18.3".to_owned()),
            properties: BTreeMap::from([
                ("plan".to_owned(), "pro".to_owned()),
                ("provider".to_owned(), "strava".to_owned()),
                ("region".to_owned(), "eu".to_owned()),
            ]),
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

    async fn session_daily_snapshot(
        pool: &PgPool,
        day: NaiveDate,
    ) -> Option<(i64, i64, i64, DateTime<Utc>)> {
        sqlx::query(
            r#"
            SELECT sessions_count, active_installs, total_duration_seconds, updated_at
            FROM session_daily
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(day)
        .fetch_optional(pool)
        .await
        .expect("fetch session_daily snapshot")
        .map(|row| {
            (
                row.try_get::<i64, _>("sessions_count")
                    .expect("sessions count"),
                row.try_get::<i64, _>("active_installs")
                    .expect("active installs"),
                row.try_get::<i64, _>("total_duration_seconds")
                    .expect("duration"),
                row.try_get::<DateTime<Utc>, _>("updated_at")
                    .expect("updated_at"),
            )
        })
    }

    #[test]
    fn max_dimension_event_metrics_fanout_stays_bounded() {
        let rollups = build_event_metric_rollups(&[raw_event_with_max_dimensions()]);
        let total_fanout = rollups.total_deltas.len()
            + rollups.dim1_deltas.len()
            + rollups.dim2_deltas.len()
            + rollups.dim3_deltas.len();

        assert_eq!(rollups.total_deltas.len(), 1);
        assert_eq!(rollups.dim1_deltas.len(), 6);
        assert_eq!(rollups.dim2_deltas.len(), 15);
        assert_eq!(rollups.dim3_deltas.len(), 20);
        assert_eq!(total_fanout, 42);
        assert!(
            rollups
                .dim3_deltas
                .iter()
                .all(|delta| delta.event_count == 1),
            "single events must increment each bounded cube exactly once"
        );
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
    async fn older_than_tail_event_repairs_historical_sessions(pool: PgPool) {
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

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 20)])
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
            1
        );
        let repaired_session = sqlx::query(
            r#"
            SELECT session_start, session_end, event_count, duration_seconds
            FROM sessions
            WHERE project_id = $1
              AND install_id = $2
            "#,
        )
        .bind(project_id())
        .bind("install-1")
        .fetch_one(&pool)
        .await
        .expect("fetch repaired session");
        assert_eq!(
            repaired_session
                .try_get::<chrono::DateTime<Utc>, _>("session_start")
                .unwrap(),
            timestamp(1, 0, 0)
        );
        assert_eq!(
            repaired_session
                .try_get::<chrono::DateTime<Utc>, _>("session_end")
                .unwrap(),
            timestamp(1, 0, 45)
        );
        assert_eq!(
            repaired_session.try_get::<i32, _>("event_count").unwrap(),
            3
        );
        assert_eq!(
            repaired_session
                .try_get::<i32, _>("duration_seconds")
                .unwrap(),
            45 * 60
        );
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 45 * 60))
        );
    }

    #[sqlx::test]
    async fn cross_midnight_repair_stays_bucketed_on_session_start_day(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[
                event("install-1", 1, 23, 55),
                event("install-1", 2, 0, 20),
                event("install-1", 2, 1, 0),
            ],
        )
        .await
        .expect("insert events");

        process_session_batch(&pool, 100)
            .await
            .expect("process batch");

        let jan_2_before =
            session_daily_snapshot(&pool, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap())
                .await
                .expect("january 2 row exists");

        insert_events(&pool, project_id(), &[event("install-1", 2, 0, 25)])
            .await
            .expect("insert late cross-midnight event");
        process_session_batch(&pool, 100)
            .await
            .expect("process repair batch");

        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load tail")
            .expect("tail exists");
        assert_eq!(tail.tail_day, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap());
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 30 * 60))
        );
        assert_eq!(
            session_daily_snapshot(&pool, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()).await,
            Some(jan_2_before)
        );
    }

    #[sqlx::test]
    async fn out_of_order_repairs_rebuild_only_touched_days(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[
                event("install-a", 1, 0, 0),
                event("install-a", 1, 0, 45),
                event("install-b", 2, 12, 0),
                event("install-c", 3, 0, 0),
                event("install-c", 3, 0, 45),
            ],
        )
        .await
        .expect("insert seed events");
        process_session_batch(&pool, 100)
            .await
            .expect("process seed batch");

        let day_2 = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        let untouched_middle_day = session_daily_snapshot(&pool, day_2)
            .await
            .expect("middle day row exists");

        insert_events(
            &pool,
            project_id(),
            &[event("install-a", 1, 0, 20), event("install-c", 3, 0, 20)],
        )
        .await
        .expect("insert late repair events");
        process_session_batch(&pool, 100)
            .await
            .expect("process repair batch");

        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 45 * 60))
        );
        assert_eq!(
            session_daily_snapshot(&pool, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()).await,
            Some(untouched_middle_day)
        );
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 3).unwrap()).await,
            Some((1, 1, 45 * 60))
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
            load_worker_offset(&pool, SESSION_WORKER_NAME)
                .await
                .expect("load offset"),
            2
        );
    }
}
