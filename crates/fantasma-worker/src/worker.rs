use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Timelike, Utc};
use fantasma_core::MetricGranularity;
use fantasma_store::{
    EventCountDailyDim1Delta, EventCountDailyDim2Delta, EventCountDailyDim3Delta,
    EventCountDailyTotalDelta, InstallFirstSeenRecord, InstallSessionStateRecord, PgPool,
    RawEventRecord, SessionDailyActiveInstallDelta, SessionDailyDelta, SessionDailyDurationDelta,
    SessionDailyInstallDelta, SessionMetricDim1Delta, SessionMetricDim2Delta,
    SessionMetricTotalDelta, SessionRecord, SessionTailUpdate, StoreError,
    add_session_daily_duration_deltas_in_tx, delete_pending_session_rebuild_buckets_in_tx,
    delete_sessions_overlapping_window_in_tx, enqueue_session_rebuild_buckets_in_tx,
    fetch_events_after_in_tx, fetch_events_for_install_between_in_tx,
    fetch_latest_session_for_install_in_tx, fetch_sessions_overlapping_window_in_tx,
    insert_install_first_seen_in_tx, insert_session_in_tx, insert_sessions_in_tx,
    load_install_session_states_in_tx, load_pending_session_rebuild_buckets_in_tx,
    load_tail_sessions_for_installs_in_tx, lock_worker_offset, rebuild_session_daily_days_in_tx,
    save_worker_offset_in_tx, update_session_tail_in_tx, upsert_event_count_daily_dim1_in_tx,
    upsert_event_count_daily_dim2_in_tx, upsert_event_count_daily_dim3_in_tx,
    upsert_event_count_daily_totals_in_tx, upsert_install_session_state_in_tx,
    upsert_session_daily_deltas_in_tx, upsert_session_daily_install_deltas_in_tx,
    upsert_session_metric_dim1_in_tx, upsert_session_metric_dim2_in_tx,
    upsert_session_metric_totals_in_tx,
};
use sqlx::{Postgres, Row, Transaction};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::sessionization;

const SESSION_TIMEOUT_MINS: i64 = 30;
pub(crate) const SESSION_WORKER_NAME: &str = "sessions";
pub(crate) const EVENT_METRICS_WORKER_NAME: &str = "event_metrics";
const DEFAULT_SESSION_INCREMENTAL_CONCURRENCY: usize = 8;
const DEFAULT_SESSION_REPAIR_CONCURRENCY: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecomputeWindow {
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Default)]
struct SessionRepairBuckets {
    days: BTreeSet<NaiveDate>,
    hours: BTreeSet<DateTime<Utc>>,
}

impl SessionRepairBuckets {
    fn insert_timestamp(&mut self, timestamp: DateTime<Utc>) {
        self.days.insert(timestamp.date_naive());
        self.hours.insert(bucket_start_for_granularity(
            timestamp,
            MetricGranularity::Hour,
        ));
    }

    fn insert_session(&mut self, session: &SessionRecord) {
        self.insert_timestamp(session.session_start);
    }

    fn is_empty(&self) -> bool {
        self.days.is_empty() && self.hours.is_empty()
    }

    fn day_bucket_starts(&self) -> Vec<DateTime<Utc>> {
        self.days
            .iter()
            .copied()
            .map(bucket_start_for_day)
            .collect()
    }

    fn hour_bucket_starts(&self) -> Vec<DateTime<Utc>> {
        self.hours.iter().copied().collect()
    }
}

#[derive(Debug)]
struct IncrementalSessionPlan {
    first_seen: Option<InstallFirstSeenRecord>,
    tail_update: Option<SessionRecord>,
    new_sessions: Vec<SessionRecord>,
    next_state: InstallSessionStateRecord,
    daily_install_deltas: Vec<SessionDailyInstallDelta>,
    daily_session_counts_by_day: BTreeMap<NaiveDate, i64>,
    daily_duration_deltas_by_day: BTreeMap<NaiveDate, i64>,
    session_metric_accumulator: SessionMetricAccumulator,
}

#[derive(Debug, Default)]
struct SessionMetricAccumulator {
    totals: BTreeMap<(MetricGranularity, DateTime<Utc>), SessionMetricDeltaValue>,
    dim1: BTreeMap<(MetricGranularity, DateTime<Utc>, String, String), SessionMetricDeltaValue>,
    dim2: BTreeMap<SessionMetricDim2Key, SessionMetricDeltaValue>,
}

#[derive(Debug, Clone, Copy, Default)]
struct SessionMetricDeltaValue {
    session_count: i64,
    duration_total_seconds: i64,
    new_installs: i64,
}

type SessionMetricDim2Key = (
    MetricGranularity,
    DateTime<Utc>,
    String,
    String,
    String,
    String,
);

impl SessionMetricAccumulator {
    fn add_session_delta(
        &mut self,
        session: &SessionRecord,
        session_count_delta: i64,
        duration_delta: i64,
    ) {
        for granularity in [MetricGranularity::Day, MetricGranularity::Hour] {
            let bucket_start = bucket_start_for_granularity(session.session_start, granularity);
            self.totals
                .entry((granularity, bucket_start))
                .or_default()
                .add(session_count_delta, duration_delta, 0);

            let platform_value = platform_dimension_value(&session.platform);
            self.dim1
                .entry((
                    granularity,
                    bucket_start,
                    "platform".to_owned(),
                    platform_value.clone(),
                ))
                .or_default()
                .add(session_count_delta, duration_delta, 0);

            if let Some(app_version) = session.app_version.as_ref() {
                self.dim1
                    .entry((
                        granularity,
                        bucket_start,
                        "app_version".to_owned(),
                        app_version.clone(),
                    ))
                    .or_default()
                    .add(session_count_delta, duration_delta, 0);
                self.dim2
                    .entry((
                        granularity,
                        bucket_start,
                        "app_version".to_owned(),
                        app_version.clone(),
                        "platform".to_owned(),
                        platform_value.clone(),
                    ))
                    .or_default()
                    .add(session_count_delta, duration_delta, 0);
            }
        }
    }

    fn add_new_install_delta(&mut self, first_seen: &InstallFirstSeenRecord) {
        for granularity in [MetricGranularity::Day, MetricGranularity::Hour] {
            let bucket_start = bucket_start_for_granularity(first_seen.first_seen_at, granularity);
            self.totals
                .entry((granularity, bucket_start))
                .or_default()
                .add(0, 0, 1);

            let platform_value = platform_dimension_value(&first_seen.platform);
            self.dim1
                .entry((
                    granularity,
                    bucket_start,
                    "platform".to_owned(),
                    platform_value.clone(),
                ))
                .or_default()
                .add(0, 0, 1);

            if let Some(app_version) = first_seen.app_version.as_ref() {
                self.dim1
                    .entry((
                        granularity,
                        bucket_start,
                        "app_version".to_owned(),
                        app_version.clone(),
                    ))
                    .or_default()
                    .add(0, 0, 1);
                self.dim2
                    .entry((
                        granularity,
                        bucket_start,
                        "app_version".to_owned(),
                        app_version.clone(),
                        "platform".to_owned(),
                        platform_value.clone(),
                    ))
                    .or_default()
                    .add(0, 0, 1);
            }
        }
    }

    fn into_store_deltas(
        self,
        project_id: Uuid,
    ) -> (
        Vec<SessionMetricTotalDelta>,
        Vec<SessionMetricDim1Delta>,
        Vec<SessionMetricDim2Delta>,
    ) {
        let totals = self
            .totals
            .into_iter()
            .map(
                |((granularity, bucket_start), value)| SessionMetricTotalDelta {
                    project_id,
                    granularity,
                    bucket_start,
                    session_count: value.session_count,
                    duration_total_seconds: value.duration_total_seconds,
                    new_installs: value.new_installs,
                },
            )
            .collect::<Vec<_>>();
        let dim1 = self
            .dim1
            .into_iter()
            .map(
                |((granularity, bucket_start, dim1_key, dim1_value), value)| {
                    SessionMetricDim1Delta {
                        project_id,
                        granularity,
                        bucket_start,
                        dim1_key,
                        dim1_value,
                        session_count: value.session_count,
                        duration_total_seconds: value.duration_total_seconds,
                        new_installs: value.new_installs,
                    }
                },
            )
            .collect::<Vec<_>>();
        let dim2 = self
            .dim2
            .into_iter()
            .map(
                |(
                    (granularity, bucket_start, dim1_key, dim1_value, dim2_key, dim2_value),
                    value,
                )| SessionMetricDim2Delta {
                    project_id,
                    granularity,
                    bucket_start,
                    dim1_key,
                    dim1_value,
                    dim2_key,
                    dim2_value,
                    session_count: value.session_count,
                    duration_total_seconds: value.duration_total_seconds,
                    new_installs: value.new_installs,
                },
            )
            .collect::<Vec<_>>();

        (totals, dim1, dim2)
    }
}

impl SessionMetricDeltaValue {
    fn add(&mut self, session_count: i64, duration_total_seconds: i64, new_installs: i64) {
        self.session_count += session_count;
        self.duration_total_seconds += duration_total_seconds;
        self.new_installs += new_installs;
    }
}

pub async fn process_session_batch(pool: &PgPool, batch_size: i64) -> Result<usize, StoreError> {
    Ok(process_session_batch_with_config(
        pool,
        SessionBatchConfig {
            batch_size,
            incremental_concurrency: DEFAULT_SESSION_INCREMENTAL_CONCURRENCY,
            repair_concurrency: DEFAULT_SESSION_REPAIR_CONCURRENCY,
        },
    )
    .await?
    .processed_events)
}

#[derive(Debug, Clone, Copy)]
pub struct SessionBatchConfig {
    pub batch_size: i64,
    pub incremental_concurrency: usize,
    pub repair_concurrency: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionBatchOutcome {
    pub(crate) processed_events: usize,
    pub(crate) advanced_offset: bool,
}

impl SessionBatchConfig {
    fn normalized(self) -> Self {
        Self {
            batch_size: self.batch_size.max(1),
            incremental_concurrency: self.incremental_concurrency.max(1),
            repair_concurrency: self.repair_concurrency.max(1),
        }
    }
}

#[derive(Debug)]
struct SessionWorkItem {
    project_id: Uuid,
    install_id: String,
    events: Vec<RawEventRecord>,
    tail_state: Option<InstallSessionStateRecord>,
    tail_session: Option<SessionRecord>,
    highest_processed_event_id: i64,
    kind: SessionWorkKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionWorkKind {
    Incremental,
    Repair,
}

pub(crate) async fn process_session_batch_with_config(
    pool: &PgPool,
    config: SessionBatchConfig,
) -> Result<SessionBatchOutcome, StoreError> {
    process_session_batch_inner(pool, config, SessionBatchHooks::default()).await
}

async fn process_session_batch_inner(
    pool: &PgPool,
    config: SessionBatchConfig,
    hooks: SessionBatchHooks,
) -> Result<SessionBatchOutcome, StoreError> {
    let config = config.normalized();
    let mut tx = pool.begin().await.map_err(StoreError::from)?;
    let last_processed_event_id = lock_worker_offset(&mut tx, SESSION_WORKER_NAME).await?;
    let batch =
        fetch_events_after_in_tx(&mut tx, last_processed_event_id, config.batch_size).await?;
    if batch.is_empty() {
        tx.commit().await.map_err(StoreError::from)?;
        return Ok(SessionBatchOutcome {
            processed_events: 0,
            advanced_offset: false,
        });
    }

    let next_offset = batch
        .last()
        .map(|event| event.id)
        .expect("non-empty batch has last event");
    let mut processed_events = 0_usize;
    let mut incremental_items = Vec::new();
    let mut repair_items = Vec::new();
    let grouped = group_events(batch);
    let project_ids = grouped
        .keys()
        .map(|(project_id, _)| *project_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let install_keys = grouped
        .keys()
        .map(|(project_id, install_id)| (*project_id, install_id.clone()))
        .collect::<Vec<_>>();
    let tail_states = load_install_session_states_in_tx(&mut tx, &install_keys).await?;
    let tail_sessions = load_tail_sessions_for_installs_in_tx(&mut tx, &install_keys).await?;

    for ((project_id, install_id), events) in grouped {
        let tail_state = tail_states.get(&(project_id, install_id.clone())).cloned();
        let tail_session = tail_sessions
            .get(&(project_id, install_id.clone()))
            .cloned();
        let events = filter_replayed_events(tail_state.as_ref(), events);
        if events.is_empty() {
            continue;
        }

        processed_events += events.len();
        let highest_processed_event_id = events
            .iter()
            .map(|event| event.id)
            .max()
            .expect("non-empty install batch has last event");
        let kind = if needs_exact_day_repair(tail_state.as_ref(), &events) {
            SessionWorkKind::Repair
        } else {
            SessionWorkKind::Incremental
        };
        let item = SessionWorkItem {
            project_id,
            install_id,
            events,
            tail_state,
            tail_session,
            highest_processed_event_id,
            kind,
        };

        match kind {
            SessionWorkKind::Incremental => incremental_items.push(item),
            SessionWorkKind::Repair => repair_items.push(item),
        }
    }

    let (incremental_queue, repair_queue) = tokio::join!(
        run_session_work_queue(
            pool.clone(),
            incremental_items,
            config.incremental_concurrency
        ),
        run_session_work_queue(pool.clone(), repair_items, config.repair_concurrency),
    );
    let incremental_queue = incremental_queue?;
    let repair_queue = repair_queue?;

    rebuild_pending_session_buckets_in_tx(&mut tx, &project_ids).await?;
    hooks.fail_before_coordinator_finalize()?;

    if let Some(error) = merge_session_queue_errors(incremental_queue.error, repair_queue.error) {
        tx.commit().await.map_err(StoreError::from)?;
        return Err(error);
    }

    save_worker_offset_in_tx(&mut tx, SESSION_WORKER_NAME, next_offset).await?;
    tx.commit().await.map_err(StoreError::from)?;

    Ok(SessionBatchOutcome {
        processed_events,
        advanced_offset: true,
    })
}

struct SessionWorkQueueResult {
    error: Option<StoreError>,
}

async fn run_session_work_queue(
    pool: PgPool,
    items: Vec<SessionWorkItem>,
    concurrency: usize,
) -> Result<SessionWorkQueueResult, StoreError> {
    if items.is_empty() {
        return Ok(SessionWorkQueueResult { error: None });
    }

    let mut work_items = items.into_iter();
    let mut work_set = JoinSet::new();
    let concurrency = concurrency.max(1);
    let mut error = None;

    for _ in 0..concurrency {
        let Some(item) = work_items.next() else {
            break;
        };
        let pool = pool.clone();
        work_set.spawn(async move { process_session_work_item(&pool, item).await });
    }

    while let Some(join_result) = work_set.join_next().await {
        match join_result {
            Ok(Ok(())) => {
                if error.is_none()
                    && let Some(item) = work_items.next()
                {
                    let pool = pool.clone();
                    work_set.spawn(async move { process_session_work_item(&pool, item).await });
                }
            }
            Ok(Err(join_error)) => {
                if error.is_none() {
                    error = Some(join_error);
                    work_set.abort_all();
                }
            }
            Err(join_error) if join_error.is_cancelled() => {}
            Err(join_error) => {
                if error.is_none() {
                    error = Some(StoreError::InvariantViolation(format!(
                        "session work item task failed: {join_error}"
                    )));
                    work_set.abort_all();
                }
            }
        }
    }

    Ok(SessionWorkQueueResult { error })
}

async fn process_session_work_item(pool: &PgPool, item: SessionWorkItem) -> Result<(), StoreError> {
    let mut tx = pool.begin().await.map_err(StoreError::from)?;
    let touched_buckets = match item.kind {
        SessionWorkKind::Incremental => {
            process_install_batch_incremental(
                &mut tx,
                item.project_id,
                &item.install_id,
                item.events,
                item.tail_state,
                item.tail_session,
            )
            .await?
        }
        SessionWorkKind::Repair => {
            repair_install_batch(
                &mut tx,
                item.project_id,
                &item.install_id,
                item.events,
                item.highest_processed_event_id,
            )
            .await?
        }
    };
    if item.kind == SessionWorkKind::Repair {
        enqueue_session_rebuild_buckets_in_tx(
            &mut tx,
            item.project_id,
            &touched_buckets.day_bucket_starts(),
            &touched_buckets.hour_bucket_starts(),
        )
        .await?;
    }
    tx.commit().await.map_err(StoreError::from)?;

    Ok(())
}

async fn rebuild_pending_session_buckets_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_ids: &[Uuid],
) -> Result<(), StoreError> {
    let pending_buckets = load_pending_session_rebuild_buckets_in_tx(tx, project_ids).await?;
    let mut touched_buckets_by_project = BTreeMap::<Uuid, SessionRepairBuckets>::new();

    for bucket in pending_buckets {
        let touched_buckets = touched_buckets_by_project
            .entry(bucket.project_id)
            .or_default();
        match bucket.granularity {
            MetricGranularity::Day => {
                touched_buckets
                    .days
                    .insert(bucket.bucket_start.date_naive());
            }
            MetricGranularity::Hour => {
                touched_buckets.hours.insert(bucket.bucket_start);
            }
        }
    }

    for (project_id, touched_buckets) in touched_buckets_by_project {
        if !touched_buckets.is_empty() {
            let days = touched_buckets.days.iter().copied().collect::<Vec<_>>();
            if !days.is_empty() {
                rebuild_session_daily_days_in_tx(tx, project_id, &days).await?;
                rebuild_session_metric_buckets_in_tx(
                    tx,
                    project_id,
                    MetricGranularity::Day,
                    &days
                        .iter()
                        .map(|day| bucket_start_for_day(*day))
                        .collect::<Vec<_>>(),
                )
                .await?;
            }

            let hours = touched_buckets.hours.into_iter().collect::<Vec<_>>();
            if !hours.is_empty() {
                rebuild_session_metric_buckets_in_tx(
                    tx,
                    project_id,
                    MetricGranularity::Hour,
                    &hours,
                )
                .await?;
            }
        }
    }

    delete_pending_session_rebuild_buckets_in_tx(tx, project_ids).await?;

    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct SessionBatchHooks {
    fail_before_coordinator_finalize: bool,
}

impl SessionBatchHooks {
    fn fail_before_coordinator_finalize(self) -> Result<(), StoreError> {
        if self.fail_before_coordinator_finalize {
            return Err(StoreError::InvariantViolation(
                "session batch hook forced coordinator finalize failure".to_owned(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default)]
struct SessionBatchTestHooks(SessionBatchHooks);

#[cfg(test)]
impl SessionBatchTestHooks {
    fn fail_before_coordinator_finalize() -> Self {
        Self(SessionBatchHooks {
            fail_before_coordinator_finalize: true,
        })
    }
}

#[cfg(test)]
async fn process_session_batch_with_hooks(
    pool: &PgPool,
    config: SessionBatchConfig,
    hooks: SessionBatchTestHooks,
) -> Result<SessionBatchOutcome, StoreError> {
    process_session_batch_inner(pool, config, hooks.0).await
}

fn merge_session_queue_errors(
    left: Option<StoreError>,
    right: Option<StoreError>,
) -> Option<StoreError> {
    match (left, right) {
        (Some(left), Some(right)) => Some(StoreError::InvariantViolation(format!(
            "{left}; secondary session queue failure: {right}"
        ))),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

pub async fn process_event_metrics_batch(
    pool: &PgPool,
    batch_size: i64,
) -> Result<usize, StoreError> {
    process_event_metrics_batch_with_config(pool, batch_size).await
}

pub(crate) async fn process_event_metrics_batch_with_config(
    pool: &PgPool,
    batch_size: i64,
) -> Result<usize, StoreError> {
    let batch_size = batch_size.max(1);
    let mut tx = pool.begin().await.map_err(StoreError::from)?;
    let last_processed_event_id = lock_worker_offset(&mut tx, EVENT_METRICS_WORKER_NAME).await?;
    let batch = fetch_events_after_in_tx(&mut tx, last_processed_event_id, batch_size).await?;
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
    let mut totals = BTreeMap::<(Uuid, MetricGranularity, DateTime<Utc>, String), i64>::new();
    let mut dim1 = BTreeMap::<
        (
            Uuid,
            MetricGranularity,
            DateTime<Utc>,
            String,
            String,
            String,
        ),
        i64,
    >::new();
    let mut dim2 = BTreeMap::<
        (
            Uuid,
            MetricGranularity,
            DateTime<Utc>,
            String,
            String,
            String,
            String,
            String,
        ),
        i64,
    >::new();
    let mut dim3 = BTreeMap::<
        (
            Uuid,
            MetricGranularity,
            DateTime<Utc>,
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
        let event_name = event.event_name.clone();
        let dimensions = event_metric_dimensions(event);
        for granularity in [MetricGranularity::Day, MetricGranularity::Hour] {
            let bucket_start = bucket_start_for_granularity(event.timestamp, granularity);

            *totals
                .entry((
                    event.project_id,
                    granularity,
                    bucket_start,
                    event_name.clone(),
                ))
                .or_default() += 1;

            for dimension in &dimensions {
                *dim1
                    .entry((
                        event.project_id,
                        granularity,
                        bucket_start,
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
                            granularity,
                            bucket_start,
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
                                granularity,
                                bucket_start,
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
    }

    EventMetricRollups {
        total_deltas: totals
            .into_iter()
            .map(
                |((project_id, granularity, bucket_start, event_name), event_count)| {
                    EventCountDailyTotalDelta {
                        project_id,
                        granularity,
                        bucket_start,
                        event_name,
                        event_count,
                    }
                },
            )
            .collect(),
        dim1_deltas: dim1
            .into_iter()
            .map(
                |(
                    (project_id, granularity, bucket_start, event_name, dim1_key, dim1_value),
                    event_count,
                )| {
                    EventCountDailyDim1Delta {
                        project_id,
                        granularity,
                        bucket_start,
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
                    (
                        project_id,
                        granularity,
                        bucket_start,
                        event_name,
                        dim1_key,
                        dim1_value,
                        dim2_key,
                        dim2_value,
                    ),
                    event_count,
                )| EventCountDailyDim2Delta {
                    project_id,
                    granularity,
                    bucket_start,
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
                        granularity,
                        bucket_start,
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
                    granularity,
                    bucket_start,
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

fn filter_replayed_events(
    tail_state: Option<&InstallSessionStateRecord>,
    events: Vec<RawEventRecord>,
) -> Vec<RawEventRecord> {
    let Some(tail_state) = tail_state else {
        return events;
    };

    events
        .into_iter()
        .filter(|event| event.id > tail_state.last_processed_event_id)
        .collect()
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
    tail_state: Option<InstallSessionStateRecord>,
    tail_session: Option<SessionRecord>,
) -> Result<SessionRepairBuckets, StoreError> {
    let plan =
        plan_incremental_session_batch(project_id, install_id, events, tail_state, tail_session)?;
    apply_incremental_session_plan(tx, plan).await?;

    Ok(SessionRepairBuckets::default())
}

fn plan_incremental_session_batch(
    project_id: Uuid,
    install_id: &str,
    events: Vec<RawEventRecord>,
    tail_state: Option<InstallSessionStateRecord>,
    tail_session: Option<SessionRecord>,
) -> Result<IncrementalSessionPlan, StoreError> {
    if events.is_empty() {
        return Err(StoreError::InvariantViolation(format!(
            "incremental session batch for project {project_id} install {install_id} was empty"
        )));
    }

    if let Some(state) = tail_state.as_ref()
        && tail_session.is_none()
    {
        return Err(StoreError::InvariantViolation(format!(
            "tail session {} missing for project {} install {}",
            state.tail_session_id, state.project_id, state.install_id
        )));
    }

    let first_seen =
        events
            .iter()
            .min_by_key(|event| event.id)
            .map(|event| InstallFirstSeenRecord {
                project_id,
                install_id: install_id.to_owned(),
                first_seen_event_id: event.id,
                first_seen_at: event.timestamp,
                platform: event.platform.clone(),
                app_version: event.app_version.clone(),
            });
    let highest_processed_event_id = events
        .iter()
        .map(|event| event.id)
        .max()
        .expect("non-empty events have a max id");
    let original_tail = tail_session.clone();
    let append_sessions = sessionization::derive_append_sessions(
        tail_session,
        &events
            .iter()
            .map(|event| sessionization::SessionEvent {
                project_id: event.project_id,
                timestamp: event.timestamp,
                install_id: event.install_id.clone(),
                platform: event.platform.clone(),
                app_version: event.app_version.clone(),
            })
            .collect::<Vec<_>>(),
    );
    let final_session = append_sessions
        .new_sessions
        .last()
        .cloned()
        .or_else(|| append_sessions.updated_tail.clone())
        .ok_or_else(|| {
            StoreError::InvariantViolation(format!(
                "incremental session batch for project {project_id} install {install_id} produced no sessions"
            ))
        })?;
    let next_state = install_state_from_session(&final_session, highest_processed_event_id);

    let mut session_metric_accumulator = SessionMetricAccumulator::default();
    let mut daily_session_counts_by_day = BTreeMap::<NaiveDate, i64>::new();
    let mut daily_duration_deltas_by_day = BTreeMap::<NaiveDate, i64>::new();
    let mut daily_install_counts = BTreeMap::<NaiveDate, i32>::new();

    if let (Some(original_tail), Some(updated_tail)) = (
        original_tail.as_ref(),
        append_sessions.updated_tail.as_ref(),
    ) {
        let duration_delta =
            i64::from(updated_tail.duration_seconds - original_tail.duration_seconds);
        if duration_delta > 0 {
            *daily_duration_deltas_by_day
                .entry(updated_tail.session_start.date_naive())
                .or_default() += duration_delta;
            session_metric_accumulator.add_session_delta(updated_tail, 0, duration_delta);
        }
    }

    for session in &append_sessions.new_sessions {
        let day = session.session_start.date_naive();
        *daily_session_counts_by_day.entry(day).or_default() += 1;
        *daily_duration_deltas_by_day.entry(day).or_default() +=
            i64::from(session.duration_seconds);
        *daily_install_counts.entry(day).or_default() += 1;
        session_metric_accumulator.add_session_delta(
            session,
            1,
            i64::from(session.duration_seconds),
        );
    }

    let daily_install_deltas = daily_install_counts
        .into_iter()
        .map(|(day, session_count)| SessionDailyInstallDelta {
            project_id,
            day,
            install_id: install_id.to_owned(),
            session_count,
        })
        .collect::<Vec<_>>();

    Ok(IncrementalSessionPlan {
        first_seen,
        tail_update: tail_update_from_append_sessions(original_tail, append_sessions.updated_tail),
        new_sessions: append_sessions.new_sessions,
        next_state,
        daily_install_deltas,
        daily_session_counts_by_day,
        daily_duration_deltas_by_day,
        session_metric_accumulator,
    })
}

async fn apply_incremental_session_plan(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    mut plan: IncrementalSessionPlan,
) -> Result<(), StoreError> {
    let first_seen_inserted = if let Some(first_seen) = plan.first_seen.as_ref() {
        insert_install_first_seen_in_tx(tx, first_seen).await?
    } else {
        false
    };
    if first_seen_inserted {
        let first_seen = plan
            .first_seen
            .as_ref()
            .expect("inserted first_seen requires a record");
        plan.session_metric_accumulator
            .add_new_install_delta(first_seen);
    }

    if let Some(tail_update) = plan.tail_update.as_ref() {
        let updated_session_rows = update_session_tail_in_tx(
            tx,
            SessionTailUpdate {
                project_id: tail_update.project_id,
                session_id: &tail_update.session_id,
                session_end: tail_update.session_end,
                event_count: tail_update.event_count,
                duration_seconds: tail_update.duration_seconds,
            },
        )
        .await?;
        ensure_rows_affected(
            updated_session_rows,
            1,
            format!(
                "tail session {} missing for project {} install {}",
                tail_update.session_id, tail_update.project_id, tail_update.install_id
            ),
        )?;
    }

    let inserted_sessions = insert_sessions_in_tx(tx, &plan.new_sessions).await?;
    ensure_rows_affected(
        inserted_sessions,
        plan.new_sessions.len() as u64,
        format!(
            "expected to insert {} sessions for project {} install {}",
            plan.new_sessions.len(),
            plan.next_state.project_id,
            plan.next_state.install_id
        ),
    )?;

    let active_install_deltas =
        upsert_session_daily_install_deltas_in_tx(tx, &plan.daily_install_deltas).await?;
    let (daily_upserts, duration_updates) = build_session_daily_deltas(
        plan.next_state.project_id,
        plan.daily_session_counts_by_day,
        plan.daily_duration_deltas_by_day,
        active_install_deltas,
    );
    let upserted_daily_rows = upsert_session_daily_deltas_in_tx(tx, &daily_upserts).await?;
    ensure_rows_affected(
        upserted_daily_rows,
        daily_upserts.len() as u64,
        format!(
            "expected to upsert {} session_daily rows for project {} install {}",
            daily_upserts.len(),
            plan.next_state.project_id,
            plan.next_state.install_id
        ),
    )?;
    let updated_duration_rows =
        add_session_daily_duration_deltas_in_tx(tx, &duration_updates).await?;
    ensure_rows_affected(
        updated_duration_rows,
        duration_updates.len() as u64,
        format!(
            "expected to update {} existing session_daily rows for project {} install {}",
            duration_updates.len(),
            plan.next_state.project_id,
            plan.next_state.install_id
        ),
    )?;

    let (session_total_deltas, session_dim1_deltas, session_dim2_deltas) = plan
        .session_metric_accumulator
        .into_store_deltas(plan.next_state.project_id);
    upsert_session_metric_totals_in_tx(tx, &session_total_deltas).await?;
    upsert_session_metric_dim1_in_tx(tx, &session_dim1_deltas).await?;
    upsert_session_metric_dim2_in_tx(tx, &session_dim2_deltas).await?;

    upsert_install_session_state_in_tx(tx, &plan.next_state).await?;

    Ok(())
}

async fn repair_install_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    project_id: Uuid,
    install_id: &str,
    batch_events: Vec<RawEventRecord>,
    highest_processed_event_id: i64,
) -> Result<SessionRepairBuckets, StoreError> {
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
    let mut repaired_sessions = derive_sessions_for_window(&raw_events);
    preserve_repaired_session_dimensions(&mut repaired_sessions, &overlapping_sessions);
    let mut touched_buckets = SessionRepairBuckets::default();
    for session in overlapping_sessions.iter().chain(repaired_sessions.iter()) {
        touched_buckets.insert_session(session);
    }

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

    let next_state = install_state_from_session(&latest_session, highest_processed_event_id);
    upsert_install_session_state_in_tx(tx, &next_state).await?;

    Ok(touched_buckets)
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

        let next = recompute_window_from_sessions(&sessions, batch_min_ts, batch_max_ts);
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
    batch_min_ts: chrono::DateTime<chrono::Utc>,
    batch_max_ts: chrono::DateTime<chrono::Utc>,
) -> RecomputeWindow {
    let start = sessions
        .first()
        .map(|session| session.session_start)
        .expect("sessions window is non-empty");
    let start = start.min(batch_min_ts);
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

fn preserve_repaired_session_dimensions(
    repaired_sessions: &mut [SessionRecord],
    existing_sessions: &[SessionRecord],
) {
    let mut existing_index = 0;

    for repaired_session in repaired_sessions {
        while existing_index < existing_sessions.len()
            && existing_sessions[existing_index].session_end < repaired_session.session_start
        {
            existing_index += 1;
        }

        if let Some(existing_session) = existing_sessions[existing_index..].iter().find(|session| {
            session.session_start <= repaired_session.session_end
                && session.session_end >= repaired_session.session_start
        }) {
            // Preserve grouped session dimensions from the earliest pre-repair session that
            // overlaps this rebuilt session so late events cannot rebucket existing rollups.
            repaired_session.platform = existing_session.platform.clone();
            repaired_session.app_version = existing_session.app_version.clone();
        }
    }
}

fn tail_update_from_append_sessions(
    original_tail: Option<SessionRecord>,
    updated_tail: Option<SessionRecord>,
) -> Option<SessionRecord> {
    match (original_tail, updated_tail) {
        (Some(original), Some(updated))
            if original.session_end != updated.session_end
                || original.event_count != updated.event_count
                || original.duration_seconds != updated.duration_seconds =>
        {
            Some(updated)
        }
        _ => None,
    }
}

fn build_session_daily_deltas(
    project_id: Uuid,
    daily_session_counts_by_day: BTreeMap<NaiveDate, i64>,
    mut daily_duration_deltas_by_day: BTreeMap<NaiveDate, i64>,
    active_install_deltas: Vec<SessionDailyActiveInstallDelta>,
) -> (Vec<SessionDailyDelta>, Vec<SessionDailyDurationDelta>) {
    let mut active_by_day = active_install_deltas
        .into_iter()
        .map(|delta| (delta.day, delta.active_installs))
        .collect::<BTreeMap<_, _>>();
    let mut daily_upserts = Vec::new();

    let days = daily_session_counts_by_day
        .keys()
        .copied()
        .chain(active_by_day.keys().copied())
        .collect::<BTreeSet<_>>();

    for day in days {
        let sessions_count = daily_session_counts_by_day
            .get(&day)
            .copied()
            .unwrap_or_default();
        let active_installs = active_by_day.remove(&day).unwrap_or_default();
        let total_duration_seconds = daily_duration_deltas_by_day
            .remove(&day)
            .unwrap_or_default();
        daily_upserts.push(SessionDailyDelta {
            project_id,
            day,
            sessions_count,
            active_installs,
            total_duration_seconds,
        });
    }

    let duration_updates = daily_duration_deltas_by_day
        .into_iter()
        .filter_map(|(day, duration_delta)| {
            (duration_delta != 0).then_some(SessionDailyDurationDelta {
                project_id,
                day,
                duration_delta,
            })
        })
        .collect::<Vec<_>>();

    (daily_upserts, duration_updates)
}

fn ensure_rows_affected(
    rows_affected: u64,
    expected: u64,
    message: String,
) -> Result<(), StoreError> {
    if rows_affected == expected {
        return Ok(());
    }

    Err(StoreError::InvariantViolation(message))
}

fn install_state_from_session(
    session: &SessionRecord,
    last_processed_event_id: i64,
) -> InstallSessionStateRecord {
    InstallSessionStateRecord {
        project_id: session.project_id,
        install_id: session.install_id.clone(),
        tail_session_id: session.session_id.clone(),
        tail_session_start: session.session_start,
        tail_session_end: session.session_end,
        tail_event_count: session.event_count,
        tail_duration_seconds: session.duration_seconds,
        tail_day: session.session_start.date_naive(),
        last_processed_event_id,
    }
}

fn bucket_start_for_granularity(
    timestamp: DateTime<Utc>,
    granularity: MetricGranularity,
) -> DateTime<Utc> {
    match granularity {
        MetricGranularity::Day => bucket_start_for_day(timestamp.date_naive()),
        MetricGranularity::Hour => DateTime::from_naive_utc_and_offset(
            timestamp
                .date_naive()
                .and_hms_opt(timestamp.hour(), 0, 0)
                .expect("top-of-hour is a valid UTC datetime"),
            Utc,
        ),
    }
}

fn bucket_start_for_day(day: NaiveDate) -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(
        day.and_hms_opt(0, 0, 0)
            .expect("midnight is a valid UTC datetime"),
        Utc,
    )
}

fn bucket_sql(granularity: MetricGranularity, column: &str) -> String {
    let precision = match granularity {
        MetricGranularity::Hour => "hour",
        MetricGranularity::Day => "day",
    };

    format!("date_trunc('{precision}', {column} AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'")
}

async fn rebuild_session_metric_buckets_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    granularity: MetricGranularity,
    bucket_starts: &[DateTime<Utc>],
) -> Result<(), StoreError> {
    let bucket_starts = bucket_starts
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if bucket_starts.is_empty() {
        return Ok(());
    }

    let bucket_expr = bucket_sql(granularity, "session_start");
    let first_seen_bucket_expr = bucket_sql(granularity, "first_seen_at");

    for table in [
        "session_metric_buckets_total",
        "session_metric_buckets_dim1",
        "session_metric_buckets_dim2",
    ] {
        sqlx::query(&format!(
            "DELETE FROM {table} WHERE project_id = $1 AND granularity = $2 AND bucket_start = ANY($3)"
        ))
        .bind(project_id)
        .bind(granularity.as_str())
        .bind(&bucket_starts)
        .execute(&mut **tx)
        .await?;
    }

    let session_total_rows = sqlx::query(&format!(
        r#"
        SELECT
            {bucket_expr} AS bucket_start,
            COUNT(*)::BIGINT AS session_count,
            COALESCE(SUM(duration_seconds), 0)::BIGINT AS duration_total_seconds
        FROM sessions
        WHERE project_id = $1
          AND {bucket_expr} = ANY($2)
        GROUP BY bucket_start
        ORDER BY bucket_start ASC
        "#
    ))
    .bind(project_id)
    .bind(&bucket_starts)
    .fetch_all(&mut **tx)
    .await?;
    let session_total_deltas = session_total_rows
        .into_iter()
        .map(|row| SessionMetricTotalDelta {
            project_id,
            granularity,
            bucket_start: row.try_get("bucket_start").expect("bucket_start"),
            session_count: row.try_get("session_count").expect("session_count"),
            duration_total_seconds: row
                .try_get("duration_total_seconds")
                .expect("duration_total_seconds"),
            new_installs: 0,
        })
        .collect::<Vec<_>>();
    upsert_session_metric_totals_in_tx(tx, &session_total_deltas).await?;

    let session_platform_rows = sqlx::query(&format!(
        r#"
        SELECT
            {bucket_expr} AS bucket_start,
            platform,
            COUNT(*)::BIGINT AS session_count,
            COALESCE(SUM(duration_seconds), 0)::BIGINT AS duration_total_seconds
        FROM sessions
        WHERE project_id = $1
          AND {bucket_expr} = ANY($2)
        GROUP BY bucket_start, platform
        ORDER BY bucket_start ASC, platform ASC
        "#
    ))
    .bind(project_id)
    .bind(&bucket_starts)
    .fetch_all(&mut **tx)
    .await?;
    let session_platform_deltas = session_platform_rows
        .into_iter()
        .map(|row| SessionMetricDim1Delta {
            project_id,
            granularity,
            bucket_start: row.try_get("bucket_start").expect("bucket_start"),
            dim1_key: "platform".to_owned(),
            dim1_value: row.try_get("platform").expect("platform"),
            session_count: row.try_get("session_count").expect("session_count"),
            duration_total_seconds: row
                .try_get("duration_total_seconds")
                .expect("duration_total_seconds"),
            new_installs: 0,
        })
        .collect::<Vec<_>>();
    upsert_session_metric_dim1_in_tx(tx, &session_platform_deltas).await?;

    let session_app_version_rows = sqlx::query(&format!(
        r#"
        SELECT
            {bucket_expr} AS bucket_start,
            app_version,
            COUNT(*)::BIGINT AS session_count,
            COALESCE(SUM(duration_seconds), 0)::BIGINT AS duration_total_seconds
        FROM sessions
        WHERE project_id = $1
          AND {bucket_expr} = ANY($2)
          AND app_version IS NOT NULL
        GROUP BY bucket_start, app_version
        ORDER BY bucket_start ASC, app_version ASC
        "#
    ))
    .bind(project_id)
    .bind(&bucket_starts)
    .fetch_all(&mut **tx)
    .await?;
    let session_app_version_deltas = session_app_version_rows
        .into_iter()
        .map(|row| SessionMetricDim1Delta {
            project_id,
            granularity,
            bucket_start: row.try_get("bucket_start").expect("bucket_start"),
            dim1_key: "app_version".to_owned(),
            dim1_value: row.try_get("app_version").expect("app_version"),
            session_count: row.try_get("session_count").expect("session_count"),
            duration_total_seconds: row
                .try_get("duration_total_seconds")
                .expect("duration_total_seconds"),
            new_installs: 0,
        })
        .collect::<Vec<_>>();
    upsert_session_metric_dim1_in_tx(tx, &session_app_version_deltas).await?;

    let session_dim2_rows = sqlx::query(&format!(
        r#"
        SELECT
            {bucket_expr} AS bucket_start,
            platform,
            app_version,
            COUNT(*)::BIGINT AS session_count,
            COALESCE(SUM(duration_seconds), 0)::BIGINT AS duration_total_seconds
        FROM sessions
        WHERE project_id = $1
          AND {bucket_expr} = ANY($2)
          AND app_version IS NOT NULL
        GROUP BY bucket_start, platform, app_version
        ORDER BY bucket_start ASC, platform ASC, app_version ASC
        "#
    ))
    .bind(project_id)
    .bind(&bucket_starts)
    .fetch_all(&mut **tx)
    .await?;
    let session_dim2_deltas = session_dim2_rows
        .into_iter()
        .map(|row| SessionMetricDim2Delta {
            project_id,
            granularity,
            bucket_start: row.try_get("bucket_start").expect("bucket_start"),
            dim1_key: "app_version".to_owned(),
            dim1_value: row.try_get("app_version").expect("app_version"),
            dim2_key: "platform".to_owned(),
            dim2_value: row.try_get("platform").expect("platform"),
            session_count: row.try_get("session_count").expect("session_count"),
            duration_total_seconds: row
                .try_get("duration_total_seconds")
                .expect("duration_total_seconds"),
            new_installs: 0,
        })
        .collect::<Vec<_>>();
    upsert_session_metric_dim2_in_tx(tx, &session_dim2_deltas).await?;

    let first_seen_total_rows = sqlx::query(&format!(
        r#"
        SELECT
            {first_seen_bucket_expr} AS bucket_start,
            COUNT(*)::BIGINT AS new_installs
        FROM install_first_seen
        WHERE project_id = $1
          AND {first_seen_bucket_expr} = ANY($2)
        GROUP BY bucket_start
        ORDER BY bucket_start ASC
        "#
    ))
    .bind(project_id)
    .bind(&bucket_starts)
    .fetch_all(&mut **tx)
    .await?;
    let first_seen_total_deltas = first_seen_total_rows
        .into_iter()
        .map(|row| SessionMetricTotalDelta {
            project_id,
            granularity,
            bucket_start: row.try_get("bucket_start").expect("bucket_start"),
            session_count: 0,
            duration_total_seconds: 0,
            new_installs: row.try_get("new_installs").expect("new_installs"),
        })
        .collect::<Vec<_>>();
    upsert_session_metric_totals_in_tx(tx, &first_seen_total_deltas).await?;

    let first_seen_platform_rows = sqlx::query(&format!(
        r#"
        SELECT
            {first_seen_bucket_expr} AS bucket_start,
            platform,
            COUNT(*)::BIGINT AS new_installs
        FROM install_first_seen
        WHERE project_id = $1
          AND {first_seen_bucket_expr} = ANY($2)
        GROUP BY bucket_start, platform
        ORDER BY bucket_start ASC, platform ASC
        "#
    ))
    .bind(project_id)
    .bind(&bucket_starts)
    .fetch_all(&mut **tx)
    .await?;
    let first_seen_platform_deltas = first_seen_platform_rows
        .into_iter()
        .map(|row| SessionMetricDim1Delta {
            project_id,
            granularity,
            bucket_start: row.try_get("bucket_start").expect("bucket_start"),
            dim1_key: "platform".to_owned(),
            dim1_value: row.try_get("platform").expect("platform"),
            session_count: 0,
            duration_total_seconds: 0,
            new_installs: row.try_get("new_installs").expect("new_installs"),
        })
        .collect::<Vec<_>>();
    upsert_session_metric_dim1_in_tx(tx, &first_seen_platform_deltas).await?;

    let first_seen_app_version_rows = sqlx::query(&format!(
        r#"
        SELECT
            {first_seen_bucket_expr} AS bucket_start,
            app_version,
            COUNT(*)::BIGINT AS new_installs
        FROM install_first_seen
        WHERE project_id = $1
          AND {first_seen_bucket_expr} = ANY($2)
          AND app_version IS NOT NULL
        GROUP BY bucket_start, app_version
        ORDER BY bucket_start ASC, app_version ASC
        "#
    ))
    .bind(project_id)
    .bind(&bucket_starts)
    .fetch_all(&mut **tx)
    .await?;
    let first_seen_app_version_deltas = first_seen_app_version_rows
        .into_iter()
        .map(|row| SessionMetricDim1Delta {
            project_id,
            granularity,
            bucket_start: row.try_get("bucket_start").expect("bucket_start"),
            dim1_key: "app_version".to_owned(),
            dim1_value: row.try_get("app_version").expect("app_version"),
            session_count: 0,
            duration_total_seconds: 0,
            new_installs: row.try_get("new_installs").expect("new_installs"),
        })
        .collect::<Vec<_>>();
    upsert_session_metric_dim1_in_tx(tx, &first_seen_app_version_deltas).await?;

    let first_seen_dim2_rows = sqlx::query(&format!(
        r#"
        SELECT
            {first_seen_bucket_expr} AS bucket_start,
            platform,
            app_version,
            COUNT(*)::BIGINT AS new_installs
        FROM install_first_seen
        WHERE project_id = $1
          AND {first_seen_bucket_expr} = ANY($2)
          AND app_version IS NOT NULL
        GROUP BY bucket_start, platform, app_version
        ORDER BY bucket_start ASC, platform ASC, app_version ASC
        "#
    ))
    .bind(project_id)
    .bind(&bucket_starts)
    .fetch_all(&mut **tx)
    .await?;
    let first_seen_dim2_deltas = first_seen_dim2_rows
        .into_iter()
        .map(|row| SessionMetricDim2Delta {
            project_id,
            granularity,
            bucket_start: row.try_get("bucket_start").expect("bucket_start"),
            dim1_key: "app_version".to_owned(),
            dim1_value: row.try_get("app_version").expect("app_version"),
            dim2_key: "platform".to_owned(),
            dim2_value: row.try_get("platform").expect("platform"),
            session_count: 0,
            duration_total_seconds: 0,
            new_installs: row.try_get("new_installs").expect("new_installs"),
        })
        .collect::<Vec<_>>();
    upsert_session_metric_dim2_in_tx(tx, &first_seen_dim2_deltas).await?;

    Ok(())
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
        load_install_session_state, load_pending_session_rebuild_buckets_in_tx, load_worker_offset,
        save_worker_offset,
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
        event_with_app_version(install_id, day, hour, minute, Some("1.0.0"))
    }

    fn event_with_app_version(
        install_id: &str,
        day: u32,
        hour: u32,
        minute: u32,
        app_version: Option<&str>,
    ) -> EventPayload {
        EventPayload {
            event: "app_open".to_owned(),
            timestamp: timestamp(day, hour, minute),
            install_id: install_id.to_owned(),
            platform: Platform::Ios,
            app_version: app_version.map(str::to_owned),
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

    async fn pending_session_rebuild_buckets(
        pool: &PgPool,
    ) -> Vec<fantasma_store::PendingSessionRebuildBucketRecord> {
        let mut tx = pool.begin().await.expect("begin pending rebuild bucket tx");
        let pending = load_pending_session_rebuild_buckets_in_tx(&mut tx, &[project_id()])
            .await
            .expect("load pending session rebuild buckets");
        tx.commit().await.expect("commit pending rebuild bucket tx");
        pending
    }

    #[test]
    fn max_dimension_event_metrics_fanout_stays_bounded() {
        let rollups = build_event_metric_rollups(&[raw_event_with_max_dimensions()]);
        let total_fanout = rollups.total_deltas.len()
            + rollups.dim1_deltas.len()
            + rollups.dim2_deltas.len()
            + rollups.dim3_deltas.len();

        assert_eq!(rollups.total_deltas.len(), 2);
        assert_eq!(rollups.dim1_deltas.len(), 12);
        assert_eq!(rollups.dim2_deltas.len(), 30);
        assert_eq!(rollups.dim3_deltas.len(), 40);
        assert_eq!(total_fanout, 84);
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
    async fn session_worker_records_install_progress_for_processed_events(pool: PgPool) {
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

        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load tail state")
            .expect("tail state exists");

        assert_eq!(tail.last_processed_event_id, 2);
    }

    #[sqlx::test]
    async fn session_worker_persists_max_raw_id_when_timestamp_order_differs(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 0, 10), event("install-1", 1, 0, 0)],
        )
        .await
        .expect("insert events");

        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load tail state")
            .expect("tail state exists");
        assert_eq!(tail.last_processed_event_id, 2);

        save_worker_offset(&pool, SESSION_WORKER_NAME, 0)
            .await
            .expect("rewind worker offset");

        let replayed = process_session_batch(&pool, 100)
            .await
            .expect("replay batch");

        assert_eq!(replayed, 0);
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
    }

    #[sqlx::test]
    async fn replayed_install_batch_is_a_no_op_once_install_progress_is_recorded(pool: PgPool) {
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
            .expect("process initial batch");

        save_worker_offset(&pool, SESSION_WORKER_NAME, 0)
            .await
            .expect("rewind worker offset");

        let replayed = process_session_batch(&pool, 100)
            .await
            .expect("replay batch");

        assert_eq!(replayed, 0);
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
    async fn late_repair_keeps_existing_session_app_version_assignment(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[
                event_with_app_version("install-1", 1, 1, 0, None),
                event_with_app_version("install-1", 1, 1, 10, Some("1.0.0")),
            ],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        insert_events(
            &pool,
            project_id(),
            &[event_with_app_version("install-1", 1, 0, 50, Some("0.9.0"))],
        )
        .await
        .expect("insert late event");
        process_session_batch(&pool, 100)
            .await
            .expect("process late batch");

        let repaired_session = fetch_latest_session_for_install(&pool, project_id(), "install-1")
            .await
            .expect("fetch repaired session")
            .expect("session exists");

        assert_eq!(repaired_session.session_start, timestamp(1, 0, 50));
        assert_eq!(repaired_session.session_end, timestamp(1, 1, 10));
        assert_eq!(repaired_session.duration_seconds, 20 * 60);
        assert_eq!(repaired_session.app_version, None);
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
    async fn successful_incremental_work_commits_append_deltas_before_later_batch_failure(
        pool: PgPool,
    ) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[
                event("install-a", 1, 0, 0),
                event("install-a", 1, 0, 10),
                event("install-b", 1, 1, 0),
                event("install-b", 1, 1, 10),
            ],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        sqlx::query("DELETE FROM sessions WHERE project_id = $1 AND install_id = $2")
            .bind(project_id())
            .bind("install-b")
            .execute(&pool)
            .await
            .expect("delete broken install session row");

        insert_events(
            &pool,
            project_id(),
            &[event("install-a", 1, 0, 20), event("install-b", 1, 1, 20)],
        )
        .await
        .expect("insert mixed batch");

        let error = process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect_err("later install should fail batch");

        assert!(matches!(error, StoreError::InvariantViolation(_)));
        assert!(
            pending_session_rebuild_buckets(&pool).await.is_empty(),
            "append work should not enqueue rebuild buckets even when a later install fails",
        );
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((2, 2, 30 * 60))
        );

        let hour_zero_duration = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT duration_total_seconds
            FROM session_metric_buckets_total
            WHERE project_id = $1
              AND granularity = 'hour'
              AND bucket_start = $2
            "#,
        )
        .bind(project_id())
        .bind(timestamp(1, 0, 0))
        .fetch_one(&pool)
        .await
        .expect("fetch hour-zero duration");
        let day_duration = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT duration_total_seconds
            FROM session_metric_buckets_total
            WHERE project_id = $1
              AND granularity = 'day'
              AND bucket_start = $2
            "#,
        )
        .bind(project_id())
        .bind(timestamp(1, 0, 0))
        .fetch_one(&pool)
        .await
        .expect("fetch day duration");

        assert_eq!(hour_zero_duration, 20 * 60);
        assert_eq!(day_duration, 30 * 60);
        assert_eq!(
            load_worker_offset(&pool, SESSION_WORKER_NAME)
                .await
                .expect("load session offset"),
            4,
            "offset should not advance past the failed batch",
        );
    }

    #[sqlx::test]
    async fn replayed_append_batch_only_advances_offset_after_coordinator_failure(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 50)])
            .await
            .expect("insert initial event");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        insert_events(&pool, project_id(), &[event("install-1", 1, 1, 10)])
            .await
            .expect("insert extending event");

        let error = process_session_batch_with_hooks(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
            SessionBatchTestHooks::fail_before_coordinator_finalize(),
        )
        .await
        .expect_err("coordinator finalize should fail after child commit");

        assert!(matches!(error, StoreError::InvariantViolation(_)));

        let session = fetch_latest_session_for_install(&pool, project_id(), "install-1")
            .await
            .expect("fetch extended session")
            .expect("session exists");
        assert_eq!(session.session_end, timestamp(1, 1, 10));
        assert_eq!(session.duration_seconds, 20 * 60);
        assert_eq!(
            load_worker_offset(&pool, SESSION_WORKER_NAME)
                .await
                .expect("load worker offset"),
            1
        );
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 20 * 60))
        );
        assert!(
            pending_session_rebuild_buckets(&pool).await.is_empty(),
            "append child commit should not leave rebuild work behind"
        );

        let hour_zero_duration = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT duration_total_seconds
            FROM session_metric_buckets_total
            WHERE project_id = $1
              AND granularity = 'hour'
              AND bucket_start = $2
            "#,
        )
        .bind(project_id())
        .bind(timestamp(1, 0, 0))
        .fetch_one(&pool)
        .await
        .expect("fetch hour-zero duration");
        let day_duration = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT duration_total_seconds
            FROM session_metric_buckets_total
            WHERE project_id = $1
              AND granularity = 'day'
              AND bucket_start = $2
            "#,
        )
        .bind(project_id())
        .bind(timestamp(1, 0, 0))
        .fetch_one(&pool)
        .await
        .expect("fetch day duration");

        assert_eq!(hour_zero_duration, 20 * 60);
        assert_eq!(day_duration, 20 * 60);

        let replayed = process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("replay failed coordinator batch");

        assert_eq!(
            replayed,
            SessionBatchOutcome {
                processed_events: 0,
                advanced_offset: true,
            }
        );
        assert_eq!(
            load_worker_offset(&pool, SESSION_WORKER_NAME)
                .await
                .expect("load advanced worker offset"),
            2
        );
        assert!(
            pending_session_rebuild_buckets(&pool).await.is_empty(),
            "replay must drain the durable rebuild queue"
        );
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 20 * 60))
        );

        assert_eq!(hour_zero_duration, 20 * 60);
        assert_eq!(day_duration, 20 * 60);
    }

    #[sqlx::test]
    async fn cross_midnight_append_commit_leaves_daily_and_metric_state_final(pool: PgPool) {
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
        .expect("insert append events");

        let error = process_session_batch_with_hooks(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
            SessionBatchTestHooks::fail_before_coordinator_finalize(),
        )
        .await
        .expect_err("coordinator finalize should fail after append child commit");

        assert!(matches!(error, StoreError::InvariantViolation(_)));
        assert_eq!(
            load_worker_offset(&pool, SESSION_WORKER_NAME)
                .await
                .expect("load worker offset"),
            0
        );
        assert!(
            pending_session_rebuild_buckets(&pool).await.is_empty(),
            "append child commit should not rely on queued rebuilds"
        );
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 25 * 60))
        );
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()).await,
            Some((1, 1, 0))
        );

        let jan_1_hour_duration = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT duration_total_seconds
            FROM session_metric_buckets_total
            WHERE project_id = $1
              AND granularity = 'hour'
              AND bucket_start = $2
            "#,
        )
        .bind(project_id())
        .bind(timestamp(1, 23, 0))
        .fetch_one(&pool)
        .await
        .expect("fetch january 1 hourly duration");
        let jan_2_day_bucket = sqlx::query(
            r#"
            SELECT session_count, duration_total_seconds
            FROM session_metric_buckets_total
            WHERE project_id = $1
              AND granularity = 'day'
              AND bucket_start = $2
            "#,
        )
        .bind(project_id())
        .bind(timestamp(2, 0, 0))
        .fetch_one(&pool)
        .await
        .expect("fetch january 2 daily bucket");
        let jan_1_hour_dim1_bucket = sqlx::query(
            r#"
            SELECT session_count, duration_total_seconds
            FROM session_metric_buckets_dim1
            WHERE project_id = $1
              AND granularity = 'hour'
              AND bucket_start = $2
              AND dim1_key = 'platform'
              AND dim1_value = 'ios'
            "#,
        )
        .bind(project_id())
        .bind(timestamp(1, 23, 0))
        .fetch_one(&pool)
        .await
        .expect("fetch january 1 hourly dim1 bucket");
        let jan_1_hour_dim2_bucket = sqlx::query(
            r#"
            SELECT session_count, duration_total_seconds
            FROM session_metric_buckets_dim2
            WHERE project_id = $1
              AND granularity = 'hour'
              AND bucket_start = $2
              AND dim1_key = 'app_version'
              AND dim1_value = '1.0.0'
              AND dim2_key = 'platform'
              AND dim2_value = 'ios'
            "#,
        )
        .bind(project_id())
        .bind(timestamp(1, 23, 0))
        .fetch_one(&pool)
        .await
        .expect("fetch january 1 hourly dim2 bucket");
        let jan_2_day_dim2_bucket = sqlx::query(
            r#"
            SELECT session_count, duration_total_seconds
            FROM session_metric_buckets_dim2
            WHERE project_id = $1
              AND granularity = 'day'
              AND bucket_start = $2
              AND dim1_key = 'app_version'
              AND dim1_value = '1.0.0'
              AND dim2_key = 'platform'
              AND dim2_value = 'ios'
            "#,
        )
        .bind(project_id())
        .bind(timestamp(2, 0, 0))
        .fetch_one(&pool)
        .await
        .expect("fetch january 2 daily dim2 bucket");

        assert_eq!(jan_1_hour_duration, 25 * 60);
        assert_eq!(
            jan_1_hour_dim1_bucket
                .try_get::<i64, _>("session_count")
                .expect("session count"),
            1
        );
        assert_eq!(
            jan_1_hour_dim1_bucket
                .try_get::<i64, _>("duration_total_seconds")
                .expect("duration"),
            25 * 60
        );
        assert_eq!(
            jan_1_hour_dim2_bucket
                .try_get::<i64, _>("session_count")
                .expect("session count"),
            1
        );
        assert_eq!(
            jan_1_hour_dim2_bucket
                .try_get::<i64, _>("duration_total_seconds")
                .expect("duration"),
            25 * 60
        );
        assert_eq!(
            jan_2_day_bucket
                .try_get::<i64, _>("session_count")
                .expect("session count"),
            1
        );
        assert_eq!(
            jan_2_day_bucket
                .try_get::<i64, _>("duration_total_seconds")
                .expect("duration"),
            0
        );
        assert_eq!(
            jan_2_day_dim2_bucket
                .try_get::<i64, _>("session_count")
                .expect("session count"),
            1
        );
        assert_eq!(
            jan_2_day_dim2_bucket
                .try_get::<i64, _>("duration_total_seconds")
                .expect("duration"),
            0
        );
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

    #[sqlx::test]
    async fn session_worker_records_per_install_progress_after_batch(pool: PgPool) {
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

        let last_processed_event_id = sqlx::query_scalar::<_, i64>(
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
        .expect("fetch install progress");

        assert_eq!(last_processed_event_id, 2);
    }

    #[sqlx::test]
    async fn event_worker_writes_hour_and_day_rollups(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(&pool, project_id(), &[event("install-1", 1, 1, 15)])
            .await
            .expect("insert event");

        process_event_metrics_batch(&pool, 100)
            .await
            .expect("process batch");

        let bucket_rows = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM event_metric_buckets_total
            WHERE project_id = $1
              AND event_name = $2
            "#,
        )
        .bind(project_id())
        .bind("app_open")
        .fetch_one(&pool)
        .await
        .expect("count event bucket rows");

        assert_eq!(bucket_rows, 2);
    }

    #[sqlx::test]
    async fn session_worker_keeps_duration_total_on_session_start_bucket(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 0, 50), event("install-1", 1, 1, 10)],
        )
        .await
        .expect("insert events");

        process_session_batch(&pool, 100)
            .await
            .expect("process batch");

        let start_bucket_duration = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT duration_total_seconds
            FROM session_metric_buckets_total
            WHERE project_id = $1
              AND granularity = 'hour'
              AND bucket_start = $2
            "#,
        )
        .bind(project_id())
        .bind(timestamp(1, 0, 0))
        .fetch_one(&pool)
        .await
        .expect("fetch start bucket duration");

        let later_bucket_duration = sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT duration_total_seconds
            FROM session_metric_buckets_total
            WHERE project_id = $1
              AND granularity = 'hour'
              AND bucket_start = $2
            "#,
        )
        .bind(project_id())
        .bind(timestamp(1, 1, 0))
        .fetch_optional(&pool)
        .await
        .expect("fetch later bucket duration")
        .flatten();

        assert_eq!(start_bucket_duration, 20 * 60);
        assert_eq!(later_bucket_duration.unwrap_or_default(), 0);
    }

    #[sqlx::test]
    async fn session_worker_keeps_first_seen_install_assignment_fixed(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(&pool, project_id(), &[event("install-1", 1, 1, 0)])
            .await
            .expect("insert first event");

        process_session_batch(&pool, 100)
            .await
            .expect("process first batch");

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 0)])
            .await
            .expect("insert late event");

        process_session_batch(&pool, 100)
            .await
            .expect("process late batch");

        let first_seen_bucket = sqlx::query_scalar::<_, DateTime<Utc>>(
            r#"
            SELECT first_seen_at
            FROM install_first_seen
            WHERE project_id = $1
              AND install_id = $2
            "#,
        )
        .bind(project_id())
        .bind("install-1")
        .fetch_one(&pool)
        .await
        .expect("fetch first seen row");

        let bucket_values = sqlx::query(
            r#"
            SELECT bucket_start, new_installs
            FROM session_metric_buckets_total
            WHERE project_id = $1
              AND granularity = 'hour'
            ORDER BY bucket_start ASC
            "#,
        )
        .bind(project_id())
        .fetch_all(&pool)
        .await
        .expect("fetch hourly new-install buckets");

        assert_eq!(first_seen_bucket, timestamp(1, 1, 0));
        assert_eq!(bucket_values.len(), 2);
        assert_eq!(
            bucket_values[0]
                .try_get::<DateTime<Utc>, _>("bucket_start")
                .unwrap(),
            timestamp(1, 0, 0)
        );
        assert_eq!(
            bucket_values[0].try_get::<i64, _>("new_installs").unwrap(),
            0
        );
        assert_eq!(
            bucket_values[1]
                .try_get::<DateTime<Utc>, _>("bucket_start")
                .unwrap(),
            timestamp(1, 1, 0)
        );
        assert_eq!(
            bucket_values[1].try_get::<i64, _>("new_installs").unwrap(),
            1
        );
    }
}
