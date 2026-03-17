#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{LazyLock, Mutex},
    time::Instant,
};

use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, Timelike, Utc};
use fantasma_core::MetricGranularity;
use fantasma_store::{
    EventMetricBucketDim1Delta, EventMetricBucketDim2Delta, EventMetricBucketTotalDelta,
    InstallFirstSeenRecord, InstallSessionStateRecord, LiveInstallStateUpsert,
    PendingSessionDailyInstallRebuildRecord, PgPool, RawEventRecord,
    SessionActiveInstallSliceDelta, SessionDailyActiveInstallDelta, SessionDailyDelta,
    SessionDailyDurationDelta, SessionDailyInstallDelta, SessionRecord, SessionRepairJobRecord,
    SessionRepairJobUpsert, SessionTailUpdate, StoreError, add_session_daily_duration_deltas_in_tx,
    claim_and_sweep_pending_session_rebuild_buckets_in_tx, claim_pending_session_repair_jobs_in_tx,
    complete_session_repair_job_in_tx, delete_sessions_overlapping_window_in_tx,
    enqueue_session_daily_installs_rebuilds_in_tx, enqueue_session_rebuild_buckets_in_tx,
    fetch_events_after_in_tx, fetch_events_for_install_after_id_through_in_tx,
    fetch_events_for_install_between_in_tx, fetch_latest_session_for_install_in_tx,
    fetch_sessions_overlapping_window_in_tx, insert_install_first_seen_in_tx, insert_session_in_tx,
    insert_sessions_in_tx, load_install_session_states_in_tx,
    load_pending_session_daily_install_rebuilds_in_tx, load_session_repair_jobs_for_installs_in_tx,
    load_tail_sessions_for_installs_in_tx, lock_worker_offset,
    rebuild_session_active_install_slices_days_in_tx,
    rebuild_session_daily_days_from_pending_install_rebuilds_with_telemetry_in_tx,
    release_session_repair_job_claim_in_tx, save_worker_offset_in_tx, update_session_tail_in_tx,
    upsert_event_metric_buckets_dim1_in_tx, upsert_event_metric_buckets_dim2_in_tx,
    upsert_event_metric_buckets_total_in_tx, upsert_install_session_state_in_tx,
    upsert_live_install_state_in_tx, upsert_session_active_install_slices_in_tx,
    upsert_session_daily_deltas_in_tx, upsert_session_daily_install_deltas_in_tx,
    upsert_session_repair_jobs_in_tx,
};
use serde::Serialize;
use sqlx::{Postgres, Row, Transaction};
use tokio::{sync::Mutex as AsyncMutex, task::JoinSet};
use tracing::warn;
use uuid::Uuid;

use crate::sessionization;

const SESSION_TIMEOUT_MINS: i64 = 30;
pub(crate) const SESSION_WORKER_NAME: &str = "sessions";
pub(crate) const EVENT_METRICS_WORKER_NAME: &str = "event_metrics";
const DEFAULT_SESSION_INCREMENTAL_CONCURRENCY: usize = 8;
const DEFAULT_SESSION_REPAIR_CONCURRENCY: usize = 2;
const STAGE_B_TRACE_PATH_ENV: &str = "FANTASMA_WORKER_STAGE_B_TRACE_PATH";
const EVENT_LANE_TRACE_PATH_ENV: &str = "FANTASMA_WORKER_EVENT_LANE_TRACE_PATH";
const STAGE_B_TRACE_REPAIR_JOB: &str = "repair_job";
const STAGE_B_TRACE_REBUILD_FINALIZE: &str = "rebuild_finalize";
static STAGE_B_TRACE_WRITE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
static EVENT_LANE_TRACE_WRITE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
static SESSION_REBUILD_DRAIN_LOCK: LazyLock<AsyncMutex<()>> = LazyLock::new(|| AsyncMutex::new(()));
static ACTIVE_INSTALL_RANGE_REBUILD_DRAIN_LOCK: LazyLock<AsyncMutex<()>> =
    LazyLock::new(|| AsyncMutex::new(()));

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StageBTraceRecord {
    record_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    install_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_event_id: Option<i64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    phases_ms: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    finalization_subphases_ms: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    session_daily_subphases_ms: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    counts: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct EventLaneTraceRecord {
    record_type: String,
    last_processed_event_id: i64,
    next_offset: i64,
    processed_events: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    phases_ms: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    counts: BTreeMap<String, u64>,
}

impl StageBTraceRecord {
    fn repair_job(install_id: &str, target_event_id: i64) -> Self {
        Self {
            record_type: STAGE_B_TRACE_REPAIR_JOB.to_owned(),
            install_id: Some(install_id.to_owned()),
            target_event_id: Some(target_event_id),
            phases_ms: BTreeMap::new(),
            finalization_subphases_ms: BTreeMap::new(),
            session_daily_subphases_ms: BTreeMap::new(),
            counts: BTreeMap::new(),
        }
    }

    fn rebuild_finalize() -> Self {
        Self {
            record_type: STAGE_B_TRACE_REBUILD_FINALIZE.to_owned(),
            install_id: None,
            target_event_id: None,
            phases_ms: BTreeMap::new(),
            finalization_subphases_ms: BTreeMap::new(),
            session_daily_subphases_ms: BTreeMap::new(),
            counts: BTreeMap::new(),
        }
    }

    fn add_phase_duration(&mut self, phase: &str, elapsed: std::time::Duration) {
        self.add_phase_ms(phase, duration_ms(elapsed));
    }

    fn add_phase_ms(&mut self, phase: &str, elapsed_ms: u64) {
        *self.phases_ms.entry(phase.to_owned()).or_default() += elapsed_ms;
    }

    fn add_finalization_subphase_duration(&mut self, phase: &str, elapsed: std::time::Duration) {
        self.add_finalization_subphase_ms(phase, duration_ms(elapsed));
    }

    fn add_finalization_subphase_ms(&mut self, phase: &str, elapsed_ms: u64) {
        *self
            .finalization_subphases_ms
            .entry(phase.to_owned())
            .or_default() += elapsed_ms;
    }

    fn add_session_daily_subphase_ms(&mut self, phase: &str, elapsed_ms: u64) {
        *self
            .session_daily_subphases_ms
            .entry(phase.to_owned())
            .or_default() += elapsed_ms;
    }

    fn add_count(&mut self, name: &str, value: usize) {
        self.add_count_u64(name, value as u64);
    }

    fn add_count_u64(&mut self, name: &str, value: u64) {
        *self.counts.entry(name.to_owned()).or_default() += value;
    }
}

impl EventLaneTraceRecord {
    fn event_metrics_batch(
        last_processed_event_id: i64,
        next_offset: i64,
        processed_events: usize,
    ) -> Self {
        Self {
            record_type: "event_metrics_batch".to_owned(),
            last_processed_event_id,
            next_offset,
            processed_events,
            phases_ms: BTreeMap::new(),
            counts: BTreeMap::new(),
        }
    }

    fn add_phase_duration(&mut self, phase: &str, elapsed: std::time::Duration) {
        *self.phases_ms.entry(phase.to_owned()).or_default() += duration_ms(elapsed);
    }

    fn add_count(&mut self, name: &str, value: usize) {
        *self.counts.entry(name.to_owned()).or_default() += value as u64;
    }
}

fn duration_ms(elapsed: std::time::Duration) -> u64 {
    elapsed.as_millis().min(u128::from(u64::MAX)) as u64
}

fn row_mutation_lag_before_claim_ms(
    job: &SessionRepairJobRecord,
    claim_elapsed: std::time::Duration,
) -> u64 {
    let row_mutation_lag_ms = job
        .claimed_at
        .unwrap_or(job.updated_at)
        .signed_duration_since(job.updated_at)
        .num_milliseconds()
        .max(0) as u64;
    row_mutation_lag_ms.saturating_add(duration_ms(claim_elapsed))
}

fn record_stage_b_trace(record: &StageBTraceRecord) -> Result<(), StoreError> {
    let Ok(path) = std::env::var(STAGE_B_TRACE_PATH_ENV) else {
        return Ok(());
    };
    let _guard = STAGE_B_TRACE_WRITE_LOCK.lock().map_err(|error| {
        StoreError::InvariantViolation(format!("lock Stage B trace file {path}: {error}"))
    })?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| {
            StoreError::InvariantViolation(format!("open Stage B trace file {path}: {error}"))
        })?;
    serde_json::to_writer(&mut file, record).map_err(|error| {
        StoreError::InvariantViolation(format!("serialize Stage B trace record: {error}"))
    })?;
    use std::io::Write as _;
    file.write_all(b"\n").map_err(|error| {
        StoreError::InvariantViolation(format!("append Stage B trace newline to {path}: {error}"))
    })?;

    Ok(())
}

fn record_event_lane_trace(record: &EventLaneTraceRecord) -> Result<(), StoreError> {
    let Ok(path) = std::env::var(EVENT_LANE_TRACE_PATH_ENV) else {
        return Ok(());
    };
    let _guard = EVENT_LANE_TRACE_WRITE_LOCK.lock().map_err(|error| {
        StoreError::InvariantViolation(format!("lock event lane trace file {path}: {error}"))
    })?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| {
            StoreError::InvariantViolation(format!("open event lane trace file {path}: {error}"))
        })?;
    serde_json::to_writer(&mut file, record).map_err(|error| {
        StoreError::InvariantViolation(format!("serialize event lane trace record: {error}"))
    })?;
    use std::io::Write as _;
    file.write_all(b"\n").map_err(|error| {
        StoreError::InvariantViolation(format!(
            "append event lane trace newline to {path}: {error}"
        ))
    })?;

    Ok(())
}

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
    metric_rebuild_scope: SessionRepairBuckets,
}

pub async fn process_session_batch(pool: &PgPool, batch_size: i64) -> Result<usize, StoreError> {
    let config = SessionBatchConfig {
        batch_size,
        incremental_concurrency: DEFAULT_SESSION_INCREMENTAL_CONCURRENCY,
        repair_concurrency: DEFAULT_SESSION_REPAIR_CONCURRENCY,
    };
    let outcome = process_session_batch_with_config(pool, config).await?;
    drain_session_repair_jobs(pool, config.repair_concurrency).await?;

    Ok(outcome.processed_events)
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
struct IncrementalSessionWorkItem {
    project_id: Uuid,
    install_id: String,
    events: Vec<RawEventRecord>,
    tail_state: Option<InstallSessionStateRecord>,
    tail_session: Option<SessionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionRepairEnqueueItem {
    project_id: Uuid,
    install_id: String,
    target_event_id: i64,
}

#[derive(Debug, Default)]
struct SessionRepairTaskResult {
    processed: bool,
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
    let install_keys = grouped
        .keys()
        .map(|(project_id, install_id)| (*project_id, install_id.clone()))
        .collect::<Vec<_>>();
    let tail_states = load_install_session_states_in_tx(&mut tx, &install_keys).await?;
    let tail_sessions = load_tail_sessions_for_installs_in_tx(&mut tx, &install_keys).await?;
    let repair_jobs = load_session_repair_jobs_for_installs_in_tx(&mut tx, &install_keys).await?;

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
        let pending_repair = repair_jobs
            .get(&(project_id, install_id.clone()))
            .map(|job| job.target_event_id);
        let has_pending_repair = pending_repair.is_some_and(|target_event_id| {
            tail_state
                .as_ref()
                .is_none_or(|state| target_event_id > state.last_processed_event_id)
        });

        if has_pending_repair || needs_exact_day_repair(tail_state.as_ref(), &events) {
            repair_items.push(SessionRepairEnqueueItem {
                project_id,
                install_id,
                target_event_id: highest_processed_event_id,
            });
        } else {
            incremental_items.push(IncrementalSessionWorkItem {
                project_id,
                install_id,
                events,
                tail_state,
                tail_session,
            });
        }
    }

    let incremental_queue = run_session_incremental_queue(
        pool.clone(),
        incremental_items,
        config.incremental_concurrency,
    )
    .await?;
    if let Some(error) = incremental_queue.error {
        tx.commit().await.map_err(StoreError::from)?;
        return Err(error);
    }

    if let Err(error) = enqueue_session_repair_items_in_tx(&mut tx, repair_items).await {
        tx.commit().await.map_err(StoreError::from)?;
        return Err(error);
    }

    if let Err(error) = drain_pending_session_rebuild_buckets(pool).await {
        tx.commit().await.map_err(StoreError::from)?;
        return Err(error);
    }
    if let Err(error) = drain_pending_active_install_range_rebuild_buckets(pool).await {
        tx.commit().await.map_err(StoreError::from)?;
        return Err(error);
    }

    hooks.fail_before_coordinator_finalize()?;
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

async fn run_session_incremental_queue(
    pool: PgPool,
    items: Vec<IncrementalSessionWorkItem>,
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
        work_set.spawn(async move { process_incremental_session_work_item(&pool, item).await });
    }

    while let Some(join_result) = work_set.join_next().await {
        match join_result {
            Ok(Ok(())) => {
                if error.is_none()
                    && let Some(item) = work_items.next()
                {
                    let pool = pool.clone();
                    work_set.spawn(async move {
                        process_incremental_session_work_item(&pool, item).await
                    });
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

async fn process_incremental_session_work_item(
    pool: &PgPool,
    item: IncrementalSessionWorkItem,
) -> Result<(), StoreError> {
    let mut tx = pool.begin().await.map_err(StoreError::from)?;
    process_install_batch_incremental(
        &mut tx,
        item.project_id,
        &item.install_id,
        item.events,
        item.tail_state,
        item.tail_session,
    )
    .await?;
    tx.commit().await.map_err(StoreError::from)?;

    Ok(())
}

async fn enqueue_session_repair_items_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    items: Vec<SessionRepairEnqueueItem>,
) -> Result<(), StoreError> {
    let jobs = items
        .into_iter()
        .map(|item| SessionRepairJobUpsert {
            project_id: item.project_id,
            install_id: item.install_id,
            target_event_id: item.target_event_id,
        })
        .collect::<Vec<_>>();
    upsert_session_repair_jobs_in_tx(tx, &jobs).await?;

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

#[cfg(test)]
#[derive(Debug, Default)]
struct RepairClaimBlocker {
    claimed: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
struct SessionRepairBatchHooks {
    claim_blocker: Option<std::sync::Arc<RepairClaimBlocker>>,
    blocked_install_id: Option<String>,
    panic_install_id: Option<String>,
    fail_join_error_release_once_install_id: Option<String>,
    join_error_release_failed_once: Option<std::sync::Arc<AtomicBool>>,
}

#[cfg(test)]
impl SessionRepairBatchHooks {
    fn disabled() -> Self {
        Self::default()
    }

    fn block_after_claim(claim_blocker: std::sync::Arc<RepairClaimBlocker>) -> Self {
        Self {
            claim_blocker: Some(claim_blocker),
            blocked_install_id: None,
            panic_install_id: None,
            fail_join_error_release_once_install_id: None,
            join_error_release_failed_once: None,
        }
    }

    fn block_after_claim_for_install(
        install_id: &str,
        claim_blocker: std::sync::Arc<RepairClaimBlocker>,
    ) -> Self {
        Self {
            claim_blocker: Some(claim_blocker),
            blocked_install_id: Some(install_id.to_owned()),
            panic_install_id: None,
            fail_join_error_release_once_install_id: None,
            join_error_release_failed_once: None,
        }
    }

    fn panic_after_claim_for_install(install_id: &str) -> Self {
        Self {
            claim_blocker: None,
            blocked_install_id: None,
            panic_install_id: Some(install_id.to_owned()),
            fail_join_error_release_once_install_id: None,
            join_error_release_failed_once: None,
        }
    }

    fn panic_after_claim_and_fail_join_error_release_once_for_install(install_id: &str) -> Self {
        Self {
            claim_blocker: None,
            blocked_install_id: None,
            panic_install_id: Some(install_id.to_owned()),
            fail_join_error_release_once_install_id: Some(install_id.to_owned()),
            join_error_release_failed_once: Some(std::sync::Arc::new(AtomicBool::new(false))),
        }
    }

    async fn after_claim(&self, install_id: &str) {
        if self
            .panic_install_id
            .as_deref()
            .is_some_and(|panic_install_id| panic_install_id == install_id)
        {
            panic!("session repair test hook forced panic after claim");
        }

        if let Some(claim_blocker) = &self.claim_blocker {
            if self
                .blocked_install_id
                .as_deref()
                .is_some_and(|blocked_install_id| blocked_install_id != install_id)
            {
                return;
            }
            claim_blocker.claimed.notify_one();
            claim_blocker.resume.notified().await;
        }
    }

    fn before_join_error_claim_release(&self, install_id: &str) -> Result<(), StoreError> {
        if self
            .fail_join_error_release_once_install_id
            .as_deref()
            .is_some_and(|failed_install_id| failed_install_id == install_id)
            && self
                .join_error_release_failed_once
                .as_ref()
                .is_some_and(|flag| !flag.swap(true, Ordering::SeqCst))
        {
            return Err(StoreError::InvariantViolation(
                "session repair test hook forced one join-error claim release failure".to_owned(),
            ));
        }

        Ok(())
    }
}

#[cfg(not(test))]
#[derive(Debug, Clone, Default)]
struct SessionRepairBatchHooks;

#[cfg(not(test))]
impl SessionRepairBatchHooks {
    fn disabled() -> Self {
        Self
    }

    async fn after_claim(&self, _install_id: &str) {}

    fn before_join_error_claim_release(&self, _install_id: &str) -> Result<(), StoreError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct SessionRepairTaskContext {
    claimed_job: std::sync::Mutex<Option<SessionRepairJobRecord>>,
}

impl SessionRepairTaskContext {
    fn set_claimed_job(&self, job: SessionRepairJobRecord) {
        *self
            .claimed_job
            .lock()
            .expect("session repair task context lock poisoned") = Some(job);
    }

    fn clear_claimed_job(&self) {
        *self
            .claimed_job
            .lock()
            .expect("session repair task context lock poisoned") = None;
    }

    fn claimed_job(&self) -> Option<SessionRepairJobRecord> {
        self.claimed_job
            .lock()
            .expect("session repair task context lock poisoned")
            .clone()
    }
}

pub async fn process_session_repair_batch(
    pool: &PgPool,
    concurrency: usize,
) -> Result<usize, StoreError> {
    process_session_repair_batch_with_config(pool, concurrency).await
}

pub(crate) async fn process_session_repair_batch_with_config(
    pool: &PgPool,
    concurrency: usize,
) -> Result<usize, StoreError> {
    process_session_repair_batch_inner(pool, concurrency, SessionRepairBatchHooks::disabled()).await
}

async fn process_session_repair_batch_inner(
    pool: &PgPool,
    concurrency: usize,
    hooks: SessionRepairBatchHooks,
) -> Result<usize, StoreError> {
    let mut work_set = JoinSet::new();
    let mut task_contexts = HashMap::new();
    for _ in 0..concurrency.max(1) {
        let pool = pool.clone();
        let hooks = hooks.clone();
        let task_context = std::sync::Arc::new(SessionRepairTaskContext::default());
        let spawned_context = task_context.clone();
        let abort_handle = work_set.spawn(async move {
            process_next_session_repair_job(&pool, hooks, spawned_context).await
        });
        task_contexts.insert(abort_handle.id(), task_context);
    }

    let mut processed = 0_usize;
    let mut batch_error = None;
    while let Some(join_result) = work_set.join_next_with_id().await {
        match join_result {
            Ok((task_id, Ok(result))) => {
                task_contexts.remove(&task_id);
                processed += usize::from(result.processed);
            }
            Ok((task_id, Err(error))) => {
                if let Err(cleanup_error) = release_join_error_session_repair_claim(
                    pool,
                    &mut task_contexts,
                    task_id,
                    &hooks,
                )
                .await
                {
                    if batch_error.is_none() {
                        batch_error = Some(StoreError::InvariantViolation(format!(
                            "session repair task returned {error}; releasing any unresolved claim also failed with {cleanup_error}"
                        )));
                    }
                    continue;
                }
                if batch_error.is_none() {
                    batch_error = Some(error);
                }
            }
            Err(join_error) if join_error.is_cancelled() => {
                let task_id = join_error.id();
                if let Err(cleanup_error) = release_join_error_session_repair_claim(
                    pool,
                    &mut task_contexts,
                    task_id,
                    &hooks,
                )
                .await
                {
                    if batch_error.is_none() {
                        batch_error = Some(StoreError::InvariantViolation(format!(
                            "session repair job task {task_id} was cancelled; releasing its claim also failed with {cleanup_error}"
                        )));
                    }
                    continue;
                }
                if batch_error.is_none() {
                    batch_error = Some(StoreError::InvariantViolation(
                        "session repair job task cancelled before resolving its claim".to_owned(),
                    ));
                }
            }
            Err(join_error) => {
                let task_id = join_error.id();
                if let Err(cleanup_error) = release_join_error_session_repair_claim(
                    pool,
                    &mut task_contexts,
                    task_id,
                    &hooks,
                )
                .await
                {
                    if batch_error.is_none() {
                        batch_error = Some(StoreError::InvariantViolation(format!(
                            "session repair job task {task_id} failed with {join_error}; releasing its claim also failed with {cleanup_error}"
                        )));
                    }
                    continue;
                }
                if batch_error.is_none() {
                    batch_error = Some(StoreError::InvariantViolation(format!(
                        "session repair job task failed: {join_error}"
                    )));
                }
            }
        }
    }

    if let Err(cleanup_error) =
        drain_join_error_session_repair_claims(pool, &mut task_contexts, &hooks).await
    {
        if let Some(batch_error) = batch_error {
            return Err(StoreError::InvariantViolation(format!(
                "session repair batch failed with {batch_error}; releasing unresolved join-error claims also failed with {cleanup_error}"
            )));
        }

        return Err(cleanup_error);
    }

    if let Err(drain_error) = drain_pending_session_rebuild_buckets(pool).await {
        if let Some(batch_error) = batch_error {
            return Err(StoreError::InvariantViolation(format!(
                "session repair batch failed with {batch_error}; draining queued rebuilds also failed with {drain_error}"
            )));
        }

        return Err(drain_error);
    }
    if let Err(drain_error) = drain_pending_active_install_range_rebuild_buckets(pool).await {
        if let Some(batch_error) = batch_error {
            return Err(StoreError::InvariantViolation(format!(
                "session repair batch failed with {batch_error}; draining active-install range rebuilds also failed with {drain_error}"
            )));
        }

        return Err(drain_error);
    }

    if let Some(batch_error) = batch_error {
        return Err(batch_error);
    }

    Ok(processed)
}

#[cfg(test)]
async fn process_session_repair_batch_with_hooks(
    pool: &PgPool,
    concurrency: usize,
    hooks: SessionRepairBatchHooks,
) -> Result<usize, StoreError> {
    process_session_repair_batch_inner(pool, concurrency, hooks).await
}

async fn drain_session_repair_jobs(pool: &PgPool, concurrency: usize) -> Result<(), StoreError> {
    while process_session_repair_batch_with_config(pool, concurrency).await? > 0 {}

    Ok(())
}

async fn process_next_session_repair_job(
    pool: &PgPool,
    hooks: SessionRepairBatchHooks,
    task_context: std::sync::Arc<SessionRepairTaskContext>,
) -> Result<SessionRepairTaskResult, StoreError> {
    let claim_started_at = Instant::now();
    let mut claim_tx = pool.begin().await.map_err(StoreError::from)?;
    let Some(job) = claim_pending_session_repair_jobs_in_tx(&mut claim_tx, 1)
        .await?
        .into_iter()
        .next()
    else {
        claim_tx.commit().await.map_err(StoreError::from)?;
        return Ok(SessionRepairTaskResult::default());
    };
    claim_tx.commit().await.map_err(StoreError::from)?;
    task_context.set_claimed_job(job.clone());
    let mut trace = StageBTraceRecord::repair_job(&job.install_id, job.target_event_id);
    trace.add_phase_ms(
        "row_mutation_lag_before_claim",
        row_mutation_lag_before_claim_ms(&job, claim_started_at.elapsed()),
    );

    hooks.after_claim(&job.install_id).await;
    let repair_result = async {
        let mut tx = pool.begin().await.map_err(StoreError::from)?;
        process_session_repair_job(&mut tx, job.clone(), &mut trace).await?;
        tx.commit().await.map_err(StoreError::from)
    }
    .await;
    if let Err(error) = repair_result {
        if let Err(cleanup_error) = release_failed_session_repair_claim(pool, &job).await {
            return Err(StoreError::InvariantViolation(format!(
                "repair job for project {} install {} failed with {error}; releasing the claim also failed with {cleanup_error}",
                job.project_id, job.install_id
            )));
        }

        task_context.clear_claimed_job();
        return Err(error);
    }

    task_context.clear_claimed_job();
    if let Err(error) = record_stage_b_trace(&trace) {
        warn!(install_id = %job.install_id, target_event_id = job.target_event_id, %error, "failed to record Stage B repair trace");
    }
    Ok(SessionRepairTaskResult { processed: true })
}

async fn release_join_error_session_repair_claim(
    pool: &PgPool,
    task_contexts: &mut HashMap<tokio::task::Id, std::sync::Arc<SessionRepairTaskContext>>,
    task_id: tokio::task::Id,
    hooks: &SessionRepairBatchHooks,
) -> Result<(), StoreError> {
    let Some(task_context) = task_contexts.get(&task_id) else {
        return Ok(());
    };
    let Some(job) = task_context.claimed_job() else {
        task_contexts.remove(&task_id);
        return Ok(());
    };

    hooks.before_join_error_claim_release(&job.install_id)?;
    release_failed_session_repair_claim(pool, &job).await?;
    task_context.clear_claimed_job();
    task_contexts.remove(&task_id);

    Ok(())
}

async fn drain_join_error_session_repair_claims(
    pool: &PgPool,
    task_contexts: &mut HashMap<tokio::task::Id, std::sync::Arc<SessionRepairTaskContext>>,
    hooks: &SessionRepairBatchHooks,
) -> Result<(), StoreError> {
    while !task_contexts.is_empty() {
        let task_ids: Vec<_> = task_contexts.keys().copied().collect();
        let mut progress = false;
        let mut last_error = None;

        for task_id in task_ids {
            match release_join_error_session_repair_claim(pool, task_contexts, task_id, hooks).await
            {
                Ok(()) => {
                    progress = true;
                }
                Err(error) => {
                    last_error = Some(error);
                }
            }
        }

        if task_contexts.is_empty() {
            return Ok(());
        }

        if !progress {
            return Err(last_error.unwrap_or_else(|| {
                StoreError::InvariantViolation(
                    "session repair join-error claim cleanup made no progress".to_owned(),
                )
            }));
        }
    }

    Ok(())
}

async fn release_failed_session_repair_claim(
    pool: &PgPool,
    job: &SessionRepairJobRecord,
) -> Result<(), StoreError> {
    let mut cleanup_tx = pool.begin().await.map_err(StoreError::from)?;
    release_session_repair_job_claim_in_tx(&mut cleanup_tx, job.project_id, &job.install_id)
        .await?;
    cleanup_tx.commit().await.map_err(StoreError::from)?;

    Ok(())
}

async fn process_session_repair_job(
    tx: &mut Transaction<'_, Postgres>,
    job: SessionRepairJobRecord,
    trace: &mut StageBTraceRecord,
) -> Result<(), StoreError> {
    let install_key = vec![(job.project_id, job.install_id.clone())];
    let tail_states = load_install_session_states_in_tx(tx, &install_key).await?;
    let Some(tail_state) = tail_states
        .get(&(job.project_id, job.install_id.clone()))
        .cloned()
    else {
        return Err(StoreError::InvariantViolation(format!(
            "repair frontier for project {} install {} has no install_session_state",
            job.project_id, job.install_id
        )));
    };

    if tail_state.last_processed_event_id >= job.target_event_id {
        complete_session_repair_job_in_tx(tx, job.project_id, &job.install_id, job.target_event_id)
            .await?;
        return Ok(());
    }

    let touched_buckets = repair_install_batch(
        tx,
        job.project_id,
        &job.install_id,
        tail_state.last_processed_event_id,
        job.target_event_id,
        trace,
    )
    .await?;
    let enqueue_started_at = Instant::now();
    enqueue_repair_bucket_finalization_in_tx(tx, job.project_id, &job.install_id, &touched_buckets)
        .await?;
    trace.add_phase_duration("touched_bucket_enqueue", enqueue_started_at.elapsed());
    trace.add_count("touched_day_buckets", touched_buckets.days.len());
    trace.add_count("touched_hour_buckets", touched_buckets.hours.len());
    complete_session_repair_job_in_tx(tx, job.project_id, &job.install_id, job.target_event_id)
        .await?;

    Ok(())
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
    let mut trace = EventLaneTraceRecord::event_metrics_batch(
        last_processed_event_id,
        next_offset,
        processed_events,
    );
    let build_started_at = Instant::now();
    let rollups = build_event_metric_rollups(&batch);
    let live_install_upserts = build_live_install_state_upserts(&batch);
    trace.add_phase_duration("build_rollups", build_started_at.elapsed());
    trace.add_count("total_delta_rows", rollups.total_deltas.len());
    trace.add_count("dim1_delta_rows", rollups.dim1_deltas.len());
    trace.add_count("dim2_delta_rows", rollups.dim2_deltas.len());
    trace.add_count("live_install_rows", live_install_upserts.len());

    let total_upsert_started_at = Instant::now();
    upsert_event_metric_buckets_total_in_tx(&mut tx, &rollups.total_deltas).await?;
    trace.add_phase_duration("upsert_total", total_upsert_started_at.elapsed());
    let dim1_upsert_started_at = Instant::now();
    upsert_event_metric_buckets_dim1_in_tx(&mut tx, &rollups.dim1_deltas).await?;
    trace.add_phase_duration("upsert_dim1", dim1_upsert_started_at.elapsed());
    let dim2_upsert_started_at = Instant::now();
    upsert_event_metric_buckets_dim2_in_tx(&mut tx, &rollups.dim2_deltas).await?;
    trace.add_phase_duration("upsert_dim2", dim2_upsert_started_at.elapsed());
    let live_install_upsert_started_at = Instant::now();
    upsert_live_install_state_in_tx(&mut tx, &live_install_upserts).await?;
    trace.add_phase_duration(
        "upsert_live_install_state",
        live_install_upsert_started_at.elapsed(),
    );
    let save_offset_started_at = Instant::now();
    save_worker_offset_in_tx(&mut tx, EVENT_METRICS_WORKER_NAME, next_offset).await?;
    trace.add_phase_duration("save_offset", save_offset_started_at.elapsed());
    let commit_started_at = Instant::now();
    tx.commit().await.map_err(StoreError::from)?;
    trace.add_phase_duration("commit", commit_started_at.elapsed());
    record_event_lane_trace(&trace)?;

    Ok(processed_events)
}

#[derive(Debug, Default)]
struct EventMetricRollups {
    total_deltas: Vec<EventMetricBucketTotalDelta>,
    dim1_deltas: Vec<EventMetricBucketDim1Delta>,
    dim2_deltas: Vec<EventMetricBucketDim2Delta>,
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
        }
    }

    EventMetricRollups {
        total_deltas: totals
            .into_iter()
            .map(
                |((project_id, granularity, bucket_start, event_name), event_count)| {
                    EventMetricBucketTotalDelta {
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
                    EventMetricBucketDim1Delta {
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
                )| EventMetricBucketDim2Delta {
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
    }
}

fn build_live_install_state_upserts(batch: &[RawEventRecord]) -> Vec<LiveInstallStateUpsert> {
    let mut latest_by_install = BTreeMap::<(Uuid, String), DateTime<Utc>>::new();

    for event in batch {
        let key = (event.project_id, event.install_id.clone());
        latest_by_install
            .entry(key)
            .and_modify(|received_at| {
                if event.received_at > *received_at {
                    *received_at = event.received_at;
                }
            })
            .or_insert(event.received_at);
    }

    latest_by_install
        .into_iter()
        .map(
            |((project_id, install_id), last_received_at)| LiveInstallStateUpsert {
                project_id,
                install_id,
                last_received_at,
            },
        )
        .collect()
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

fn active_install_slice_delta(session: &SessionRecord) -> SessionActiveInstallSliceDelta {
    SessionActiveInstallSliceDelta {
        project_id: session.project_id,
        day: session.session_start.date_naive(),
        install_id: session.install_id.clone(),
        platform: platform_dimension_value(&session.platform),
        app_version: session.app_version.clone(),
        app_version_is_null: session.app_version.is_none(),
        os_version: session.os_version.clone(),
        os_version_is_null: session.os_version.is_none(),
        properties: session.properties.clone(),
    }
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
                os_version: event.os_version.clone(),
                properties: event.properties.clone(),
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
                os_version: event.os_version.clone(),
                properties: event.properties.clone(),
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

    let mut daily_session_counts_by_day = BTreeMap::<NaiveDate, i64>::new();
    let mut daily_duration_deltas_by_day = BTreeMap::<NaiveDate, i64>::new();
    let mut daily_install_counts = BTreeMap::<NaiveDate, i32>::new();
    let mut metric_rebuild_scope = SessionRepairBuckets::default();

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
            metric_rebuild_scope.insert_session(updated_tail);
        }
    }

    for session in &append_sessions.new_sessions {
        let day = session.session_start.date_naive();
        *daily_session_counts_by_day.entry(day).or_default() += 1;
        *daily_duration_deltas_by_day.entry(day).or_default() +=
            i64::from(session.duration_seconds);
        *daily_install_counts.entry(day).or_default() += 1;
        metric_rebuild_scope.insert_session(session);
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
        metric_rebuild_scope,
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
        plan.metric_rebuild_scope
            .insert_timestamp(first_seen.first_seen_at);
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

    let active_install_slices = plan
        .new_sessions
        .iter()
        .map(active_install_slice_delta)
        .collect::<Vec<_>>();
    upsert_session_active_install_slices_in_tx(tx, &active_install_slices).await?;

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

    enqueue_touched_session_metric_buckets_in_tx(
        tx,
        plan.next_state.project_id,
        &plan.metric_rebuild_scope,
    )
    .await?;
    enqueue_touched_active_install_range_buckets_in_tx(
        tx,
        plan.next_state.project_id,
        &plan.metric_rebuild_scope.days,
    )
    .await?;

    upsert_install_session_state_in_tx(tx, &plan.next_state).await?;

    Ok(())
}

async fn repair_install_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    project_id: Uuid,
    install_id: &str,
    last_processed_event_id: i64,
    target_event_id: i64,
    trace: &mut StageBTraceRecord,
) -> Result<SessionRepairBuckets, StoreError> {
    let frontier_fetch_started_at = Instant::now();
    let pending_events = fetch_events_for_install_after_id_through_in_tx(
        tx,
        project_id,
        install_id,
        last_processed_event_id,
        target_event_id,
    )
    .await?;
    trace.add_phase_duration("frontier_fetch", frontier_fetch_started_at.elapsed());
    trace.add_count("pending_frontier_events", pending_events.len());
    let mut pending_events_iter = pending_events.iter();
    let first_pending_event = pending_events_iter.next().ok_or_else(|| {
            StoreError::InvariantViolation(format!(
                "repair frontier for project {project_id} install {install_id} had no raw events in ({last_processed_event_id}, {target_event_id}]"
            ))
        })?;
    let first_pending_event_ts = first_pending_event.timestamp.to_owned();
    let (batch_min_ts, batch_max_ts) = pending_events_iter.fold(
        (
            first_pending_event_ts.to_owned(),
            first_pending_event_ts.to_owned(),
        ),
        |(batch_min_ts, batch_max_ts), event| {
            let timestamp = event.timestamp.to_owned();
            (batch_min_ts.min(timestamp), batch_max_ts.max(timestamp))
        },
    );

    let recompute_window_started_at = Instant::now();
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
    trace.add_phase_duration(
        "recompute_window_load",
        recompute_window_started_at.elapsed(),
    );
    trace.add_count("window_raw_events", raw_events.len());
    trace.add_count("overlapping_sessions", overlapping_sessions.len());
    let session_rewrite_started_at = Instant::now();
    let repaired_sessions = derive_sessions_for_window(&raw_events);
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

    let next_state = install_state_from_session(&latest_session, target_event_id);
    upsert_install_session_state_in_tx(tx, &next_state).await?;
    trace.add_phase_duration("session_rewrite", session_rewrite_started_at.elapsed());
    trace.add_count("repaired_sessions_written", repaired_sessions.len());

    Ok(touched_buckets)
}

async fn rebuild_touched_session_buckets_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    touched_buckets: &SessionRepairBuckets,
    trace: &mut StageBTraceRecord,
) -> Result<(), StoreError> {
    if touched_buckets.is_empty() {
        return Ok(());
    }

    let days = touched_buckets.days.iter().copied().collect::<Vec<_>>();
    if !days.is_empty() {
        let repair_days = load_pending_session_daily_install_rebuilds_in_tx(tx, &[project_id])
            .await?
            .into_iter()
            .map(|row| row.day)
            .filter(|day| touched_buckets.days.contains(day))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if !repair_days.is_empty() {
            let session_daily_started_at = Instant::now();
            let session_daily_telemetry =
                rebuild_session_daily_days_from_pending_install_rebuilds_with_telemetry_in_tx(
                    tx,
                    project_id,
                    &repair_days,
                )
                .await?;
            trace.add_finalization_subphase_duration(
                "finalization_session_daily_rebuild",
                session_daily_started_at.elapsed(),
            );
            if session_daily_telemetry.installs_write_ms > 0 {
                trace.add_session_daily_subphase_ms(
                    "session_daily_installs_write",
                    session_daily_telemetry.installs_write_ms,
                );
            }
            if session_daily_telemetry.aggregate_write_ms > 0 {
                trace.add_session_daily_subphase_ms(
                    "session_daily_aggregate_write",
                    session_daily_telemetry.aggregate_write_ms,
                );
            }
            if let Some(cleanup_ms) = session_daily_telemetry.cleanup_ms
                && cleanup_ms > 0
            {
                trace.add_session_daily_subphase_ms("session_daily_cleanup", cleanup_ms);
            }
            let active_install_membership_started_at = Instant::now();
            rebuild_session_active_install_slices_days_in_tx(tx, project_id, &repair_days).await?;
            trace.add_finalization_subphase_duration(
                "finalization_active_install_slice_rebuild",
                active_install_membership_started_at.elapsed(),
            );
        }
        let metric_rebuild_started_at = Instant::now();
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
        trace.add_finalization_subphase_duration(
            "finalization_metric_rebuild",
            metric_rebuild_started_at.elapsed(),
        );
    }

    let hours = touched_buckets.hours.iter().copied().collect::<Vec<_>>();
    if !hours.is_empty() {
        let metric_rebuild_started_at = Instant::now();
        rebuild_session_metric_buckets_in_tx(tx, project_id, MetricGranularity::Hour, &hours)
            .await?;
        trace.add_finalization_subphase_duration(
            "finalization_metric_rebuild",
            metric_rebuild_started_at.elapsed(),
        );
    }

    Ok(())
}

async fn enqueue_touched_session_metric_buckets_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    touched_buckets: &SessionRepairBuckets,
) -> Result<(), StoreError> {
    if touched_buckets.is_empty() {
        return Ok(());
    }

    let day_bucket_starts = touched_buckets
        .days
        .iter()
        .copied()
        .map(bucket_start_for_day)
        .collect::<Vec<_>>();
    let hour_bucket_starts = touched_buckets.hours.iter().copied().collect::<Vec<_>>();
    enqueue_session_rebuild_buckets_in_tx(tx, project_id, &day_bucket_starts, &hour_bucket_starts)
        .await
}

async fn enqueue_touched_active_install_range_buckets_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    touched_days: &BTreeSet<NaiveDate>,
) -> Result<(), StoreError> {
    if touched_days.is_empty() {
        return Ok(());
    }

    let mut pending_rows = Vec::with_capacity(touched_days.len() * 3);
    for day in touched_days {
        pending_rows.push((MetricGranularity::Week, bucket_start_for_week(*day)));
        pending_rows.push((MetricGranularity::Month, bucket_start_for_month(*day)));
        pending_rows.push((MetricGranularity::Year, bucket_start_for_year(*day)));
    }
    pending_rows.sort();
    pending_rows.dedup();

    let mut builder = sqlx::QueryBuilder::<Postgres>::new(
        "INSERT INTO active_install_range_rebuild_queue (project_id, granularity, bucket_start) ",
    );
    builder.push_values(pending_rows, |mut row, (granularity, bucket_start)| {
        row.push_bind(project_id)
            .push_bind(granularity.as_str())
            .push_bind(bucket_start);
    });
    builder.push(" ON CONFLICT (project_id, granularity, bucket_start) DO NOTHING");
    builder.build().execute(&mut **tx).await?;

    Ok(())
}

async fn enqueue_repair_bucket_finalization_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    project_id: Uuid,
    install_id: &str,
    touched_buckets: &SessionRepairBuckets,
) -> Result<(), StoreError> {
    let day_bucket_starts = touched_buckets
        .days
        .iter()
        .copied()
        .map(bucket_start_for_day)
        .collect::<Vec<_>>();
    let hour_bucket_starts = touched_buckets.hours.iter().copied().collect::<Vec<_>>();

    let install_rebuild_rows = touched_buckets
        .days
        .iter()
        .copied()
        .map(|day| PendingSessionDailyInstallRebuildRecord {
            project_id,
            day,
            install_id: install_id.to_owned(),
        })
        .collect::<Vec<_>>();
    // Keep install-day queue rows and shared day/hour bucket rows in the same transaction so a
    // later finalizer only sees install-day work once the matching shared bucket scope is visible.
    enqueue_session_daily_installs_rebuilds_in_tx(tx, &install_rebuild_rows).await?;
    enqueue_session_rebuild_buckets_in_tx(tx, project_id, &day_bucket_starts, &hour_bucket_starts)
        .await?;
    enqueue_touched_active_install_range_buckets_in_tx(tx, project_id, &touched_buckets.days).await
}

async fn drain_pending_session_rebuild_buckets(pool: &PgPool) -> Result<(), StoreError> {
    let _drain_guard = SESSION_REBUILD_DRAIN_LOCK.lock().await;

    loop {
        let mut tx = pool.begin().await.map_err(StoreError::from)?;
        let swept = claim_and_sweep_pending_session_rebuild_buckets_in_tx(&mut tx, 512).await?;
        if swept.is_empty() {
            tx.commit().await.map_err(StoreError::from)?;
            break;
        }
        let finalize_started_at = Instant::now();
        let drain_setup_started_at = Instant::now();
        let mut trace = StageBTraceRecord::rebuild_finalize();
        trace.add_count("claimed_rebuild_bucket_rows", swept.len());

        let mut buckets_by_project = BTreeMap::<Uuid, SessionRepairBuckets>::new();
        for bucket in swept {
            let touched_buckets = buckets_by_project.entry(bucket.project_id).or_default();
            match bucket.granularity {
                MetricGranularity::Day => {
                    touched_buckets
                        .days
                        .insert(bucket.bucket_start.date_naive());
                }
                MetricGranularity::Hour => {
                    touched_buckets.hours.insert(bucket.bucket_start);
                }
                MetricGranularity::Week | MetricGranularity::Month | MetricGranularity::Year => {
                    return Err(StoreError::InvariantViolation(format!(
                        "session rebuild queue should not contain {} buckets",
                        bucket.granularity.as_str()
                    )));
                }
            }
        }

        trace.add_count("drain_projects", buckets_by_project.len());
        let touched_day_buckets = buckets_by_project
            .values()
            .map(|buckets| buckets.days.len())
            .sum::<usize>();
        let touched_hour_buckets = buckets_by_project
            .values()
            .map(|buckets| buckets.hours.len())
            .sum::<usize>();
        trace.add_count("touched_day_buckets", touched_day_buckets);
        trace.add_count("touched_hour_buckets", touched_hour_buckets);
        trace.add_finalization_subphase_duration(
            "finalization_drain_setup",
            drain_setup_started_at.elapsed(),
        );

        for (project_id, touched_buckets) in buckets_by_project {
            rebuild_touched_session_buckets_in_tx(
                &mut tx,
                project_id,
                &touched_buckets,
                &mut trace,
            )
            .await?;
        }

        let commit_started_at = Instant::now();
        tx.commit().await.map_err(StoreError::from)?;
        trace.add_finalization_subphase_duration(
            "finalization_drain_setup",
            commit_started_at.elapsed(),
        );
        trace.add_phase_duration("shared_bucket_finalize", finalize_started_at.elapsed());
        if let Err(error) = record_stage_b_trace(&trace) {
            warn!(%error, "failed to record Stage B rebuild-finalize trace");
        }
    }

    Ok(())
}

async fn drain_pending_active_install_range_rebuild_buckets(
    pool: &PgPool,
) -> Result<(), StoreError> {
    let _drain_guard = ACTIVE_INSTALL_RANGE_REBUILD_DRAIN_LOCK.lock().await;

    loop {
        let mut tx = pool.begin().await.map_err(StoreError::from)?;
        let rows = sqlx::query(
            r#"
            WITH claimable AS (
                SELECT project_id, granularity, bucket_start
                FROM active_install_range_rebuild_queue
                ORDER BY project_id ASC, granularity ASC, bucket_start ASC
                LIMIT 512
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM active_install_range_rebuild_queue AS queue
            USING claimable
            WHERE queue.project_id = claimable.project_id
              AND queue.granularity = claimable.granularity
              AND queue.bucket_start = claimable.bucket_start
            RETURNING queue.project_id, queue.granularity, queue.bucket_start
            "#,
        )
        .fetch_all(&mut *tx)
        .await?;

        if rows.is_empty() {
            tx.commit().await.map_err(StoreError::from)?;
            break;
        }

        let mut buckets_by_scope = BTreeMap::<(Uuid, MetricGranularity), Vec<DateTime<Utc>>>::new();
        for row in rows {
            let granularity = match row.try_get::<&str, _>("granularity")? {
                "week" => MetricGranularity::Week,
                "month" => MetricGranularity::Month,
                "year" => MetricGranularity::Year,
                other => {
                    return Err(StoreError::InvariantViolation(format!(
                        "invalid active-install range rebuild granularity {other}"
                    )));
                }
            };
            buckets_by_scope
                .entry((row.try_get("project_id")?, granularity))
                .or_default()
                .push(row.try_get("bucket_start")?);
        }

        for ((project_id, granularity), bucket_starts) in buckets_by_scope {
            rebuild_active_install_range_buckets_in_tx(
                &mut tx,
                project_id,
                granularity,
                &bucket_starts,
            )
            .await?;
        }

        tx.commit().await.map_err(StoreError::from)?;
    }

    Ok(())
}

async fn rebuild_active_install_range_buckets_in_tx(
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

    debug_assert!(matches!(
        granularity,
        MetricGranularity::Week | MetricGranularity::Month | MetricGranularity::Year
    ));

    for table in [
        "active_install_metric_buckets_total",
        "active_install_metric_buckets_dim1",
        "active_install_metric_buckets_dim2",
    ] {
        let sql = format!(
            "DELETE FROM {table} WHERE project_id = $1 AND granularity = $2 AND bucket_start = ANY($3)"
        );
        sqlx::query(&sql)
            .bind(project_id)
            .bind(granularity.as_str())
            .bind(&bucket_starts)
            .execute(&mut **tx)
            .await?;
    }

    let bucket_expr = bucket_sql(granularity, "session_start");

    let total_sql = format!(
        r#"
        WITH scoped_sessions AS MATERIALIZED (
            SELECT
                {bucket_expr} AS bucket_start,
                install_id
            FROM sessions
            WHERE project_id = $1
              AND {bucket_expr} = ANY($2)
        ),
        distinct_installs AS MATERIALIZED (
            SELECT DISTINCT bucket_start, install_id
            FROM scoped_sessions
        )
        INSERT INTO active_install_metric_buckets_total (
            project_id,
            granularity,
            bucket_start,
            active_installs
        )
        SELECT
            $1,
            $3,
            bucket_start,
            COUNT(*)::BIGINT AS active_installs
        FROM distinct_installs
        GROUP BY bucket_start
        "#,
    );
    sqlx::query(&total_sql)
        .bind(project_id)
        .bind(&bucket_starts)
        .bind(granularity.as_str())
        .execute(&mut **tx)
        .await?;

    let dim1_sql = format!(
        r#"
        WITH scoped_sessions AS MATERIALIZED (
            SELECT
                {bucket_expr} AS bucket_start,
                install_id,
                platform,
                app_version,
                os_version,
                properties
            FROM sessions
            WHERE project_id = $1
              AND {bucket_expr} = ANY($2)
        ),
        distinct_dims AS MATERIALIZED (
            SELECT DISTINCT
                bucket_start,
                install_id,
                dim_key,
                dim_value,
                dim_value_is_null
            FROM (
                SELECT
                    bucket_start,
                    install_id,
                    'platform'::text AS dim_key,
                    platform::text AS dim_value,
                    false AS dim_value_is_null
                FROM scoped_sessions
                UNION ALL
                SELECT
                    bucket_start,
                    install_id,
                    'app_version'::text,
                    COALESCE(app_version, ''),
                    app_version IS NULL
                FROM scoped_sessions
                UNION ALL
                SELECT
                    bucket_start,
                    install_id,
                    'os_version'::text,
                    COALESCE(os_version, ''),
                    os_version IS NULL
                FROM scoped_sessions
                UNION ALL
                SELECT
                    scoped_sessions.bucket_start,
                    scoped_sessions.install_id,
                    props.key,
                    props.value,
                    false
                FROM scoped_sessions
                CROSS JOIN LATERAL jsonb_each_text(COALESCE(scoped_sessions.properties, '{{}}'::jsonb)) AS props(key, value)
            ) dims
        )
        INSERT INTO active_install_metric_buckets_dim1 (
            project_id,
            granularity,
            bucket_start,
            dim1_key,
            dim1_value,
            dim1_value_is_null,
            active_installs
        )
        SELECT
            $1,
            $3,
            bucket_start,
            dim_key,
            dim_value,
            dim_value_is_null,
            COUNT(*)::BIGINT AS active_installs
        FROM distinct_dims
        GROUP BY bucket_start, dim_key, dim_value, dim_value_is_null
        "#,
    );
    sqlx::query(&dim1_sql)
        .bind(project_id)
        .bind(&bucket_starts)
        .bind(granularity.as_str())
        .execute(&mut **tx)
        .await?;

    let dim2_sql = format!(
        r#"
        WITH scoped_sessions AS MATERIALIZED (
            SELECT
                {bucket_expr} AS bucket_start,
                install_id,
                platform,
                app_version,
                os_version,
                properties
            FROM sessions
            WHERE project_id = $1
              AND {bucket_expr} = ANY($2)
        ),
        distinct_dims AS MATERIALIZED (
            SELECT DISTINCT
                bucket_start,
                install_id,
                dim_key,
                dim_value,
                dim_value_is_null
            FROM (
                SELECT
                    bucket_start,
                    install_id,
                    'platform'::text AS dim_key,
                    platform::text AS dim_value,
                    false AS dim_value_is_null
                FROM scoped_sessions
                UNION ALL
                SELECT
                    bucket_start,
                    install_id,
                    'app_version'::text,
                    COALESCE(app_version, ''),
                    app_version IS NULL
                FROM scoped_sessions
                UNION ALL
                SELECT
                    bucket_start,
                    install_id,
                    'os_version'::text,
                    COALESCE(os_version, ''),
                    os_version IS NULL
                FROM scoped_sessions
                UNION ALL
                SELECT
                    scoped_sessions.bucket_start,
                    scoped_sessions.install_id,
                    props.key,
                    props.value,
                    false
                FROM scoped_sessions
                CROSS JOIN LATERAL jsonb_each_text(COALESCE(scoped_sessions.properties, '{{}}'::jsonb)) AS props(key, value)
            ) dims
        ),
        distinct_pairs AS MATERIALIZED (
            SELECT DISTINCT
                left_dims.bucket_start,
                left_dims.install_id,
                left_dims.dim_key AS dim1_key,
                left_dims.dim_value AS dim1_value,
                left_dims.dim_value_is_null AS dim1_value_is_null,
                right_dims.dim_key AS dim2_key,
                right_dims.dim_value AS dim2_value,
                right_dims.dim_value_is_null AS dim2_value_is_null
            FROM distinct_dims AS left_dims
            INNER JOIN distinct_dims AS right_dims
                ON left_dims.bucket_start = right_dims.bucket_start
               AND left_dims.install_id = right_dims.install_id
               AND left_dims.dim_key < right_dims.dim_key
        )
        INSERT INTO active_install_metric_buckets_dim2 (
            project_id,
            granularity,
            bucket_start,
            dim1_key,
            dim1_value,
            dim1_value_is_null,
            dim2_key,
            dim2_value,
            dim2_value_is_null,
            active_installs
        )
        SELECT
            $1,
            $3,
            bucket_start,
            dim1_key,
            dim1_value,
            dim1_value_is_null,
            dim2_key,
            dim2_value,
            dim2_value_is_null,
            COUNT(*)::BIGINT AS active_installs
        FROM distinct_pairs
        GROUP BY
            bucket_start,
            dim1_key,
            dim1_value,
            dim1_value_is_null,
            dim2_key,
            dim2_value,
            dim2_value_is_null
        "#,
    );
    sqlx::query(&dim2_sql)
        .bind(project_id)
        .bind(&bucket_starts)
        .bind(granularity.as_str())
        .execute(&mut **tx)
        .await?;

    Ok(())
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
            os_version: event.os_version.clone(),
            properties: event.properties.clone(),
        })
        .collect::<Vec<_>>();

    sessionization::derive_sessions(&session_events)
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
        MetricGranularity::Hour => DateTime::from_naive_utc_and_offset(
            timestamp
                .date_naive()
                .and_hms_opt(timestamp.hour(), 0, 0)
                .expect("top-of-hour is a valid UTC datetime"),
            Utc,
        ),
        MetricGranularity::Day => bucket_start_for_day(timestamp.date_naive()),
        MetricGranularity::Week => bucket_start_for_week(timestamp.date_naive()),
        MetricGranularity::Month => bucket_start_for_month(timestamp.date_naive()),
        MetricGranularity::Year => bucket_start_for_year(timestamp.date_naive()),
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
        MetricGranularity::Week => "week",
        MetricGranularity::Month => "month",
        MetricGranularity::Year => "year",
    };

    format!("date_trunc('{precision}', {column} AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'")
}

fn bucket_start_for_week(day: NaiveDate) -> DateTime<Utc> {
    let week_start = day - ChronoDuration::days(i64::from(day.weekday().num_days_from_monday()));
    bucket_start_for_day(week_start)
}

fn bucket_start_for_month(day: NaiveDate) -> DateTime<Utc> {
    bucket_start_for_day(
        NaiveDate::from_ymd_opt(day.year(), day.month(), 1).expect("valid month start"),
    )
}

fn bucket_start_for_year(day: NaiveDate) -> DateTime<Utc> {
    bucket_start_for_day(NaiveDate::from_ymd_opt(day.year(), 1, 1).expect("valid year start"))
}

fn session_dimension_sql(source_alias: &str) -> String {
    format!(
        r#"
        SELECT 'platform'::text AS dim_key, {source_alias}.platform::text AS dim_value
        UNION ALL
        SELECT 'app_version'::text, {source_alias}.app_version
        WHERE {source_alias}.app_version IS NOT NULL
        UNION ALL
        SELECT 'os_version'::text, {source_alias}.os_version
        WHERE {source_alias}.os_version IS NOT NULL
        UNION ALL
        SELECT props.key, props.value
        FROM jsonb_each_text(COALESCE({source_alias}.properties, '{{}}'::jsonb)) AS props(key, value)
        "#
    )
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

    let bucket_expr = bucket_sql(granularity, "scope.session_start");
    let first_seen_bucket_expr = bucket_sql(granularity, "scope.first_seen_at");
    let session_dims = session_dimension_sql("scoped_sessions");
    let install_dims = session_dimension_sql("scoped_installs");

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

    sqlx::query(&format!(
        r#"
        WITH scoped_sessions AS MATERIALIZED (
            SELECT project_id, {bucket_expr} AS bucket_start, duration_seconds
            FROM sessions AS scope
            WHERE scope.project_id = $1
              AND {bucket_expr} = ANY($3)
        ),
        scoped_installs AS MATERIALIZED (
            SELECT project_id, {first_seen_bucket_expr} AS bucket_start
            FROM install_first_seen AS scope
            WHERE scope.project_id = $1
              AND {first_seen_bucket_expr} = ANY($3)
        ),
        combined AS (
            SELECT project_id, bucket_start, COUNT(*)::BIGINT AS session_count,
                   COALESCE(SUM(duration_seconds), 0)::BIGINT AS duration_total_seconds,
                   0::BIGINT AS new_installs
            FROM scoped_sessions
            GROUP BY project_id, bucket_start
            UNION ALL
            SELECT project_id, bucket_start, 0::BIGINT, 0::BIGINT, COUNT(*)::BIGINT
            FROM scoped_installs
            GROUP BY project_id, bucket_start
        )
        INSERT INTO session_metric_buckets_total (
            project_id, granularity, bucket_start, session_count, duration_total_seconds, new_installs
        )
        SELECT
            project_id,
            $2,
            bucket_start,
            SUM(session_count)::BIGINT,
            SUM(duration_total_seconds)::BIGINT,
            SUM(new_installs)::BIGINT
        FROM combined
        GROUP BY project_id, bucket_start
        "#
    ))
    .bind(project_id)
    .bind(granularity.as_str())
    .bind(&bucket_starts)
    .execute(&mut **tx)
    .await?;

    sqlx::query(&format!(
        r#"
        WITH scoped_sessions AS MATERIALIZED (
            SELECT project_id, {bucket_expr} AS bucket_start, duration_seconds, platform, app_version, os_version, properties
            FROM sessions AS scope
            WHERE scope.project_id = $1
              AND {bucket_expr} = ANY($3)
        ),
        scoped_installs AS MATERIALIZED (
            SELECT project_id, {first_seen_bucket_expr} AS bucket_start, platform, app_version, os_version, properties
            FROM install_first_seen AS scope
            WHERE scope.project_id = $1
              AND {first_seen_bucket_expr} = ANY($3)
        ),
        combined AS (
            SELECT
                scoped_sessions.project_id,
                scoped_sessions.bucket_start,
                dims.dim_key AS dim1_key,
                dims.dim_value AS dim1_value,
                COUNT(*)::BIGINT AS session_count,
                COALESCE(SUM(scoped_sessions.duration_seconds), 0)::BIGINT AS duration_total_seconds,
                0::BIGINT AS new_installs
            FROM scoped_sessions
            CROSS JOIN LATERAL ({session_dims}) AS dims(dim_key, dim_value)
            GROUP BY scoped_sessions.project_id, scoped_sessions.bucket_start, dims.dim_key, dims.dim_value
            UNION ALL
            SELECT
                scoped_installs.project_id,
                scoped_installs.bucket_start,
                dims.dim_key,
                dims.dim_value,
                0::BIGINT,
                0::BIGINT,
                COUNT(*)::BIGINT
            FROM scoped_installs
            CROSS JOIN LATERAL ({install_dims}) AS dims(dim_key, dim_value)
            GROUP BY scoped_installs.project_id, scoped_installs.bucket_start, dims.dim_key, dims.dim_value
        )
        INSERT INTO session_metric_buckets_dim1 (
            project_id, granularity, bucket_start, dim1_key, dim1_value, session_count, duration_total_seconds, new_installs
        )
        SELECT
            project_id,
            $2,
            bucket_start,
            dim1_key,
            dim1_value,
            SUM(session_count)::BIGINT,
            SUM(duration_total_seconds)::BIGINT,
            SUM(new_installs)::BIGINT
        FROM combined
        GROUP BY project_id, bucket_start, dim1_key, dim1_value
        "#
    ))
    .bind(project_id)
    .bind(granularity.as_str())
    .bind(&bucket_starts)
    .execute(&mut **tx)
    .await?;

    sqlx::query(&format!(
        r#"
        WITH scoped_sessions AS MATERIALIZED (
            SELECT project_id, session_id, {bucket_expr} AS bucket_start, duration_seconds, platform, app_version, os_version, properties
            FROM sessions AS scope
            WHERE scope.project_id = $1
              AND {bucket_expr} = ANY($3)
        ),
        scoped_installs AS MATERIALIZED (
            SELECT project_id, install_id, {first_seen_bucket_expr} AS bucket_start, platform, app_version, os_version, properties
            FROM install_first_seen AS scope
            WHERE scope.project_id = $1
              AND {first_seen_bucket_expr} = ANY($3)
        ),
        session_dim_source AS MATERIALIZED (
            SELECT scoped_sessions.project_id, scoped_sessions.session_id, scoped_sessions.bucket_start, scoped_sessions.duration_seconds, dims.dim_key, dims.dim_value
            FROM scoped_sessions
            CROSS JOIN LATERAL ({session_dims}) AS dims(dim_key, dim_value)
        ),
        install_dim_source AS MATERIALIZED (
            SELECT scoped_installs.project_id, scoped_installs.install_id, scoped_installs.bucket_start, dims.dim_key, dims.dim_value
            FROM scoped_installs
            CROSS JOIN LATERAL ({install_dims}) AS dims(dim_key, dim_value)
        ),
        combined AS (
            SELECT
                left_dims.project_id,
                left_dims.bucket_start,
                left_dims.dim_key AS dim1_key,
                left_dims.dim_value AS dim1_value,
                right_dims.dim_key AS dim2_key,
                right_dims.dim_value AS dim2_value,
                COUNT(*)::BIGINT AS session_count,
                COALESCE(SUM(left_dims.duration_seconds), 0)::BIGINT AS duration_total_seconds,
                0::BIGINT AS new_installs
            FROM session_dim_source AS left_dims
            JOIN session_dim_source AS right_dims
              ON left_dims.project_id = right_dims.project_id
             AND left_dims.session_id = right_dims.session_id
             AND left_dims.bucket_start = right_dims.bucket_start
             AND left_dims.dim_key < right_dims.dim_key
            GROUP BY left_dims.project_id, left_dims.bucket_start, left_dims.dim_key, left_dims.dim_value, right_dims.dim_key, right_dims.dim_value
            UNION ALL
            SELECT
                left_dims.project_id,
                left_dims.bucket_start,
                left_dims.dim_key,
                left_dims.dim_value,
                right_dims.dim_key,
                right_dims.dim_value,
                0::BIGINT,
                0::BIGINT,
                COUNT(*)::BIGINT
            FROM install_dim_source AS left_dims
            JOIN install_dim_source AS right_dims
              ON left_dims.project_id = right_dims.project_id
             AND left_dims.install_id = right_dims.install_id
             AND left_dims.bucket_start = right_dims.bucket_start
             AND left_dims.dim_key < right_dims.dim_key
            GROUP BY left_dims.project_id, left_dims.bucket_start, left_dims.dim_key, left_dims.dim_value, right_dims.dim_key, right_dims.dim_value
        )
        INSERT INTO session_metric_buckets_dim2 (
            project_id, granularity, bucket_start, dim1_key, dim1_value, dim2_key, dim2_value, session_count, duration_total_seconds, new_installs
        )
        SELECT
            project_id,
            $2,
            bucket_start,
            dim1_key,
            dim1_value,
            dim2_key,
            dim2_value,
            SUM(session_count)::BIGINT,
            SUM(duration_total_seconds)::BIGINT,
            SUM(new_installs)::BIGINT
        FROM combined
        GROUP BY project_id, bucket_start, dim1_key, dim1_value, dim2_key, dim2_value
        "#
    ))
    .bind(project_id)
    .bind(granularity.as_str())
    .bind(&bucket_starts)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, fs, sync::Arc, sync::LazyLock, sync::Mutex, time::Duration};

    use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, TimeZone, Utc};
    use fantasma_core::{EventPayload, Platform};
    use fantasma_store::{
        BootstrapConfig, RawEventRecord, average_session_duration_seconds, count_active_installs,
        count_sessions, fetch_latest_session_for_install, insert_events,
        load_install_session_state, load_pending_session_rebuild_buckets_in_tx, load_worker_offset,
        save_worker_offset,
    };
    use sqlx::Row;
    use tokio::sync::Notify;

    static TRACE_ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn lock_trace_env() -> std::sync::MutexGuard<'static, ()> {
        TRACE_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

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
            received_at: timestamp(1, 0, 1),
            install_id: "install-1".to_owned(),
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
            os_version: Some("18.3".to_owned()),
            properties: BTreeMap::from([
                ("plan".to_owned(), "pro".to_owned()),
                ("provider".to_owned(), "strava".to_owned()),
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

    async fn pending_session_daily_install_rebuild_rows(pool: &PgPool) -> Vec<(NaiveDate, String)> {
        sqlx::query(
            r#"
            SELECT day, install_id
            FROM session_daily_installs_rebuild_queue
            WHERE project_id = $1
            ORDER BY day ASC, install_id ASC
            "#,
        )
        .bind(project_id())
        .fetch_all(pool)
        .await
        .expect("fetch pending session_daily install rebuild rows")
        .into_iter()
        .map(|row| {
            (
                row.try_get::<NaiveDate, _>("day").expect("day"),
                row.try_get::<String, _>("install_id").expect("install_id"),
            )
        })
        .collect()
    }

    async fn pending_session_repair_frontier(pool: &PgPool, install_id: &str) -> Option<i64> {
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT target_event_id
            FROM session_repair_jobs
            WHERE project_id = $1
              AND install_id = $2
            "#,
        )
        .bind(project_id())
        .bind(install_id)
        .fetch_optional(pool)
        .await
        .expect("fetch pending repair frontier")
    }

    async fn run_session_repair_batch(pool: &PgPool, concurrency: usize) -> usize {
        process_session_repair_batch(pool, concurrency)
            .await
            .expect("run session repair batch")
    }

    fn sample_stage_b_trace_record() -> StageBTraceRecord {
        StageBTraceRecord {
            record_type: "repair_job".to_owned(),
            install_id: Some("install-1".to_owned()),
            target_event_id: Some(77),
            phases_ms: BTreeMap::from([
                ("row_mutation_lag_before_claim".to_owned(), 5),
                ("frontier_fetch".to_owned(), 7),
                ("recompute_window_load".to_owned(), 11),
                ("session_rewrite".to_owned(), 17),
                ("touched_bucket_enqueue".to_owned(), 3),
            ]),
            finalization_subphases_ms: BTreeMap::new(),
            session_daily_subphases_ms: BTreeMap::new(),
            counts: BTreeMap::from([
                ("pending_frontier_events".to_owned(), 4),
                ("window_raw_events".to_owned(), 9),
            ]),
        }
    }

    fn sample_event_lane_trace_record() -> EventLaneTraceRecord {
        EventLaneTraceRecord {
            record_type: "event_metrics_batch".to_owned(),
            last_processed_event_id: 10,
            next_offset: 42,
            processed_events: 32,
            phases_ms: BTreeMap::from([
                ("build_rollups".to_owned(), 17),
                ("upsert_total".to_owned(), 5),
                ("upsert_dim1".to_owned(), 7),
                ("upsert_dim2".to_owned(), 11),
                ("save_offset".to_owned(), 2),
                ("commit".to_owned(), 3),
            ]),
            counts: BTreeMap::from([
                ("total_delta_rows".to_owned(), 4),
                ("dim1_delta_rows".to_owned(), 12),
                ("dim2_delta_rows".to_owned(), 18),
            ]),
        }
    }

    #[test]
    fn stage_b_trace_writer_emits_jsonl_when_trace_path_is_set() {
        let _guard = lock_trace_env();
        let path = std::env::temp_dir().join(format!("stage-b-trace-{}.jsonl", Uuid::new_v4()));
        unsafe {
            std::env::set_var("FANTASMA_WORKER_STAGE_B_TRACE_PATH", &path);
        }

        record_stage_b_trace(&sample_stage_b_trace_record()).expect("write trace");

        let contents = fs::read_to_string(&path).expect("read trace file");
        let line = contents.lines().next().expect("trace line");
        let parsed: serde_json::Value =
            serde_json::from_str(line).expect("parse trace record as json");

        assert_eq!(parsed["record_type"], "repair_job");
        assert_eq!(parsed["install_id"], "install-1");
        assert_eq!(parsed["target_event_id"], 77);
        assert_eq!(parsed["phases_ms"]["session_rewrite"], 17);
        assert_eq!(parsed["counts"]["window_raw_events"], 9);

        let _ = fs::remove_file(&path);
        unsafe {
            std::env::remove_var("FANTASMA_WORKER_STAGE_B_TRACE_PATH");
        }
    }

    #[test]
    fn stage_b_trace_writer_is_noop_when_trace_path_is_unset() {
        let _guard = lock_trace_env();
        unsafe {
            std::env::remove_var("FANTASMA_WORKER_STAGE_B_TRACE_PATH");
        }
        let path = std::env::temp_dir().join(format!("stage-b-trace-{}.jsonl", Uuid::new_v4()));

        record_stage_b_trace(&sample_stage_b_trace_record()).expect("skip trace write");

        assert!(
            !path.exists(),
            "trace writer should not create files when the env var is unset"
        );
    }

    #[test]
    fn event_lane_trace_writer_emits_jsonl_when_trace_path_is_set() {
        let _guard = lock_trace_env();
        let path = std::env::temp_dir().join(format!("event-lane-trace-{}.jsonl", Uuid::new_v4()));
        unsafe {
            std::env::set_var("FANTASMA_WORKER_EVENT_LANE_TRACE_PATH", &path);
        }

        record_event_lane_trace(&sample_event_lane_trace_record()).expect("write event lane trace");

        let contents = fs::read_to_string(&path).expect("read event lane trace file");
        let line = contents.lines().next().expect("event lane trace line");
        let parsed: serde_json::Value =
            serde_json::from_str(line).expect("parse event lane trace record as json");

        assert_eq!(parsed["record_type"], "event_metrics_batch");
        assert_eq!(parsed["last_processed_event_id"], 10);
        assert_eq!(parsed["next_offset"], 42);
        assert_eq!(parsed["processed_events"], 32);
        assert_eq!(parsed["phases_ms"]["build_rollups"], 17);
        assert_eq!(parsed["counts"]["dim2_delta_rows"], 18);

        let _ = fs::remove_file(&path);
        unsafe {
            std::env::remove_var("FANTASMA_WORKER_EVENT_LANE_TRACE_PATH");
        }
    }

    #[test]
    fn event_lane_trace_writer_is_noop_when_trace_path_is_unset() {
        let _guard = lock_trace_env();
        unsafe {
            std::env::remove_var("FANTASMA_WORKER_EVENT_LANE_TRACE_PATH");
        }
        let path = std::env::temp_dir().join(format!("event-lane-trace-{}.jsonl", Uuid::new_v4()));

        record_event_lane_trace(&sample_event_lane_trace_record())
            .expect("skip event lane trace write");

        assert!(
            !path.exists(),
            "event lane trace writer should not create files when the env var is unset"
        );
    }

    #[test]
    fn stage_b_trace_writer_emits_finalization_subphases_for_rebuild_finalize_records() {
        let _guard = lock_trace_env();
        let path = std::env::temp_dir().join(format!("stage-b-trace-{}.jsonl", Uuid::new_v4()));
        unsafe {
            std::env::set_var("FANTASMA_WORKER_STAGE_B_TRACE_PATH", &path);
        }

        let mut record = StageBTraceRecord::rebuild_finalize();
        record.add_phase_ms("shared_bucket_finalize", 31);
        record.add_finalization_subphase_ms("finalization_metric_dim1_rebuild", 12);
        record.add_session_daily_subphase_ms("session_daily_source_build", 7);
        record.add_count("claimed_rebuild_bucket_rows", 12);
        record.add_count("touched_day_buckets", 2);

        record_stage_b_trace(&record).expect("write rebuild finalize trace");

        let contents = fs::read_to_string(&path).expect("read trace file");
        let line = contents.lines().next().expect("trace line");
        let parsed: serde_json::Value =
            serde_json::from_str(line).expect("parse trace record as json");

        assert_eq!(parsed["record_type"], "rebuild_finalize");
        assert_eq!(
            parsed["finalization_subphases_ms"]["finalization_metric_dim1_rebuild"],
            serde_json::json!(12),
            "rebuild-finalize traces should carry nested finalization timing"
        );
        assert_eq!(
            parsed["session_daily_subphases_ms"]["session_daily_source_build"],
            serde_json::json!(7),
            "rebuild-finalize traces should carry nested session_daily timing"
        );

        let _ = fs::remove_file(&path);
        unsafe {
            std::env::remove_var("FANTASMA_WORKER_STAGE_B_TRACE_PATH");
        }
    }

    #[test]
    fn stage_b_trace_writer_serializes_concurrent_records() {
        let _guard = lock_trace_env();
        let path = std::env::temp_dir().join(format!("stage-b-trace-{}.jsonl", Uuid::new_v4()));
        unsafe {
            std::env::set_var("FANTASMA_WORKER_STAGE_B_TRACE_PATH", &path);
        }

        let threads = (0..8)
            .map(|index| {
                std::thread::spawn(move || {
                    for offset in 0..16 {
                        let mut record = sample_stage_b_trace_record();
                        record.install_id = Some(format!("install-{index}-{offset}"));
                        record.target_event_id = Some((index * 100 + offset) as i64);
                        record_stage_b_trace(&record).expect("write concurrent trace");
                    }
                })
            })
            .collect::<Vec<_>>();

        for thread in threads {
            thread.join().expect("join concurrent trace writer");
        }

        let contents = fs::read_to_string(&path).expect("read concurrent trace file");
        let lines = contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect::<Vec<_>>();
        assert_eq!(lines.len(), 128);
        for line in lines {
            let parsed: serde_json::Value =
                serde_json::from_str(line).expect("parse concurrent trace record");
            assert_eq!(parsed["record_type"], "repair_job");
        }

        let _ = fs::remove_file(&path);
        unsafe {
            std::env::remove_var("FANTASMA_WORKER_STAGE_B_TRACE_PATH");
        }
    }

    #[allow(clippy::await_holding_lock)]
    #[sqlx::test]
    async fn rebuild_finalization_sweeps_visible_project_queue_in_one_pass(pool: PgPool) {
        let _guard = lock_trace_env();
        let path = std::env::temp_dir().join(format!("stage-b-trace-{}.jsonl", Uuid::new_v4()));
        unsafe {
            std::env::set_var("FANTASMA_WORKER_STAGE_B_TRACE_PATH", &path);
        }

        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");

        let hour_bucket_starts = (0..513_i64)
            .map(|offset| timestamp(1, 0, 0) + ChronoDuration::hours(offset))
            .collect::<Vec<_>>();
        let mut seed_tx = pool.begin().await.expect("begin queue seed tx");
        enqueue_session_rebuild_buckets_in_tx(&mut seed_tx, project_id(), &[], &hour_bucket_starts)
            .await
            .expect("enqueue rebuild buckets");
        seed_tx.commit().await.expect("commit queue seed tx");

        drain_pending_session_rebuild_buckets(&pool)
            .await
            .expect("drain swept rebuild queue");

        assert!(
            pending_session_rebuild_buckets(&pool).await.is_empty(),
            "project sweep should drain the visible queue in one call"
        );

        let contents = fs::read_to_string(&path).expect("read rebuild trace");
        let rebuild_records = contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("parse trace"))
            .filter(|record| record["record_type"] == "rebuild_finalize")
            .collect::<Vec<_>>();
        assert_eq!(
            rebuild_records.len(),
            1,
            "project sweep should collapse visible queue work into one rebuild-finalize pass"
        );
        assert_eq!(
            rebuild_records[0]["counts"]["claimed_rebuild_bucket_rows"],
            serde_json::json!(513)
        );
        assert!(
            rebuild_records[0]["finalization_subphases_ms"].is_object(),
            "rebuild-finalize trace records should carry nested finalization timing"
        );
        assert!(
            rebuild_records[0]["finalization_subphases_ms"]
                .get("finalization_drain_setup")
                .is_some(),
            "drain setup timing should be recorded inside rebuild-finalize traces"
        );

        let _ = fs::remove_file(&path);
        unsafe {
            std::env::remove_var("FANTASMA_WORKER_STAGE_B_TRACE_PATH");
        }
    }

    #[test]
    fn max_dimension_event_metrics_fanout_stays_bounded() {
        let rollups = build_event_metric_rollups(&[raw_event_with_max_dimensions()]);
        let total_fanout =
            rollups.total_deltas.len() + rollups.dim1_deltas.len() + rollups.dim2_deltas.len();

        assert_eq!(rollups.total_deltas.len(), 2);
        assert_eq!(rollups.dim1_deltas.len(), 10);
        assert_eq!(rollups.dim2_deltas.len(), 20);
        assert_eq!(total_fanout, 32);
        assert!(
            rollups
                .dim2_deltas
                .iter()
                .all(|delta| delta.event_count == 1),
            "single events must increment each bounded cube exactly once"
        );
    }

    #[test]
    fn live_install_state_updates_keep_latest_received_at_per_install() {
        let first = RawEventRecord {
            id: 1,
            project_id: project_id(),
            event_name: "app_open".to_owned(),
            timestamp: timestamp(1, 0, 0),
            received_at: timestamp(1, 0, 1),
            install_id: "install-1".to_owned(),
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
            os_version: Some("18.3".to_owned()),
            properties: BTreeMap::new(),
        };
        let second = RawEventRecord {
            id: 2,
            project_id: project_id(),
            event_name: "app_open".to_owned(),
            timestamp: timestamp(1, 0, 0),
            received_at: timestamp(1, 0, 3),
            install_id: "install-1".to_owned(),
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
            os_version: Some("18.3".to_owned()),
            properties: BTreeMap::new(),
        };
        let third = RawEventRecord {
            id: 3,
            project_id: project_id(),
            event_name: "app_open".to_owned(),
            timestamp: timestamp(1, 0, 0),
            received_at: timestamp(1, 0, 2),
            install_id: "install-2".to_owned(),
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
            os_version: Some("18.3".to_owned()),
            properties: BTreeMap::new(),
        };

        let updates = build_live_install_state_upserts(&[first, second, third]);

        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].install_id, "install-1");
        assert_eq!(updates[0].last_received_at, timestamp(1, 0, 3));
        assert_eq!(updates[1].install_id, "install-2");
        assert_eq!(updates[1].last_received_at, timestamp(1, 0, 2));
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
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue older event repair");
        assert_eq!(run_session_repair_batch(&pool, 1).await, 1);

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
    async fn late_repair_moves_session_app_version_to_repaired_first_event(pool: PgPool) {
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
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue late repair batch");
        assert_eq!(run_session_repair_batch(&pool, 1).await, 1);

        let repaired_session = fetch_latest_session_for_install(&pool, project_id(), "install-1")
            .await
            .expect("fetch repaired session")
            .expect("session exists");

        assert_eq!(repaired_session.session_start, timestamp(1, 0, 50));
        assert_eq!(repaired_session.session_end, timestamp(1, 1, 10));
        assert_eq!(repaired_session.duration_seconds, 20 * 60);
        assert_eq!(repaired_session.app_version, Some("0.9.0".to_owned()));
    }

    #[sqlx::test]
    async fn repair_window_uses_timestamp_extrema_for_out_of_order_pending_events(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 1, 10), event("install-1", 1, 1, 20)],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 2, 0), event("install-1", 1, 0, 50)],
        )
        .await
        .expect("insert out-of-order late events");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue late repair batch");
        assert_eq!(run_session_repair_batch(&pool, 1).await, 1);

        let sessions = sqlx::query(
            r#"
            SELECT session_start, session_end, duration_seconds
            FROM sessions
            WHERE project_id = $1
              AND install_id = $2
            ORDER BY session_start ASC
            "#,
        )
        .bind(project_id())
        .bind("install-1")
        .fetch_all(&pool)
        .await
        .expect("fetch repaired sessions");

        assert_eq!(sessions.len(), 2);
        assert_eq!(
            sessions[0]
                .try_get::<chrono::DateTime<Utc>, _>("session_start")
                .unwrap(),
            timestamp(1, 0, 50)
        );
        assert_eq!(
            sessions[0]
                .try_get::<chrono::DateTime<Utc>, _>("session_end")
                .unwrap(),
            timestamp(1, 1, 20)
        );
        assert_eq!(
            sessions[0].try_get::<i32, _>("duration_seconds").unwrap(),
            30 * 60
        );
        assert_eq!(
            sessions[1]
                .try_get::<chrono::DateTime<Utc>, _>("session_start")
                .unwrap(),
            timestamp(1, 2, 0)
        );
        assert_eq!(
            sessions[1]
                .try_get::<chrono::DateTime<Utc>, _>("session_end")
                .unwrap(),
            timestamp(1, 2, 0)
        );
        assert_eq!(
            sessions[1].try_get::<i32, _>("duration_seconds").unwrap(),
            0
        );
    }

    #[sqlx::test]
    async fn repair_job_defers_project_bucket_rebuild_until_batch_finalization(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 1, 0), event("install-1", 1, 1, 10)],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 10 * 60))
        );

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 50)])
            .await
            .expect("insert late event");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue repair frontier");

        let mut tx = pool.begin().await.expect("begin repair job tx");
        let job = claim_pending_session_repair_jobs_in_tx(&mut tx, 1)
            .await
            .expect("claim repair job")
            .into_iter()
            .next()
            .expect("repair job exists");
        process_session_repair_job(
            &mut tx,
            job,
            &mut StageBTraceRecord::repair_job("install-1", 0),
        )
        .await
        .expect("process repair job");

        let duration = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT total_duration_seconds
            FROM session_daily
            WHERE project_id = $1
              AND day = $2
            "#,
        )
        .bind(project_id())
        .bind(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())
        .fetch_one(&mut *tx)
        .await
        .expect("fetch inline rebuilt duration");

        assert_eq!(
            duration,
            10 * 60,
            "repair jobs should leave shared project buckets for batch finalization"
        );
    }

    #[sqlx::test]
    async fn mixed_repair_batch_still_drains_shared_bucket_finalization_before_returning_error(
        pool: PgPool,
    ) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 1, 0), event("install-1", 1, 1, 10)],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 10 * 60))
        );

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 50)])
            .await
            .expect("insert late event");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 2,
            },
        )
        .await
        .expect("enqueue repair frontier");

        sqlx::query(
            r#"
            INSERT INTO session_repair_jobs (project_id, install_id, target_event_id)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(project_id())
        .bind("missing-install-state")
        .bind(7_i64)
        .execute(&pool)
        .await
        .expect("insert failing repair frontier");

        let claim_blocker = Arc::new(RepairClaimBlocker {
            claimed: Notify::new(),
            resume: Notify::new(),
        });
        let repair_pool = pool.clone();
        let repair_claim_blocker = claim_blocker.clone();
        let repair_task = tokio::spawn(async move {
            process_session_repair_batch_with_hooks(
                &repair_pool,
                2,
                SessionRepairBatchHooks::block_after_claim_for_install(
                    "missing-install-state",
                    repair_claim_blocker,
                ),
            )
            .await
        });
        claim_blocker.claimed.notified().await;

        let mut saw_queued_rebuilds = false;
        for _ in 0..20 {
            let queued_buckets = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM session_rebuild_queue WHERE project_id = $1",
            )
            .bind(project_id())
            .fetch_one(&pool)
            .await
            .expect("count pending rebuild buckets");
            if queued_buckets > 0 {
                saw_queued_rebuilds = true;
                break;
            }

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            saw_queued_rebuilds,
            "successful repair work should enqueue shared-bucket finalization before the blocked failure resumes"
        );

        claim_blocker.resume.notify_waiters();
        let error = repair_task
            .await
            .expect("repair task joins")
            .expect_err("mixed repair batch should still return the failing install error");
        assert!(matches!(error, StoreError::InvariantViolation(_)));

        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 20 * 60)),
            "queued shared-bucket finalization should still drain before the batch returns an error"
        );
    }

    #[sqlx::test]
    async fn repair_batch_waits_for_claimed_jobs_to_finish_before_returning_error(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 1, 0), event("install-1", 1, 1, 10)],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 50)])
            .await
            .expect("insert late event");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 2,
            },
        )
        .await
        .expect("enqueue repair frontier");

        sqlx::query(
            r#"
            INSERT INTO session_repair_jobs (project_id, install_id, target_event_id)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(project_id())
        .bind("missing-install-state")
        .bind(7_i64)
        .execute(&pool)
        .await
        .expect("insert failing repair frontier");

        let claim_blocker = Arc::new(RepairClaimBlocker {
            claimed: Notify::new(),
            resume: Notify::new(),
        });
        let repair_pool = pool.clone();
        let repair_claim_blocker = claim_blocker.clone();
        let repair_task = tokio::spawn(async move {
            process_session_repair_batch_with_hooks(
                &repair_pool,
                2,
                SessionRepairBatchHooks::block_after_claim_for_install(
                    "install-1",
                    repair_claim_blocker,
                ),
            )
            .await
        });
        claim_blocker.claimed.notified().await;

        let mut saw_failed_peer_released = false;
        for _ in 0..20 {
            let mut verify_tx = pool.begin().await.expect("begin failed peer verify tx");
            let failed_peer = load_session_repair_jobs_for_installs_in_tx(
                &mut verify_tx,
                &[(project_id(), "missing-install-state".to_owned())],
            )
            .await
            .expect("load failed peer frontier");
            verify_tx
                .commit()
                .await
                .expect("commit failed peer verify tx");

            if failed_peer
                .get(&(project_id(), "missing-install-state".to_owned()))
                .is_some_and(|job| job.claimed_at.is_none())
            {
                saw_failed_peer_released = true;
                break;
            }

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            saw_failed_peer_released,
            "the failing peer should release its own claim while the blocked claimed job is still in flight"
        );
        assert!(
            !repair_task.is_finished(),
            "repair batch should wait for already-claimed peer jobs to finish instead of cancelling them"
        );

        claim_blocker.resume.notify_waiters();
        let error = repair_task
            .await
            .expect("repair task joins")
            .expect_err("repair batch should still return the failing peer error");
        assert!(matches!(error, StoreError::InvariantViolation(_)));

        let mut verify_tx = pool.begin().await.expect("begin final verify tx");
        let jobs = load_session_repair_jobs_for_installs_in_tx(
            &mut verify_tx,
            &[
                (project_id(), "install-1".to_owned()),
                (project_id(), "missing-install-state".to_owned()),
            ],
        )
        .await
        .expect("load repair frontiers after batch");
        let reclaimed = claim_pending_session_repair_jobs_in_tx(&mut verify_tx, 10)
            .await
            .expect("claim remaining repair frontiers");
        verify_tx.commit().await.expect("commit final verify tx");

        assert!(
            !jobs.contains_key(&(project_id(), "install-1".to_owned())),
            "the blocked claimed repair should complete and clear its frontier instead of staying stranded"
        );
        let failed_peer = jobs
            .get(&(project_id(), "missing-install-state".to_owned()))
            .expect("failed peer frontier remains queued");
        assert_eq!(failed_peer.claimed_at, None);
        assert!(reclaimed.iter().any(|job| {
            job.project_id == project_id()
                && job.install_id == "missing-install-state"
                && job.claimed_at.is_some()
        }));
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 20 * 60))
        );
    }

    #[sqlx::test]
    async fn panicked_repair_worker_releases_claim_for_retry(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 1, 0), event("install-1", 1, 1, 10)],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 50)])
            .await
            .expect("insert late event");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue repair frontier");

        let error = process_session_repair_batch_with_hooks(
            &pool,
            1,
            SessionRepairBatchHooks::panic_after_claim_for_install("install-1"),
        )
        .await
        .expect_err("repair batch should surface the panicking worker");
        assert!(matches!(error, StoreError::InvariantViolation(_)));

        let mut verify_tx = pool.begin().await.expect("begin panic verify tx");
        let queued = load_session_repair_jobs_for_installs_in_tx(
            &mut verify_tx,
            &[(project_id(), "install-1".to_owned())],
        )
        .await
        .expect("load queued repair frontier");
        let reclaimed = claim_pending_session_repair_jobs_in_tx(&mut verify_tx, 1)
            .await
            .expect("reclaim panic-released frontier");
        verify_tx.commit().await.expect("commit panic verify tx");

        let queued = queued
            .get(&(project_id(), "install-1".to_owned()))
            .expect("repair frontier remains queued");
        assert_eq!(queued.claimed_at, None);
        assert_eq!(queued.target_event_id, 3);
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].install_id, "install-1");
        assert_eq!(reclaimed[0].target_event_id, 3);
        assert!(reclaimed[0].claimed_at.is_some());
    }

    #[sqlx::test]
    async fn join_error_claim_cleanup_retries_before_dropping_task_context(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 1, 0), event("install-1", 1, 1, 10)],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 50)])
            .await
            .expect("insert late event");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue repair frontier");

        let error = process_session_repair_batch_with_hooks(
            &pool,
            1,
            SessionRepairBatchHooks::panic_after_claim_and_fail_join_error_release_once_for_install(
                "install-1",
            ),
        )
        .await
        .expect_err("repair batch should surface the panicking worker");
        assert!(matches!(error, StoreError::InvariantViolation(_)));

        let mut verify_tx = pool.begin().await.expect("begin retry verify tx");
        let queued = load_session_repair_jobs_for_installs_in_tx(
            &mut verify_tx,
            &[(project_id(), "install-1".to_owned())],
        )
        .await
        .expect("load queued repair frontier");
        let reclaimed = claim_pending_session_repair_jobs_in_tx(&mut verify_tx, 1)
            .await
            .expect("reclaim retry-released frontier");
        verify_tx.commit().await.expect("commit retry verify tx");

        let queued = queued
            .get(&(project_id(), "install-1".to_owned()))
            .expect("repair frontier remains queued");
        assert_eq!(queued.claimed_at, None);
        assert_eq!(queued.target_event_id, 3);
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].install_id, "install-1");
        assert_eq!(reclaimed[0].target_event_id, 3);
        assert!(reclaimed[0].claimed_at.is_some());
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
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue repair batch");
        assert_eq!(run_session_repair_batch(&pool, 1).await, 1);

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
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 2,
            },
        )
        .await
        .expect("enqueue repair batch");
        assert_eq!(run_session_repair_batch(&pool, 2).await, 2);

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
    async fn repeated_same_install_day_repairs_collapse_to_one_pending_membership_rewrite(
        pool: PgPool,
    ) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 1, 0), event("install-1", 1, 1, 10)],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 50)])
            .await
            .expect("insert first late event");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue first repair frontier");

        let mut first_tx = pool.begin().await.expect("begin first repair tx");
        let first_job = claim_pending_session_repair_jobs_in_tx(&mut first_tx, 1)
            .await
            .expect("claim first repair job")
            .into_iter()
            .next()
            .expect("first repair job exists");
        process_session_repair_job(
            &mut first_tx,
            first_job,
            &mut StageBTraceRecord::repair_job("install-1", 0),
        )
        .await
        .expect("process first repair job");
        first_tx.commit().await.expect("commit first repair tx");

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 40)])
            .await
            .expect("insert second late event");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue second repair frontier");

        let mut second_tx = pool.begin().await.expect("begin second repair tx");
        let second_job = claim_pending_session_repair_jobs_in_tx(&mut second_tx, 1)
            .await
            .expect("claim second repair job")
            .into_iter()
            .next()
            .expect("second repair job exists");
        process_session_repair_job(
            &mut second_tx,
            second_job,
            &mut StageBTraceRecord::repair_job("install-1", 0),
        )
        .await
        .expect("process second repair job");
        second_tx.commit().await.expect("commit second repair tx");

        assert_eq!(
            pending_session_daily_install_rebuild_rows(&pool).await,
            vec![(
                NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
                "install-1".to_owned()
            )],
            "multiple repairs on the same install/day should collapse to one queued membership rewrite"
        );
    }

    #[sqlx::test]
    async fn session_repair_batch_drains_pending_membership_rebuild_rows_during_project_sweep(
        pool: PgPool,
    ) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 1, 0), event("install-1", 1, 1, 10)],
        )
        .await
        .expect("insert initial events");
        process_session_batch(&pool, 100)
            .await
            .expect("process initial batch");

        insert_events(&pool, project_id(), &[event("install-1", 1, 0, 50)])
            .await
            .expect("insert late event");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue repair frontier");

        assert_eq!(run_session_repair_batch(&pool, 1).await, 1);
        assert!(
            pending_session_daily_install_rebuild_rows(&pool)
                .await
                .is_empty(),
            "batch finalization should drain pending membership rebuild rows"
        );
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 20 * 60))
        );
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
    async fn replayed_repair_enqueue_only_advances_raw_offset_after_coordinator_failure(
        pool: PgPool,
    ) {
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
            .expect("insert late repair event");

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
        .expect_err("coordinator finalize should fail after repair enqueue");

        assert!(matches!(error, StoreError::InvariantViolation(_)));
        assert_eq!(
            load_worker_offset(&pool, SESSION_WORKER_NAME)
                .await
                .expect("load worker offset"),
            2
        );
        assert_eq!(
            pending_session_repair_frontier(&pool, "install-1").await,
            None
        );

        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load install state")
            .expect("install state exists");
        assert_eq!(tail.last_processed_event_id, 2);

        let replayed = process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("replay repair enqueue batch");

        assert!(replayed.advanced_offset);
        assert_eq!(
            load_worker_offset(&pool, SESSION_WORKER_NAME)
                .await
                .expect("load replayed worker offset"),
            3
        );
        assert_eq!(
            pending_session_repair_frontier(&pool, "install-1").await,
            Some(3)
        );
        let replay_tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load replay install state")
            .expect("install state exists");
        assert_eq!(replay_tail.last_processed_event_id, 2);
    }

    #[sqlx::test]
    async fn apply_lane_does_not_hold_raw_offset_lock_while_waiting_on_claimed_repair(
        pool: PgPool,
    ) {
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
            .expect("insert late repair event");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue repair frontier");

        let claim_blocker = Arc::new(RepairClaimBlocker {
            claimed: Notify::new(),
            resume: Notify::new(),
        });
        let repair_pool = pool.clone();
        let repair_claim_blocker = claim_blocker.clone();
        let repair_task = tokio::spawn(async move {
            process_session_repair_batch_with_hooks(
                &repair_pool,
                1,
                SessionRepairBatchHooks::block_after_claim(repair_claim_blocker),
            )
            .await
        });
        claim_blocker.claimed.notified().await;

        insert_events(&pool, project_id(), &[event("install-1", 1, 1, 10)])
            .await
            .expect("insert newer event while repair is claimed");

        let apply_pool = pool.clone();
        let apply_task = tokio::spawn(async move {
            process_session_batch_with_config(
                &apply_pool,
                SessionBatchConfig {
                    batch_size: 100,
                    incremental_concurrency: 1,
                    repair_concurrency: 1,
                },
            )
            .await
        });

        let mut saw_offset_lock_timeout = false;
        for _ in 0..10 {
            if apply_task.is_finished() {
                break;
            }

            tokio::time::sleep(Duration::from_millis(25)).await;
            let mut verify_tx = pool.begin().await.expect("begin offset verify tx");
            sqlx::query("SET LOCAL lock_timeout = '50ms'")
                .execute(&mut *verify_tx)
                .await
                .expect("set local lock timeout");

            if lock_worker_offset(&mut verify_tx, SESSION_WORKER_NAME)
                .await
                .is_err()
            {
                saw_offset_lock_timeout = true;
                verify_tx
                    .rollback()
                    .await
                    .expect("rollback timed out verify tx");
                break;
            }

            verify_tx.rollback().await.expect("rollback verify tx");
        }

        claim_blocker.resume.notify_waiters();
        let repair_processed = repair_task
            .await
            .expect("repair task joins")
            .expect("repair task succeeds");
        let apply_result = apply_task.await.expect("apply task joins");

        assert_eq!(repair_processed, 1);
        assert!(
            !saw_offset_lock_timeout,
            "apply lane should not keep the raw offset lock while a claimed repair frontier is blocked"
        );
        assert!(
            apply_result.is_ok(),
            "apply batch should finish after repair resumes"
        );
    }

    #[sqlx::test]
    async fn repair_job_failure_releases_claim_for_retry(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");

        sqlx::query(
            r#"
            INSERT INTO session_repair_jobs (project_id, install_id, target_event_id)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(project_id())
        .bind("missing-install-state")
        .bind(7_i64)
        .execute(&pool)
        .await
        .expect("insert repair frontier without install state");

        let error = process_session_repair_batch_with_config(&pool, 1)
            .await
            .expect_err("repair batch should fail");
        assert!(matches!(error, StoreError::InvariantViolation(_)));

        let mut verify_tx = pool.begin().await.expect("begin verify tx");
        let queued = load_session_repair_jobs_for_installs_in_tx(
            &mut verify_tx,
            &[(project_id(), "missing-install-state".to_owned())],
        )
        .await
        .expect("load queued repair frontier");
        let reclaimed = claim_pending_session_repair_jobs_in_tx(&mut verify_tx, 1)
            .await
            .expect("reclaim repair frontier");
        verify_tx.commit().await.expect("commit verify tx");

        let queued = queued
            .get(&(project_id(), "missing-install-state".to_owned()))
            .expect("repair frontier remains queued");
        assert_eq!(queued.target_event_id, 7);
        assert_eq!(
            queued.claimed_at, None,
            "failed repair jobs should release their claim for retry"
        );
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].install_id, "missing-install-state");
        assert_eq!(reclaimed[0].target_event_id, 7);
        assert!(reclaimed[0].claimed_at.is_some());
    }

    #[sqlx::test]
    async fn pending_repair_frontier_widens_for_newer_events_without_advancing_install_progress(
        pool: PgPool,
    ) {
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
            .expect("insert late event");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue repair frontier");

        insert_events(&pool, project_id(), &[event("install-1", 1, 1, 10)])
            .await
            .expect("insert newer event while repair is pending");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("widen repair frontier");

        assert_eq!(
            pending_session_repair_frontier(&pool, "install-1").await,
            Some(4)
        );
        assert_eq!(
            load_worker_offset(&pool, SESSION_WORKER_NAME)
                .await
                .expect("load worker offset"),
            4
        );

        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load install state")
            .expect("install state exists");
        assert_eq!(tail.last_processed_event_id, 2);
    }

    #[sqlx::test]
    async fn repair_lane_commit_leaves_daily_and_metric_state_final_without_rebuild_queue(
        pool: PgPool,
    ) {
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
            .expect("insert late repair event");
        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue repair frontier");

        let processed = process_session_repair_batch(&pool, 1)
            .await
            .expect("run repair lane");

        assert_eq!(processed, 1);
        assert!(
            pending_session_rebuild_buckets(&pool).await.is_empty(),
            "repair lane should finalize directly without leaving rebuild queue work behind"
        );
        assert_eq!(
            pending_session_repair_frontier(&pool, "install-1").await,
            None
        );
        assert_eq!(
            session_daily_row(&pool, NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()).await,
            Some((1, 1, 45 * 60))
        );

        let tail = load_install_session_state(&pool, project_id(), "install-1")
            .await
            .expect("load install state")
            .expect("install state exists");
        assert_eq!(tail.last_processed_event_id, 3);

        let repaired_session = fetch_latest_session_for_install(&pool, project_id(), "install-1")
            .await
            .expect("fetch repaired session")
            .expect("session exists");
        assert_eq!(repaired_session.event_count, 3);
        assert_eq!(repaired_session.duration_seconds, 45 * 60);
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
    async fn event_worker_updates_live_install_state(pool: PgPool) {
        fantasma_store::bootstrap(&pool, &bootstrap_config())
            .await
            .expect("bootstrap succeeds");
        insert_events(
            &pool,
            project_id(),
            &[event("install-1", 1, 1, 15), event("install-2", 1, 1, 16)],
        )
        .await
        .expect("insert events");

        process_event_metrics_batch(&pool, 100)
            .await
            .expect("process batch");

        let live_installs = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM live_install_state
            WHERE project_id = $1
            "#,
        )
        .bind(project_id())
        .fetch_one(&pool)
        .await
        .expect("count live installs");

        assert_eq!(live_installs, 2);
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

        process_session_batch_with_config(
            &pool,
            SessionBatchConfig {
                batch_size: 100,
                incremental_concurrency: 1,
                repair_concurrency: 1,
            },
        )
        .await
        .expect("enqueue late repair batch");
        assert_eq!(run_session_repair_batch(&pool, 1).await, 1);

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
