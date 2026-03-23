use std::{
    future::Future,
    time::{Duration, Instant},
};

use fantasma_store::{PgPool, StoreError, load_raw_event_tail_id, load_worker_offset};
use tracing::{debug, error, info};

use crate::worker::{
    EVENT_METRICS_WORKER_NAME, SESSION_WORKER_NAME, SessionBatchConfig,
    process_event_metrics_batch, process_project_deletion_batch, process_session_batch_with_config,
    process_session_repair_batch_with_config,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    pub idle_poll_interval: Duration,
    pub session_batch_size: i64,
    pub event_batch_size: i64,
    pub session_incremental_concurrency: usize,
    pub session_repair_concurrency: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            idle_poll_interval: Duration::from_millis(2_000),
            session_batch_size: 2_000,
            event_batch_size: 5_000,
            session_incremental_concurrency: 8,
            session_repair_concurrency: 2,
        }
    }
}

impl WorkerConfig {
    pub fn required_db_connections(&self) -> u32 {
        let required = self
            .session_incremental_concurrency
            .saturating_add(self.session_repair_concurrency)
            .saturating_add(6);
        required as u32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaneTickResult {
    pub(crate) processed_events: usize,
    pub(crate) backlog_events: i64,
    pub(crate) made_progress: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaneRunStats {
    pub(crate) busy_ticks: u64,
    pub(crate) idle_ticks: u64,
}

pub async fn run_worker(pool: PgPool, config: WorkerConfig) -> Result<(), StoreError> {
    let session_apply_pool = pool.clone();
    let session_repair_pool = pool.clone();
    let event_pool = pool.clone();
    let deletion_pool = pool;
    let session_apply_config = config.clone();
    let session_repair_config = config.clone();
    let event_config = config.clone();
    let deletion_config = config;

    tokio::try_join!(
        run_lane_forever(
            "session_apply",
            session_apply_config.idle_poll_interval,
            move || {
                let pool = session_apply_pool.clone();
                let config = session_apply_config.clone();
                async move { session_apply_lane_tick(&pool, &config).await }
            }
        ),
        run_lane_forever(
            "session_repair",
            session_repair_config.idle_poll_interval,
            move || {
                let pool = session_repair_pool.clone();
                let config = session_repair_config.clone();
                async move { session_repair_lane_tick(&pool, &config).await }
            }
        ),
        run_lane_forever(
            "event_metrics",
            event_config.idle_poll_interval,
            move || {
                let pool = event_pool.clone();
                let config = event_config.clone();
                async move { event_metrics_lane_tick(&pool, &config).await }
            }
        ),
        run_lane_forever(
            "project_deletions",
            deletion_config.idle_poll_interval,
            move || {
                let pool = deletion_pool.clone();
                async move { project_deletion_lane_tick(&pool).await }
            }
        ),
    )?;

    Ok(())
}

async fn run_lane_forever<Tick, TickFut>(
    lane: &'static str,
    idle_poll_interval: Duration,
    tick: Tick,
) -> Result<(), StoreError>
where
    Tick: FnMut() -> TickFut,
    TickFut: Future<Output = Result<LaneTickResult, StoreError>>,
{
    drive_lane_until(lane, idle_poll_interval, tick, tokio::time::sleep, || false).await?;

    Ok(())
}

pub(crate) async fn drive_lane_until<Tick, TickFut, Sleep, SleepFut, Stop>(
    lane: &'static str,
    idle_poll_interval: Duration,
    mut tick: Tick,
    mut sleep: Sleep,
    mut should_stop: Stop,
) -> Result<LaneRunStats, StoreError>
where
    Tick: FnMut() -> TickFut,
    TickFut: Future<Output = Result<LaneTickResult, StoreError>>,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
    Stop: FnMut() -> bool,
{
    let mut stats = LaneRunStats::default();

    loop {
        let tick_started = Instant::now();
        match tick().await {
            Ok(result) if !result.made_progress => {
                stats.idle_ticks += 1;
                debug!(
                    lane,
                    tick_state = "idle",
                    batch_elapsed_ms = tick_started.elapsed().as_millis() as u64,
                    processed_events = result.processed_events,
                    backlog_events = result.backlog_events,
                    busy_ticks = stats.busy_ticks,
                    idle_ticks = stats.idle_ticks,
                    "worker lane tick"
                );
                sleep(idle_poll_interval).await;
            }
            Ok(result) => {
                stats.busy_ticks += 1;
                info!(
                    lane,
                    tick_state = "busy",
                    batch_elapsed_ms = tick_started.elapsed().as_millis() as u64,
                    processed_events = result.processed_events,
                    backlog_events = result.backlog_events,
                    busy_ticks = stats.busy_ticks,
                    idle_ticks = stats.idle_ticks,
                    "worker lane tick"
                );
            }
            Err(err) => {
                info!(
                    lane,
                    tick_state = "error",
                    batch_elapsed_ms = tick_started.elapsed().as_millis() as u64,
                    busy_ticks = stats.busy_ticks,
                    idle_ticks = stats.idle_ticks,
                    "worker lane tick"
                );
                error!(lane, ?err, "worker lane tick failed");
                sleep(idle_poll_interval).await;
            }
        }

        if should_stop() {
            return Ok(stats);
        }
    }
}

async fn session_lane_tick(
    pool: &PgPool,
    config: &WorkerConfig,
) -> Result<LaneTickResult, StoreError> {
    let processed_events = process_session_batch_with_config(
        pool,
        SessionBatchConfig {
            batch_size: config.session_batch_size,
            incremental_concurrency: config.session_incremental_concurrency,
            repair_concurrency: config.session_repair_concurrency,
        },
    )
    .await?;
    let backlog_events = lane_backlog(pool, SESSION_WORKER_NAME).await?;

    Ok(LaneTickResult {
        processed_events: processed_events.processed_events,
        backlog_events,
        made_progress: processed_events.advanced_offset,
    })
}

async fn session_apply_lane_tick(
    pool: &PgPool,
    config: &WorkerConfig,
) -> Result<LaneTickResult, StoreError> {
    session_lane_tick(pool, config).await
}

async fn session_repair_lane_tick(
    pool: &PgPool,
    config: &WorkerConfig,
) -> Result<LaneTickResult, StoreError> {
    let processed_jobs =
        process_session_repair_batch_with_config(pool, config.session_repair_concurrency).await?;

    Ok(LaneTickResult {
        processed_events: processed_jobs,
        backlog_events: 0,
        made_progress: processed_jobs > 0,
    })
}

async fn event_metrics_lane_tick(
    pool: &PgPool,
    config: &WorkerConfig,
) -> Result<LaneTickResult, StoreError> {
    let processed_events = process_event_metrics_batch(pool, config.event_batch_size).await?;
    let backlog_events = lane_backlog(pool, EVENT_METRICS_WORKER_NAME).await?;

    Ok(LaneTickResult {
        processed_events,
        backlog_events,
        made_progress: processed_events > 0,
    })
}

async fn project_deletion_lane_tick(pool: &PgPool) -> Result<LaneTickResult, StoreError> {
    let processed_jobs = process_project_deletion_batch(pool).await?;

    Ok(LaneTickResult {
        processed_events: processed_jobs,
        backlog_events: 0,
        made_progress: processed_jobs > 0,
    })
}

async fn lane_backlog(pool: &PgPool, worker_name: &str) -> Result<i64, StoreError> {
    let tail = load_raw_event_tail_id(pool).await?;
    let offset = load_worker_offset(pool, worker_name).await?;

    Ok(tail.saturating_sub(offset))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use fantasma_store::StoreError;
    use tracing::dispatcher::Dispatch;
    use tracing_subscriber::{Layer, layer::SubscriberExt};

    use super::{LaneRunStats, LaneTickResult, WorkerConfig, drive_lane_until};

    #[derive(Clone, Default)]
    struct InfoEventCounter {
        count: Arc<AtomicUsize>,
    }

    impl InfoEventCounter {
        fn new() -> Self {
            Self::default()
        }

        fn load(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    impl<S> Layer<S> for InfoEventCounter
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() == tracing::Level::INFO {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn worker_config_defaults_match_repo_defaults() {
        let config = WorkerConfig::default();

        assert_eq!(config.idle_poll_interval, Duration::from_millis(2_000));
        assert_eq!(config.session_batch_size, 2_000);
        assert_eq!(config.event_batch_size, 5_000);
        assert_eq!(config.session_incremental_concurrency, 8);
        assert_eq!(config.session_repair_concurrency, 2);
        assert_eq!(config.required_db_connections(), 16);
    }

    #[tokio::test]
    async fn lane_sleeps_only_after_idle_ticks() {
        let outcomes = Arc::new(Mutex::new(VecDeque::from([
            Ok(LaneTickResult {
                processed_events: 3,
                backlog_events: 9,
                made_progress: true,
            }),
            Ok(LaneTickResult {
                processed_events: 0,
                backlog_events: 0,
                made_progress: false,
            }),
        ])));
        let sleeps = Arc::new(Mutex::new(Vec::<Duration>::new()));
        let tick_count = Arc::new(AtomicUsize::new(0));

        let stats = drive_lane_until(
            "sessions",
            Duration::from_millis(250),
            {
                let outcomes = Arc::clone(&outcomes);
                let tick_count = Arc::clone(&tick_count);
                move || {
                    let outcomes = Arc::clone(&outcomes);
                    let tick_count = Arc::clone(&tick_count);
                    async move {
                        tick_count.fetch_add(1, Ordering::SeqCst);
                        outcomes
                            .lock()
                            .expect("lock outcomes")
                            .pop_front()
                            .expect("tick outcome")
                    }
                }
            },
            {
                let sleeps = Arc::clone(&sleeps);
                move |duration| {
                    let sleeps = Arc::clone(&sleeps);
                    async move {
                        sleeps.lock().expect("lock sleeps").push(duration);
                    }
                }
            },
            {
                let tick_count = Arc::clone(&tick_count);
                move || tick_count.load(Ordering::SeqCst) >= 2
            },
        )
        .await
        .expect("drive lane");

        assert_eq!(
            stats,
            LaneRunStats {
                busy_ticks: 1,
                idle_ticks: 1,
            }
        );
        assert_eq!(
            sleeps.lock().expect("lock sleeps").as_slice(),
            &[Duration::from_millis(250)]
        );
    }

    #[tokio::test]
    async fn replay_only_ticks_repoll_without_idle_sleep() {
        let outcomes = Arc::new(Mutex::new(VecDeque::from([
            Ok(LaneTickResult {
                processed_events: 0,
                backlog_events: 12,
                made_progress: true,
            }),
            Ok(LaneTickResult {
                processed_events: 0,
                backlog_events: 0,
                made_progress: false,
            }),
        ])));
        let sleeps = Arc::new(Mutex::new(Vec::<Duration>::new()));
        let tick_count = Arc::new(AtomicUsize::new(0));

        let stats = drive_lane_until(
            "sessions",
            Duration::from_millis(250),
            {
                let outcomes = Arc::clone(&outcomes);
                let tick_count = Arc::clone(&tick_count);
                move || {
                    let outcomes = Arc::clone(&outcomes);
                    let tick_count = Arc::clone(&tick_count);
                    async move {
                        tick_count.fetch_add(1, Ordering::SeqCst);
                        outcomes
                            .lock()
                            .expect("lock outcomes")
                            .pop_front()
                            .expect("tick outcome")
                    }
                }
            },
            {
                let sleeps = Arc::clone(&sleeps);
                move |duration| {
                    let sleeps = Arc::clone(&sleeps);
                    async move {
                        sleeps.lock().expect("lock sleeps").push(duration);
                    }
                }
            },
            {
                let tick_count = Arc::clone(&tick_count);
                move || tick_count.load(Ordering::SeqCst) >= 2
            },
        )
        .await
        .expect("drive lane");

        assert_eq!(
            stats,
            LaneRunStats {
                busy_ticks: 1,
                idle_ticks: 1,
            }
        );
        assert_eq!(
            sleeps.lock().expect("lock sleeps").as_slice(),
            &[Duration::from_millis(250)]
        );
    }

    #[tokio::test]
    async fn idle_ticks_do_not_emit_info_logs() {
        let counter = InfoEventCounter::new();
        let subscriber = tracing_subscriber::registry().with(counter.clone());
        let dispatch = Dispatch::new(subscriber);

        let stats = {
            let _guard = tracing::dispatcher::set_default(&dispatch);
            drive_lane_until(
                "sessions",
                Duration::from_millis(250),
                || async {
                    Ok::<_, StoreError>(LaneTickResult {
                        processed_events: 0,
                        backlog_events: 0,
                        made_progress: false,
                    })
                },
                |_duration| async {},
                || true,
            )
            .await
        }
        .expect("drive lane");

        assert_eq!(
            stats,
            LaneRunStats {
                busy_ticks: 0,
                idle_ticks: 1,
            }
        );
        assert!(
            counter.load() == 0,
            "idle ticks should not emit info-level logs"
        );
    }

    #[tokio::test]
    async fn lanes_can_run_concurrently_without_blocking_each_other() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));

        let lane = |label: &'static str| {
            let in_flight = Arc::clone(&in_flight);
            let max_in_flight = Arc::clone(&max_in_flight);
            async move {
                drive_lane_until(
                    label,
                    Duration::from_millis(10),
                    move || {
                        let in_flight = Arc::clone(&in_flight);
                        let max_in_flight = Arc::clone(&max_in_flight);
                        async move {
                            let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                            max_in_flight.fetch_max(current, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(25)).await;
                            in_flight.fetch_sub(1, Ordering::SeqCst);
                            Ok::<_, StoreError>(LaneTickResult {
                                processed_events: 1,
                                backlog_events: 0,
                                made_progress: true,
                            })
                        }
                    },
                    |_duration| async {},
                    || true,
                )
                .await
            }
        };

        let (left, right) = tokio::join!(lane("sessions"), lane("event_metrics"));
        left.expect("left lane succeeds");
        right.expect("right lane succeeds");

        assert_eq!(max_in_flight.load(Ordering::SeqCst), 2);
    }
}
