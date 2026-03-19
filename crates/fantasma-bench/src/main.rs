use std::{
    collections::{BTreeMap, HashMap},
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, TimeZone, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

const BENCHMARK_COMPOSE_PROJECT_NAME: &str = "fantasma-bench";
const BENCHMARK_PROJECT_NAME: &str = "Fantasma Benchmark";
const BENCHMARK_ENVIRONMENT_LABEL: &str = "local";
const INGEST_BASE_URL: &str = "http://127.0.0.1:18081";
const API_BASE_URL: &str = "http://127.0.0.1:18082";
const POST_BATCH_SIZE: usize = 100;
const HEALTH_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const PROVIDERS: [&str; 4] = ["strava", "garmin", "polar", "oura"];
const PLANS: [&str; 3] = ["free", "pro", "team"];
const APP_VERSIONS: [&str; 3] = ["1.0.0", "1.1.0", "1.2.0"];
const OS_VERSIONS: [&str; 2] = ["18.3", "18.4"];
const SLO_INSTALL_COUNT_PER_DAY: usize = 1_000;
const SLO_EVENTS_PER_INSTALL_PER_DAY: usize = 30;
const SLO_SESSION_DURATION_SECONDS: u64 = 29 * 60;
const SLO_REPAIR_DURATION_DELTA_SECONDS: u64 = 10 * 60;
const SLO_REPAIR_INSTALL_MODULUS: usize = 20;
const SLO_REPAIR_LATE_EVENTS_PER_INSTALL_DAY: usize = 2;
const SLO_WARMUP_QUERIES: usize = 10;
const SLO_MEASURED_QUERIES: usize = 100;
const DEFAULT_SLO_PUBLICATION_REPETITIONS: usize = 1;
const DEFAULT_SLO_SESSION_BATCH_SIZE: i64 = 2_000;
const DEFAULT_SLO_EVENT_BATCH_SIZE: i64 = 5_000;
const DEFAULT_SLO_SESSION_INCREMENTAL_CONCURRENCY: usize = 8;
const DEFAULT_SLO_SESSION_REPAIR_CONCURRENCY: usize = 2;
const DEFAULT_SLO_IDLE_POLL_INTERVAL_MS: u64 = 250;
const DEFAULT_SLO_ITERATIVE_INSTALL_COUNT: usize = 250;
const SLO_REPRESENTATIVE_WINDOW_DAYS: usize = 30;
const SLO_REPRESENTATIVE_INSTALL_COUNT: usize = 1_000;
const SLO_REPRESENTATIVE_SESSIONS_PER_INSTALL_PER_DAY: usize = 6;
const SLO_REPRESENTATIVE_EVENTS_PER_SESSION: usize = 5;
const SLO_REPRESENTATIVE_SESSION_DURATION_SECONDS: u64 = 4 * 60;
const SLO_REPRESENTATIVE_CHECKPOINT_APPEND_INTERVAL: usize = 20;
const SLO_REPRESENTATIVE_OFFLINE_FLUSH_INSTALL_MODULUS: usize = 10;
const SLO_REPRESENTATIVE_OFFLINE_FLUSH_SESSION_COUNT: usize = 4;
const SLO_REPRESENTATIVE_LIGHT_REPAIR_INSTALL_MODULUS: usize = 100;
const SLO_REPRESENTATIVE_HOT_PROJECT_INSTALL_MODULUS: usize = 10;
const SLO_REPRESENTATIVE_HOT_PROJECT_REPAIR_INSTALL_MODULUS: usize = 50;
const SLO_REPRESENTATIVE_REPAIR_LATE_EVENTS_PER_SESSION: usize = 2;
const SLO_REPRESENTATIVE_REPAIR_DURATION_DELTA_SECONDS: u64 = 10 * 60;
const WORKER_STAGE_B_TRACE_ENV_VAR: &str = "FANTASMA_WORKER_STAGE_B_TRACE_PATH";
const WORKER_STAGE_B_TRACE_CONTAINER_PATH: &str = "/tmp/stage-b-trace.jsonl";
const WORKER_EVENT_LANE_TRACE_ENV_VAR: &str = "FANTASMA_WORKER_EVENT_LANE_TRACE_PATH";
const WORKER_EVENT_LANE_TRACE_CONTAINER_PATH: &str = "/tmp/event-lane-trace.jsonl";
const STAGE_B_TRACE_FILENAME: &str = "stage-b-trace.jsonl";
const STAGE_B_SUMMARY_FILENAME: &str = "stage-b.json";
const CHECKPOINT_SUMMARY_FILENAME: &str = "checkpoint-readiness.json";
const APPEND_ATTRIBUTION_FILENAME: &str = "append-attribution.json";
const EVENT_LANE_TRACE_FILENAME: &str = "event-lane-trace.jsonl";
const BENCHMARK_EVENT_METRICS_WORKER_NAME: &str = "event_metrics";
const BENCHMARK_SESSION_WORKER_NAME: &str = "sessions";
const STAGE_B_DOMINANT_MIN_SHARE: f64 = 0.40;
const STAGE_B_DOMINANT_MIN_LEAD: f64 = 0.10;
const STAGE_B_SLOW_RECORD_LIMIT: usize = 10;
const STAGE_B_QUEUE_LAG_CONTEXT_NAMES: [&str; 2] =
    ["queue_wait_claim", "row_mutation_lag_before_claim"];
const SLO_ITERATIVE_APPEND_CHECKPOINT_INTERVAL: usize = 200;
const SLO_ITERATIVE_OFFLINE_FLUSH_CHECKPOINT_INTERVAL: usize = 25;
const SLO_ITERATIVE_REPAIR_CHECKPOINT_INTERVAL: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum Scenario {
    #[serde(rename = "hot-path")]
    #[value(name = "hot-path")]
    Hot,
    #[serde(rename = "repair-path")]
    #[value(name = "repair-path")]
    Repair,
    #[serde(rename = "scale-path")]
    #[value(name = "scale-path")]
    Scale,
}

impl Scenario {
    fn key(self) -> &'static str {
        match self {
            Scenario::Hot => "hot-path",
            Scenario::Repair => "repair-path",
            Scenario::Scale => "scale-path",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum Profile {
    Ci,
    Extended,
    Heavy,
}

impl Profile {
    fn key(self) -> &'static str {
        match self {
            Profile::Ci => "ci",
            Profile::Extended => "extended",
            Profile::Heavy => "heavy",
        }
    }

    fn title(self) -> &'static str {
        match self {
            Profile::Ci => "CI",
            Profile::Extended => "Extended",
            Profile::Heavy => "Heavy",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BenchCommand {
    Stack(StackArgs),
    Series(SeriesArgs),
    Slo(SloArgs),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StackArgs {
    scenario: Scenario,
    profile: Profile,
    output: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SeriesArgs {
    profile: Profile,
    repetitions: usize,
    output_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SloArgs {
    output_dir: PathBuf,
    scenarios: Vec<String>,
    suites: Vec<String>,
    run_config: SloRunConfig,
}

#[derive(Debug, Parser)]
#[command(author, version, about = "Fantasma benchmark harness")]
struct Cli {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    Stack {
        #[arg(long, value_enum)]
        scenario: Scenario,
        #[arg(long, value_enum, default_value_t = Profile::Ci)]
        profile: Profile,
        #[arg(long)]
        output: PathBuf,
    },
    Series {
        #[arg(long, value_enum, default_value_t = Profile::Ci)]
        profile: Profile,
        #[arg(long, default_value_t = 1)]
        repetitions: usize,
        #[arg(long)]
        output_dir: PathBuf,
    },
    Slo {
        #[arg(long)]
        output_dir: PathBuf,
        #[arg(long = "scenario")]
        scenarios: Vec<String>,
        #[arg(long = "suite")]
        suites: Vec<String>,
        #[arg(long, value_enum, default_value_t = SloMode::Iterative)]
        mode: SloMode,
        #[arg(long, default_value_t = DEFAULT_SLO_PUBLICATION_REPETITIONS)]
        publication_repetitions: usize,
        #[arg(long, default_value_t = DEFAULT_SLO_SESSION_BATCH_SIZE)]
        worker_session_batch_size: i64,
        #[arg(long, default_value_t = DEFAULT_SLO_EVENT_BATCH_SIZE)]
        worker_event_batch_size: i64,
        #[arg(long, default_value_t = DEFAULT_SLO_SESSION_INCREMENTAL_CONCURRENCY)]
        worker_session_incremental_concurrency: usize,
        #[arg(long, default_value_t = DEFAULT_SLO_SESSION_REPAIR_CONCURRENCY)]
        worker_session_repair_concurrency: usize,
        #[arg(long, default_value_t = DEFAULT_SLO_IDLE_POLL_INTERVAL_MS)]
        worker_idle_poll_interval_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ScenarioBudget {
    min_phase_events_per_second: BTreeMap<String, f64>,
    max_readiness_ms: BTreeMap<String, u64>,
    max_query_p95_ms: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BudgetFile {
    #[serde(rename = "hot-path")]
    hot_path: ScenarioBudget,
    #[serde(rename = "repair-path")]
    repair_path: ScenarioBudget,
    #[serde(rename = "scale-path")]
    scale_path: ScenarioBudget,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PhaseMeasurement {
    name: String,
    events_sent: usize,
    elapsed_ms: u64,
    events_per_second: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ReadinessMeasurement {
    name: String,
    elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct QueryMeasurement {
    name: String,
    iterations: usize,
    min_ms: u64,
    p50_ms: u64,
    p95_ms: u64,
    max_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BudgetEvaluation {
    passed: bool,
    failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ScenarioResult {
    scenario: Scenario,
    profile: Profile,
    phases: Vec<PhaseMeasurement>,
    readiness: Vec<ReadinessMeasurement>,
    queries: Vec<QueryMeasurement>,
    budget: Option<BudgetEvaluation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct HostMetadata {
    environment: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SeriesSummary {
    profile: Profile,
    repetitions: usize,
    host: HostMetadata,
    scenarios: Vec<ScenarioResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SloWindow {
    Days30,
    Days90,
    Days180,
}

impl SloWindow {
    fn key(self) -> &'static str {
        match self {
            Self::Days30 => "30d",
            Self::Days90 => "90d",
            Self::Days180 => "180d",
        }
    }

    fn days(self) -> usize {
        match self {
            Self::Days30 => 30,
            Self::Days90 => 90,
            Self::Days180 => 180,
        }
    }

    fn total_events(self) -> usize {
        self.days() * SLO_INSTALL_COUNT_PER_DAY * SLO_EVENTS_PER_INSTALL_PER_DAY
    }

    fn readiness_timeout(self) -> Duration {
        match self {
            Self::Days30 => Duration::from_secs(5 * 60),
            Self::Days90 => Duration::from_secs(20 * 60),
            Self::Days180 => Duration::from_secs(40 * 60),
        }
    }

    fn grouped_day_budget_ms(self) -> u64 {
        match self {
            Self::Days30 => 100,
            Self::Days90 => 200,
            Self::Days180 => 350,
        }
    }

    fn grouped_hour_budget_ms(self) -> u64 {
        match self {
            Self::Days30 => 150,
            Self::Days90 => 300,
            Self::Days180 => 500,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum SloSuite {
    Representative,
    MixedRepair,
    BurstReadiness,
    Stress,
    ReadsVisibility,
}

impl SloSuite {
    fn key(self) -> &'static str {
        match self {
            Self::Representative => "representative",
            Self::MixedRepair => "mixed-repair",
            Self::BurstReadiness => "burst-readiness",
            Self::Stress => "stress",
            Self::ReadsVisibility => "reads-visibility",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SloScenarioKind {
    LiveAppendSmallBlobs,
    LiveAppendOfflineFlush,
    LiveAppendPlusLightRepair,
    LiveAppendPlusRepairHotProject,
    BurstReadiness300InstallsX1,
    BurstReadiness150InstallsX2,
    BurstReadiness100InstallsX3,
    StressAppend,
    StressBackfill,
    StressRepair,
    ReadsVisibility,
}

impl SloScenarioKind {
    fn suite(self) -> SloSuite {
        match self {
            Self::LiveAppendSmallBlobs | Self::LiveAppendOfflineFlush => SloSuite::Representative,
            Self::LiveAppendPlusLightRepair | Self::LiveAppendPlusRepairHotProject => {
                SloSuite::MixedRepair
            }
            Self::BurstReadiness300InstallsX1
            | Self::BurstReadiness150InstallsX2
            | Self::BurstReadiness100InstallsX3 => SloSuite::BurstReadiness,
            Self::StressAppend | Self::StressBackfill | Self::StressRepair => SloSuite::Stress,
            Self::ReadsVisibility => SloSuite::ReadsVisibility,
        }
    }

    fn key(self) -> &'static str {
        match self {
            Self::LiveAppendSmallBlobs => "live-append-small-blobs",
            Self::LiveAppendOfflineFlush => "live-append-offline-flush",
            Self::LiveAppendPlusLightRepair => "live-append-plus-light-repair",
            Self::LiveAppendPlusRepairHotProject => "live-append-plus-repair-hot-project",
            Self::BurstReadiness300InstallsX1 => "burst-readiness-300-installs-x1",
            Self::BurstReadiness150InstallsX2 => "burst-readiness-150-installs-x2",
            Self::BurstReadiness100InstallsX3 => "burst-readiness-100-installs-x3",
            Self::StressAppend => "append",
            Self::StressBackfill => "backfill",
            Self::StressRepair => "repair",
            Self::ReadsVisibility => "reads-visibility",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SloScenarioDefinition {
    suite: SloSuite,
    kind: SloScenarioKind,
    window: SloWindow,
    repetitions: usize,
}

impl SloScenarioDefinition {
    fn key(&self) -> String {
        match self.kind {
            SloScenarioKind::LiveAppendSmallBlobs
            | SloScenarioKind::LiveAppendOfflineFlush
            | SloScenarioKind::LiveAppendPlusLightRepair
            | SloScenarioKind::LiveAppendPlusRepairHotProject
            | SloScenarioKind::BurstReadiness300InstallsX1
            | SloScenarioKind::BurstReadiness150InstallsX2
            | SloScenarioKind::BurstReadiness100InstallsX3 => self.kind.key().to_owned(),
            SloScenarioKind::StressAppend
            | SloScenarioKind::StressBackfill
            | SloScenarioKind::StressRepair => {
                format!("stress-{}-{}", self.kind.key(), self.window.key())
            }
            SloScenarioKind::ReadsVisibility => {
                format!("reads-visibility-{}", self.window.key())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SloReadinessPolicy {
    allow_timeout_publication: bool,
    wait_for_full_readiness_before_queries: bool,
}

fn slo_readiness_policy(kind: SloScenarioKind, window: SloWindow) -> SloReadinessPolicy {
    match (kind, window) {
        (SloScenarioKind::ReadsVisibility, SloWindow::Days90 | SloWindow::Days180) => {
            SloReadinessPolicy {
                allow_timeout_publication: true,
                wait_for_full_readiness_before_queries: true,
            }
        }
        (
            SloScenarioKind::StressAppend
            | SloScenarioKind::StressBackfill
            | SloScenarioKind::StressRepair,
            SloWindow::Days90 | SloWindow::Days180,
        ) => SloReadinessPolicy {
            allow_timeout_publication: true,
            wait_for_full_readiness_before_queries: false,
        },
        _ => SloReadinessPolicy {
            allow_timeout_publication: false,
            wait_for_full_readiness_before_queries: false,
        },
    }
}

async fn wait_for_full_readiness_before_query_benchmark(
    client: &Client,
    project: &ProvisionedProject,
    scenario: &SloScenarioDefinition,
    expectation: &SloExpectation,
) -> Result<()> {
    let query_specs = slo_query_matrix(scenario.window, slo_start_day());
    let event_specs = query_specs
        .iter()
        .filter(|query| query.hard_gate && query.family == "event")
        .cloned()
        .collect::<Vec<_>>();
    let session_specs = query_specs
        .iter()
        .filter(|query| query.hard_gate && query.family == "session")
        .cloned()
        .collect::<Vec<_>>();
    let event_label = "query benchmark readiness (event metrics)";
    let session_label = "query benchmark readiness (session metrics)";

    tokio::try_join!(
        poll_until_ready(
            || slo_queries_ready(client, project, &event_specs, scenario, expectation),
            event_label,
        ),
        poll_until_ready(
            || slo_queries_ready(client, project, &session_specs, scenario, expectation),
            session_label,
        ),
    )?;

    Ok(())
}

fn slo_readiness_query_specs(window: SloWindow, start_day: NaiveDate) -> Vec<SloQuerySpec> {
    let readiness_names = [
        "events_count_day_grouped",
        "sessions_count_day_grouped",
        "sessions_duration_total_day_grouped",
        "sessions_new_installs_day_grouped",
    ];

    slo_query_matrix(window, start_day)
        .into_iter()
        .filter(|query| readiness_names.iter().any(|name| *name == query.name))
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SloScenarioResult {
    scenario: String,
    run_config: SloRunConfig,
    phases: Vec<PhaseMeasurement>,
    readiness: Vec<ReadinessMeasurement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint_readiness: Option<CheckpointReadinessSummary>,
    queries: Vec<QueryMeasurement>,
    budget: Option<BudgetEvaluation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SloSummary {
    host: HostMetadata,
    suites: Vec<String>,
    run_config: SloRunConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verdict: Option<SloVerdict>,
    scenarios: Vec<SloScenarioResult>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum SloMode {
    Iterative,
    Publication,
}

impl SloMode {
    fn key(self) -> &'static str {
        match self {
            Self::Iterative => "iterative",
            Self::Publication => "publication",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SloRunConfig {
    mode: SloMode,
    publication_repetitions: usize,
    worker_config: BenchWorkerConfig,
}

impl Default for SloRunConfig {
    fn default() -> Self {
        Self {
            mode: SloMode::Iterative,
            publication_repetitions: DEFAULT_SLO_PUBLICATION_REPETITIONS,
            worker_config: BenchWorkerConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SloVerdict {
    baseline_source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    representative_append: Option<SloScenarioVerdict>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mixed_repair: Option<SloScenarioVerdict>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dominant_stage_b_phase: Option<SloStageBPhaseVerdict>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    overall: Option<SloVerdictStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SloScenarioVerdict {
    overall: SloVerdictStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint_sample_count_matches: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint_derived: Option<SloVerdictStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    final_derived: Option<SloVerdictStatus>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum SloVerdictStatus {
    Better,
    Flat,
    Worse,
}

impl SloVerdictStatus {
    fn key(self) -> &'static str {
        match self {
            Self::Better => "better",
            Self::Flat => "flat",
            Self::Worse => "worse",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum SloStageBPhaseVerdict {
    Changed,
    Unchanged,
    Unknown,
}

impl SloStageBPhaseVerdict {
    fn key(self) -> &'static str {
        match self {
            Self::Changed => "changed",
            Self::Unchanged => "unchanged",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SloIterativeBaseline {
    mode: SloMode,
    source_artifact: String,
    scenarios: BTreeMap<String, SloIterativeBaselineScenario>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SloIterativeBaselineScenario {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint_sample_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint_derived_p95_ms: Option<u64>,
    final_derived_ready_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dominant_stage_b_phase: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LiveSloPolicy {
    install_count: usize,
    append_checkpoint_interval: usize,
    offline_flush_checkpoint_interval: usize,
    repair_checkpoint_interval: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CheckpointReadinessSummary {
    sample_count: usize,
    event_metrics_ready_p50_ms: u64,
    event_metrics_ready_p95_ms: u64,
    event_metrics_ready_max_ms: u64,
    session_metrics_ready_p50_ms: u64,
    session_metrics_ready_p95_ms: u64,
    session_metrics_ready_max_ms: u64,
    derived_metrics_ready_p50_ms: u64,
    derived_metrics_ready_p95_ms: u64,
    derived_metrics_ready_max_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CheckpointReadinessSample {
    sequence: usize,
    blob_kind: String,
    events_in_blob: usize,
    cumulative_events: u64,
    event_metrics_ready_ms: u64,
    session_metrics_ready_ms: u64,
    derived_metrics_ready_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct AppendAttributionSample {
    sequence: usize,
    blob_kind: String,
    events_in_blob: usize,
    cumulative_events: u64,
    checkpoint_target_event_id: i64,
    upload_complete_ms: u64,
    event_lane_expected_ready_ms: u64,
    session_lane_expected_ready_ms: u64,
    derived_ready_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct AppendAttributionSummary {
    sample_count: usize,
    upload_to_event_lane_ready_p50_ms: u64,
    upload_to_event_lane_ready_p95_ms: u64,
    upload_to_event_lane_ready_max_ms: u64,
    upload_to_session_lane_ready_p50_ms: u64,
    upload_to_session_lane_ready_p95_ms: u64,
    upload_to_session_lane_ready_max_ms: u64,
    latest_lane_to_derived_visible_p50_ms: u64,
    latest_lane_to_derived_visible_p95_ms: u64,
    latest_lane_to_derived_visible_max_ms: u64,
    overall_derived_ready_p50_ms: u64,
    overall_derived_ready_p95_ms: u64,
    overall_derived_ready_max_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dominant_contributor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct AppendAttributionSidecar {
    scenario: String,
    run_config: SloRunConfig,
    summary: AppendAttributionSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_lane_summary: Option<EventLaneBatchSummary>,
    samples: Vec<AppendAttributionSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct EventLaneTraceRecord {
    record_type: String,
    last_processed_event_id: i64,
    next_offset: i64,
    processed_events: usize,
    #[serde(default)]
    phases_ms: BTreeMap<String, u64>,
    #[serde(default)]
    counts: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct EventLanePhaseSummary {
    name: String,
    total_ms: u64,
    share: f64,
    distribution: StageBDistributionStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct EventLaneBatchSummary {
    record_count: usize,
    processed_events_total: usize,
    measured_event_lane_ms: u64,
    phases: Vec<EventLanePhaseSummary>,
    #[serde(default)]
    delta_rows_by_family: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dominant_phase: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UploadBlobKind {
    Append,
    OfflineFlush,
    Repair,
}

impl UploadBlobKind {
    fn key(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::OfflineFlush => "offline-flush",
            Self::Repair => "repair",
        }
    }
}

#[derive(Debug, Clone)]
struct SloExpectationDelta {
    total_events: u64,
    total_sessions: u64,
    total_duration_seconds: u64,
    total_new_installs: u64,
    total_active_installs: u64,
    pro_total_events: u64,
    pro_total_sessions: u64,
    pro_total_duration_seconds: u64,
    pro_total_new_installs: u64,
}

#[derive(Debug, Clone)]
struct UploadBlob {
    kind: UploadBlobKind,
    events: Vec<BenchEvent>,
    delta: SloExpectationDelta,
    checkpoint: bool,
}

#[derive(Clone, Copy)]
struct LiveSloIngestContext<'a> {
    client: &'a Client,
    project: &'a ProvisionedProject,
    append_probe_pool: Option<&'a PgPool>,
    scenario: &'a SloScenarioDefinition,
    phase_name: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
struct BenchWorkerConfig {
    session_batch_size: i64,
    event_batch_size: i64,
    session_incremental_concurrency: usize,
    session_repair_concurrency: usize,
    idle_poll_interval_ms: u64,
}

impl Default for BenchWorkerConfig {
    fn default() -> Self {
        Self {
            session_batch_size: DEFAULT_SLO_SESSION_BATCH_SIZE,
            event_batch_size: DEFAULT_SLO_EVENT_BATCH_SIZE,
            session_incremental_concurrency: DEFAULT_SLO_SESSION_INCREMENTAL_CONCURRENCY,
            session_repair_concurrency: DEFAULT_SLO_SESSION_REPAIR_CONCURRENCY,
            idle_poll_interval_ms: DEFAULT_SLO_IDLE_POLL_INTERVAL_MS,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StageBTraceRecord {
    record_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    install_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_event_id: Option<i64>,
    #[serde(default)]
    phases_ms: BTreeMap<String, u64>,
    #[serde(default)]
    finalization_subphases_ms: BTreeMap<String, u64>,
    #[serde(default)]
    session_daily_subphases_ms: BTreeMap<String, u64>,
    #[serde(default)]
    counts: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct StageBSummary {
    scenario: String,
    run_config: SloRunConfig,
    record_count: usize,
    measured_stage_b_ms: u64,
    record_type_counts: BTreeMap<String, usize>,
    phases: Vec<StageBPhaseSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    finalization_subphases: Vec<StageBPhaseSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    session_daily_subphases: Vec<StageBPhaseSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    backlog_context: Vec<StageBContextSummary>,
    total_counts: BTreeMap<String, u64>,
    slowest_records_by_type: BTreeMap<String, Vec<StageBSlowRecord>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dominant_phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dominant_finalization_subphase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dominant_session_daily_subphase: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StageBSidecarSummary {
    #[serde(default)]
    dominant_phase: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct StageBPhaseSummary {
    name: String,
    total_ms: u64,
    share: f64,
    distribution: StageBDistributionStats,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StageBContextSummary {
    name: String,
    total_ms: u64,
    distribution: StageBDistributionStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StageBDistributionStats {
    count: usize,
    min_ms: u64,
    p50_ms: u64,
    p95_ms: u64,
    max_ms: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct StageBSlowRecord {
    record_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    install_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_event_id: Option<i64>,
    total_ms: u64,
    phases_ms: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    finalization_subphases_ms: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    session_daily_subphases_ms: BTreeMap<String, u64>,
    counts: BTreeMap<String, u64>,
}

#[derive(Debug, Clone)]
struct SloQuerySpec {
    name: String,
    url: String,
    hard_gate: bool,
    family: &'static str,
    expected_total: SloQueryExpectedTotal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SloQueryExpectedTotal {
    Events(SloEventCountExpectation),
    SessionsCount,
    SessionsCountFilteredByPlan { plan: &'static str },
    SessionsDurationTotal,
    SessionsDurationTotalFilteredByPlan { plan: &'static str },
    SessionsNewInstalls,
    SessionsNewInstallsFilteredByPlan { plan: &'static str },
    SessionsActiveInstalls,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SloEventCountExpectation {
    All,
    FilteredByPlan { plan: &'static str },
}

#[derive(Debug, Clone, Serialize)]
struct BenchEvent {
    event: String,
    timestamp: String,
    install_id: String,
    platform: String,
    app_version: String,
    os_version: String,
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct EventBatch<'a> {
    events: &'a [BenchEvent],
}

#[derive(Debug, Clone)]
struct ProvisionedProject {
    project_id: Uuid,
    ingest_key: String,
    read_key: String,
}

#[derive(Debug, Deserialize)]
struct CreatedProjectResponse {
    project: CreatedProject,
    ingest_key: CreatedKey,
}

#[derive(Debug, Deserialize)]
struct CreatedProject {
    id: Uuid,
}

#[derive(Debug, Deserialize)]
struct CreatedKeyResponse {
    key: CreatedKey,
}

#[derive(Debug, Deserialize)]
struct CreatedKey {
    secret: String,
}

#[derive(Debug, Clone, Copy)]
struct ProfileConfig {
    hot_path_install_count: usize,
    repair_group_count: usize,
    scale_day_count: usize,
    scale_install_count_per_day: usize,
    warmup_queries: usize,
    measured_queries: usize,
    settle_timeout: Duration,
}

impl ProfileConfig {
    fn for_profile(profile: Profile) -> Self {
        match profile {
            Profile::Ci => Self {
                hot_path_install_count: 200,
                repair_group_count: 50,
                scale_day_count: 30,
                scale_install_count_per_day: 200,
                warmup_queries: 3,
                measured_queries: 15,
                settle_timeout: Duration::from_secs(30),
            },
            Profile::Extended => Self {
                hot_path_install_count: 1_000,
                repair_group_count: 200,
                scale_day_count: 30,
                scale_install_count_per_day: 500,
                warmup_queries: 5,
                measured_queries: 40,
                settle_timeout: Duration::from_secs(60),
            },
            Profile::Heavy => Self {
                hot_path_install_count: 3_000,
                repair_group_count: 600,
                scale_day_count: 60,
                scale_install_count_per_day: 1_000,
                warmup_queries: 10,
                measured_queries: 100,
                settle_timeout: Duration::from_secs(180),
            },
        }
    }
}

#[derive(Debug)]
struct RepairScenario {
    baseline_events: Vec<BenchEvent>,
    late_events: Vec<BenchEvent>,
    baseline_total_events: usize,
    late_total_events: usize,
    group_count: usize,
}

#[derive(Debug)]
struct ScaleScenario {
    events: Vec<BenchEvent>,
    total_events: usize,
    total_install_count: usize,
    total_duration_seconds: u64,
    start_day: NaiveDate,
    end_day: NaiveDate,
}

fn parse_args<I, T>(args: I) -> Result<BenchCommand>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let cli = Cli::try_parse_from(args).context("parse benchmark CLI arguments")?;

    Ok(match cli.command {
        CliCommand::Stack {
            scenario,
            profile,
            output,
        } => BenchCommand::Stack(StackArgs {
            scenario,
            profile,
            output,
        }),
        CliCommand::Series {
            profile,
            repetitions,
            output_dir,
        } => BenchCommand::Series(SeriesArgs {
            profile,
            repetitions,
            output_dir,
        }),
        CliCommand::Slo {
            output_dir,
            scenarios,
            suites,
            mode,
            publication_repetitions,
            worker_session_batch_size,
            worker_event_batch_size,
            worker_session_incremental_concurrency,
            worker_session_repair_concurrency,
            worker_idle_poll_interval_ms,
        } => BenchCommand::Slo(SloArgs {
            output_dir,
            scenarios,
            suites,
            run_config: SloRunConfig {
                mode,
                publication_repetitions: publication_repetitions.max(1),
                worker_config: BenchWorkerConfig {
                    session_batch_size: worker_session_batch_size.max(1),
                    event_batch_size: worker_event_batch_size.max(1),
                    session_incremental_concurrency: worker_session_incremental_concurrency.max(1),
                    session_repair_concurrency: worker_session_repair_concurrency.max(1),
                    idle_poll_interval_ms: worker_idle_poll_interval_ms.max(1),
                },
            },
        }),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    match parse_args(std::env::args_os())? {
        BenchCommand::Stack(args) => run_stack(args).await,
        BenchCommand::Series(args) => run_series(args).await,
        BenchCommand::Slo(args) => run_slo(args).await,
    }
}

async fn run_stack(args: StackArgs) -> Result<()> {
    let result = execute_scenario(args.scenario, args.profile).await?;
    write_result(&args.output, &result)?;
    println!("{}", render_markdown_summary(&result));
    enforce_budget(&result)
}

async fn run_series(args: SeriesArgs) -> Result<()> {
    if args.repetitions == 0 {
        bail!("series repetitions must be at least 1");
    }

    let host = collect_host_metadata()?;
    write_pretty_json(&host_metadata_output_path(&args.output_dir), &host)?;

    let mut scenarios = Vec::new();
    for scenario in [Scenario::Hot, Scenario::Repair, Scenario::Scale] {
        let paths = series_paths(&args.output_dir, scenario);
        let mut runs = Vec::with_capacity(args.repetitions);

        for run_number in 1..=args.repetitions {
            let result = execute_scenario(scenario, args.profile).await?;
            write_result(&paths.run_output(run_number), &result)?;
            enforce_budget(&result)?;
            runs.push(result);
        }

        let aggregated = aggregate_scenario_runs(scenario, args.profile, &runs)?;
        write_result(&paths.median_output(), &aggregated)?;
        scenarios.push(aggregated);
    }

    let summary = SeriesSummary {
        profile: args.profile,
        repetitions: args.repetitions,
        host,
        scenarios,
    };
    write_series_summary(&args.output_dir, &summary)?;
    println!("{}", render_series_markdown_summary(&summary));

    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct SloExpectation {
    total_events: u64,
    total_sessions: u64,
    total_duration_seconds: u64,
    total_new_installs: u64,
    total_active_installs: u64,
    pro_total_events: u64,
    pro_total_sessions: u64,
    pro_total_duration_seconds: u64,
    pro_total_new_installs: u64,
    include_repair_late_events: bool,
}

impl SloExpectation {
    fn apply_delta(&mut self, delta: &SloExpectationDelta) {
        self.total_events += delta.total_events;
        self.total_sessions += delta.total_sessions;
        self.total_duration_seconds += delta.total_duration_seconds;
        self.total_new_installs += delta.total_new_installs;
        self.total_active_installs += delta.total_active_installs;
        self.pro_total_events += delta.pro_total_events;
        self.pro_total_sessions += delta.pro_total_sessions;
        self.pro_total_duration_seconds += delta.pro_total_duration_seconds;
        self.pro_total_new_installs += delta.pro_total_new_installs;
    }
}

#[derive(Debug, Clone)]
struct ExecutedSloScenario {
    result: SloScenarioResult,
    checkpoint_samples: Vec<CheckpointReadinessSample>,
    append_attribution_samples: Vec<AppendAttributionSample>,
}

#[derive(Debug, Clone)]
struct SloScenarioPaths {
    scenario_dir: PathBuf,
    repetitions: usize,
}

impl SloScenarioPaths {
    fn run_output(&self, run_number: usize) -> PathBuf {
        if self.repetitions == 1 {
            self.scenario_dir.join("result.json")
        } else {
            self.scenario_dir.join(format!("run-{run_number:02}.json"))
        }
    }

    fn median_output(&self) -> PathBuf {
        self.scenario_dir.join("median.json")
    }
}

async fn run_slo(args: SloArgs) -> Result<()> {
    let host = collect_host_metadata()?;
    let scenarios =
        select_requested_slo_scenarios(&args.scenarios, &args.suites, &args.run_config)?;

    run_slo_with_executor(
        &args.output_dir,
        host,
        &args.run_config,
        &scenarios,
        |scenario| {
            let output_dir = args.output_dir.clone();
            let run_config = args.run_config.clone();
            async move { execute_slo_scenario(&output_dir, &scenario, &run_config).await }
        },
    )
    .await
}

#[cfg(test)]
async fn run_slo_suite<Execute, ExecuteFut>(
    output_dir: &Path,
    host: HostMetadata,
    run_config: SloRunConfig,
    mut execute: Execute,
) -> Result<()>
where
    Execute: FnMut(SloScenarioDefinition) -> ExecuteFut,
    ExecuteFut: std::future::Future<Output = Result<SloScenarioResult>>,
{
    let scenarios = select_slo_scenarios(&[], &run_config)?;
    run_slo_with_executor(output_dir, host, &run_config, &scenarios, move |scenario| {
        execute(scenario)
    })
    .await
}

async fn run_slo_with_executor<Execute, ExecuteFut>(
    output_dir: &Path,
    host: HostMetadata,
    run_config: &SloRunConfig,
    scenarios: &[SloScenarioDefinition],
    mut execute: Execute,
) -> Result<()>
where
    Execute: FnMut(SloScenarioDefinition) -> ExecuteFut,
    ExecuteFut: std::future::Future<Output = Result<SloScenarioResult>>,
{
    prepare_clean_output_dir(output_dir)?;
    write_pretty_json(&host_metadata_output_path(output_dir), &host)?;

    let mut summary_results = Vec::new();
    let mut suite_failures = Vec::new();

    'suite: for scenario in scenarios.iter().cloned() {
        let paths = slo_paths(output_dir, &scenario);
        let mut runs = Vec::with_capacity(scenario.repetitions);

        for run_number in 1..=scenario.repetitions {
            let mut result = match execute(scenario.clone()).await {
                Ok(result) => result,
                Err(error) => {
                    let failure = format!("scenario execution failed: {error:#}");
                    let failed_result =
                        slo_execution_failure_result(&scenario, run_config, failure.clone());
                    write_slo_result(&paths.run_output(run_number), &failed_result)?;
                    suite_failures.push(format!("{}: {}", scenario.key(), failure));
                    summary_results.push(failed_result);
                    break 'suite;
                }
            };
            result.budget = Some(evaluate_slo_budget(&scenario, &result));
            write_slo_result(&paths.run_output(run_number), &result)?;
            runs.push(result);
        }

        let published = if scenario.repetitions > 1 {
            let mut median = aggregate_slo_runs(&scenario, &runs)?;
            median.budget = Some(evaluate_slo_budget(&scenario, &median));
            write_slo_result(&paths.median_output(), &median)?;
            median
        } else {
            runs.into_iter()
                .next()
                .expect("single-run scenarios publish their raw result")
        };

        if let Some(budget) = &published.budget
            && !budget.passed
        {
            suite_failures.push(format!(
                "{}: {}",
                published.scenario,
                budget.failures.join("; ")
            ));
        }

        summary_results.push(published);
    }

    let summary = SloSummary {
        host,
        suites: summarized_slo_suites(&summary_results),
        run_config: run_config.clone(),
        verdict: derive_slo_verdict(output_dir, run_config, &summary_results)?,
        scenarios: summary_results,
    };
    write_slo_summary(output_dir, &summary)?;
    println!("{}", render_slo_markdown_summary(&summary));

    if suite_failures.is_empty() {
        Ok(())
    } else {
        bail!(
            "derived metrics SLO suite failed:\n{}",
            suite_failures.join("\n")
        )
    }
}

fn prepare_clean_output_dir(output_dir: &Path) -> Result<()> {
    match fs::metadata(output_dir) {
        Ok(metadata) if metadata.is_dir() => {
            fs::remove_dir_all(output_dir).with_context(|| {
                format!("remove stale output directory {}", output_dir.display())
            })?;
        }
        Ok(_) => {
            fs::remove_file(output_dir)
                .with_context(|| format!("remove stale output file {}", output_dir.display()))?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect output path {}", output_dir.display()));
        }
    }

    fs::create_dir_all(output_dir)
        .with_context(|| format!("create output directory {}", output_dir.display()))
}

fn slo_execution_failure_result(
    scenario: &SloScenarioDefinition,
    run_config: &SloRunConfig,
    failure: String,
) -> SloScenarioResult {
    SloScenarioResult {
        scenario: scenario.key(),
        run_config: run_config.clone(),
        phases: Vec::new(),
        readiness: Vec::new(),
        checkpoint_readiness: None,
        queries: Vec::new(),
        budget: Some(BudgetEvaluation {
            passed: false,
            failures: vec![failure.clone()],
        }),
        failure: Some(failure),
    }
}

async fn execute_slo_scenario(
    output_dir: &Path,
    scenario: &SloScenarioDefinition,
    run_config: &SloRunConfig,
) -> Result<SloScenarioResult> {
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("build SLO HTTP client")?;

    let rendered_compose_file = render_slo_compose_file(run_config)?;
    let mut stack = StackGuard::with_cleanup(rendered_compose_file, true);
    stack.start()?;
    wait_for_health(&client).await?;
    let project = provision_project(&client).await?;
    let append_probe_pool = if scenario.kind == SloScenarioKind::LiveAppendSmallBlobs {
        Some(connect_benchmark_database(&stack.compose_file).await?)
    } else {
        None
    };

    let executed = match scenario.kind {
        SloScenarioKind::LiveAppendSmallBlobs
        | SloScenarioKind::LiveAppendOfflineFlush
        | SloScenarioKind::LiveAppendPlusLightRepair
        | SloScenarioKind::LiveAppendPlusRepairHotProject
        | SloScenarioKind::BurstReadiness300InstallsX1
        | SloScenarioKind::BurstReadiness150InstallsX2
        | SloScenarioKind::BurstReadiness100InstallsX3 => {
            run_live_slo_scenario(
                &client,
                &project,
                append_probe_pool.as_ref(),
                scenario,
                run_config,
            )
            .await
        }
        SloScenarioKind::StressAppend => {
            run_slo_append_like(&client, &project, scenario, run_config, false, false).await
        }
        SloScenarioKind::StressBackfill => {
            run_slo_append_like(&client, &project, scenario, run_config, true, false).await
        }
        SloScenarioKind::StressRepair => {
            run_slo_repair(&client, &project, scenario, run_config).await
        }
        SloScenarioKind::ReadsVisibility => {
            run_slo_append_like(&client, &project, scenario, run_config, false, true).await
        }
    };

    let trace_capture = copy_stage_b_trace_from_worker(&stack.compose_file);
    let event_lane_trace_capture = copy_event_lane_trace_from_worker(&stack.compose_file);
    match executed {
        Ok(executed) => {
            if !executed.checkpoint_samples.is_empty() {
                let scenario_dir = slo_paths(output_dir, scenario).scenario_dir;
                write_checkpoint_readiness_sidecar(&scenario_dir, &executed.checkpoint_samples)?;
                if !executed.append_attribution_samples.is_empty() {
                    write_append_attribution_sidecar(
                        &scenario_dir,
                        &scenario.key(),
                        run_config,
                        &executed.append_attribution_samples,
                        event_lane_trace_capture?.as_deref(),
                    )?;
                }
            }
            if let Some(trace_contents) = trace_capture? {
                let scenario_dir = slo_paths(output_dir, scenario).scenario_dir;
                write_stage_b_sidecars(
                    &scenario_dir,
                    &scenario.key(),
                    run_config,
                    &trace_contents,
                )?;
            }
            Ok(executed.result)
        }
        Err(error) => {
            let _ = event_lane_trace_capture;
            if let Ok(Some(trace_contents)) = trace_capture {
                let scenario_dir = slo_paths(output_dir, scenario).scenario_dir;
                let _ = write_stage_b_sidecars(
                    &scenario_dir,
                    &scenario.key(),
                    run_config,
                    &trace_contents,
                );
            }
            Err(error)
        }
    }
}

async fn run_live_slo_scenario(
    client: &Client,
    project: &ProvisionedProject,
    append_probe_pool: Option<&PgPool>,
    scenario: &SloScenarioDefinition,
    run_config: &SloRunConfig,
) -> Result<ExecutedSloScenario> {
    let (append_blobs, repair_blobs) = live_slo_schedule(scenario, run_config)?;
    let mut expectation = SloExpectation::default();
    let mut checkpoint_samples = Vec::new();
    let mut append_attribution_samples = Vec::new();
    let append_ingest = LiveSloIngestContext {
        client,
        project,
        append_probe_pool,
        scenario,
        phase_name: "append_uploads",
    };

    let append_phase = ingest_live_slo_blobs(
        append_ingest,
        &append_blobs,
        &mut expectation,
        &mut checkpoint_samples,
        &mut append_attribution_samples,
    )
    .await?;

    let mut phases = vec![append_phase];

    if !repair_blobs.is_empty() {
        phases.push(
            ingest_live_slo_blobs(
                LiveSloIngestContext {
                    client,
                    project,
                    append_probe_pool: None,
                    scenario,
                    phase_name: "repair_uploads",
                },
                &repair_blobs,
                &mut expectation,
                &mut checkpoint_samples,
                &mut append_attribution_samples,
            )
            .await?,
        );
    }

    let readiness = wait_for_slo_readiness(
        client,
        project,
        scenario,
        &expectation,
        Instant::now(),
        false,
    )
    .await?;
    let checkpoint_readiness = checkpoint_readiness_summary(&checkpoint_samples);

    Ok(ExecutedSloScenario {
        result: SloScenarioResult {
            scenario: scenario.key(),
            run_config: run_config.clone(),
            phases,
            readiness,
            checkpoint_readiness,
            queries: Vec::new(),
            budget: None,
            failure: None,
        },
        checkpoint_samples,
        append_attribution_samples,
    })
}

async fn ingest_live_slo_blobs(
    ingest: LiveSloIngestContext<'_>,
    blobs: &[UploadBlob],
    expectation: &mut SloExpectation,
    checkpoint_samples: &mut Vec<CheckpointReadinessSample>,
    append_attribution_samples: &mut Vec<AppendAttributionSample>,
) -> Result<PhaseMeasurement> {
    let started = Instant::now();
    let mut events_sent = 0usize;

    for blob in blobs {
        post_upload_blob(ingest.client, ingest.project, blob).await?;
        expectation.apply_delta(&blob.delta);
        events_sent += blob.events.len();

        if blob.checkpoint {
            let checkpoint_started_at = Instant::now();
            let checkpoint_target_event_id = if let Some(pool) = ingest.append_probe_pool {
                Some(load_project_raw_event_tail_id(pool, ingest.project.project_id).await?)
            } else {
                None
            };
            let deadline = checkpoint_started_at + ingest.scenario.window.readiness_timeout();
            let readiness = wait_for_slo_expectation(
                ingest.client,
                ingest.project,
                ingest.scenario,
                expectation,
                checkpoint_started_at,
                "checkpoint readiness",
                false,
            );
            if let (Some(pool), Some(checkpoint_target_event_id)) =
                (ingest.append_probe_pool, checkpoint_target_event_id)
            {
                let (event_lane_expected_ready_ms, session_lane_expected_ready_ms, readiness) = tokio::try_join!(
                    wait_for_worker_offset_target(
                        pool,
                        BENCHMARK_EVENT_METRICS_WORKER_NAME,
                        checkpoint_target_event_id,
                        deadline,
                        checkpoint_started_at,
                    ),
                    wait_for_worker_offset_target(
                        pool,
                        BENCHMARK_SESSION_WORKER_NAME,
                        checkpoint_target_event_id,
                        deadline,
                        checkpoint_started_at,
                    ),
                    readiness,
                )?;
                append_attribution_samples.push(append_attribution_sample(
                    blob,
                    checkpoint_samples.len() + 1,
                    expectation.total_events,
                    checkpoint_target_event_id,
                    event_lane_expected_ready_ms,
                    session_lane_expected_ready_ms,
                    readiness_measurement(&readiness, "derived_metrics_ready_ms")?,
                )?);
                checkpoint_samples.push(checkpoint_sample(
                    checkpoint_samples.len() + 1,
                    blob,
                    expectation.total_events,
                    &readiness,
                )?);
            } else {
                let readiness = readiness.await?;
                checkpoint_samples.push(checkpoint_sample(
                    checkpoint_samples.len() + 1,
                    blob,
                    expectation.total_events,
                    &readiness,
                )?);
            }
        }
    }

    let elapsed = started.elapsed();
    Ok(PhaseMeasurement {
        name: ingest.phase_name.to_owned(),
        events_sent,
        elapsed_ms: elapsed.as_millis() as u64,
        events_per_second: throughput(events_sent, elapsed),
    })
}

async fn run_slo_append_like(
    client: &Client,
    project: &ProvisionedProject,
    scenario: &SloScenarioDefinition,
    run_config: &SloRunConfig,
    reverse_day_order: bool,
    benchmark_reads: bool,
) -> Result<ExecutedSloScenario> {
    let expectation = slo_expectation(scenario);
    let readiness_policy = slo_readiness_policy(scenario.kind, scenario.window);
    let phase_name = if benchmark_reads {
        "seed_ingest"
    } else {
        "ingest"
    };
    let (ingest_phase, readiness) = run_slo_phase_and_wait_for_readiness(
        || {
            ingest_slo_window(
                client,
                project,
                scenario.window,
                reverse_day_order,
                phase_name,
            )
        },
        |readiness_started| {
            wait_for_slo_readiness(
                client,
                project,
                scenario,
                &expectation,
                readiness_started,
                readiness_policy.allow_timeout_publication,
            )
        },
    )
    .await?;
    let queries = if benchmark_reads {
        if readiness_policy.wait_for_full_readiness_before_queries {
            wait_for_full_readiness_before_query_benchmark(client, project, scenario, &expectation)
                .await?;
        }
        let query_specs = slo_query_matrix(scenario.window, slo_start_day());
        measure_slo_queries(
            client,
            project,
            &query_specs,
            SLO_WARMUP_QUERIES,
            SLO_MEASURED_QUERIES,
        )
        .await?
    } else {
        Vec::new()
    };

    Ok(ExecutedSloScenario {
        result: SloScenarioResult {
            scenario: scenario.key(),
            run_config: run_config.clone(),
            phases: vec![ingest_phase],
            readiness,
            checkpoint_readiness: None,
            queries,
            budget: None,
            failure: None,
        },
        checkpoint_samples: Vec::new(),
        append_attribution_samples: Vec::new(),
    })
}

async fn run_slo_repair(
    client: &Client,
    project: &ProvisionedProject,
    scenario: &SloScenarioDefinition,
    run_config: &SloRunConfig,
) -> Result<ExecutedSloScenario> {
    let seed_phase =
        ingest_slo_window(client, project, scenario.window, false, "seed_ingest").await?;
    wait_for_slo_expectation(
        client,
        project,
        scenario,
        &slo_base_expectation(scenario.window),
        Instant::now(),
        "repair seed readiness",
        false,
    )
    .await?;

    let expectation = slo_expectation(scenario);
    let (repair_phase, readiness) = run_slo_phase_and_wait_for_readiness(
        || ingest_slo_repair_window(client, project, scenario.window),
        |readiness_started| {
            wait_for_slo_expectation(
                client,
                project,
                scenario,
                &expectation,
                readiness_started,
                "repair readiness",
                !matches!(scenario.window, SloWindow::Days30),
            )
        },
    )
    .await?;

    Ok(ExecutedSloScenario {
        result: SloScenarioResult {
            scenario: scenario.key(),
            run_config: run_config.clone(),
            phases: vec![seed_phase, repair_phase],
            readiness,
            checkpoint_readiness: None,
            queries: Vec::new(),
            budget: None,
            failure: None,
        },
        checkpoint_samples: Vec::new(),
        append_attribution_samples: Vec::new(),
    })
}

async fn run_slo_phase_and_wait_for_readiness<Phase, PhaseFut, Wait, WaitFut, PhaseOutput>(
    phase: Phase,
    wait_for_readiness: Wait,
) -> Result<(PhaseOutput, Vec<ReadinessMeasurement>)>
where
    Phase: FnOnce() -> PhaseFut,
    PhaseFut: std::future::Future<Output = Result<PhaseOutput>>,
    Wait: FnOnce(Instant) -> WaitFut,
    WaitFut: std::future::Future<Output = Result<Vec<ReadinessMeasurement>>>,
{
    let phase_output = phase().await?;
    let readiness_started = Instant::now();
    let readiness = wait_for_readiness(readiness_started).await?;

    Ok((phase_output, readiness))
}

fn slo_scenario_definitions(run_config: &SloRunConfig) -> Vec<SloScenarioDefinition> {
    let mut scenarios = vec![
        SloScenarioDefinition {
            suite: SloSuite::Representative,
            kind: SloScenarioKind::LiveAppendSmallBlobs,
            window: SloWindow::Days30,
            repetitions: representative_repetitions(run_config),
        },
        SloScenarioDefinition {
            suite: SloSuite::Representative,
            kind: SloScenarioKind::LiveAppendOfflineFlush,
            window: SloWindow::Days30,
            repetitions: representative_repetitions(run_config),
        },
        SloScenarioDefinition {
            suite: SloSuite::MixedRepair,
            kind: SloScenarioKind::LiveAppendPlusLightRepair,
            window: SloWindow::Days30,
            repetitions: representative_repetitions(run_config),
        },
        SloScenarioDefinition {
            suite: SloSuite::MixedRepair,
            kind: SloScenarioKind::LiveAppendPlusRepairHotProject,
            window: SloWindow::Days30,
            repetitions: representative_repetitions(run_config),
        },
        SloScenarioDefinition {
            suite: SloSuite::BurstReadiness,
            kind: SloScenarioKind::BurstReadiness300InstallsX1,
            window: SloWindow::Days30,
            repetitions: representative_repetitions(run_config),
        },
        SloScenarioDefinition {
            suite: SloSuite::BurstReadiness,
            kind: SloScenarioKind::BurstReadiness150InstallsX2,
            window: SloWindow::Days30,
            repetitions: representative_repetitions(run_config),
        },
        SloScenarioDefinition {
            suite: SloSuite::BurstReadiness,
            kind: SloScenarioKind::BurstReadiness100InstallsX3,
            window: SloWindow::Days30,
            repetitions: representative_repetitions(run_config),
        },
    ];

    for window in [SloWindow::Days30, SloWindow::Days90, SloWindow::Days180] {
        for kind in [
            SloScenarioKind::StressAppend,
            SloScenarioKind::StressBackfill,
            SloScenarioKind::StressRepair,
            SloScenarioKind::ReadsVisibility,
        ] {
            scenarios.push(SloScenarioDefinition {
                suite: kind.suite(),
                kind,
                window,
                repetitions: 1,
            });
        }
    }

    scenarios
}

fn select_slo_scenarios(
    requested: &[String],
    run_config: &SloRunConfig,
) -> Result<Vec<SloScenarioDefinition>> {
    let scenarios = slo_scenario_definitions(run_config);
    if requested.is_empty() {
        return Ok(default_slo_scenarios(run_config));
    }

    let known = scenarios
        .iter()
        .map(|scenario| (scenario.key(), scenario.clone()))
        .collect::<HashMap<_, _>>();
    let unknown = requested
        .iter()
        .filter(|key| !known.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        bail!("unknown slo scenario: {}", unknown.join(", "));
    }

    let requested = requested.to_vec();
    Ok(scenarios
        .into_iter()
        .filter(|scenario| requested.iter().any(|key| key == &scenario.key()))
        .collect())
}

fn select_requested_slo_scenarios(
    requested_scenarios: &[String],
    requested_suites: &[String],
    run_config: &SloRunConfig,
) -> Result<Vec<SloScenarioDefinition>> {
    if !requested_scenarios.is_empty() {
        return select_slo_scenarios(requested_scenarios, run_config);
    }

    if requested_suites.is_empty() {
        return select_slo_scenarios(&[], run_config);
    }

    let scenarios = slo_scenario_definitions(run_config);
    let known_suites = [
        SloSuite::Representative,
        SloSuite::MixedRepair,
        SloSuite::BurstReadiness,
        SloSuite::Stress,
        SloSuite::ReadsVisibility,
    ]
    .into_iter()
    .map(|suite| suite.key())
    .collect::<Vec<_>>();
    let unknown = requested_suites
        .iter()
        .filter(|suite| !known_suites.iter().any(|known| known == &suite.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        bail!("unknown slo suite: {}", unknown.join(", "));
    }

    Ok(scenarios
        .into_iter()
        .filter(|scenario| {
            requested_suites
                .iter()
                .any(|suite| suite == scenario.suite.key())
        })
        .collect())
}

fn representative_repetitions(run_config: &SloRunConfig) -> usize {
    match run_config.mode {
        SloMode::Iterative => 1,
        SloMode::Publication => run_config.publication_repetitions.max(1),
    }
}

fn default_slo_scenarios(run_config: &SloRunConfig) -> Vec<SloScenarioDefinition> {
    let default_keys = match run_config.mode {
        SloMode::Iterative => {
            ["live-append-small-blobs", "live-append-plus-light-repair"].as_slice()
        }
        SloMode::Publication => [
            "live-append-small-blobs",
            "live-append-offline-flush",
            "live-append-plus-light-repair",
            "live-append-plus-repair-hot-project",
        ]
        .as_slice(),
    };

    slo_scenario_definitions(run_config)
        .into_iter()
        .filter(|scenario| default_keys.iter().any(|key| scenario.key() == *key))
        .collect()
}

fn live_slo_policy(run_config: &SloRunConfig) -> LiveSloPolicy {
    match run_config.mode {
        SloMode::Iterative => LiveSloPolicy {
            install_count: DEFAULT_SLO_ITERATIVE_INSTALL_COUNT,
            append_checkpoint_interval: SLO_ITERATIVE_APPEND_CHECKPOINT_INTERVAL,
            offline_flush_checkpoint_interval: SLO_ITERATIVE_OFFLINE_FLUSH_CHECKPOINT_INTERVAL,
            repair_checkpoint_interval: SLO_ITERATIVE_REPAIR_CHECKPOINT_INTERVAL,
        },
        SloMode::Publication => LiveSloPolicy {
            install_count: SLO_REPRESENTATIVE_INSTALL_COUNT,
            append_checkpoint_interval: SLO_REPRESENTATIVE_CHECKPOINT_APPEND_INTERVAL,
            offline_flush_checkpoint_interval: 1,
            repair_checkpoint_interval: 1,
        },
    }
}

fn summarized_slo_suites(results: &[SloScenarioResult]) -> Vec<String> {
    let mut suites = Vec::new();
    for suite in [
        SloSuite::Representative,
        SloSuite::MixedRepair,
        SloSuite::BurstReadiness,
        SloSuite::Stress,
        SloSuite::ReadsVisibility,
    ] {
        if results.iter().any(|result| {
            result.scenario.starts_with(match suite {
                SloSuite::Representative => "live-append-",
                SloSuite::MixedRepair => "live-append-plus-",
                SloSuite::BurstReadiness => "burst-readiness-",
                SloSuite::Stress => "stress-",
                SloSuite::ReadsVisibility => "reads-visibility-",
            })
        }) {
            suites.push(suite.key().to_owned());
        }
    }
    suites
}

fn slo_paths(output_dir: &Path, scenario: &SloScenarioDefinition) -> SloScenarioPaths {
    SloScenarioPaths {
        scenario_dir: output_dir.join(scenario.key()),
        repetitions: scenario.repetitions,
    }
}

fn slo_start_day() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid SLO suite start day")
}

fn slo_base_expectation(window: SloWindow) -> SloExpectation {
    SloExpectation {
        total_events: window.total_events() as u64,
        total_sessions: (window.days() * SLO_INSTALL_COUNT_PER_DAY) as u64,
        total_duration_seconds: (window.days() * SLO_INSTALL_COUNT_PER_DAY) as u64
            * SLO_SESSION_DURATION_SECONDS,
        total_new_installs: SLO_INSTALL_COUNT_PER_DAY as u64,
        total_active_installs: (window.days() * SLO_INSTALL_COUNT_PER_DAY) as u64,
        pro_total_events: (window.days()
            * install_count_for_plan(SLO_INSTALL_COUNT_PER_DAY, "pro")
            * SLO_EVENTS_PER_INSTALL_PER_DAY) as u64,
        pro_total_sessions: (window.days()
            * install_count_for_plan(SLO_INSTALL_COUNT_PER_DAY, "pro"))
            as u64,
        pro_total_duration_seconds: (window.days()
            * install_count_for_plan(SLO_INSTALL_COUNT_PER_DAY, "pro"))
            as u64
            * SLO_SESSION_DURATION_SECONDS,
        pro_total_new_installs: install_count_for_plan(SLO_INSTALL_COUNT_PER_DAY, "pro") as u64,
        include_repair_late_events: false,
    }
}

fn slo_expectation(scenario: &SloScenarioDefinition) -> SloExpectation {
    let mut expectation = slo_base_expectation(scenario.window);

    if matches!(scenario.kind, SloScenarioKind::StressRepair) {
        let repaired_install_days = repaired_install_days_for_plan(scenario.window, None) as u64;
        let repaired_pro_install_days =
            repaired_install_days_for_plan(scenario.window, Some("pro")) as u64;
        expectation.total_events +=
            repaired_install_days * SLO_REPAIR_LATE_EVENTS_PER_INSTALL_DAY as u64;
        expectation.total_duration_seconds +=
            repaired_install_days * SLO_REPAIR_DURATION_DELTA_SECONDS;
        expectation.pro_total_events +=
            repaired_pro_install_days * SLO_REPAIR_LATE_EVENTS_PER_INSTALL_DAY as u64;
        expectation.pro_total_duration_seconds +=
            repaired_pro_install_days * SLO_REPAIR_DURATION_DELTA_SECONDS;
        expectation.include_repair_late_events = true;
    }

    expectation
}

fn slo_query_matrix(window: SloWindow, start_day: NaiveDate) -> Vec<SloQuerySpec> {
    let end_day = start_day + ChronoDuration::days(window.days().saturating_sub(1) as i64);
    let day_start = start_day.to_string();
    let day_end = end_day.to_string();
    let hour_start = format!("{start_day}T00:00:00Z");
    let hour_end = format!("{end_day}T23:00:00Z");

    let mut queries = Vec::with_capacity(32);

    queries.extend([
        SloQuerySpec {
            name: "events_count_day_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=day&start={day_start}&end={day_end}&group_by=plan&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "event",
            expected_total: SloQueryExpectedTotal::Events(SloEventCountExpectation::All),
        },
        SloQuerySpec {
            name: "events_count_hour_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=hour&start={hour_start}&end={hour_end}&group_by=plan&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "event",
            expected_total: SloQueryExpectedTotal::Events(SloEventCountExpectation::All),
        },
        SloQuerySpec {
            name: "events_count_day_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=day&start={day_start}&end={day_end}&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "event",
            expected_total: SloQueryExpectedTotal::Events(
                SloEventCountExpectation::FilteredByPlan { plan: "pro" },
            ),
        },
        SloQuerySpec {
            name: "events_count_hour_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=hour&start={hour_start}&end={hour_end}&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "event",
            expected_total: SloQueryExpectedTotal::Events(
                SloEventCountExpectation::FilteredByPlan { plan: "pro" },
            ),
        },
        SloQuerySpec {
            name: "sessions_count_day_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=count&granularity=day&start={day_start}&end={day_end}&group_by=platform&group_by=app_version",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsCount,
        },
        SloQuerySpec {
            name: "sessions_count_hour_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=count&granularity=hour&start={hour_start}&end={hour_end}&group_by=platform&group_by=app_version",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsCount,
        },
        SloQuerySpec {
            name: "sessions_count_day_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=count&granularity=day&start={day_start}&end={day_end}&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsCountFilteredByPlan { plan: "pro" },
        },
        SloQuerySpec {
            name: "sessions_count_hour_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=count&granularity=hour&start={hour_start}&end={hour_end}&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsCountFilteredByPlan { plan: "pro" },
        },
        SloQuerySpec {
            name: "sessions_duration_total_day_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=duration_total&granularity=day&start={day_start}&end={day_end}&group_by=platform&group_by=app_version",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsDurationTotal,
        },
        SloQuerySpec {
            name: "sessions_duration_total_hour_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=duration_total&granularity=hour&start={hour_start}&end={hour_end}&group_by=platform&group_by=app_version",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsDurationTotal,
        },
        SloQuerySpec {
            name: "sessions_duration_total_day_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=duration_total&granularity=day&start={day_start}&end={day_end}&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsDurationTotalFilteredByPlan {
                plan: "pro",
            },
        },
        SloQuerySpec {
            name: "sessions_duration_total_hour_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=duration_total&granularity=hour&start={hour_start}&end={hour_end}&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsDurationTotalFilteredByPlan {
                plan: "pro",
            },
        },
        SloQuerySpec {
            name: "sessions_new_installs_day_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=new_installs&granularity=day&start={day_start}&end={day_end}&group_by=platform&group_by=app_version",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsNewInstalls,
        },
        SloQuerySpec {
            name: "sessions_new_installs_hour_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=new_installs&granularity=hour&start={hour_start}&end={hour_end}&group_by=platform&group_by=app_version",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsNewInstalls,
        },
        SloQuerySpec {
            name: "sessions_new_installs_day_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=new_installs&granularity=day&start={day_start}&end={day_end}&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsNewInstallsFilteredByPlan {
                plan: "pro",
            },
        },
        SloQuerySpec {
            name: "sessions_new_installs_hour_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=new_installs&granularity=hour&start={hour_start}&end={hour_end}&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: true,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsNewInstallsFilteredByPlan {
                plan: "pro",
            },
        },
    ]);

    queries.extend([
        SloQuerySpec {
            name: "events_count_day_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=day&start={day_start}&end={day_end}",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "event",
            expected_total: SloQueryExpectedTotal::Events(SloEventCountExpectation::All),
        },
        SloQuerySpec {
            name: "events_count_hour_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=hour&start={hour_start}&end={hour_end}",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "event",
            expected_total: SloQueryExpectedTotal::Events(SloEventCountExpectation::All),
        },
        SloQuerySpec {
            name: "sessions_count_day_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=count&granularity=day&start={day_start}&end={day_end}",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsCount,
        },
        SloQuerySpec {
            name: "sessions_count_hour_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=count&granularity=hour&start={hour_start}&end={hour_end}",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsCount,
        },
        SloQuerySpec {
            name: "sessions_duration_total_day_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=duration_total&granularity=day&start={day_start}&end={day_end}",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsDurationTotal,
        },
        SloQuerySpec {
            name: "sessions_duration_total_hour_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=duration_total&granularity=hour&start={hour_start}&end={hour_end}",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsDurationTotal,
        },
        SloQuerySpec {
            name: "sessions_new_installs_day_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=new_installs&granularity=day&start={day_start}&end={day_end}",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsNewInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_day_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=day",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_day_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=day&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_day_dim2_filtered".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=day&plan=pro&provider=strava",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_week_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=week",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_week_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=week&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_week_dim2_filtered".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=week&plan=pro&provider=strava",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_month_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=month",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_month_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=month&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_month_dim2_filtered".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=month&plan=pro&provider=strava",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_year_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=year",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_year_dim2_grouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=year&plan=pro&group_by=provider",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_active_installs_year_dim2_filtered".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=active_installs&start={day_start}&end={day_end}&interval=year&plan=pro&provider=strava",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsActiveInstalls,
        },
        SloQuerySpec {
            name: "sessions_new_installs_hour_ungrouped".to_owned(),
            url: format!(
                "{}/v1/metrics/sessions?metric=new_installs&granularity=hour&start={hour_start}&end={hour_end}",
                benchmark_api_base_url()
            ),
            hard_gate: false,
            family: "session",
            expected_total: SloQueryExpectedTotal::SessionsNewInstalls,
        },
    ]);

    queries
}

async fn ingest_slo_window(
    client: &Client,
    project: &ProvisionedProject,
    window: SloWindow,
    reverse_day_order: bool,
    phase_name: &str,
) -> Result<PhaseMeasurement> {
    let started = Instant::now();
    let mut events_sent = 0usize;

    for day_offset in slo_day_offsets(window, reverse_day_order) {
        let day = slo_start_day() + ChronoDuration::days(day_offset as i64);
        let day_events = slo_day_events(day, day_offset)?;
        events_sent += day_events.len();
        post_event_chunks(client, project, &day_events).await?;
    }

    let elapsed = started.elapsed();
    Ok(PhaseMeasurement {
        name: phase_name.to_owned(),
        events_sent,
        elapsed_ms: elapsed.as_millis() as u64,
        events_per_second: throughput(events_sent, elapsed),
    })
}

async fn ingest_slo_repair_window(
    client: &Client,
    project: &ProvisionedProject,
    window: SloWindow,
) -> Result<PhaseMeasurement> {
    let started = Instant::now();
    let mut events_sent = 0usize;

    for day_offset in 0..window.days() {
        let day = slo_start_day() + ChronoDuration::days(day_offset as i64);
        let late_events = slo_day_repair_events(day, day_offset)?;
        events_sent += late_events.len();
        if !late_events.is_empty() {
            post_event_chunks(client, project, &late_events).await?;
        }
    }

    let elapsed = started.elapsed();
    Ok(PhaseMeasurement {
        name: "repair_ingest".to_owned(),
        events_sent,
        elapsed_ms: elapsed.as_millis() as u64,
        events_per_second: throughput(events_sent, elapsed),
    })
}

fn slo_day_offsets(window: SloWindow, reverse_day_order: bool) -> Vec<usize> {
    let mut offsets = (0..window.days()).collect::<Vec<_>>();
    if reverse_day_order {
        offsets.reverse();
    }
    offsets
}

fn slo_day_events(day: NaiveDate, day_offset: usize) -> Result<Vec<BenchEvent>> {
    let mut events = Vec::with_capacity(SLO_INSTALL_COUNT_PER_DAY * SLO_EVENTS_PER_INSTALL_PER_DAY);

    for install_index in 0..SLO_INSTALL_COUNT_PER_DAY {
        events.extend(slo_install_day_events(day, day_offset, install_index)?);
    }

    events.sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
    Ok(events)
}

fn slo_day_repair_events(day: NaiveDate, day_offset: usize) -> Result<Vec<BenchEvent>> {
    let mut events = Vec::with_capacity(
        (SLO_INSTALL_COUNT_PER_DAY / SLO_REPAIR_INSTALL_MODULUS)
            * SLO_REPAIR_LATE_EVENTS_PER_INSTALL_DAY,
    );

    for install_index in 0..SLO_INSTALL_COUNT_PER_DAY {
        if install_index % SLO_REPAIR_INSTALL_MODULUS != day_offset % SLO_REPAIR_INSTALL_MODULUS {
            continue;
        }

        let base = slo_install_base_time(day, day_offset, install_index)?;
        let mut repair_events = Vec::with_capacity(SLO_REPAIR_LATE_EVENTS_PER_INSTALL_DAY);
        for offset_minutes in 0..SLO_REPAIR_LATE_EVENTS_PER_INSTALL_DAY {
            repair_events.push(slo_event(
                install_index,
                day_offset,
                base - ChronoDuration::minutes((10 - offset_minutes) as i64),
            ));
        }
        events.extend(repair_events);
    }

    events.sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
    Ok(events)
}

fn slo_install_day_events(
    day: NaiveDate,
    day_offset: usize,
    install_index: usize,
) -> Result<Vec<BenchEvent>> {
    let base = slo_install_base_time(day, day_offset, install_index)?;
    let mut events = Vec::with_capacity(SLO_EVENTS_PER_INSTALL_PER_DAY);

    for event_offset in 0..SLO_EVENTS_PER_INSTALL_PER_DAY {
        events.push(slo_event(
            install_index,
            day_offset,
            base + ChronoDuration::minutes(event_offset as i64),
        ));
    }

    Ok(events)
}

fn slo_install_base_time(
    day: NaiveDate,
    day_offset: usize,
    install_index: usize,
) -> Result<chrono::DateTime<Utc>> {
    let start_hour = ((day_offset * 7) + install_index) % 24;
    Utc.with_ymd_and_hms(day.year(), day.month(), day.day(), start_hour as u32, 10, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid SLO event timestamp for {}", day))
}

fn slo_event(
    install_index: usize,
    day_offset: usize,
    timestamp: chrono::DateTime<Utc>,
) -> BenchEvent {
    let install_id = format!("slo-install-{install_index:04}");
    let platform = if install_index.is_multiple_of(2) {
        "ios"
    } else {
        "android"
    };
    let app_version = APP_VERSIONS[(day_offset + install_index) % APP_VERSIONS.len()];
    let os_version = OS_VERSIONS[(day_offset + install_index) % OS_VERSIONS.len()];
    let provider = PROVIDERS[install_index % PROVIDERS.len()];
    let plan = PLANS[install_index % PLANS.len()];

    BenchEvent {
        event: "app_open".to_owned(),
        timestamp: timestamp.to_rfc3339(),
        install_id,
        platform: platform.to_owned(),
        app_version: app_version.to_owned(),
        os_version: os_version.to_owned(),
        properties: BTreeMap::from([
            ("plan".to_owned(), plan.to_owned()),
            ("provider".to_owned(), provider.to_owned()),
        ]),
    }
}

async fn wait_for_slo_readiness(
    client: &Client,
    project: &ProvisionedProject,
    scenario: &SloScenarioDefinition,
    expectation: &SloExpectation,
    scenario_started: Instant,
    allow_timeout_publication: bool,
) -> Result<Vec<ReadinessMeasurement>> {
    wait_for_slo_expectation(
        client,
        project,
        scenario,
        expectation,
        scenario_started,
        "derived metrics readiness",
        allow_timeout_publication,
    )
    .await
}

async fn wait_for_slo_expectation(
    client: &Client,
    project: &ProvisionedProject,
    scenario: &SloScenarioDefinition,
    expectation: &SloExpectation,
    scenario_started: Instant,
    label: &str,
    allow_timeout_publication: bool,
) -> Result<Vec<ReadinessMeasurement>> {
    let query_specs = slo_readiness_query_specs(scenario.window, slo_start_day());
    let event_specs = query_specs
        .iter()
        .filter(|query| query.hard_gate && query.family == "event")
        .cloned()
        .collect::<Vec<_>>();
    let session_specs = query_specs
        .iter()
        .filter(|query| query.hard_gate && query.family == "session")
        .cloned()
        .collect::<Vec<_>>();

    let event_label = format!("{label} (event metrics)");
    let session_label = format!("{label} (session metrics)");
    let deadline = scenario_started + scenario.window.readiness_timeout();
    let (event_ready_ms, session_ready_ms) = tokio::try_join!(
        poll_until_elapsed(
            || slo_queries_ready(client, project, &event_specs, scenario, expectation),
            deadline,
            &event_label,
            scenario_started,
            allow_timeout_publication,
        ),
        poll_until_elapsed(
            || slo_queries_ready(client, project, &session_specs, scenario, expectation),
            deadline,
            &session_label,
            scenario_started,
            allow_timeout_publication,
        ),
    )?;
    let timeout_ms = scenario.window.readiness_timeout().as_millis() as u64;
    let event_ready_ms = event_ready_ms.unwrap_or(timeout_ms);
    let session_ready_ms = session_ready_ms.unwrap_or(timeout_ms);

    Ok(vec![
        ReadinessMeasurement {
            name: "event_metrics_ready_ms".to_owned(),
            elapsed_ms: event_ready_ms,
        },
        ReadinessMeasurement {
            name: "session_metrics_ready_ms".to_owned(),
            elapsed_ms: session_ready_ms,
        },
        ReadinessMeasurement {
            name: "derived_metrics_ready_ms".to_owned(),
            elapsed_ms: event_ready_ms.max(session_ready_ms),
        },
    ])
}

fn checkpoint_sample(
    sequence: usize,
    blob: &UploadBlob,
    cumulative_events: u64,
    readiness: &[ReadinessMeasurement],
) -> Result<CheckpointReadinessSample> {
    Ok(CheckpointReadinessSample {
        sequence,
        blob_kind: blob.kind.key().to_owned(),
        events_in_blob: blob.events.len(),
        cumulative_events,
        event_metrics_ready_ms: readiness_measurement(readiness, "event_metrics_ready_ms")?,
        session_metrics_ready_ms: readiness_measurement(readiness, "session_metrics_ready_ms")?,
        derived_metrics_ready_ms: readiness_measurement(readiness, "derived_metrics_ready_ms")?,
    })
}

fn readiness_measurement(readiness: &[ReadinessMeasurement], name: &str) -> Result<u64> {
    readiness
        .iter()
        .find(|measurement| measurement.name == name)
        .map(|measurement| measurement.elapsed_ms)
        .ok_or_else(|| anyhow!("missing readiness measurement for {}", name))
}

fn checkpoint_readiness_summary(
    samples: &[CheckpointReadinessSample],
) -> Option<CheckpointReadinessSummary> {
    if samples.is_empty() {
        return None;
    }

    let mut event_samples = samples
        .iter()
        .map(|sample| sample.event_metrics_ready_ms)
        .collect::<Vec<_>>();
    let mut session_samples = samples
        .iter()
        .map(|sample| sample.session_metrics_ready_ms)
        .collect::<Vec<_>>();
    let mut derived_samples = samples
        .iter()
        .map(|sample| sample.derived_metrics_ready_ms)
        .collect::<Vec<_>>();
    event_samples.sort_unstable();
    session_samples.sort_unstable();
    derived_samples.sort_unstable();

    Some(CheckpointReadinessSummary {
        sample_count: samples.len(),
        event_metrics_ready_p50_ms: percentile(&event_samples, 0.50),
        event_metrics_ready_p95_ms: percentile(&event_samples, 0.95),
        event_metrics_ready_max_ms: *event_samples.last().unwrap_or(&0),
        session_metrics_ready_p50_ms: percentile(&session_samples, 0.50),
        session_metrics_ready_p95_ms: percentile(&session_samples, 0.95),
        session_metrics_ready_max_ms: *session_samples.last().unwrap_or(&0),
        derived_metrics_ready_p50_ms: percentile(&derived_samples, 0.50),
        derived_metrics_ready_p95_ms: percentile(&derived_samples, 0.95),
        derived_metrics_ready_max_ms: *derived_samples.last().unwrap_or(&0),
    })
}

fn append_attribution_sample(
    blob: &UploadBlob,
    sequence: usize,
    cumulative_events: u64,
    checkpoint_target_event_id: i64,
    event_lane_expected_ready_ms: u64,
    session_lane_expected_ready_ms: u64,
    derived_ready_ms: u64,
) -> Result<AppendAttributionSample> {
    Ok(AppendAttributionSample {
        sequence,
        blob_kind: blob.kind.key().to_owned(),
        events_in_blob: blob.events.len(),
        cumulative_events,
        checkpoint_target_event_id,
        upload_complete_ms: 0,
        event_lane_expected_ready_ms,
        session_lane_expected_ready_ms,
        derived_ready_ms,
    })
}

async fn wait_for_worker_offset_target(
    pool: &PgPool,
    worker_name: &str,
    checkpoint_target_event_id: i64,
    deadline: Instant,
    started_at: Instant,
) -> Result<u64> {
    let elapsed =
        poll_until_elapsed(
            || async {
                Ok(load_benchmark_worker_offset(pool, worker_name).await?
                    >= checkpoint_target_event_id)
            },
            deadline,
            worker_name,
            started_at,
            false,
        )
        .await?;
    Ok(elapsed
        .expect("worker offset target should not timeout when publication timeout is disabled"))
}

fn summarize_append_attribution_samples(
    samples: &[AppendAttributionSample],
) -> Option<AppendAttributionSummary> {
    if samples.is_empty() {
        return None;
    }

    let mut upload_to_event = samples
        .iter()
        .map(|sample| {
            sample
                .event_lane_expected_ready_ms
                .saturating_sub(sample.upload_complete_ms)
        })
        .collect::<Vec<_>>();
    let mut upload_to_session = samples
        .iter()
        .map(|sample| {
            sample
                .session_lane_expected_ready_ms
                .saturating_sub(sample.upload_complete_ms)
        })
        .collect::<Vec<_>>();
    let mut latest_lane_to_derived = samples
        .iter()
        .map(|sample| {
            sample.derived_ready_ms.saturating_sub(
                sample
                    .event_lane_expected_ready_ms
                    .max(sample.session_lane_expected_ready_ms),
            )
        })
        .collect::<Vec<_>>();
    let mut overall_derived = samples
        .iter()
        .map(|sample| sample.derived_ready_ms)
        .collect::<Vec<_>>();
    upload_to_event.sort_unstable();
    upload_to_session.sort_unstable();
    latest_lane_to_derived.sort_unstable();
    overall_derived.sort_unstable();

    let contributor_p95 = [
        (
            "upload_to_event_lane_ready",
            percentile(&upload_to_event, 0.95),
        ),
        (
            "upload_to_session_lane_ready",
            percentile(&upload_to_session, 0.95),
        ),
        (
            "latest_lane_to_derived_visible",
            percentile(&latest_lane_to_derived, 0.95),
        ),
    ];
    let dominant_contributor = contributor_p95
        .into_iter()
        .max_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(right.0)))
        .map(|(name, _)| name.to_owned());

    Some(AppendAttributionSummary {
        sample_count: samples.len(),
        upload_to_event_lane_ready_p50_ms: percentile(&upload_to_event, 0.50),
        upload_to_event_lane_ready_p95_ms: percentile(&upload_to_event, 0.95),
        upload_to_event_lane_ready_max_ms: *upload_to_event.last().unwrap_or(&0),
        upload_to_session_lane_ready_p50_ms: percentile(&upload_to_session, 0.50),
        upload_to_session_lane_ready_p95_ms: percentile(&upload_to_session, 0.95),
        upload_to_session_lane_ready_max_ms: *upload_to_session.last().unwrap_or(&0),
        latest_lane_to_derived_visible_p50_ms: percentile(&latest_lane_to_derived, 0.50),
        latest_lane_to_derived_visible_p95_ms: percentile(&latest_lane_to_derived, 0.95),
        latest_lane_to_derived_visible_max_ms: *latest_lane_to_derived.last().unwrap_or(&0),
        overall_derived_ready_p50_ms: percentile(&overall_derived, 0.50),
        overall_derived_ready_p95_ms: percentile(&overall_derived, 0.95),
        overall_derived_ready_max_ms: *overall_derived.last().unwrap_or(&0),
        dominant_contributor,
    })
}

fn parse_event_lane_trace_records(trace_contents: &str) -> Result<Vec<EventLaneTraceRecord>> {
    trace_contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).context("parse event lane trace record"))
        .collect()
}

fn summarize_event_lane_trace_records(
    records: &[EventLaneTraceRecord],
) -> Option<EventLaneBatchSummary> {
    if records.is_empty() {
        return None;
    }

    let mut phase_totals = BTreeMap::<String, u64>::new();
    let mut phase_samples = BTreeMap::<String, Vec<u64>>::new();
    let mut delta_rows_by_family = BTreeMap::<String, u64>::new();
    let mut processed_events_total = 0usize;

    for record in records {
        processed_events_total += record.processed_events;
        for (phase, elapsed_ms) in &record.phases_ms {
            *phase_totals.entry(phase.clone()).or_default() += *elapsed_ms;
            phase_samples
                .entry(phase.clone())
                .or_default()
                .push(*elapsed_ms);
        }
        for (name, value) in &record.counts {
            if name.ends_with("_delta_rows") {
                *delta_rows_by_family.entry(name.clone()).or_default() += *value;
            }
        }
    }

    let measured_event_lane_ms: u64 = phase_totals.values().copied().sum();
    let mut phases = phase_totals
        .into_iter()
        .map(|(name, total_ms)| {
            let mut samples = phase_samples
                .remove(&name)
                .expect("event lane phase samples tracked alongside totals");
            samples.sort_unstable();
            EventLanePhaseSummary {
                name,
                total_ms,
                share: if measured_event_lane_ms == 0 {
                    0.0
                } else {
                    total_ms as f64 / measured_event_lane_ms as f64
                },
                distribution: StageBDistributionStats {
                    count: samples.len(),
                    min_ms: *samples.first().unwrap_or(&0),
                    p50_ms: percentile(&samples, 0.50),
                    p95_ms: percentile(&samples, 0.95),
                    max_ms: *samples.last().unwrap_or(&0),
                },
            }
        })
        .collect::<Vec<_>>();
    phases.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| left.name.cmp(&right.name))
    });

    Some(EventLaneBatchSummary {
        record_count: records.len(),
        processed_events_total,
        measured_event_lane_ms,
        dominant_phase: dominant_event_lane_phase(&phases),
        phases,
        delta_rows_by_family,
    })
}

fn dominant_event_lane_phase(phases: &[EventLanePhaseSummary]) -> Option<String> {
    let largest = phases.first()?;
    let second_share = phases.get(1).map_or(0.0, |phase| phase.share);

    if largest.share >= STAGE_B_DOMINANT_MIN_SHARE
        && (largest.share - second_share) >= STAGE_B_DOMINANT_MIN_LEAD
    {
        Some(largest.name.clone())
    } else {
        None
    }
}

fn write_checkpoint_readiness_sidecar(
    scenario_dir: &Path,
    samples: &[CheckpointReadinessSample],
) -> Result<()> {
    if samples.is_empty() {
        return Ok(());
    }

    fs::create_dir_all(scenario_dir).with_context(|| {
        format!(
            "create scenario sidecar directory {}",
            scenario_dir.display()
        )
    })?;
    write_pretty_json(&scenario_dir.join(CHECKPOINT_SUMMARY_FILENAME), &samples)
}

fn write_append_attribution_sidecar(
    scenario_dir: &Path,
    scenario: &str,
    run_config: &SloRunConfig,
    samples: &[AppendAttributionSample],
    event_lane_trace_contents: Option<&str>,
) -> Result<()> {
    let Some(summary) = summarize_append_attribution_samples(samples) else {
        return Ok(());
    };
    let event_lane_summary = event_lane_trace_contents
        .map(parse_event_lane_trace_records)
        .transpose()?
        .and_then(|records| summarize_event_lane_trace_records(&records));

    fs::create_dir_all(scenario_dir).with_context(|| {
        format!(
            "create scenario sidecar directory {}",
            scenario_dir.display()
        )
    })?;
    if let Some(trace_contents) = event_lane_trace_contents.filter(|contents| !contents.is_empty())
    {
        fs::write(event_lane_trace_output_path(scenario_dir), trace_contents).with_context(
            || {
                format!(
                    "write event lane trace sidecar {}",
                    event_lane_trace_output_path(scenario_dir).display()
                )
            },
        )?;
    }
    write_pretty_json(
        &scenario_dir.join(APPEND_ATTRIBUTION_FILENAME),
        &AppendAttributionSidecar {
            scenario: scenario.to_owned(),
            run_config: run_config.clone(),
            summary,
            event_lane_summary,
            samples: samples.to_vec(),
        },
    )
}

async fn poll_until_elapsed<F, Fut>(
    mut check: F,
    deadline: Instant,
    label: &str,
    started_at: Instant,
    allow_timeout_publication: bool,
) -> Result<Option<u64>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    loop {
        if check().await? {
            return Ok(Some(started_at.elapsed().as_millis() as u64));
        }

        if Instant::now() >= deadline {
            if allow_timeout_publication {
                return Ok(None);
            }
            bail!("timed out waiting for {}", label);
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn poll_until_ready<F, Fut>(mut check: F, _label: &str) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    loop {
        if check().await? {
            return Ok(());
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn slo_queries_ready(
    client: &Client,
    project: &ProvisionedProject,
    queries: &[SloQuerySpec],
    scenario: &SloScenarioDefinition,
    expectation: &SloExpectation,
) -> Result<bool> {
    for query in queries {
        let value = fetch_json(client, project, &query.url).await?;
        if sum_metric_series(&value["series"])
            != expected_total_for_slo_query(query, scenario, expectation)
        {
            return Ok(false);
        }
    }

    Ok(true)
}

fn expected_total_for_slo_query(
    query: &SloQuerySpec,
    _scenario: &SloScenarioDefinition,
    expectation: &SloExpectation,
) -> u64 {
    match query.expected_total {
        SloQueryExpectedTotal::Events(SloEventCountExpectation::All) => expectation.total_events,
        SloQueryExpectedTotal::Events(SloEventCountExpectation::FilteredByPlan { plan }) => {
            filtered_event_total_for_slo_expectation(expectation, plan)
        }
        SloQueryExpectedTotal::SessionsCount => expectation.total_sessions,
        SloQueryExpectedTotal::SessionsCountFilteredByPlan { plan } => {
            filtered_session_count_for_slo_expectation(expectation, plan)
        }
        SloQueryExpectedTotal::SessionsDurationTotal => expectation.total_duration_seconds,
        SloQueryExpectedTotal::SessionsDurationTotalFilteredByPlan { plan } => {
            filtered_session_duration_total_for_slo_expectation(expectation, plan)
        }
        SloQueryExpectedTotal::SessionsNewInstalls => expectation.total_new_installs,
        SloQueryExpectedTotal::SessionsNewInstallsFilteredByPlan { plan } => {
            filtered_session_new_installs_for_slo_expectation(expectation, plan)
        }
        SloQueryExpectedTotal::SessionsActiveInstalls => expectation.total_active_installs,
    }
}

fn filtered_event_total_for_slo_expectation(expectation: &SloExpectation, plan: &str) -> u64 {
    match plan {
        "pro" => expectation.pro_total_events,
        _ => 0,
    }
}

fn filtered_session_count_for_slo_expectation(expectation: &SloExpectation, plan: &str) -> u64 {
    match plan {
        "pro" => expectation.pro_total_sessions,
        _ => 0,
    }
}

fn filtered_session_duration_total_for_slo_expectation(
    expectation: &SloExpectation,
    plan: &str,
) -> u64 {
    match plan {
        "pro" => expectation.pro_total_duration_seconds,
        _ => 0,
    }
}

fn filtered_session_new_installs_for_slo_expectation(
    expectation: &SloExpectation,
    plan: &str,
) -> u64 {
    match plan {
        "pro" => expectation.pro_total_new_installs,
        _ => 0,
    }
}

fn slo_event_matches_plan(install_index: usize, plan: &str) -> bool {
    PLANS[install_index % PLANS.len()] == plan
}

fn install_count_for_plan(install_count: usize, plan: &str) -> usize {
    (0..install_count)
        .filter(|install_index| slo_event_matches_plan(*install_index, plan))
        .count()
}

fn repaired_install_days_for_plan(window: SloWindow, plan: Option<&str>) -> usize {
    let mut repaired = 0usize;

    for day_offset in 0..window.days() {
        for install_index in 0..SLO_INSTALL_COUNT_PER_DAY {
            if let Some(plan) = plan
                && !slo_event_matches_plan(install_index, plan)
            {
                continue;
            }

            if install_index % SLO_REPAIR_INSTALL_MODULUS == day_offset % SLO_REPAIR_INSTALL_MODULUS
            {
                repaired += 1;
            }
        }
    }

    repaired
}

async fn measure_slo_queries(
    client: &Client,
    project: &ProvisionedProject,
    queries: &[SloQuerySpec],
    warmup_iterations: usize,
    measured_iterations: usize,
) -> Result<Vec<QueryMeasurement>> {
    let mut measurements = Vec::with_capacity(queries.len());

    for query in queries {
        for _ in 0..warmup_iterations {
            fetch_json(client, project, &query.url).await?;
        }

        let mut samples = Vec::with_capacity(measured_iterations);
        for _ in 0..measured_iterations {
            let started = Instant::now();
            fetch_json(client, project, &query.url).await?;
            samples.push(started.elapsed().as_millis() as u64);
        }
        samples.sort_unstable();

        measurements.push(QueryMeasurement {
            name: query.name.to_owned(),
            iterations: measured_iterations,
            min_ms: *samples.first().unwrap_or(&0),
            p50_ms: percentile(&samples, 0.50),
            p95_ms: percentile(&samples, 0.95),
            max_ms: *samples.last().unwrap_or(&0),
        });
    }

    Ok(measurements)
}

fn evaluate_slo_budget(
    scenario: &SloScenarioDefinition,
    result: &SloScenarioResult,
) -> BudgetEvaluation {
    let mut failures = Vec::new();

    if matches!(
        scenario.suite,
        SloSuite::Representative | SloSuite::MixedRepair
    ) {
        match &result.checkpoint_readiness {
            Some(summary) => {
                for (name, observed, maximum) in [
                    (
                        "event_metrics_ready_p95_ms",
                        summary.event_metrics_ready_p95_ms,
                        30_000_u64,
                    ),
                    (
                        "session_metrics_ready_p95_ms",
                        summary.session_metrics_ready_p95_ms,
                        60_000_u64,
                    ),
                    (
                        "derived_metrics_ready_p95_ms",
                        summary.derived_metrics_ready_p95_ms,
                        60_000_u64,
                    ),
                ] {
                    if observed > maximum {
                        failures.push(format!(
                            "{} readiness {}ms exceeded budget {}ms",
                            name, observed, maximum
                        ));
                    }
                }
            }
            None => failures.push("missing checkpoint readiness summary".to_owned()),
        }
    }

    if matches!(scenario.suite, SloSuite::ReadsVisibility) {
        for query in result
            .queries
            .iter()
            .filter(|query| is_slo_grouped_query(&query.name))
        {
            let maximum = if query.name.contains("_day_") {
                scenario.window.grouped_day_budget_ms()
            } else {
                scenario.window.grouped_hour_budget_ms()
            };

            if query.p95_ms > maximum {
                failures.push(format!(
                    "{} p95 {}ms exceeded budget {}ms",
                    query.name, query.p95_ms, maximum
                ));
            }
        }
    }

    BudgetEvaluation {
        passed: failures.is_empty(),
        failures,
    }
}

fn is_slo_grouped_query(name: &str) -> bool {
    name.ends_with("_grouped")
}

fn derive_slo_verdict(
    output_dir: &Path,
    run_config: &SloRunConfig,
    results: &[SloScenarioResult],
) -> Result<Option<SloVerdict>> {
    if run_config.mode != SloMode::Iterative {
        return Ok(None);
    }

    let has_iterative_gate_result = results.iter().any(|result| {
        matches!(
            result.scenario.as_str(),
            "live-append-small-blobs" | "live-append-plus-light-repair"
        )
    });
    if !has_iterative_gate_result {
        return Ok(None);
    }

    let baseline = load_slo_iterative_baseline(&slo_iterative_baseline_path())?;
    let representative_append = scenario_verdict(
        results
            .iter()
            .find(|result| result.scenario == "live-append-small-blobs"),
        baseline.scenarios.get("live-append-small-blobs"),
    );
    let mixed_repair = scenario_verdict(
        results
            .iter()
            .find(|result| result.scenario == "live-append-plus-light-repair"),
        baseline.scenarios.get("live-append-plus-light-repair"),
    );
    let dominant_stage_b_phase = stage_b_phase_verdict(
        output_dir,
        results
            .iter()
            .find(|result| result.scenario == "live-append-plus-light-repair"),
        baseline.scenarios.get("live-append-plus-light-repair"),
    )?;

    let overall = overall_slo_verdict([
        representative_append
            .as_ref()
            .map(|verdict| verdict.overall),
        mixed_repair.as_ref().map(|verdict| verdict.overall),
    ]);
    if representative_append.is_none()
        && mixed_repair.is_none()
        && dominant_stage_b_phase.is_none()
        && overall.is_none()
    {
        return Ok(None);
    }

    Ok(Some(SloVerdict {
        baseline_source: baseline.source_artifact,
        representative_append,
        mixed_repair,
        dominant_stage_b_phase,
        overall,
    }))
}

fn scenario_verdict(
    result: Option<&SloScenarioResult>,
    baseline: Option<&SloIterativeBaselineScenario>,
) -> Option<SloScenarioVerdict> {
    let baseline = baseline?;
    let result = result?;
    let checkpoint_sample_count_matches = checkpoint_sample_count_matches(
        result
            .checkpoint_readiness
            .as_ref()
            .map(|summary| summary.sample_count),
        baseline.checkpoint_sample_count,
    );
    let checkpoint_derived = if checkpoint_sample_count_matches {
        metric_verdict(
            result
                .checkpoint_readiness
                .as_ref()
                .map(|summary| summary.derived_metrics_ready_p95_ms),
            baseline.checkpoint_derived_p95_ms,
        )
    } else {
        None
    };
    let final_derived = metric_verdict(
        slo_result_readiness_ms(result, "derived_metrics_ready_ms"),
        Some(baseline.final_derived_ready_ms),
    );
    let overall = overall_slo_verdict([checkpoint_derived, final_derived])?;

    Some(SloScenarioVerdict {
        overall,
        checkpoint_sample_count_matches: baseline
            .checkpoint_sample_count
            .map(|_| checkpoint_sample_count_matches),
        checkpoint_derived,
        final_derived,
    })
}

fn checkpoint_sample_count_matches(current: Option<usize>, baseline: Option<usize>) -> bool {
    match (current, baseline) {
        (_, None) => true,
        (Some(current), Some(baseline)) => current == baseline,
        (None, Some(_)) => false,
    }
}

fn metric_verdict(current: Option<u64>, baseline: Option<u64>) -> Option<SloVerdictStatus> {
    match (current, baseline) {
        (None, None) | (Some(_), None) => None,
        (None, Some(_)) => Some(SloVerdictStatus::Worse),
        (Some(current), Some(baseline)) => {
            let threshold = baseline.max(1).div_ceil(20).max(1_000);
            if baseline.saturating_sub(current) >= threshold {
                Some(SloVerdictStatus::Better)
            } else if current.saturating_sub(baseline) >= threshold {
                Some(SloVerdictStatus::Worse)
            } else {
                Some(SloVerdictStatus::Flat)
            }
        }
    }
}

fn overall_slo_verdict(
    scenario_verdicts: [Option<SloVerdictStatus>; 2],
) -> Option<SloVerdictStatus> {
    if scenario_verdicts
        .iter()
        .flatten()
        .any(|verdict| *verdict == SloVerdictStatus::Worse)
    {
        Some(SloVerdictStatus::Worse)
    } else if scenario_verdicts
        .iter()
        .flatten()
        .any(|verdict| *verdict == SloVerdictStatus::Better)
    {
        Some(SloVerdictStatus::Better)
    } else if scenario_verdicts.iter().flatten().next().is_some() {
        Some(SloVerdictStatus::Flat)
    } else {
        None
    }
}

fn stage_b_phase_verdict(
    output_dir: &Path,
    result: Option<&SloScenarioResult>,
    baseline: Option<&SloIterativeBaselineScenario>,
) -> Result<Option<SloStageBPhaseVerdict>> {
    let (Some(baseline), Some(result)) = (baseline, result) else {
        return Ok(None);
    };
    let current = load_stage_b_dominant_phase(output_dir, &result.scenario)?;

    Ok(
        match (
            current.as_deref(),
            baseline.dominant_stage_b_phase.as_deref(),
        ) {
            (None, None) => None,
            (None, Some(_)) | (Some(_), None) => Some(SloStageBPhaseVerdict::Unknown),
            (Some(current), Some(baseline)) if current == baseline => {
                Some(SloStageBPhaseVerdict::Unchanged)
            }
            _ => Some(SloStageBPhaseVerdict::Changed),
        },
    )
}

fn slo_result_readiness_ms(result: &SloScenarioResult, name: &str) -> Option<u64> {
    result
        .readiness
        .iter()
        .find(|measurement| measurement.name == name)
        .map(|measurement| measurement.elapsed_ms)
}

fn load_slo_iterative_baseline(path: &Path) -> Result<SloIterativeBaseline> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read iterative SLO baseline {}", path.display()))?;
    let baseline: SloIterativeBaseline = serde_json::from_str(&contents)
        .with_context(|| format!("parse iterative SLO baseline {}", path.display()))?;
    if baseline.mode != SloMode::Iterative {
        bail!(
            "iterative SLO baseline {} must declare iterative mode",
            path.display()
        );
    }
    Ok(baseline)
}

fn load_stage_b_dominant_phase(output_dir: &Path, scenario: &str) -> Result<Option<String>> {
    let path = output_dir.join(scenario).join(STAGE_B_SUMMARY_FILENAME);
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("read Stage B summary {}", path.display()));
        }
    };
    let summary: StageBSidecarSummary = serde_json::from_str(&contents)
        .with_context(|| format!("parse Stage B summary {}", path.display()))?;
    Ok(summary.dominant_phase)
}

fn slo_iterative_baseline_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("baselines/slo-iterative-default.json")
}

fn aggregate_slo_runs(
    scenario: &SloScenarioDefinition,
    runs: &[SloScenarioResult],
) -> Result<SloScenarioResult> {
    let first = runs
        .first()
        .ok_or_else(|| anyhow!("cannot aggregate empty SLO run list"))?;

    for run in runs {
        if run.scenario != scenario.key() {
            bail!(
                "SLO scenario mismatch while aggregating {}: got {}",
                scenario.key(),
                run.scenario
            );
        }
    }

    let phases = first
        .phases
        .iter()
        .map(|phase| {
            let mut elapsed_samples = Vec::with_capacity(runs.len());
            let mut throughput_samples = Vec::with_capacity(runs.len());

            for run in runs {
                let candidate = run
                    .phases
                    .iter()
                    .find(|measurement| measurement.name == phase.name)
                    .ok_or_else(|| anyhow!("missing phase measurement for {}", phase.name))?;
                if candidate.events_sent != phase.events_sent {
                    bail!("phase {} changed events_sent across SLO runs", phase.name);
                }
                elapsed_samples.push(candidate.elapsed_ms);
                throughput_samples.push(candidate.events_per_second);
            }

            Ok(PhaseMeasurement {
                name: phase.name.clone(),
                events_sent: phase.events_sent,
                elapsed_ms: median_u64(&elapsed_samples),
                events_per_second: median_f64(&throughput_samples),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let readiness = first
        .readiness
        .iter()
        .map(|measurement| {
            let mut samples = Vec::with_capacity(runs.len());
            for run in runs {
                let candidate = run
                    .readiness
                    .iter()
                    .find(|entry| entry.name == measurement.name)
                    .ok_or_else(|| {
                        anyhow!("missing readiness measurement for {}", measurement.name)
                    })?;
                samples.push(candidate.elapsed_ms);
            }

            Ok(ReadinessMeasurement {
                name: measurement.name.clone(),
                elapsed_ms: median_u64(&samples),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let queries = first
        .queries
        .iter()
        .map(|query| {
            let mut min_samples = Vec::with_capacity(runs.len());
            let mut p50_samples = Vec::with_capacity(runs.len());
            let mut p95_samples = Vec::with_capacity(runs.len());
            let mut max_samples = Vec::with_capacity(runs.len());

            for run in runs {
                let candidate = run
                    .queries
                    .iter()
                    .find(|entry| entry.name == query.name)
                    .ok_or_else(|| anyhow!("missing query measurement for {}", query.name))?;
                if candidate.iterations != query.iterations {
                    bail!(
                        "query {} changed iteration count across SLO runs",
                        query.name
                    );
                }
                min_samples.push(candidate.min_ms);
                p50_samples.push(candidate.p50_ms);
                p95_samples.push(candidate.p95_ms);
                max_samples.push(candidate.max_ms);
            }

            Ok(QueryMeasurement {
                name: query.name.clone(),
                iterations: query.iterations,
                min_ms: median_u64(&min_samples),
                p50_ms: median_u64(&p50_samples),
                p95_ms: median_u64(&p95_samples),
                max_ms: median_u64(&max_samples),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let checkpoint_readiness = if let Some(summary) = &first.checkpoint_readiness {
        let mut event_p50 = Vec::with_capacity(runs.len());
        let mut event_p95 = Vec::with_capacity(runs.len());
        let mut event_max = Vec::with_capacity(runs.len());
        let mut session_p50 = Vec::with_capacity(runs.len());
        let mut session_p95 = Vec::with_capacity(runs.len());
        let mut session_max = Vec::with_capacity(runs.len());
        let mut derived_p50 = Vec::with_capacity(runs.len());
        let mut derived_p95 = Vec::with_capacity(runs.len());
        let mut derived_max = Vec::with_capacity(runs.len());

        for run in runs {
            let candidate = run.checkpoint_readiness.as_ref().ok_or_else(|| {
                anyhow!("missing checkpoint readiness summary for {}", run.scenario)
            })?;
            if candidate.sample_count != summary.sample_count {
                bail!("checkpoint sample_count changed across SLO runs");
            }
            event_p50.push(candidate.event_metrics_ready_p50_ms);
            event_p95.push(candidate.event_metrics_ready_p95_ms);
            event_max.push(candidate.event_metrics_ready_max_ms);
            session_p50.push(candidate.session_metrics_ready_p50_ms);
            session_p95.push(candidate.session_metrics_ready_p95_ms);
            session_max.push(candidate.session_metrics_ready_max_ms);
            derived_p50.push(candidate.derived_metrics_ready_p50_ms);
            derived_p95.push(candidate.derived_metrics_ready_p95_ms);
            derived_max.push(candidate.derived_metrics_ready_max_ms);
        }

        Some(CheckpointReadinessSummary {
            sample_count: summary.sample_count,
            event_metrics_ready_p50_ms: median_u64(&event_p50),
            event_metrics_ready_p95_ms: median_u64(&event_p95),
            event_metrics_ready_max_ms: median_u64(&event_max),
            session_metrics_ready_p50_ms: median_u64(&session_p50),
            session_metrics_ready_p95_ms: median_u64(&session_p95),
            session_metrics_ready_max_ms: median_u64(&session_max),
            derived_metrics_ready_p50_ms: median_u64(&derived_p50),
            derived_metrics_ready_p95_ms: median_u64(&derived_p95),
            derived_metrics_ready_max_ms: median_u64(&derived_max),
        })
    } else {
        None
    };

    Ok(SloScenarioResult {
        scenario: scenario.key(),
        run_config: first.run_config.clone(),
        phases,
        readiness,
        checkpoint_readiness,
        queries,
        budget: None,
        failure: None,
    })
}

fn write_slo_result(output: &Path, result: &SloScenarioResult) -> Result<()> {
    write_pretty_json(output, result)?;
    write_markdown(
        output.with_extension("md"),
        render_slo_markdown_result(result),
    )?;
    Ok(())
}

fn write_slo_summary(output_dir: &Path, summary: &SloSummary) -> Result<()> {
    let output = summary_output_path(output_dir);
    write_pretty_json(&output, summary)?;
    write_markdown(
        output.with_extension("md"),
        render_slo_markdown_summary(summary),
    )?;
    Ok(())
}

fn render_slo_markdown_result(result: &SloScenarioResult) -> String {
    render_slo_scenario_section(result, None)
}

fn render_slo_markdown_summary(summary: &SloSummary) -> String {
    let mut lines = vec![
        "# Fantasma Derived Metrics SLO Suite".to_owned(),
        String::new(),
        format!("- Environment: {}", summary.host.environment),
        format!("- Published suites: {}", summary.suites.join(", ")),
        String::new(),
    ];
    lines.extend(render_slo_run_config_lines(&summary.run_config));
    lines.push(String::new());
    if let Some(verdict) = &summary.verdict {
        lines.push("## Iterative Verdict".to_owned());
        lines.push(String::new());
        lines.push(format!("- baseline: {}", verdict.baseline_source));
        if let Some(verdict) = &verdict.representative_append {
            lines.push(format!(
                "- representative append verdict: {}",
                verdict.overall.key()
            ));
            if verdict.checkpoint_sample_count_matches == Some(false) {
                lines.push(
                    "- representative checkpoint sample count: mismatch vs baseline".to_owned(),
                );
            }
            if let Some(component) = verdict.checkpoint_derived {
                lines.push(format!(
                    "- representative checkpoint derived verdict: {}",
                    component.key()
                ));
            }
            if let Some(component) = verdict.final_derived {
                lines.push(format!(
                    "- representative final derived verdict: {}",
                    component.key()
                ));
            }
        }
        if let Some(verdict) = &verdict.mixed_repair {
            lines.push(format!("- mixed repair verdict: {}", verdict.overall.key()));
            if verdict.checkpoint_sample_count_matches == Some(false) {
                lines.push(
                    "- mixed repair checkpoint sample count: mismatch vs baseline".to_owned(),
                );
            }
            if let Some(component) = verdict.checkpoint_derived {
                lines.push(format!(
                    "- mixed repair checkpoint derived verdict: {}",
                    component.key()
                ));
            }
            if let Some(component) = verdict.final_derived {
                lines.push(format!(
                    "- mixed repair final derived verdict: {}",
                    component.key()
                ));
            }
        }
        if let Some(verdict) = verdict.dominant_stage_b_phase {
            lines.push(format!("- dominant Stage B phase: {}", verdict.key()));
        }
        if let Some(verdict) = verdict.overall {
            lines.push(format!("- overall verdict: {}", verdict.key()));
        }
        lines.push(String::new());
    }

    for scenario in &summary.scenarios {
        lines.push(render_slo_scenario_section(scenario, Some("##")));
        lines.push(String::new());
    }

    lines.join("\n").trim_end().to_owned()
}

fn render_slo_scenario_section(result: &SloScenarioResult, heading_prefix: Option<&str>) -> String {
    let mut lines = Vec::new();

    if let Some(prefix) = heading_prefix {
        lines.push(format!("{prefix} {}", result.scenario));
        lines.push(String::new());
    } else {
        lines.push(format!(
            "# Fantasma Derived Metrics SLO: {}",
            result.scenario
        ));
        lines.push(String::new());
    }
    lines.extend(render_slo_run_config_lines(&result.run_config));
    lines.push(String::new());

    for phase in &result.phases {
        lines.push(format!(
            "- {}: {} events in {}ms ({:.2} events/s)",
            phase.name, phase.events_sent, phase.elapsed_ms, phase.events_per_second
        ));
    }
    for readiness in &result.readiness {
        lines.push(format!("- {}: {}ms", readiness.name, readiness.elapsed_ms));
    }
    if let Some(checkpoint) = &result.checkpoint_readiness {
        lines.push(format!("- checkpoint samples: {}", checkpoint.sample_count));
        lines.push(format!(
            "- checkpoint event p95: {}ms",
            checkpoint.event_metrics_ready_p95_ms
        ));
        lines.push(format!(
            "- checkpoint session p95: {}ms",
            checkpoint.session_metrics_ready_p95_ms
        ));
        lines.push(format!(
            "- checkpoint derived p95: {}ms",
            checkpoint.derived_metrics_ready_p95_ms
        ));
    }
    if let Some(failure) = &result.failure {
        lines.push(format!("- Failure: {}", failure));
    }
    if let Some(budget) = &result.budget {
        lines.push(format!(
            "- Budget: {}",
            if budget.passed { "PASS" } else { "FAIL" }
        ));
        for failure in &budget.failures {
            lines.push(format!("  - {}", failure));
        }
    }

    if !result.queries.is_empty() {
        lines.push(String::new());
        lines.push("| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |".to_owned());
        lines.push("| --- | ---: | ---: | ---: | ---: |".to_owned());
        for query in &result.queries {
            lines.push(format!(
                "| {} | {} | {} | {} | {} |",
                query.name, query.p50_ms, query.p95_ms, query.min_ms, query.max_ms
            ));
        }
    }

    lines.join("\n")
}

fn render_slo_run_config_lines(run_config: &SloRunConfig) -> Vec<String> {
    vec![
        "- Run config:".to_owned(),
        format!("  - mode: {}", run_config.mode.key()),
        format!(
            "  - publication_repetitions: {}",
            run_config.publication_repetitions
        ),
        format!(
            "  - worker_session_batch_size: {}",
            run_config.worker_config.session_batch_size
        ),
        format!(
            "  - worker_event_batch_size: {}",
            run_config.worker_config.event_batch_size
        ),
        format!(
            "  - worker_session_incremental_concurrency: {}",
            run_config.worker_config.session_incremental_concurrency
        ),
        format!(
            "  - worker_session_repair_concurrency: {}",
            run_config.worker_config.session_repair_concurrency
        ),
        format!(
            "  - worker_idle_poll_interval_ms: {}",
            run_config.worker_config.idle_poll_interval_ms
        ),
    ]
}

async fn execute_scenario(scenario: Scenario, profile: Profile) -> Result<ScenarioResult> {
    let profile_config = ProfileConfig::for_profile(profile);
    let budget = load_budget(scenario, profile)?;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("build HTTP client")?;

    let mut stack = StackGuard::new(compose_file_path());
    stack.start()?;
    wait_for_health(&client).await?;
    let project = provision_project(&client).await?;

    let mut result = match scenario {
        Scenario::Hot => run_hot_path(&client, &project, profile, profile_config).await?,
        Scenario::Repair => run_repair_path(&client, &project, profile, profile_config).await?,
        Scenario::Scale => run_scale_path(&client, &project, profile, profile_config).await?,
    };
    result.budget = budget
        .as_ref()
        .map(|budget| evaluate_budget(budget, &result));
    Ok(result)
}

fn enforce_budget(result: &ScenarioResult) -> Result<()> {
    if let Some(evaluation) = &result.budget
        && !evaluation.passed
    {
        bail!("benchmark exceeded configured budgets");
    }

    Ok(())
}

async fn run_hot_path(
    client: &Client,
    project: &ProvisionedProject,
    profile: Profile,
    config: ProfileConfig,
) -> Result<ScenarioResult> {
    let events = hot_path_events(config.hot_path_install_count)?;
    let event_count = events.len();
    let batches = chunk_events(events);
    let ingest_started = Instant::now();
    post_batches(client, project, &batches).await?;
    let ingest_elapsed = ingest_started.elapsed();

    let expected = HotPathExpectation {
        total_events: event_count as u64,
        install_count: config.hot_path_install_count as u64,
        total_duration_seconds: (config.hot_path_install_count as u64) * 1_200,
        target_day: NaiveDate::from_ymd_opt(2026, 3, 1).expect("valid date"),
    };
    let derive_started = Instant::now();
    poll_until(
        || hot_path_ready(client, project, &expected),
        config.settle_timeout,
        "hot-path derived metrics readiness",
    )
    .await?;
    let derive_elapsed = derive_started.elapsed();

    let queries = measure_queries(
        client,
        project,
        &hot_path_query_urls(),
        config.warmup_queries,
        config.measured_queries,
    )
    .await?;

    Ok(ScenarioResult {
        scenario: Scenario::Hot,
        profile,
        phases: vec![PhaseMeasurement {
            name: "ingest".to_owned(),
            events_sent: event_count,
            elapsed_ms: ingest_elapsed.as_millis() as u64,
            events_per_second: throughput(event_count, ingest_elapsed),
        }],
        readiness: vec![ReadinessMeasurement {
            name: "derived_metrics_ready".to_owned(),
            elapsed_ms: derive_elapsed.as_millis() as u64,
        }],
        queries,
        budget: None,
    })
}

async fn run_repair_path(
    client: &Client,
    project: &ProvisionedProject,
    profile: Profile,
    config: ProfileConfig,
) -> Result<ScenarioResult> {
    let scenario = repair_scenario(config.repair_group_count)?;

    let baseline_batches = chunk_events(scenario.baseline_events.clone());
    let seed_ingest_started = Instant::now();
    post_batches(client, project, &baseline_batches).await?;
    let seed_ingest_elapsed = seed_ingest_started.elapsed();
    let seed_ready_started = Instant::now();
    poll_until(
        || repair_baseline_ready(client, project, &scenario),
        config.settle_timeout,
        "repair baseline readiness",
    )
    .await?;
    let seed_ready_elapsed = seed_ready_started.elapsed();

    let late_batches = chunk_events(scenario.late_events.clone());
    let ingest_started = Instant::now();
    post_batches(client, project, &late_batches).await?;
    let ingest_elapsed = ingest_started.elapsed();

    let derive_started = Instant::now();
    poll_until(
        || repair_ready(client, project, &scenario),
        config.settle_timeout,
        "repair derived metrics readiness",
    )
    .await?;
    let derive_elapsed = derive_started.elapsed();

    let queries = measure_queries(
        client,
        project,
        &repair_path_query_urls(),
        config.warmup_queries,
        config.measured_queries,
    )
    .await?;

    Ok(ScenarioResult {
        scenario: Scenario::Repair,
        profile,
        phases: vec![
            PhaseMeasurement {
                name: "seed_ingest".to_owned(),
                events_sent: scenario.baseline_total_events,
                elapsed_ms: seed_ingest_elapsed.as_millis() as u64,
                events_per_second: throughput(scenario.baseline_total_events, seed_ingest_elapsed),
            },
            PhaseMeasurement {
                name: "repair_ingest".to_owned(),
                events_sent: scenario.late_total_events,
                elapsed_ms: ingest_elapsed.as_millis() as u64,
                events_per_second: throughput(scenario.late_total_events, ingest_elapsed),
            },
        ],
        readiness: vec![
            ReadinessMeasurement {
                name: "seed_ready".to_owned(),
                elapsed_ms: seed_ready_elapsed.as_millis() as u64,
            },
            ReadinessMeasurement {
                name: "repair_ready".to_owned(),
                elapsed_ms: derive_elapsed.as_millis() as u64,
            },
        ],
        queries,
        budget: None,
    })
}

async fn run_scale_path(
    client: &Client,
    project: &ProvisionedProject,
    profile: Profile,
    config: ProfileConfig,
) -> Result<ScenarioResult> {
    let scenario = scale_scenario(config.scale_day_count, config.scale_install_count_per_day)?;
    let batches = chunk_events(scenario.events.clone());
    let ingest_started = Instant::now();
    post_batches(client, project, &batches).await?;
    let ingest_elapsed = ingest_started.elapsed();

    let derive_started = Instant::now();
    poll_until(
        || scale_path_ready(client, project, &scenario),
        config.settle_timeout,
        "scale-path derived metrics readiness",
    )
    .await?;
    let derive_elapsed = derive_started.elapsed();

    let queries = measure_queries(
        client,
        project,
        &scale_path_query_urls(scenario.start_day, scenario.end_day),
        config.warmup_queries,
        config.measured_queries,
    )
    .await?;

    Ok(ScenarioResult {
        scenario: Scenario::Scale,
        profile,
        phases: vec![PhaseMeasurement {
            name: "ingest".to_owned(),
            events_sent: scenario.total_events,
            elapsed_ms: ingest_elapsed.as_millis() as u64,
            events_per_second: throughput(scenario.total_events, ingest_elapsed),
        }],
        readiness: vec![ReadinessMeasurement {
            name: "derived_metrics_ready".to_owned(),
            elapsed_ms: derive_elapsed.as_millis() as u64,
        }],
        queries,
        budget: None,
    })
}

fn throughput(events_sent: usize, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return events_sent as f64;
    }

    events_sent as f64 / elapsed.as_secs_f64()
}

async fn post_batches(
    client: &Client,
    project: &ProvisionedProject,
    batches: &[Vec<BenchEvent>],
) -> Result<()> {
    for batch in batches {
        post_event_batch(client, project, batch).await?;
    }

    Ok(())
}

async fn post_event_chunks(
    client: &Client,
    project: &ProvisionedProject,
    events: &[BenchEvent],
) -> Result<()> {
    for batch in events.chunks(POST_BATCH_SIZE) {
        post_event_batch(client, project, batch).await?;
    }

    Ok(())
}

async fn post_event_batch(
    client: &Client,
    project: &ProvisionedProject,
    batch: &[BenchEvent],
) -> Result<()> {
    let response = client
        .post(format!("{}/v1/events", benchmark_ingest_base_url()))
        .header("x-fantasma-key", &project.ingest_key)
        .json(&EventBatch { events: batch })
        .send()
        .await
        .context("POST /v1/events")?;

    let status = response.status();
    if status != StatusCode::ACCEPTED {
        let body = response.text().await.unwrap_or_default();
        bail!("ingest returned {}: {}", status, body);
    }

    Ok(())
}

async fn post_upload_blob(
    client: &Client,
    project: &ProvisionedProject,
    blob: &UploadBlob,
) -> Result<()> {
    post_event_batch(client, project, &blob.events).await
}

fn live_slo_schedule(
    scenario: &SloScenarioDefinition,
    run_config: &SloRunConfig,
) -> Result<(Vec<UploadBlob>, Vec<UploadBlob>)> {
    let policy = live_slo_policy(run_config);
    match scenario.kind {
        SloScenarioKind::LiveAppendSmallBlobs => {
            Ok((build_live_append_blobs(policy, false, false)?, Vec::new()))
        }
        SloScenarioKind::LiveAppendOfflineFlush => {
            Ok((build_live_append_blobs(policy, true, false)?, Vec::new()))
        }
        SloScenarioKind::LiveAppendPlusLightRepair => Ok((
            build_live_append_blobs(policy, false, false)?,
            build_live_repair_blobs(policy, false)?,
        )),
        SloScenarioKind::LiveAppendPlusRepairHotProject => Ok((
            build_live_append_blobs(policy, true, true)?,
            build_live_repair_blobs(policy, true)?,
        )),
        SloScenarioKind::BurstReadiness300InstallsX1 => {
            Ok((build_burst_readiness_blobs(300, 1)?, Vec::new()))
        }
        SloScenarioKind::BurstReadiness150InstallsX2 => {
            Ok((build_burst_readiness_blobs(150, 2)?, Vec::new()))
        }
        SloScenarioKind::BurstReadiness100InstallsX3 => {
            Ok((build_burst_readiness_blobs(100, 3)?, Vec::new()))
        }
        _ => bail!(
            "live schedule requested for non-live scenario {}",
            scenario.key()
        ),
    }
}

fn build_live_append_blobs(
    policy: LiveSloPolicy,
    include_offline_flush: bool,
    hot_project_flush: bool,
) -> Result<Vec<UploadBlob>> {
    let mut scheduled = Vec::new();
    let mut append_index = 0usize;
    let mut offline_flush_index = 0usize;

    for day_offset in 0..SLO_REPRESENTATIVE_WINDOW_DAYS {
        let day = slo_start_day() + ChronoDuration::days(day_offset as i64);
        for install_index in 0..policy.install_count {
            let install_uses_offline_flush = include_offline_flush
                && install_index % SLO_REPRESENTATIVE_OFFLINE_FLUSH_INSTALL_MODULUS
                    == day_offset % SLO_REPRESENTATIVE_OFFLINE_FLUSH_INSTALL_MODULUS;

            if install_uses_offline_flush {
                for session_index in 0..2 {
                    append_index += 1;
                    scheduled.push((
                        live_append_sort_key(day_offset, session_index, install_index),
                        UploadBlob {
                            kind: UploadBlobKind::Append,
                            events: live_session_events(
                                day,
                                day_offset,
                                install_index,
                                session_index,
                            )?,
                            delta: SloExpectationDelta {
                                total_events: SLO_REPRESENTATIVE_EVENTS_PER_SESSION as u64,
                                total_sessions: 1,
                                total_duration_seconds: SLO_REPRESENTATIVE_SESSION_DURATION_SECONDS,
                                total_new_installs: if day_offset == 0 && session_index == 0 {
                                    1
                                } else {
                                    0
                                },
                                total_active_installs: if session_index == 0 { 1 } else { 0 },
                                pro_total_events: if slo_event_matches_plan(install_index, "pro") {
                                    SLO_REPRESENTATIVE_EVENTS_PER_SESSION as u64
                                } else {
                                    0
                                },
                                pro_total_sessions: if slo_event_matches_plan(install_index, "pro")
                                {
                                    1
                                } else {
                                    0
                                },
                                pro_total_duration_seconds: if slo_event_matches_plan(
                                    install_index,
                                    "pro",
                                ) {
                                    SLO_REPRESENTATIVE_SESSION_DURATION_SECONDS
                                } else {
                                    0
                                },
                                pro_total_new_installs: if day_offset == 0
                                    && session_index == 0
                                    && slo_event_matches_plan(install_index, "pro")
                                {
                                    1
                                } else {
                                    0
                                },
                            },
                            checkpoint: append_index
                                .is_multiple_of(policy.append_checkpoint_interval),
                        },
                    ));
                }

                let mut events = Vec::new();
                for session_index in 2..SLO_REPRESENTATIVE_SESSIONS_PER_INSTALL_PER_DAY {
                    events.extend(live_session_events(
                        day,
                        day_offset,
                        install_index,
                        session_index,
                    )?);
                }
                offline_flush_index += 1;
                scheduled.push((
                    live_offline_flush_sort_key(day_offset, install_index, hot_project_flush),
                    UploadBlob {
                        kind: UploadBlobKind::OfflineFlush,
                        events,
                        delta: SloExpectationDelta {
                            total_events: (SLO_REPRESENTATIVE_OFFLINE_FLUSH_SESSION_COUNT
                                * SLO_REPRESENTATIVE_EVENTS_PER_SESSION)
                                as u64,
                            total_sessions: SLO_REPRESENTATIVE_OFFLINE_FLUSH_SESSION_COUNT as u64,
                            total_duration_seconds: (SLO_REPRESENTATIVE_OFFLINE_FLUSH_SESSION_COUNT
                                as u64)
                                * SLO_REPRESENTATIVE_SESSION_DURATION_SECONDS,
                            total_new_installs: 0,
                            total_active_installs: 0,
                            pro_total_events: if slo_event_matches_plan(install_index, "pro") {
                                (SLO_REPRESENTATIVE_OFFLINE_FLUSH_SESSION_COUNT
                                    * SLO_REPRESENTATIVE_EVENTS_PER_SESSION)
                                    as u64
                            } else {
                                0
                            },
                            pro_total_sessions: if slo_event_matches_plan(install_index, "pro") {
                                SLO_REPRESENTATIVE_OFFLINE_FLUSH_SESSION_COUNT as u64
                            } else {
                                0
                            },
                            pro_total_duration_seconds: if slo_event_matches_plan(
                                install_index,
                                "pro",
                            ) {
                                (SLO_REPRESENTATIVE_OFFLINE_FLUSH_SESSION_COUNT as u64)
                                    * SLO_REPRESENTATIVE_SESSION_DURATION_SECONDS
                            } else {
                                0
                            },
                            pro_total_new_installs: 0,
                        },
                        checkpoint: offline_flush_index
                            .is_multiple_of(policy.offline_flush_checkpoint_interval),
                    },
                ));
                continue;
            }

            for session_index in 0..SLO_REPRESENTATIVE_SESSIONS_PER_INSTALL_PER_DAY {
                append_index += 1;
                scheduled.push((
                    live_append_sort_key(day_offset, session_index, install_index),
                    UploadBlob {
                        kind: UploadBlobKind::Append,
                        events: live_session_events(day, day_offset, install_index, session_index)?,
                        delta: SloExpectationDelta {
                            total_events: SLO_REPRESENTATIVE_EVENTS_PER_SESSION as u64,
                            total_sessions: 1,
                            total_duration_seconds: SLO_REPRESENTATIVE_SESSION_DURATION_SECONDS,
                            total_new_installs: if day_offset == 0 && session_index == 0 {
                                1
                            } else {
                                0
                            },
                            total_active_installs: if session_index == 0 { 1 } else { 0 },
                            pro_total_events: if slo_event_matches_plan(install_index, "pro") {
                                SLO_REPRESENTATIVE_EVENTS_PER_SESSION as u64
                            } else {
                                0
                            },
                            pro_total_sessions: if slo_event_matches_plan(install_index, "pro") {
                                1
                            } else {
                                0
                            },
                            pro_total_duration_seconds: if slo_event_matches_plan(
                                install_index,
                                "pro",
                            ) {
                                SLO_REPRESENTATIVE_SESSION_DURATION_SECONDS
                            } else {
                                0
                            },
                            pro_total_new_installs: if day_offset == 0
                                && session_index == 0
                                && slo_event_matches_plan(install_index, "pro")
                            {
                                1
                            } else {
                                0
                            },
                        },
                        checkpoint: append_index.is_multiple_of(policy.append_checkpoint_interval),
                    },
                ));
            }
        }
    }

    scheduled.sort_by_key(|(key, _)| *key);
    Ok(scheduled.into_iter().map(|(_, blob)| blob).collect())
}

fn build_burst_readiness_blobs(
    install_count: usize,
    events_per_install: usize,
) -> Result<Vec<UploadBlob>> {
    if events_per_install == 0 {
        bail!("burst readiness events_per_install must be positive");
    }

    let day = slo_start_day();
    let mut blobs = Vec::new();
    let mut current_events = Vec::new();
    let mut current_delta = SloExpectationDelta {
        total_events: 0,
        total_sessions: 0,
        total_duration_seconds: 0,
        total_new_installs: 0,
        total_active_installs: 0,
        pro_total_events: 0,
        pro_total_sessions: 0,
        pro_total_duration_seconds: 0,
        pro_total_new_installs: 0,
    };

    for install_index in 0..install_count {
        let session_start = live_session_start(day, 0, install_index, 0)?;
        let is_pro = slo_event_matches_plan(install_index, "pro");
        let install_delta = SloExpectationDelta {
            total_events: events_per_install as u64,
            total_sessions: 1,
            total_duration_seconds: ((events_per_install - 1) * 60) as u64,
            total_new_installs: 1,
            total_active_installs: 1,
            pro_total_events: if is_pro { events_per_install as u64 } else { 0 },
            pro_total_sessions: if is_pro { 1 } else { 0 },
            pro_total_duration_seconds: if is_pro {
                ((events_per_install - 1) * 60) as u64
            } else {
                0
            },
            pro_total_new_installs: if is_pro { 1 } else { 0 },
        };

        for event_offset in 0..events_per_install {
            if current_events.len() == POST_BATCH_SIZE {
                blobs.push(UploadBlob {
                    kind: UploadBlobKind::Append,
                    events: current_events,
                    delta: current_delta,
                    checkpoint: false,
                });
                current_events = Vec::new();
                current_delta = SloExpectationDelta {
                    total_events: 0,
                    total_sessions: 0,
                    total_duration_seconds: 0,
                    total_new_installs: 0,
                    total_active_installs: 0,
                    pro_total_events: 0,
                    pro_total_sessions: 0,
                    pro_total_duration_seconds: 0,
                    pro_total_new_installs: 0,
                };
            }

            current_events.push(live_slo_event_for_install(
                install_index,
                0,
                0,
                session_start + ChronoDuration::minutes(event_offset as i64),
            ));

            if event_offset + 1 == events_per_install {
                current_delta.total_events += install_delta.total_events;
                current_delta.total_sessions += install_delta.total_sessions;
                current_delta.total_duration_seconds += install_delta.total_duration_seconds;
                current_delta.total_new_installs += install_delta.total_new_installs;
                current_delta.total_active_installs += install_delta.total_active_installs;
                current_delta.pro_total_events += install_delta.pro_total_events;
                current_delta.pro_total_sessions += install_delta.pro_total_sessions;
                current_delta.pro_total_duration_seconds +=
                    install_delta.pro_total_duration_seconds;
                current_delta.pro_total_new_installs += install_delta.pro_total_new_installs;
            }
        }
    }

    if !current_events.is_empty() {
        blobs.push(UploadBlob {
            kind: UploadBlobKind::Append,
            events: current_events,
            delta: current_delta,
            checkpoint: false,
        });
    }

    Ok(blobs)
}

fn build_live_repair_blobs(policy: LiveSloPolicy, hot_project: bool) -> Result<Vec<UploadBlob>> {
    let mut scheduled = Vec::new();
    let mut repair_index = 0usize;

    for day_offset in 0..SLO_REPRESENTATIVE_WINDOW_DAYS {
        let day = slo_start_day() + ChronoDuration::days(day_offset as i64);
        let repair_modulus = if hot_project {
            SLO_REPRESENTATIVE_HOT_PROJECT_REPAIR_INSTALL_MODULUS
        } else {
            SLO_REPRESENTATIVE_LIGHT_REPAIR_INSTALL_MODULUS
        };

        for install_index in 0..policy.install_count {
            if install_index % repair_modulus != day_offset % repair_modulus {
                continue;
            }
            repair_index += 1;

            let session_start = live_session_start(day, day_offset, install_index, 0)?;
            let mut events = Vec::with_capacity(SLO_REPRESENTATIVE_REPAIR_LATE_EVENTS_PER_SESSION);
            for offset_minutes in 0..SLO_REPRESENTATIVE_REPAIR_LATE_EVENTS_PER_SESSION {
                events.push(live_slo_event_for_install(
                    install_index,
                    day_offset,
                    0,
                    session_start - ChronoDuration::minutes((10 - offset_minutes) as i64),
                ));
            }

            scheduled.push((
                live_repair_sort_key(day_offset, install_index, hot_project),
                UploadBlob {
                    kind: UploadBlobKind::Repair,
                    events,
                    delta: SloExpectationDelta {
                        total_events: SLO_REPRESENTATIVE_REPAIR_LATE_EVENTS_PER_SESSION as u64,
                        total_sessions: 0,
                        total_duration_seconds: SLO_REPRESENTATIVE_REPAIR_DURATION_DELTA_SECONDS,
                        total_new_installs: 0,
                        total_active_installs: 0,
                        pro_total_events: if slo_event_matches_plan(install_index, "pro") {
                            SLO_REPRESENTATIVE_REPAIR_LATE_EVENTS_PER_SESSION as u64
                        } else {
                            0
                        },
                        pro_total_sessions: 0,
                        pro_total_duration_seconds: if slo_event_matches_plan(install_index, "pro")
                        {
                            SLO_REPRESENTATIVE_REPAIR_DURATION_DELTA_SECONDS
                        } else {
                            0
                        },
                        pro_total_new_installs: 0,
                    },
                    checkpoint: repair_index.is_multiple_of(policy.repair_checkpoint_interval),
                },
            ));
        }
    }

    scheduled.sort_by_key(|(key, _)| *key);
    Ok(scheduled.into_iter().map(|(_, blob)| blob).collect())
}

fn live_session_events(
    day: NaiveDate,
    day_offset: usize,
    install_index: usize,
    session_index: usize,
) -> Result<Vec<BenchEvent>> {
    let start = live_session_start(day, day_offset, install_index, session_index)?;
    Ok((0..SLO_REPRESENTATIVE_EVENTS_PER_SESSION)
        .map(|event_offset| {
            live_slo_event_for_install(
                install_index,
                day_offset,
                session_index,
                start + ChronoDuration::minutes(event_offset as i64),
            )
        })
        .collect())
}

fn live_session_start(
    day: NaiveDate,
    day_offset: usize,
    install_index: usize,
    session_index: usize,
) -> Result<chrono::DateTime<Utc>> {
    let base_slot = (install_index + (day_offset * 7)) % 4;
    let hour = (base_slot + (session_index * 4)) as u32;
    Utc.with_ymd_and_hms(day.year(), day.month(), day.day(), hour, 10, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid live SLO event timestamp for {}", day))
}

fn live_slo_event_for_install(
    install_index: usize,
    day_offset: usize,
    session_index: usize,
    timestamp: chrono::DateTime<Utc>,
) -> BenchEvent {
    let install_id = format!("live-install-{install_index:04}");
    let platform = if install_index.is_multiple_of(2) {
        "ios"
    } else {
        "android"
    };
    let app_version =
        APP_VERSIONS[(day_offset + install_index + session_index) % APP_VERSIONS.len()];
    let os_version = OS_VERSIONS[(day_offset + install_index) % OS_VERSIONS.len()];
    let provider = PROVIDERS[install_index % PROVIDERS.len()];
    let plan = PLANS[install_index % PLANS.len()];

    BenchEvent {
        event: "app_open".to_owned(),
        timestamp: timestamp.to_rfc3339(),
        install_id,
        platform: platform.to_owned(),
        app_version: app_version.to_owned(),
        os_version: os_version.to_owned(),
        properties: BTreeMap::from([
            ("plan".to_owned(), plan.to_owned()),
            ("provider".to_owned(), provider.to_owned()),
        ]),
    }
}

fn live_append_sort_key(day_offset: usize, session_index: usize, install_index: usize) -> u64 {
    (day_offset as u64) * 1_000_000
        + (session_index as u64) * 10_000
        + ((install_index * 37 + day_offset * 11) % 10_000) as u64
}

fn live_offline_flush_sort_key(
    day_offset: usize,
    install_index: usize,
    hot_project_flush: bool,
) -> u64 {
    let hot_cluster = if hot_project_flush
        && install_index.is_multiple_of(SLO_REPRESENTATIVE_HOT_PROJECT_INSTALL_MODULUS)
    {
        80_000
    } else {
        60_000
    };
    (day_offset as u64) * 1_000_000 + hot_cluster + (install_index % 17) as u64
}

fn live_repair_sort_key(day_offset: usize, install_index: usize, hot_project: bool) -> u64 {
    let cluster = if hot_project
        && install_index.is_multiple_of(SLO_REPRESENTATIVE_HOT_PROJECT_INSTALL_MODULUS)
    {
        95_000
    } else {
        90_000
    };
    (day_offset as u64) * 1_000_000 + cluster + (install_index % 29) as u64
}

async fn provision_project(client: &Client) -> Result<ProvisionedProject> {
    let create_project_response = client
        .post(format!("{}/v1/projects", benchmark_api_base_url()))
        .bearer_auth(benchmark_admin_token())
        .json(&serde_json::json!({
            "name": BENCHMARK_PROJECT_NAME,
            "ingest_key_name": "bench-ingest"
        }))
        .send()
        .await
        .context("POST /v1/projects")?;
    let status = create_project_response.status();
    if status != StatusCode::CREATED {
        let body = create_project_response.text().await.unwrap_or_default();
        bail!("POST /v1/projects returned {}: {}", status, body);
    }

    let created_project = create_project_response
        .json::<CreatedProjectResponse>()
        .await
        .context("decode project provisioning response")?;

    let create_read_key_response = client
        .post(format!(
            "{}/v1/projects/{}/keys",
            benchmark_api_base_url(),
            created_project.project.id
        ))
        .bearer_auth(benchmark_admin_token())
        .json(&serde_json::json!({
            "name": "bench-read",
            "kind": "read"
        }))
        .send()
        .await
        .context("POST /v1/projects/{project_id}/keys")?;
    let status = create_read_key_response.status();
    if status != StatusCode::CREATED {
        let body = create_read_key_response.text().await.unwrap_or_default();
        bail!(
            "POST /v1/projects/{{project_id}}/keys returned {}: {}",
            status,
            body
        );
    }

    let created_read_key = create_read_key_response
        .json::<CreatedKeyResponse>()
        .await
        .context("decode read-key provisioning response")?;

    Ok(ProvisionedProject {
        project_id: created_project.project.id,
        ingest_key: created_project.ingest_key.secret,
        read_key: created_read_key.key.secret,
    })
}

async fn wait_for_health(client: &Client) -> Result<()> {
    poll_until(
        || async {
            let ingest = client
                .get(format!("{}/health", benchmark_ingest_base_url()))
                .send()
                .await
                .ok()
                .is_some_and(|response| response.status() == StatusCode::OK);
            let api = client
                .get(format!("{}/health", benchmark_api_base_url()))
                .send()
                .await
                .ok()
                .is_some_and(|response| response.status() == StatusCode::OK);
            Ok(ingest && api)
        },
        HEALTH_TIMEOUT,
        "stack health",
    )
    .await
}

async fn connect_benchmark_database(compose_file: &Path) -> Result<PgPool> {
    let database_url = benchmark_database_url(compose_file)?;
    let connect_started_at = Instant::now();

    loop {
        match PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(error) if connect_started_at.elapsed() < HEALTH_TIMEOUT => {
                let _ = error;
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("connect benchmark postgres at {}", database_url));
            }
        }
    }
}

fn benchmark_database_url(compose_file: &Path) -> Result<String> {
    let output = docker_compose_command(compose_file, ["port", "postgres", "5432"])
        .output()
        .context("resolve benchmark postgres host port")?;
    if !output.status.success() {
        bail!(
            "resolve benchmark postgres host port failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let port = parse_docker_compose_port_output(&String::from_utf8_lossy(&output.stdout))?;
    Ok(format!(
        "postgres://fantasma:fantasma@127.0.0.1:{port}/fantasma"
    ))
}

fn parse_docker_compose_port_output(output: &str) -> Result<u16> {
    let trimmed = output.trim();
    let port = trimmed
        .rsplit(':')
        .next()
        .ok_or_else(|| anyhow!("missing port in docker compose port output: {trimmed}"))?;
    port.parse::<u16>()
        .with_context(|| format!("parse docker compose port output: {trimmed}"))
}

async fn load_project_raw_event_tail_id(pool: &PgPool, project_id: Uuid) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, Option<i64>>(
        r#"
            SELECT MAX(id)
            FROM events_raw
            WHERE project_id = $1
            "#,
    )
    .bind(project_id)
    .fetch_one(pool)
    .await?
    .unwrap_or(0))
}

async fn load_benchmark_worker_offset(pool: &PgPool, worker_name: &str) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, Option<i64>>(
        r#"
            SELECT last_processed_event_id
            FROM worker_offsets
            WHERE worker_name = $1
            "#,
    )
    .bind(worker_name)
    .fetch_optional(pool)
    .await?
    .flatten()
    .unwrap_or(0))
}

async fn poll_until<F, Fut>(mut check: F, timeout: Duration, label: &str) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let deadline = Instant::now() + timeout;

    loop {
        if check().await? {
            return Ok(());
        }

        if Instant::now() >= deadline {
            bail!("timed out waiting for {}", label);
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[derive(Debug)]
struct HotPathExpectation {
    total_events: u64,
    install_count: u64,
    total_duration_seconds: u64,
    target_day: NaiveDate,
}

async fn hot_path_ready(
    client: &Client,
    project: &ProvisionedProject,
    expected: &HotPathExpectation,
) -> Result<bool> {
    let query_urls = hot_path_query_urls();
    let events_day = fetch_json(client, project, &query_urls["events_day_dim2"]).await?;
    let events_hour = fetch_json(client, project, &query_urls["events_hour_dim2"]).await?;
    let sessions_count = fetch_json(client, project, &query_urls["sessions_count_day"]).await?;
    let sessions_duration =
        fetch_json(client, project, &query_urls["sessions_duration_total_day"]).await?;

    let event_day_total = sum_metric_series_for_day(&events_day["series"], expected.target_day);
    let event_hour_total = sum_metric_series(&events_hour["series"]);
    let session_count = sum_metric_series_for_day(&sessions_count["series"], expected.target_day);
    let session_duration =
        sum_metric_series_for_day(&sessions_duration["series"], expected.target_day);

    Ok(event_day_total == expected.total_events
        && event_hour_total == expected.total_events
        && session_count == expected.install_count
        && session_duration == expected.total_duration_seconds)
}

async fn repair_baseline_ready(
    client: &Client,
    project: &ProvisionedProject,
    scenario: &RepairScenario,
) -> Result<bool> {
    let query_urls = repair_path_query_urls();
    let events_day = fetch_json(client, project, &query_urls["events_day_dim2"]).await?;
    let events_hour = fetch_json(client, project, &query_urls["events_hour_dim2"]).await?;
    let sessions_count = fetch_json(client, project, &query_urls["sessions_count_day"]).await?;
    let sessions_duration =
        fetch_json(client, project, &query_urls["sessions_duration_total_day"]).await?;
    let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");
    let day_2 = NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid date");
    let day_3 = NaiveDate::from_ymd_opt(2026, 1, 3).expect("valid date");

    Ok(sum_metric_series_for_day(&events_day["series"], day_1)
        + sum_metric_series_for_day(&events_day["series"], day_2)
        + sum_metric_series_for_day(&events_day["series"], day_3)
        == scenario.baseline_total_events as u64
        && sum_metric_series(&events_hour["series"]) == scenario.baseline_total_events as u64
        && sum_metric_series_for_day(&sessions_count["series"], day_1)
            == (scenario.group_count as u64) * 2
        && sum_metric_series_for_day(&sessions_count["series"], day_2)
            == scenario.group_count as u64
        && sum_metric_series_for_day(&sessions_count["series"], day_3)
            == (scenario.group_count as u64) * 2
        && sum_metric_series_for_day(&sessions_duration["series"], day_1) == 0
        && sum_metric_series_for_day(&sessions_duration["series"], day_2) == 0
        && sum_metric_series_for_day(&sessions_duration["series"], day_3) == 0)
}

async fn repair_ready(
    client: &Client,
    project: &ProvisionedProject,
    scenario: &RepairScenario,
) -> Result<bool> {
    let query_urls = repair_path_query_urls();
    let events_day = fetch_json(client, project, &query_urls["events_day_dim2"]).await?;
    let events_hour = fetch_json(client, project, &query_urls["events_hour_dim2"]).await?;
    let sessions_count = fetch_json(client, project, &query_urls["sessions_count_day"]).await?;
    let sessions_duration =
        fetch_json(client, project, &query_urls["sessions_duration_total_day"]).await?;
    let day_1 = NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date");
    let day_2 = NaiveDate::from_ymd_opt(2026, 1, 2).expect("valid date");
    let day_3 = NaiveDate::from_ymd_opt(2026, 1, 3).expect("valid date");

    Ok(sum_metric_series(&events_hour["series"])
        == (scenario.baseline_total_events + scenario.late_total_events) as u64
        && sum_metric_series_for_day(&events_day["series"], day_1)
            == (scenario.group_count as u64) * 3
        && sum_metric_series_for_day(&events_day["series"], day_2) == scenario.group_count as u64
        && sum_metric_series_for_day(&events_day["series"], day_3)
            == (scenario.group_count as u64) * 3
        && sum_metric_series_for_day(&sessions_count["series"], day_1)
            == scenario.group_count as u64
        && sum_metric_series_for_day(&sessions_count["series"], day_2)
            == scenario.group_count as u64
        && sum_metric_series_for_day(&sessions_count["series"], day_3)
            == scenario.group_count as u64
        && sum_metric_series_for_day(&sessions_duration["series"], day_1)
            == (scenario.group_count as u64) * 2_700
        && sum_metric_series_for_day(&sessions_duration["series"], day_2) == 0
        && sum_metric_series_for_day(&sessions_duration["series"], day_3)
            == (scenario.group_count as u64) * 2_700)
}

async fn scale_path_ready(
    client: &Client,
    project: &ProvisionedProject,
    scenario: &ScaleScenario,
) -> Result<bool> {
    let query_urls = scale_path_query_urls(scenario.start_day, scenario.end_day);
    let events_day = fetch_json(client, project, &query_urls["events_day_dim2"]).await?;
    let events_hour = fetch_json(client, project, &query_urls["events_hour_dim2"]).await?;
    let sessions_count = fetch_json(client, project, &query_urls["sessions_count_day"]).await?;
    let sessions_duration =
        fetch_json(client, project, &query_urls["sessions_duration_total_day"]).await?;

    Ok(
        sum_metric_series(&events_day["series"]) == scenario.total_events as u64
            && sum_metric_series(&events_hour["series"]) == scenario.total_events as u64
            && sum_metric_series(&sessions_count["series"]) == scenario.total_install_count as u64
            && sum_metric_series(&sessions_duration["series"]) == scenario.total_duration_seconds,
    )
}

async fn fetch_json(client: &Client, project: &ProvisionedProject, url: &str) -> Result<Value> {
    let response = client
        .get(url)
        .header("x-fantasma-key", &project.read_key)
        .send()
        .await
        .with_context(|| format!("GET {}", url))?;
    let status = response.status();
    if status != StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        bail!("GET {} returned {}: {}", url, status, body);
    }

    response
        .json::<Value>()
        .await
        .with_context(|| format!("decode {}", url))
}

async fn measure_queries(
    client: &Client,
    project: &ProvisionedProject,
    query_urls: &HashMap<&'static str, String>,
    warmup_iterations: usize,
    measured_iterations: usize,
) -> Result<Vec<QueryMeasurement>> {
    let mut queries = Vec::new();

    for (name, url) in query_urls {
        for _ in 0..warmup_iterations {
            fetch_json(client, project, url).await?;
        }

        let mut samples = Vec::with_capacity(measured_iterations);
        for _ in 0..measured_iterations {
            let started = Instant::now();
            fetch_json(client, project, url).await?;
            samples.push(started.elapsed().as_millis() as u64);
        }
        samples.sort_unstable();

        queries.push(QueryMeasurement {
            name: (*name).to_owned(),
            iterations: measured_iterations,
            min_ms: *samples.first().unwrap_or(&0),
            p50_ms: percentile(&samples, 0.50),
            p95_ms: percentile(&samples, 0.95),
            max_ms: *samples.last().unwrap_or(&0),
        });
    }

    queries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(queries)
}

fn percentile(samples: &[u64], percentile: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }

    let rank = ((samples.len() as f64) * percentile).ceil() as usize;
    let index = rank.saturating_sub(1).min(samples.len() - 1);
    samples[index]
}

fn sum_metric_series_for_day(series: &Value, day: NaiveDate) -> u64 {
    series
        .as_array()
        .into_iter()
        .flatten()
        .map(|entry| metric_point_for_day(&entry["points"], day))
        .sum()
}

fn sum_metric_series(series: &Value) -> u64 {
    series
        .as_array()
        .into_iter()
        .flatten()
        .map(|entry| sum_metric_points(&entry["points"]))
        .sum()
}

fn sum_metric_points(points: &Value) -> u64 {
    points
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|point| point["value"].as_u64())
        .sum()
}

fn metric_point_for_day(points: &Value, day: NaiveDate) -> u64 {
    points
        .as_array()
        .into_iter()
        .flatten()
        .find(|point| point["bucket"].as_str() == Some(&day.to_string()))
        .and_then(|point| point["value"].as_u64())
        .unwrap_or_default()
}

fn hot_path_events(install_count: usize) -> Result<Vec<BenchEvent>> {
    let base = Utc
        .with_ymd_and_hms(2026, 3, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid hot-path base timestamp"))?;
    let mut events = Vec::with_capacity(install_count * 3);

    for install_index in 0..install_count {
        let install_id = format!("hot-install-{}", install_index);
        let provider = PROVIDERS[install_index % PROVIDERS.len()];
        let plan = PLANS[install_index % PLANS.len()];
        let app_version = APP_VERSIONS[install_index % APP_VERSIONS.len()];
        let os_version = OS_VERSIONS[install_index % OS_VERSIONS.len()];

        for offset_minutes in [0_i64, 10, 20] {
            events.push(BenchEvent {
                event: "app_open".to_owned(),
                timestamp: (base + ChronoDuration::minutes(offset_minutes)).to_rfc3339(),
                install_id: install_id.clone(),
                platform: "ios".to_owned(),
                app_version: app_version.to_owned(),
                os_version: os_version.to_owned(),
                properties: BTreeMap::from([
                    ("plan".to_owned(), plan.to_owned()),
                    ("provider".to_owned(), provider.to_owned()),
                ]),
            });
        }
    }

    Ok(events)
}

fn repair_scenario(group_count: usize) -> Result<RepairScenario> {
    let mut baseline_events = Vec::with_capacity(group_count * 5);
    let mut late_events = Vec::with_capacity(group_count * 2);

    for group_index in 0..group_count {
        let provider = PROVIDERS[group_index % PROVIDERS.len()];
        let plan = PLANS[group_index % PLANS.len()];
        let app_version = APP_VERSIONS[group_index % APP_VERSIONS.len()];
        let os_version = OS_VERSIONS[group_index % OS_VERSIONS.len()];
        let properties = BTreeMap::from([
            ("plan".to_owned(), plan.to_owned()),
            ("provider".to_owned(), provider.to_owned()),
        ]);

        baseline_events.extend([
            repair_event(
                format!("repair-a-{}", group_index),
                "2026-01-01T00:00:00Z",
                app_version,
                os_version,
                &properties,
            ),
            repair_event(
                format!("repair-a-{}", group_index),
                "2026-01-01T00:45:00Z",
                app_version,
                os_version,
                &properties,
            ),
            repair_event(
                format!("repair-b-{}", group_index),
                "2026-01-02T12:00:00Z",
                app_version,
                os_version,
                &properties,
            ),
            repair_event(
                format!("repair-c-{}", group_index),
                "2026-01-03T00:00:00Z",
                app_version,
                os_version,
                &properties,
            ),
            repair_event(
                format!("repair-c-{}", group_index),
                "2026-01-03T00:45:00Z",
                app_version,
                os_version,
                &properties,
            ),
        ]);
        late_events.extend([
            repair_event(
                format!("repair-a-{}", group_index),
                "2026-01-01T00:20:00Z",
                app_version,
                os_version,
                &properties,
            ),
            repair_event(
                format!("repair-c-{}", group_index),
                "2026-01-03T00:20:00Z",
                app_version,
                os_version,
                &properties,
            ),
        ]);
    }

    Ok(RepairScenario {
        baseline_total_events: baseline_events.len(),
        late_total_events: late_events.len(),
        baseline_events,
        late_events,
        group_count,
    })
}

fn scale_scenario(day_count: usize, install_count_per_day: usize) -> Result<ScaleScenario> {
    let start_day = NaiveDate::from_ymd_opt(2026, 4, 1).ok_or_else(|| anyhow!("invalid date"))?;
    let mut events = Vec::with_capacity(day_count * install_count_per_day * 3);

    for day_offset in 0..day_count {
        let day = start_day + ChronoDuration::days(day_offset as i64);
        let base = Utc
            .with_ymd_and_hms(day.year(), day.month(), day.day(), 0, 0, 0)
            .single()
            .ok_or_else(|| anyhow!("invalid scale-path timestamp"))?;

        for install_index in 0..install_count_per_day {
            let install_id = format!("scale-{}-{}", day.format("%Y%m%d"), install_index);
            let provider = PROVIDERS[install_index % PROVIDERS.len()];
            let plan = PLANS[install_index % PLANS.len()];
            let app_version = APP_VERSIONS[(day_offset + install_index) % APP_VERSIONS.len()];
            let os_version = OS_VERSIONS[(day_offset + install_index) % OS_VERSIONS.len()];

            for offset_minutes in [0_i64, 10, 20] {
                events.push(BenchEvent {
                    event: "app_open".to_owned(),
                    timestamp: (base + ChronoDuration::minutes(offset_minutes)).to_rfc3339(),
                    install_id: install_id.clone(),
                    platform: "ios".to_owned(),
                    app_version: app_version.to_owned(),
                    os_version: os_version.to_owned(),
                    properties: BTreeMap::from([
                        ("plan".to_owned(), plan.to_owned()),
                        ("provider".to_owned(), provider.to_owned()),
                    ]),
                });
            }
        }
    }

    Ok(ScaleScenario {
        total_events: events.len(),
        total_install_count: day_count * install_count_per_day,
        total_duration_seconds: (day_count * install_count_per_day) as u64 * 1_200,
        end_day: start_day + ChronoDuration::days(day_count.saturating_sub(1) as i64),
        events,
        start_day,
    })
}

fn repair_event(
    install_id: String,
    timestamp: &str,
    app_version: &str,
    os_version: &str,
    properties: &BTreeMap<String, String>,
) -> BenchEvent {
    BenchEvent {
        event: "app_open".to_owned(),
        timestamp: timestamp.to_owned(),
        install_id,
        platform: "ios".to_owned(),
        app_version: app_version.to_owned(),
        os_version: os_version.to_owned(),
        properties: properties.clone(),
    }
}

fn chunk_events(events: Vec<BenchEvent>) -> Vec<Vec<BenchEvent>> {
    events
        .chunks(POST_BATCH_SIZE)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn hot_path_query_urls() -> HashMap<&'static str, String> {
    HashMap::from([
        (
            "events_day_dim2",
            format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-01&group_by=plan&group_by=provider",
                benchmark_api_base_url()
            ),
        ),
        (
            "events_hour_dim2",
            format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=hour&start=2026-03-01T00:00:00Z&end=2026-03-01T23:00:00Z&group_by=plan&group_by=provider",
                benchmark_api_base_url()
            ),
        ),
        (
            "sessions_count_day",
            format!(
                "{}/v1/metrics/sessions?metric=count&granularity=day&start=2026-03-01&end=2026-03-01",
                benchmark_api_base_url()
            ),
        ),
        (
            "sessions_duration_total_day",
            format!(
                "{}/v1/metrics/sessions?metric=duration_total&granularity=day&start=2026-03-01&end=2026-03-01",
                benchmark_api_base_url()
            ),
        ),
    ])
}

fn repair_path_query_urls() -> HashMap<&'static str, String> {
    HashMap::from([
        (
            "events_day_dim2",
            format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-01-01&end=2026-01-03&group_by=plan&group_by=provider",
                benchmark_api_base_url()
            ),
        ),
        (
            "events_hour_dim2",
            format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=hour&start=2026-01-01T00:00:00Z&end=2026-01-03T23:00:00Z&group_by=plan&group_by=provider",
                benchmark_api_base_url()
            ),
        ),
        (
            "sessions_count_day",
            format!(
                "{}/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-03",
                benchmark_api_base_url()
            ),
        ),
        (
            "sessions_duration_total_day",
            format!(
                "{}/v1/metrics/sessions?metric=duration_total&granularity=day&start=2026-01-01&end=2026-01-03",
                benchmark_api_base_url()
            ),
        ),
    ])
}

fn scale_path_query_urls(
    start_day: NaiveDate,
    end_day: NaiveDate,
) -> HashMap<&'static str, String> {
    HashMap::from([
        (
            "events_day_dim2",
            format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=day&start={start_day}&end={end_day}&group_by=plan&group_by=provider",
                benchmark_api_base_url()
            ),
        ),
        (
            "events_hour_dim2",
            format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=hour&start={start_day}T00:00:00Z&end={end_day}T23:00:00Z&group_by=plan&group_by=provider",
                benchmark_api_base_url()
            ),
        ),
        (
            "sessions_count_day",
            format!(
                "{}/v1/metrics/sessions?metric=count&granularity=day&start={start_day}&end={end_day}",
                benchmark_api_base_url()
            ),
        ),
        (
            "sessions_duration_total_day",
            format!(
                "{}/v1/metrics/sessions?metric=duration_total&granularity=day&start={start_day}&end={end_day}",
                benchmark_api_base_url()
            ),
        ),
    ])
}

fn load_budget(scenario: Scenario, profile: Profile) -> Result<Option<ScenarioBudget>> {
    if profile != Profile::Ci {
        return Ok(None);
    }

    let budget_file = fs::read_to_string(budget_file_path()).context("read CI budget file")?;
    let budgets: BudgetFile = serde_json::from_str(&budget_file).context("parse CI budget file")?;

    Ok(Some(match scenario {
        Scenario::Hot => budgets.hot_path,
        Scenario::Repair => budgets.repair_path,
        Scenario::Scale => budgets.scale_path,
    }))
}

fn evaluate_budget(budget: &ScenarioBudget, result: &ScenarioResult) -> BudgetEvaluation {
    let mut failures = Vec::new();

    for (phase_name, minimum) in &budget.min_phase_events_per_second {
        match result.phases.iter().find(|phase| phase.name == *phase_name) {
            Some(phase) if phase.events_per_second < *minimum => failures.push(format!(
                "{} events_per_second {:.2} fell below budget {:.2}",
                phase_name, phase.events_per_second, minimum
            )),
            Some(_) => {}
            None => failures.push(format!("missing phase measurement for {}", phase_name)),
        }
    }

    for (readiness_name, maximum) in &budget.max_readiness_ms {
        match result
            .readiness
            .iter()
            .find(|readiness| readiness.name == *readiness_name)
        {
            Some(readiness) if readiness.elapsed_ms > *maximum => failures.push(format!(
                "{} readiness {}ms exceeded budget {}ms",
                readiness_name, readiness.elapsed_ms, maximum
            )),
            Some(_) => {}
            None => failures.push(format!(
                "missing readiness measurement for {}",
                readiness_name
            )),
        }
    }

    for (query_name, maximum) in &budget.max_query_p95_ms {
        match result
            .queries
            .iter()
            .find(|query| query.name == *query_name)
        {
            Some(query) if query.p95_ms > *maximum => failures.push(format!(
                "{} p95 {}ms exceeded budget {}ms",
                query_name, query.p95_ms, maximum
            )),
            Some(_) => {}
            None => failures.push(format!("missing query measurement for {}", query_name)),
        }
    }

    BudgetEvaluation {
        passed: failures.is_empty(),
        failures,
    }
}

#[derive(Debug, Clone)]
struct ScenarioSeriesPaths {
    scenario_dir: PathBuf,
}

impl ScenarioSeriesPaths {
    fn run_output(&self, run_number: usize) -> PathBuf {
        self.scenario_dir.join(format!("run-{run_number:02}.json"))
    }

    fn median_output(&self) -> PathBuf {
        self.scenario_dir.join("median.json")
    }
}

fn series_paths(output_dir: &Path, scenario: Scenario) -> ScenarioSeriesPaths {
    ScenarioSeriesPaths {
        scenario_dir: output_dir.join(scenario.key()),
    }
}

fn summary_output_path(output_dir: &Path) -> PathBuf {
    output_dir.join("summary.json")
}

fn host_metadata_output_path(output_dir: &Path) -> PathBuf {
    output_dir.join("host.json")
}

fn aggregate_scenario_runs(
    scenario: Scenario,
    profile: Profile,
    runs: &[ScenarioResult],
) -> Result<ScenarioResult> {
    let first = runs
        .first()
        .ok_or_else(|| anyhow!("cannot aggregate empty benchmark run list"))?;

    for run in runs {
        if run.scenario != scenario {
            bail!(
                "scenario mismatch while aggregating {}: expected {}, got {}",
                scenario.key(),
                scenario.key(),
                run.scenario.key()
            );
        }
        if run.profile != profile {
            bail!(
                "profile mismatch while aggregating {}: expected {}, got {}",
                scenario.key(),
                profile.key(),
                run.profile.key()
            );
        }
    }

    let phases = first
        .phases
        .iter()
        .map(|phase| {
            let mut elapsed_samples = Vec::with_capacity(runs.len());
            let mut throughput_samples = Vec::with_capacity(runs.len());

            for run in runs {
                let candidate = run
                    .phases
                    .iter()
                    .find(|measurement| measurement.name == phase.name)
                    .ok_or_else(|| anyhow!("missing phase measurement for {}", phase.name))?;
                if candidate.events_sent != phase.events_sent {
                    bail!("phase {} changed events_sent across runs", phase.name);
                }
                elapsed_samples.push(candidate.elapsed_ms);
                throughput_samples.push(candidate.events_per_second);
            }

            Ok(PhaseMeasurement {
                name: phase.name.clone(),
                events_sent: phase.events_sent,
                elapsed_ms: median_u64(&elapsed_samples),
                events_per_second: median_f64(&throughput_samples),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let readiness = first
        .readiness
        .iter()
        .map(|measurement| {
            let mut samples = Vec::with_capacity(runs.len());
            for run in runs {
                let candidate = run
                    .readiness
                    .iter()
                    .find(|entry| entry.name == measurement.name)
                    .ok_or_else(|| {
                        anyhow!("missing readiness measurement for {}", measurement.name)
                    })?;
                samples.push(candidate.elapsed_ms);
            }

            Ok(ReadinessMeasurement {
                name: measurement.name.clone(),
                elapsed_ms: median_u64(&samples),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let queries = first
        .queries
        .iter()
        .map(|query| {
            let mut min_samples = Vec::with_capacity(runs.len());
            let mut p50_samples = Vec::with_capacity(runs.len());
            let mut p95_samples = Vec::with_capacity(runs.len());
            let mut max_samples = Vec::with_capacity(runs.len());

            for run in runs {
                let candidate = run
                    .queries
                    .iter()
                    .find(|entry| entry.name == query.name)
                    .ok_or_else(|| anyhow!("missing query measurement for {}", query.name))?;
                if candidate.iterations != query.iterations {
                    bail!("query {} changed iteration count across runs", query.name);
                }
                min_samples.push(candidate.min_ms);
                p50_samples.push(candidate.p50_ms);
                p95_samples.push(candidate.p95_ms);
                max_samples.push(candidate.max_ms);
            }

            Ok(QueryMeasurement {
                name: query.name.clone(),
                iterations: query.iterations,
                min_ms: median_u64(&min_samples),
                p50_ms: median_u64(&p50_samples),
                p95_ms: median_u64(&p95_samples),
                max_ms: median_u64(&max_samples),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ScenarioResult {
        scenario,
        profile,
        phases,
        readiness,
        queries,
        budget: None,
    })
}

fn median_u64(samples: &[u64]) -> u64 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

fn median_f64(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    sorted[sorted.len() / 2]
}

fn write_result(output: &Path, result: &ScenarioResult) -> Result<()> {
    write_pretty_json(output, result)?;
    write_markdown(output.with_extension("md"), render_markdown_summary(result))?;

    Ok(())
}

fn write_series_summary(output_dir: &Path, summary: &SeriesSummary) -> Result<()> {
    let output = summary_output_path(output_dir);
    write_pretty_json(&output, summary)?;
    write_markdown(
        output.with_extension("md"),
        render_series_markdown_summary(summary),
    )?;
    Ok(())
}

fn write_pretty_json<T: Serialize>(output: &Path, value: &T) -> Result<()> {
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }

    fs::write(
        output,
        serde_json::to_string_pretty(value)
            .with_context(|| format!("serialize {}", output.display()))?,
    )
    .with_context(|| format!("write {}", output.display()))
}

fn write_markdown(output: PathBuf, markdown: String) -> Result<()> {
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }

    fs::write(&output, markdown).with_context(|| format!("write {}", output.display()))
}

fn write_stage_b_sidecars(
    scenario_dir: &Path,
    scenario: &str,
    run_config: &SloRunConfig,
    trace_contents: &str,
) -> Result<bool> {
    let records = parse_stage_b_trace_records(trace_contents)?;
    if records.is_empty() {
        return Ok(false);
    }

    fs::create_dir_all(scenario_dir)
        .with_context(|| format!("create scenario directory {}", scenario_dir.display()))?;
    fs::write(stage_b_trace_output_path(scenario_dir), trace_contents).with_context(|| {
        format!(
            "write {}",
            stage_b_trace_output_path(scenario_dir).display()
        )
    })?;
    let summary = summarize_stage_b_trace_records(scenario, run_config, &records);
    write_pretty_json(&stage_b_summary_output_path(scenario_dir), &summary)?;
    Ok(true)
}

fn parse_stage_b_trace_records(trace_contents: &str) -> Result<Vec<StageBTraceRecord>> {
    trace_contents
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return None;
            }

            Some(
                serde_json::from_str(trimmed)
                    .with_context(|| format!("parse Stage B trace record at line {}", index + 1)),
            )
        })
        .collect()
}

fn summarize_stage_b_trace_records(
    scenario: &str,
    run_config: &SloRunConfig,
    records: &[StageBTraceRecord],
) -> StageBSummary {
    let mut active_phase_totals = BTreeMap::<String, u64>::new();
    let mut active_phase_samples = BTreeMap::<String, Vec<u64>>::new();
    let mut finalization_subphase_totals = BTreeMap::<String, u64>::new();
    let mut finalization_subphase_samples = BTreeMap::<String, Vec<u64>>::new();
    let mut session_daily_subphase_totals = BTreeMap::<String, u64>::new();
    let mut session_daily_subphase_samples = BTreeMap::<String, Vec<u64>>::new();
    let mut backlog_totals = BTreeMap::<String, u64>::new();
    let mut backlog_samples = BTreeMap::<String, Vec<u64>>::new();
    let mut total_counts = BTreeMap::<String, u64>::new();
    let mut record_type_counts = BTreeMap::<String, usize>::new();
    let mut slowest_records_by_type = BTreeMap::<String, Vec<StageBSlowRecord>>::new();

    for record in records {
        *record_type_counts
            .entry(record.record_type.clone())
            .or_default() += 1;

        let total_ms = record.phases_ms.values().copied().sum();
        slowest_records_by_type
            .entry(record.record_type.clone())
            .or_default()
            .push(StageBSlowRecord {
                record_type: record.record_type.clone(),
                install_id: record.install_id.clone(),
                target_event_id: record.target_event_id,
                total_ms,
                phases_ms: record.phases_ms.clone(),
                finalization_subphases_ms: record.finalization_subphases_ms.clone(),
                session_daily_subphases_ms: record.session_daily_subphases_ms.clone(),
                counts: record.counts.clone(),
            });

        for (phase, elapsed_ms) in &record.phases_ms {
            if is_stage_b_backlog_context(phase) {
                *backlog_totals.entry(phase.clone()).or_default() += *elapsed_ms;
                backlog_samples
                    .entry(phase.clone())
                    .or_default()
                    .push(*elapsed_ms);
            } else {
                *active_phase_totals.entry(phase.clone()).or_default() += *elapsed_ms;
                active_phase_samples
                    .entry(phase.clone())
                    .or_default()
                    .push(*elapsed_ms);
            }
        }

        for (phase, elapsed_ms) in &record.finalization_subphases_ms {
            *finalization_subphase_totals
                .entry(phase.clone())
                .or_default() += *elapsed_ms;
            finalization_subphase_samples
                .entry(phase.clone())
                .or_default()
                .push(*elapsed_ms);
        }

        for (phase, elapsed_ms) in &record.session_daily_subphases_ms {
            *session_daily_subphase_totals
                .entry(phase.clone())
                .or_default() += *elapsed_ms;
            session_daily_subphase_samples
                .entry(phase.clone())
                .or_default()
                .push(*elapsed_ms);
        }

        for (count_name, count) in &record.counts {
            *total_counts.entry(count_name.clone()).or_default() += *count;
        }
    }

    for slowest_records in slowest_records_by_type.values_mut() {
        slowest_records.sort_by(|left, right| {
            right
                .total_ms
                .cmp(&left.total_ms)
                .then_with(|| left.install_id.cmp(&right.install_id))
                .then_with(|| left.target_event_id.cmp(&right.target_event_id))
        });
        slowest_records.truncate(STAGE_B_SLOW_RECORD_LIMIT);
    }

    let measured_stage_b_ms = active_phase_totals.values().copied().sum();
    let mut phases = active_phase_totals
        .into_iter()
        .map(|(name, total_ms)| {
            let mut samples = active_phase_samples
                .remove(&name)
                .expect("phase samples tracked alongside totals");
            samples.sort_unstable();

            StageBPhaseSummary {
                name,
                total_ms,
                share: if measured_stage_b_ms == 0 {
                    0.0
                } else {
                    total_ms as f64 / measured_stage_b_ms as f64
                },
                distribution: StageBDistributionStats {
                    count: samples.len(),
                    min_ms: *samples.first().unwrap_or(&0),
                    p50_ms: percentile(&samples, 0.50),
                    p95_ms: percentile(&samples, 0.95),
                    max_ms: *samples.last().unwrap_or(&0),
                },
            }
        })
        .collect::<Vec<_>>();
    phases.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| left.name.cmp(&right.name))
    });
    let mut backlog_context = backlog_totals
        .into_iter()
        .map(|(name, total_ms)| {
            let mut samples = backlog_samples
                .remove(&name)
                .expect("backlog samples tracked alongside totals");
            samples.sort_unstable();

            StageBContextSummary {
                name,
                total_ms,
                distribution: StageBDistributionStats {
                    count: samples.len(),
                    min_ms: *samples.first().unwrap_or(&0),
                    p50_ms: percentile(&samples, 0.50),
                    p95_ms: percentile(&samples, 0.95),
                    max_ms: *samples.last().unwrap_or(&0),
                },
            }
        })
        .collect::<Vec<_>>();
    backlog_context.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| left.name.cmp(&right.name))
    });
    let dominant_phase = dominant_stage_b_phase(&phases);
    let measured_finalization_ms: u64 = finalization_subphase_totals.values().copied().sum();
    let mut finalization_subphases = finalization_subphase_totals
        .into_iter()
        .map(|(name, total_ms)| {
            let mut samples = finalization_subphase_samples
                .remove(&name)
                .expect("finalization subphase samples tracked alongside totals");
            samples.sort_unstable();

            StageBPhaseSummary {
                name,
                total_ms,
                share: if measured_finalization_ms == 0 {
                    0.0
                } else {
                    total_ms as f64 / measured_finalization_ms as f64
                },
                distribution: StageBDistributionStats {
                    count: samples.len(),
                    min_ms: *samples.first().unwrap_or(&0),
                    p50_ms: percentile(&samples, 0.50),
                    p95_ms: percentile(&samples, 0.95),
                    max_ms: *samples.last().unwrap_or(&0),
                },
            }
        })
        .collect::<Vec<_>>();
    finalization_subphases.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| left.name.cmp(&right.name))
    });
    let dominant_finalization_subphase = dominant_stage_b_phase(&finalization_subphases);
    let measured_session_daily_ms: u64 = session_daily_subphase_totals.values().copied().sum();
    let mut session_daily_subphases = session_daily_subphase_totals
        .into_iter()
        .map(|(name, total_ms)| {
            let mut samples = session_daily_subphase_samples
                .remove(&name)
                .expect("session_daily subphase samples tracked alongside totals");
            samples.sort_unstable();

            StageBPhaseSummary {
                name,
                total_ms,
                share: if measured_session_daily_ms == 0 {
                    0.0
                } else {
                    total_ms as f64 / measured_session_daily_ms as f64
                },
                distribution: StageBDistributionStats {
                    count: samples.len(),
                    min_ms: *samples.first().unwrap_or(&0),
                    p50_ms: percentile(&samples, 0.50),
                    p95_ms: percentile(&samples, 0.95),
                    max_ms: *samples.last().unwrap_or(&0),
                },
            }
        })
        .collect::<Vec<_>>();
    session_daily_subphases.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| left.name.cmp(&right.name))
    });
    let dominant_session_daily_subphase = dominant_stage_b_phase(&session_daily_subphases);

    StageBSummary {
        scenario: scenario.to_owned(),
        run_config: run_config.clone(),
        record_count: records.len(),
        measured_stage_b_ms,
        record_type_counts,
        phases,
        finalization_subphases,
        session_daily_subphases,
        backlog_context,
        total_counts,
        slowest_records_by_type,
        dominant_phase,
        dominant_finalization_subphase,
        dominant_session_daily_subphase,
    }
}

fn is_stage_b_backlog_context(name: &str) -> bool {
    STAGE_B_QUEUE_LAG_CONTEXT_NAMES.contains(&name)
}

fn dominant_stage_b_phase(phases: &[StageBPhaseSummary]) -> Option<String> {
    let largest = phases.first()?;
    let second_share = phases.get(1).map_or(0.0, |phase| phase.share);

    if largest.share >= STAGE_B_DOMINANT_MIN_SHARE
        && (largest.share - second_share) >= STAGE_B_DOMINANT_MIN_LEAD
    {
        Some(largest.name.clone())
    } else {
        None
    }
}

fn stage_b_trace_output_path(scenario_dir: &Path) -> PathBuf {
    scenario_dir.join(STAGE_B_TRACE_FILENAME)
}

fn stage_b_summary_output_path(scenario_dir: &Path) -> PathBuf {
    scenario_dir.join(STAGE_B_SUMMARY_FILENAME)
}

fn event_lane_trace_output_path(scenario_dir: &Path) -> PathBuf {
    scenario_dir.join(EVENT_LANE_TRACE_FILENAME)
}

fn copy_stage_b_trace_from_worker(compose_file: &Path) -> Result<Option<String>> {
    let local_copy = std::env::temp_dir().join(format!(
        "fantasma-stage-b-trace-{}-{}.jsonl",
        std::process::id(),
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
    ));
    let output = stage_b_trace_copy_command(compose_file, &local_copy)
        .output()
        .context("copy Stage B trace from worker container")?;

    if !output.status.success() {
        let _ = fs::remove_file(&local_copy);
        return Ok(None);
    }

    let trace_contents = fs::read_to_string(&local_copy)
        .with_context(|| format!("read copied Stage B trace {}", local_copy.display()))?;
    let _ = fs::remove_file(&local_copy);

    if trace_contents.trim().is_empty() {
        return Ok(None);
    }

    Ok(Some(trace_contents))
}

fn stage_b_trace_copy_command(compose_file: &Path, destination: &Path) -> Command {
    let source = format!("fantasma-worker:{}", WORKER_STAGE_B_TRACE_CONTAINER_PATH);
    docker_compose_command(
        compose_file,
        [
            OsString::from("cp"),
            OsString::from(source),
            destination.as_os_str().to_os_string(),
        ],
    )
}

fn copy_event_lane_trace_from_worker(compose_file: &Path) -> Result<Option<String>> {
    let local_copy = std::env::temp_dir().join(format!(
        "fantasma-event-lane-trace-{}-{}.jsonl",
        std::process::id(),
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
    ));
    let output = event_lane_trace_copy_command(compose_file, &local_copy)
        .output()
        .context("copy event lane trace from worker container")?;

    if !output.status.success() {
        let _ = fs::remove_file(&local_copy);
        return Ok(None);
    }

    let trace_contents = fs::read_to_string(&local_copy)
        .with_context(|| format!("read copied event lane trace {}", local_copy.display()))?;
    let _ = fs::remove_file(&local_copy);

    if trace_contents.trim().is_empty() {
        return Ok(None);
    }

    Ok(Some(trace_contents))
}

fn event_lane_trace_copy_command(compose_file: &Path, destination: &Path) -> Command {
    let source = format!("fantasma-worker:{}", WORKER_EVENT_LANE_TRACE_CONTAINER_PATH);
    docker_compose_command(
        compose_file,
        [
            OsString::from("cp"),
            OsString::from(source),
            destination.as_os_str().to_os_string(),
        ],
    )
}

fn render_markdown_summary(result: &ScenarioResult) -> String {
    let mut lines = vec![
        format!(
            "# Fantasma Benchmark: {} ({})",
            result.scenario.key(),
            result.profile.key()
        ),
        String::new(),
    ];

    for phase in &result.phases {
        lines.push(format!(
            "- {}: {} events in {}ms ({:.2} events/s)",
            phase.name, phase.events_sent, phase.elapsed_ms, phase.events_per_second
        ));
    }
    for readiness in &result.readiness {
        lines.push(format!("- {}: {}ms", readiness.name, readiness.elapsed_ms));
    }
    if let Some(budget) = &result.budget {
        lines.push(format!(
            "- Budget status: {}",
            if budget.passed { "PASS" } else { "FAIL" }
        ));
        for failure in &budget.failures {
            lines.push(format!("  - {}", failure));
        }
    }
    lines.push(String::new());
    lines.push("| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |".to_owned());
    lines.push("| --- | ---: | ---: | ---: | ---: |".to_owned());
    for query in &result.queries {
        lines.push(format!(
            "| {} | {} | {} | {} | {} |",
            query.name, query.p50_ms, query.p95_ms, query.min_ms, query.max_ms
        ));
    }

    lines.join("\n")
}

fn render_series_markdown_summary(summary: &SeriesSummary) -> String {
    let mut lines = vec![
        format!("# Fantasma {} Benchmark Series", summary.profile.title()),
        String::new(),
        format!("- Profile: {}", summary.profile.key()),
        format!("- Repetitions per scenario: {}", summary.repetitions),
        format!("- Environment: {}", summary.host.environment),
        String::new(),
    ];

    for scenario in &summary.scenarios {
        lines.push(format!("## {}", scenario.scenario.key()));
        lines.push(String::new());

        for phase in &scenario.phases {
            lines.push(format!(
                "- {}: {} events in {}ms ({:.2} events/s)",
                phase.name, phase.events_sent, phase.elapsed_ms, phase.events_per_second
            ));
        }
        for readiness in &scenario.readiness {
            lines.push(format!("- {}: {}ms", readiness.name, readiness.elapsed_ms));
        }
        lines.push(String::new());
        lines.push("| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |".to_owned());
        lines.push("| --- | ---: | ---: | ---: | ---: |".to_owned());
        for query in &scenario.queries {
            lines.push(format!(
                "| {} | {} | {} | {} | {} |",
                query.name, query.p50_ms, query.p95_ms, query.min_ms, query.max_ms
            ));
        }
        lines.push(String::new());
    }

    lines.join("\n").trim_end().to_owned()
}

fn collect_host_metadata() -> Result<HostMetadata> {
    Ok(HostMetadata {
        environment: BENCHMARK_ENVIRONMENT_LABEL.to_owned(),
    })
}

fn compose_file_path() -> PathBuf {
    repo_root().join("infra/docker/compose.bench.yaml")
}

fn render_slo_compose_file(run_config: &SloRunConfig) -> Result<PathBuf> {
    let template_path = compose_file_path();
    let template = fs::read_to_string(&template_path).with_context(|| {
        format!(
            "read benchmark compose template {}",
            template_path.display()
        )
    })?;
    let rendered = render_benchmark_compose_template(&template, &run_config.worker_config);
    let output = template_path
        .parent()
        .expect("benchmark compose template lives in a directory")
        .join(format!(
            "fantasma-bench-{}.yaml",
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
        ));
    fs::write(&output, rendered)
        .with_context(|| format!("write rendered benchmark compose file {}", output.display()))?;
    Ok(output)
}

fn render_benchmark_compose_template(template: &str, worker_config: &BenchWorkerConfig) -> String {
    let mut rendered_lines = Vec::new();
    let mut trace_path_rendered = false;
    let mut event_lane_trace_path_rendered = false;

    for line in template.lines() {
        if line
            .trim_start()
            .starts_with("FANTASMA_WORKER_IDLE_POLL_INTERVAL_MS:")
        {
            rendered_lines.push(format!(
                "      FANTASMA_WORKER_IDLE_POLL_INTERVAL_MS: {}",
                worker_config.idle_poll_interval_ms
            ));
        } else if line
            .trim_start()
            .starts_with("FANTASMA_WORKER_SESSION_BATCH_SIZE:")
        {
            rendered_lines.push(format!(
                "      FANTASMA_WORKER_SESSION_BATCH_SIZE: {}",
                worker_config.session_batch_size
            ));
        } else if line
            .trim_start()
            .starts_with("FANTASMA_WORKER_EVENT_BATCH_SIZE:")
        {
            rendered_lines.push(format!(
                "      FANTASMA_WORKER_EVENT_BATCH_SIZE: {}",
                worker_config.event_batch_size
            ));
        } else if line
            .trim_start()
            .starts_with("FANTASMA_WORKER_SESSION_INCREMENTAL_CONCURRENCY:")
        {
            rendered_lines.push(format!(
                "      FANTASMA_WORKER_SESSION_INCREMENTAL_CONCURRENCY: {}",
                worker_config.session_incremental_concurrency
            ));
        } else if line
            .trim_start()
            .starts_with("FANTASMA_WORKER_SESSION_REPAIR_CONCURRENCY:")
        {
            rendered_lines.push(format!(
                "      FANTASMA_WORKER_SESSION_REPAIR_CONCURRENCY: {}",
                worker_config.session_repair_concurrency
            ));
            if !trace_path_rendered {
                rendered_lines.push(format!(
                    "      {WORKER_STAGE_B_TRACE_ENV_VAR}: {WORKER_STAGE_B_TRACE_CONTAINER_PATH}"
                ));
                trace_path_rendered = true;
            }
            if !event_lane_trace_path_rendered {
                rendered_lines.push(format!(
                    "      {WORKER_EVENT_LANE_TRACE_ENV_VAR}: {WORKER_EVENT_LANE_TRACE_CONTAINER_PATH}"
                ));
                event_lane_trace_path_rendered = true;
            }
        } else if line.trim_start().starts_with(WORKER_STAGE_B_TRACE_ENV_VAR) {
            rendered_lines.push(format!(
                "      {WORKER_STAGE_B_TRACE_ENV_VAR}: {WORKER_STAGE_B_TRACE_CONTAINER_PATH}"
            ));
            trace_path_rendered = true;
        } else if line
            .trim_start()
            .starts_with(WORKER_EVENT_LANE_TRACE_ENV_VAR)
        {
            rendered_lines.push(format!(
                "      {WORKER_EVENT_LANE_TRACE_ENV_VAR}: {WORKER_EVENT_LANE_TRACE_CONTAINER_PATH}"
            ));
            event_lane_trace_path_rendered = true;
        } else {
            rendered_lines.push(line.to_owned());
        }
    }

    rendered_lines.join("\n")
}

fn benchmark_compose_project_name() -> &'static str {
    BENCHMARK_COMPOSE_PROJECT_NAME
}

fn benchmark_ingest_base_url() -> &'static str {
    INGEST_BASE_URL
}

fn benchmark_api_base_url() -> &'static str {
    API_BASE_URL
}

fn benchmark_admin_token() -> String {
    static TOKEN: OnceLock<String> = OnceLock::new();
    TOKEN
        .get_or_init(|| {
            std::env::var("FANTASMA_ADMIN_TOKEN")
                .unwrap_or_else(|_| format!("fg_pat_{}", Uuid::new_v4().simple()))
        })
        .clone()
}

fn budget_file_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("budgets/ci.json")
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crate lives under repo root")
        .to_path_buf()
}

struct StackGuard {
    compose_file: PathBuf,
    cleanup_compose_file: bool,
    started: bool,
}

impl StackGuard {
    fn new(compose_file: PathBuf) -> Self {
        Self::with_cleanup(compose_file, false)
    }

    fn with_cleanup(compose_file: PathBuf, cleanup_compose_file: bool) -> Self {
        Self {
            compose_file,
            cleanup_compose_file,
            started: false,
        }
    }

    fn start(&mut self) -> Result<()> {
        prepare_stack(&self.compose_file, run_command)?;
        self.started = true;
        Ok(())
    }
}

impl Drop for StackGuard {
    fn drop(&mut self) {
        if self.started {
            run_best_effort(&mut docker_compose_command(
                &self.compose_file,
                ["down", "--volumes", "--remove-orphans"],
            ));
        }
        if self.cleanup_compose_file {
            let _ = fs::remove_file(&self.compose_file);
        }
    }
}

fn prepare_stack<F>(compose_file: &Path, mut runner: F) -> Result<()>
where
    F: FnMut(&mut Command) -> Result<()>,
{
    runner(Command::new("docker").arg("info"))?;
    runner(&mut docker_compose_command(
        compose_file,
        ["down", "--volumes", "--remove-orphans"],
    ))?;
    runner(&mut docker_compose_command(
        compose_file,
        ["up", "-d", "--build"],
    ))?;
    Ok(())
}

fn docker_compose_command(
    compose_file: &Path,
    args: impl IntoIterator<Item = impl AsRef<OsStr>>,
) -> Command {
    let mut command = Command::new("docker");
    command
        .arg("compose")
        .arg("-p")
        .arg(benchmark_compose_project_name())
        .arg("-f")
        .arg(compose_file);
    command.env("FANTASMA_ADMIN_TOKEN", benchmark_admin_token());
    command.args(args);
    command
}

fn run_command(command: &mut Command) -> Result<()> {
    let status = command.status().context("run command")?;
    if status.success() {
        return Ok(());
    }

    bail!("command failed with status {:?}", status.code())
}

fn run_best_effort(command: &mut Command) {
    let _ = command.status();
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use std::{
        fs,
        sync::{Arc, Mutex},
    };
    use tempfile::tempdir;

    #[test]
    fn run_command_source_uses_status_instead_of_output() {
        let source = include_str!("main.rs");
        let function_start = source
            .find("fn run_command(command: &mut Command) -> Result<()> {")
            .expect("run_command must exist");
        let function_end = source[function_start..]
            .find("fn run_best_effort(command: &mut Command) {")
            .map(|offset| function_start + offset)
            .expect("run_best_effort must follow run_command");
        let function_source = &source[function_start..function_end];

        assert!(
            !function_source.contains(".output()"),
            "run_command must not block on captured child pipes"
        );
        assert!(
            function_source.contains(".status()"),
            "run_command must use process status without capturing pipes"
        );
    }

    fn snapshot_command(command: &Command) -> Vec<String> {
        std::iter::once(command.get_program().to_string_lossy().into_owned())
            .chain(
                command
                    .get_args()
                    .map(|argument| argument.to_string_lossy().into_owned()),
            )
            .collect()
    }

    fn query_measurements(
        events_day_dim2_p95_ms: u64,
        events_hour_dim2_p95_ms: u64,
        sessions_count_day_p95_ms: u64,
        sessions_duration_total_day_p95_ms: u64,
    ) -> Vec<QueryMeasurement> {
        vec![
            QueryMeasurement {
                name: "events_day_dim2".to_owned(),
                iterations: 15,
                min_ms: events_day_dim2_p95_ms,
                p50_ms: events_day_dim2_p95_ms,
                p95_ms: events_day_dim2_p95_ms,
                max_ms: events_day_dim2_p95_ms,
            },
            QueryMeasurement {
                name: "events_hour_dim2".to_owned(),
                iterations: 15,
                min_ms: events_hour_dim2_p95_ms,
                p50_ms: events_hour_dim2_p95_ms,
                p95_ms: events_hour_dim2_p95_ms,
                max_ms: events_hour_dim2_p95_ms,
            },
            QueryMeasurement {
                name: "sessions_count_day".to_owned(),
                iterations: 15,
                min_ms: sessions_count_day_p95_ms,
                p50_ms: sessions_count_day_p95_ms,
                p95_ms: sessions_count_day_p95_ms,
                max_ms: sessions_count_day_p95_ms,
            },
            QueryMeasurement {
                name: "sessions_duration_total_day".to_owned(),
                iterations: 15,
                min_ms: sessions_duration_total_day_p95_ms,
                p50_ms: sessions_duration_total_day_p95_ms,
                p95_ms: sessions_duration_total_day_p95_ms,
                max_ms: sessions_duration_total_day_p95_ms,
            },
        ]
    }

    fn dummy_slo_host() -> HostMetadata {
        HostMetadata {
            environment: "local".to_owned(),
        }
    }

    fn dummy_slo_run_config() -> SloRunConfig {
        SloRunConfig::default()
    }

    fn recent_main_runner_samples() -> Vec<(ScenarioBudget, ScenarioResult)> {
        let budget_file = fs::read_to_string(budget_file_path()).expect("read committed CI budget");
        let budgets: BudgetFile =
            serde_json::from_str(&budget_file).expect("parse committed CI budget");

        vec![
            (
                budgets.hot_path,
                ScenarioResult {
                    scenario: Scenario::Hot,
                    profile: Profile::Ci,
                    phases: vec![PhaseMeasurement {
                        name: "ingest".to_owned(),
                        events_sent: 600,
                        elapsed_ms: 46,
                        events_per_second: 13039.003898336192,
                    }],
                    readiness: vec![ReadinessMeasurement {
                        name: "derived_metrics_ready".to_owned(),
                        elapsed_ms: 1_324,
                    }],
                    queries: query_measurements(2, 2, 1, 0),
                    budget: None,
                },
            ),
            (
                budgets.repair_path,
                ScenarioResult {
                    scenario: Scenario::Repair,
                    profile: Profile::Ci,
                    phases: vec![
                        PhaseMeasurement {
                            name: "seed_ingest".to_owned(),
                            events_sent: 250,
                            elapsed_ms: 24,
                            events_per_second: 10313.51135581544,
                        },
                        PhaseMeasurement {
                            name: "repair_ingest".to_owned(),
                            events_sent: 100,
                            elapsed_ms: 9,
                            events_per_second: 10348.465964102414,
                        },
                    ],
                    readiness: vec![
                        ReadinessMeasurement {
                            name: "seed_ready".to_owned(),
                            elapsed_ms: 458,
                        },
                        ReadinessMeasurement {
                            name: "repair_ready".to_owned(),
                            elapsed_ms: 344,
                        },
                    ],
                    queries: query_measurements(3, 2, 0, 0),
                    budget: None,
                },
            ),
            (
                budgets.scale_path,
                ScenarioResult {
                    scenario: Scenario::Scale,
                    profile: Profile::Ci,
                    phases: vec![PhaseMeasurement {
                        name: "ingest".to_owned(),
                        events_sent: 3_600,
                        elapsed_ms: 343,
                        events_per_second: 10503.72034597112,
                    }],
                    readiness: vec![ReadinessMeasurement {
                        name: "derived_metrics_ready".to_owned(),
                        elapsed_ms: 14_015,
                    }],
                    queries: query_measurements(6, 7, 1, 0),
                    budget: None,
                },
            ),
        ]
    }

    #[test]
    fn parse_stack_command_reads_scenario_profile_and_output() {
        let command = parse_args([
            "fantasma-bench",
            "stack",
            "--scenario",
            "hot-path",
            "--profile",
            "ci",
            "--output",
            "artifacts/hot-path.json",
        ])
        .expect("parse stack command");

        assert_eq!(
            command,
            BenchCommand::Stack(StackArgs {
                scenario: Scenario::Hot,
                profile: Profile::Ci,
                output: PathBuf::from("artifacts/hot-path.json"),
            })
        );
    }

    #[test]
    fn parse_stack_command_accepts_scale_path() {
        let command = parse_args([
            "fantasma-bench",
            "stack",
            "--scenario",
            "scale-path",
            "--profile",
            "extended",
            "--output",
            "artifacts/scale-path.json",
        ])
        .expect("parse scale-path command");

        assert_eq!(
            command,
            BenchCommand::Stack(StackArgs {
                scenario: Scenario::Scale,
                profile: Profile::Extended,
                output: PathBuf::from("artifacts/scale-path.json"),
            })
        );
    }

    #[test]
    fn parse_series_command_reads_profile_repetitions_and_output_dir() {
        let command = parse_args([
            "fantasma-bench",
            "series",
            "--profile",
            "heavy",
            "--repetitions",
            "5",
            "--output-dir",
            "artifacts/performance/2026-03-13-m3-pro-heavy",
        ])
        .expect("parse series command");

        assert_eq!(
            command,
            BenchCommand::Series(SeriesArgs {
                profile: Profile::Heavy,
                repetitions: 5,
                output_dir: PathBuf::from("artifacts/performance/2026-03-13-m3-pro-heavy"),
            })
        );
    }

    #[test]
    fn parse_slo_command_reads_output_dir() {
        let command = parse_args([
            "fantasma-bench",
            "slo",
            "--output-dir",
            "artifacts/performance/2026-03-13-derived-metrics-slo",
        ])
        .expect("parse slo command");

        assert_eq!(
            command,
            BenchCommand::Slo(SloArgs {
                output_dir: PathBuf::from("artifacts/performance/2026-03-13-derived-metrics-slo"),
                scenarios: Vec::new(),
                suites: Vec::new(),
                run_config: dummy_slo_run_config(),
            })
        );
    }

    #[test]
    fn parse_slo_command_defaults_to_iterative_mode() {
        let BenchCommand::Slo(args) = parse_args([
            "fantasma-bench",
            "slo",
            "--output-dir",
            "artifacts/performance/2026-03-15-derived-metrics-slo",
        ])
        .expect("parse iterative default") else {
            panic!("expected SLO command");
        };

        let rendered = serde_json::to_value(&args.run_config).expect("serialize run config");

        assert_eq!(rendered["mode"], serde_json::json!("iterative"));
    }

    #[test]
    fn parse_slo_command_reads_requested_scenarios() {
        let command = parse_args([
            "fantasma-bench",
            "slo",
            "--output-dir",
            "artifacts/performance/2026-03-13-derived-metrics-slo",
            "--scenario",
            "live-append-small-blobs",
            "--scenario",
            "stress-repair-90d",
        ])
        .expect("parse slo command");

        assert_eq!(
            command,
            BenchCommand::Slo(SloArgs {
                output_dir: PathBuf::from("artifacts/performance/2026-03-13-derived-metrics-slo"),
                scenarios: vec![
                    "live-append-small-blobs".to_owned(),
                    "stress-repair-90d".to_owned(),
                ],
                suites: Vec::new(),
                run_config: dummy_slo_run_config(),
            })
        );
    }

    #[test]
    fn parse_slo_command_accepts_suite_filters_and_publication_repetition_override() {
        let command = parse_args([
            "fantasma-bench",
            "slo",
            "--output-dir",
            "artifacts/performance/2026-03-13-derived-metrics-slo",
            "--suite",
            "representative",
            "--suite",
            "mixed-repair",
            "--publication-repetitions",
            "3",
            "--worker-session-batch-size",
            "2000",
            "--worker-event-batch-size",
            "7000",
            "--worker-session-incremental-concurrency",
            "12",
            "--worker-session-repair-concurrency",
            "4",
            "--worker-idle-poll-interval-ms",
            "75",
        ])
        .expect("parse slo override command");

        match command {
            BenchCommand::Slo(args) => {
                assert_eq!(
                    args.output_dir,
                    PathBuf::from("artifacts/performance/2026-03-13-derived-metrics-slo")
                );
                assert_eq!(args.run_config.publication_repetitions, 3);
                assert_eq!(args.run_config.worker_config.session_batch_size, 2_000);
                assert_eq!(args.run_config.worker_config.event_batch_size, 7_000);
                assert_eq!(
                    args.run_config
                        .worker_config
                        .session_incremental_concurrency,
                    12
                );
                assert_eq!(args.run_config.worker_config.session_repair_concurrency, 4);
                assert_eq!(args.run_config.worker_config.idle_poll_interval_ms, 75);
            }
            other => panic!("expected SLO command, got {other:?}"),
        }
    }

    #[test]
    fn select_slo_scenarios_filters_requested_keys() {
        let scenarios = select_slo_scenarios(
            &[
                "live-append-small-blobs".to_owned(),
                "stress-repair-90d".to_owned(),
            ],
            &dummy_slo_run_config(),
        )
        .expect("select requested scenarios");

        assert_eq!(
            scenarios
                .iter()
                .map(|scenario| scenario.key())
                .collect::<Vec<_>>(),
            vec!["live-append-small-blobs", "stress-repair-90d"]
        );
    }

    #[test]
    fn select_slo_scenarios_rejects_unknown_keys() {
        let error = select_slo_scenarios(&["unknown".to_owned()], &dummy_slo_run_config())
            .expect_err("unknown scenario should fail");

        assert!(error.to_string().contains("unknown slo scenario"));
    }

    #[test]
    fn select_slo_scenarios_returns_default_worker_gate_when_no_filter_is_supplied() {
        let run_config = dummy_slo_run_config();
        let scenarios = select_slo_scenarios(&[], &run_config).expect("load default scenarios");

        assert_eq!(scenarios.len(), 2);
        assert_eq!(
            scenarios
                .iter()
                .map(|scenario| scenario.key())
                .collect::<Vec<_>>(),
            vec!["live-append-small-blobs", "live-append-plus-light-repair",]
        );
    }

    #[test]
    fn publication_mode_default_selection_keeps_the_full_live_worker_family() {
        let summary: serde_json::Value = serde_json::from_value(serde_json::json!({
            "publication_repetitions": 2,
            "mode": "publication",
            "worker_config": {
                "session_batch_size": DEFAULT_SLO_SESSION_BATCH_SIZE,
                "event_batch_size": DEFAULT_SLO_EVENT_BATCH_SIZE,
                "session_incremental_concurrency": DEFAULT_SLO_SESSION_INCREMENTAL_CONCURRENCY,
                "session_repair_concurrency": DEFAULT_SLO_SESSION_REPAIR_CONCURRENCY,
                "idle_poll_interval_ms": DEFAULT_SLO_IDLE_POLL_INTERVAL_MS
            }
        }))
        .expect("parse publication config");
        let run_config: SloRunConfig =
            serde_json::from_value(summary).expect("deserialize publication run config");
        let scenarios = select_slo_scenarios(&[], &run_config).expect("load publication defaults");

        assert_eq!(
            scenarios
                .iter()
                .map(|scenario| scenario.key())
                .collect::<Vec<_>>(),
            vec![
                "live-append-small-blobs",
                "live-append-offline-flush",
                "live-append-plus-light-repair",
                "live-append-plus-repair-hot-project",
            ]
        );
    }

    #[test]
    fn slo_scenarios_lock_suite_taxonomy_and_default_repetition_policy() {
        let scenarios = slo_scenario_definitions(&dummy_slo_run_config());

        assert_eq!(scenarios.len(), 19);
        assert_eq!(
            scenarios
                .iter()
                .map(|scenario| scenario.key())
                .collect::<Vec<_>>(),
            vec![
                "live-append-small-blobs",
                "live-append-offline-flush",
                "live-append-plus-light-repair",
                "live-append-plus-repair-hot-project",
                "burst-readiness-300-installs-x1",
                "burst-readiness-150-installs-x2",
                "burst-readiness-100-installs-x3",
                "stress-append-30d",
                "stress-backfill-30d",
                "stress-repair-30d",
                "reads-visibility-30d",
                "stress-append-90d",
                "stress-backfill-90d",
                "stress-repair-90d",
                "reads-visibility-90d",
                "stress-append-180d",
                "stress-backfill-180d",
                "stress-repair-180d",
                "reads-visibility-180d",
            ]
        );
        assert_eq!(
            scenarios
                .iter()
                .find(|scenario| scenario.key() == "live-append-small-blobs")
                .expect("live-append-small-blobs")
                .repetitions,
            1
        );
        assert_eq!(
            scenarios
                .iter()
                .find(|scenario| scenario.key() == "live-append-plus-light-repair")
                .expect("live-append-plus-light-repair")
                .repetitions,
            1
        );
        assert_eq!(
            scenarios
                .iter()
                .find(|scenario| scenario.key() == "stress-repair-180d")
                .expect("stress-repair-180d")
                .repetitions,
            1
        );
        assert_eq!(
            scenarios
                .iter()
                .find(|scenario| scenario.key() == "burst-readiness-300-installs-x1")
                .expect("burst-readiness-300-installs-x1")
                .repetitions,
            1
        );
    }

    #[test]
    fn slo_scenarios_honor_publication_repetition_override_for_representative_and_mixed_repair() {
        let command = parse_args([
            "fantasma-bench",
            "slo",
            "--output-dir",
            "artifacts/performance/2026-03-13-derived-metrics-slo",
            "--mode",
            "publication",
            "--publication-repetitions",
            "3",
        ])
        .expect("parse publication repetition override");

        let BenchCommand::Slo(args) = command else {
            panic!("expected SLO command");
        };

        let scenarios = slo_scenario_definitions(&args.run_config);

        assert_eq!(
            scenarios
                .iter()
                .find(|scenario| scenario.key() == "live-append-small-blobs")
                .expect("live-append-small-blobs")
                .repetitions,
            3
        );
        assert_eq!(
            scenarios
                .iter()
                .find(|scenario| scenario.key() == "live-append-plus-light-repair")
                .expect("live-append-plus-light-repair")
                .repetitions,
            3
        );
        assert_eq!(
            scenarios
                .iter()
                .find(|scenario| scenario.key() == "stress-append-90d")
                .expect("stress-append-90d")
                .repetitions,
            1
        );
    }

    #[test]
    fn slo_visibility_timeouts_match_publication_policy() {
        assert_eq!(
            SloWindow::Days30.readiness_timeout(),
            Duration::from_secs(5 * 60)
        );
        assert_eq!(
            SloWindow::Days90.readiness_timeout(),
            Duration::from_secs(20 * 60)
        );
        assert_eq!(
            SloWindow::Days180.readiness_timeout(),
            Duration::from_secs(40 * 60)
        );
    }

    #[test]
    fn reads_scenarios_treat_readiness_timeouts_as_visibility_only() {
        assert_eq!(
            slo_readiness_policy(SloScenarioKind::ReadsVisibility, SloWindow::Days30),
            SloReadinessPolicy {
                allow_timeout_publication: false,
                wait_for_full_readiness_before_queries: false,
            }
        );
        assert_eq!(
            slo_readiness_policy(SloScenarioKind::ReadsVisibility, SloWindow::Days90),
            SloReadinessPolicy {
                allow_timeout_publication: true,
                wait_for_full_readiness_before_queries: true,
            }
        );
        assert_eq!(
            slo_readiness_policy(SloScenarioKind::ReadsVisibility, SloWindow::Days180),
            SloReadinessPolicy {
                allow_timeout_publication: true,
                wait_for_full_readiness_before_queries: true,
            }
        );
        assert_eq!(
            slo_readiness_policy(SloScenarioKind::StressAppend, SloWindow::Days180),
            SloReadinessPolicy {
                allow_timeout_publication: true,
                wait_for_full_readiness_before_queries: false,
            }
        );
    }

    #[test]
    fn iterative_live_append_small_blob_schedule_uses_reduced_cohort_and_sparse_checkpoints() {
        let scenario = slo_scenario_definitions(&dummy_slo_run_config())
            .into_iter()
            .find(|scenario| scenario.key() == "live-append-small-blobs")
            .expect("live-append-small-blobs");

        let (append_blobs, repair_blobs) =
            live_slo_schedule(&scenario, &dummy_slo_run_config()).expect("build schedule");
        let checkpointed_append_blobs = append_blobs
            .iter()
            .filter(|blob| blob.kind == UploadBlobKind::Append && blob.checkpoint)
            .count();

        assert!(repair_blobs.is_empty());
        assert_eq!(
            append_blobs.len(),
            SLO_REPRESENTATIVE_WINDOW_DAYS * 250 * SLO_REPRESENTATIVE_SESSIONS_PER_INSTALL_PER_DAY
        );
        assert_eq!(checkpointed_append_blobs, append_blobs.len() / 200);
    }

    #[test]
    fn publication_live_append_small_blob_schedule_keeps_the_1000_install_day_baseline() {
        let run_config: SloRunConfig = serde_json::from_value(serde_json::json!({
            "publication_repetitions": 2,
            "mode": "publication",
            "worker_config": {
                "session_batch_size": DEFAULT_SLO_SESSION_BATCH_SIZE,
                "event_batch_size": DEFAULT_SLO_EVENT_BATCH_SIZE,
                "session_incremental_concurrency": DEFAULT_SLO_SESSION_INCREMENTAL_CONCURRENCY,
                "session_repair_concurrency": DEFAULT_SLO_SESSION_REPAIR_CONCURRENCY
            }
        }))
        .expect("deserialize publication run config");
        let scenario = slo_scenario_definitions(&dummy_slo_run_config())
            .into_iter()
            .find(|scenario| scenario.key() == "live-append-small-blobs")
            .expect("live-append-small-blobs");

        let (append_blobs, repair_blobs) =
            live_slo_schedule(&scenario, &run_config).expect("build schedule");

        assert!(repair_blobs.is_empty());
        assert_eq!(
            append_blobs.len(),
            SLO_REPRESENTATIVE_WINDOW_DAYS
                * SLO_INSTALL_COUNT_PER_DAY
                * SLO_REPRESENTATIVE_SESSIONS_PER_INSTALL_PER_DAY
        );
    }

    #[test]
    fn iterative_offline_flush_and_repair_live_schedules_use_bounded_checkpoint_sampling() {
        let offline_flush_scenario = slo_scenario_definitions(&dummy_slo_run_config())
            .into_iter()
            .find(|scenario| scenario.key() == "live-append-offline-flush")
            .expect("live-append-offline-flush");
        let (offline_flush_blobs, _) =
            live_slo_schedule(&offline_flush_scenario, &dummy_slo_run_config())
                .expect("offline flush schedule");
        let flush_blobs = offline_flush_blobs
            .iter()
            .filter(|blob| blob.kind == UploadBlobKind::OfflineFlush)
            .collect::<Vec<_>>();
        assert!(!flush_blobs.is_empty());
        assert_eq!(
            flush_blobs.iter().filter(|blob| blob.checkpoint).count(),
            flush_blobs.len() / 25
        );

        let repair_scenario = slo_scenario_definitions(&dummy_slo_run_config())
            .into_iter()
            .find(|scenario| scenario.key() == "live-append-plus-light-repair")
            .expect("live-append-plus-light-repair");
        let (_, repair_blobs) =
            live_slo_schedule(&repair_scenario, &dummy_slo_run_config()).expect("repair schedule");
        assert!(!repair_blobs.is_empty());
        assert!(
            repair_blobs
                .iter()
                .all(|blob| blob.kind == UploadBlobKind::Repair)
        );
        assert_eq!(
            repair_blobs.iter().filter(|blob| blob.checkpoint).count(),
            repair_blobs.len() / 10
        );
    }

    #[test]
    fn burst_readiness_scenarios_chunk_the_expected_300_event_shapes() {
        let scenarios = slo_scenario_definitions(&dummy_slo_run_config());

        for (scenario_key, expected_blob_count, expected_events_per_blob) in [
            ("burst-readiness-300-installs-x1", 3, 100),
            ("burst-readiness-150-installs-x2", 3, 100),
            ("burst-readiness-100-installs-x3", 3, 100),
        ] {
            let scenario = scenarios
                .iter()
                .find(|scenario| scenario.key() == scenario_key)
                .unwrap_or_else(|| panic!("missing {scenario_key}"));
            let (append_blobs, repair_blobs) =
                live_slo_schedule(scenario, &dummy_slo_run_config()).expect("burst schedule");

            assert!(
                repair_blobs.is_empty(),
                "{scenario_key} should not schedule repairs"
            );
            assert_eq!(
                append_blobs.len(),
                expected_blob_count,
                "{scenario_key} blob count"
            );
            assert!(
                append_blobs
                    .iter()
                    .all(|blob| blob.kind == UploadBlobKind::Append),
                "{scenario_key} should only use append blobs"
            );
            assert!(
                append_blobs.iter().all(|blob| !blob.checkpoint),
                "{scenario_key} should not emit checkpoint probes"
            );
            assert_eq!(
                append_blobs
                    .iter()
                    .map(|blob| blob.events.len())
                    .sum::<usize>(),
                300,
                "{scenario_key} total events"
            );
            assert!(
                append_blobs
                    .iter()
                    .all(|blob| blob.events.len() == expected_events_per_blob),
                "{scenario_key} should stay POST-batch-sized"
            );
        }
    }

    #[test]
    fn prepare_clean_output_dir_removes_stale_artifacts_before_a_rerun() {
        let tempdir = tempdir().expect("create tempdir");
        let output_dir = tempdir.path().join("slo-output");

        fs::create_dir_all(output_dir.join("append-30d")).expect("create stale scenario dir");
        fs::write(output_dir.join("summary.json"), "stale").expect("write stale summary");
        fs::write(
            output_dir.join("append-30d").join("run-01.json"),
            "stale scenario",
        )
        .expect("write stale scenario output");

        prepare_clean_output_dir(&output_dir).expect("prepare clean output dir");

        assert!(output_dir.is_dir());
        assert!(
            fs::read_dir(&output_dir)
                .expect("read cleaned output dir")
                .next()
                .is_none()
        );
    }

    #[test]
    fn slo_grouped_query_matrix_contains_the_required_hard_gate_queries() {
        let queries = slo_query_matrix(
            SloWindow::Days30,
            NaiveDate::from_ymd_opt(2026, 1, 1).expect("start day"),
        );

        let hard_gate_queries = queries
            .iter()
            .filter(|query| query.hard_gate)
            .map(|query| query.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            hard_gate_queries,
            vec![
                "events_count_day_grouped",
                "events_count_hour_grouped",
                "events_count_day_dim2_grouped",
                "events_count_hour_dim2_grouped",
                "sessions_count_day_grouped",
                "sessions_count_hour_grouped",
                "sessions_count_day_dim2_grouped",
                "sessions_count_hour_dim2_grouped",
                "sessions_duration_total_day_grouped",
                "sessions_duration_total_hour_grouped",
                "sessions_duration_total_day_dim2_grouped",
                "sessions_duration_total_hour_dim2_grouped",
                "sessions_new_installs_day_grouped",
                "sessions_new_installs_hour_grouped",
                "sessions_new_installs_day_dim2_grouped",
                "sessions_new_installs_hour_dim2_grouped",
            ]
        );
    }

    #[test]
    fn filtered_dim2_event_hard_gates_do_not_expect_the_full_event_total() {
        let scenario = slo_scenario_definitions(&dummy_slo_run_config())
            .into_iter()
            .find(|scenario| scenario.key() == "stress-append-30d")
            .expect("stress-append-30d scenario");
        let expectation = slo_expectation(&scenario);
        let query = slo_query_matrix(scenario.window, slo_start_day())
            .into_iter()
            .find(|query| query.name == "events_count_day_dim2_grouped")
            .expect("dim2 day grouped query");

        assert_eq!(
            expected_total_for_slo_query(&query, &scenario, &expectation),
            299_700
        );
    }

    #[test]
    fn repair_seed_filtered_dim2_event_hard_gates_do_not_include_late_events() {
        let scenario = slo_scenario_definitions(&dummy_slo_run_config())
            .into_iter()
            .find(|scenario| scenario.key() == "stress-repair-30d")
            .expect("stress-repair-30d scenario");
        let expectation = slo_base_expectation(scenario.window);
        let query = slo_query_matrix(scenario.window, slo_start_day())
            .into_iter()
            .find(|query| query.name == "events_count_day_dim2_grouped")
            .expect("dim2 day grouped query");

        assert_eq!(
            expected_total_for_slo_query(&query, &scenario, &expectation),
            299_700
        );
    }

    #[test]
    fn filtered_dim2_session_hard_gates_do_not_expect_the_full_session_total() {
        let scenario = slo_scenario_definitions(&dummy_slo_run_config())
            .into_iter()
            .find(|scenario| scenario.key() == "stress-append-30d")
            .expect("stress-append-30d scenario");
        let expectation = slo_expectation(&scenario);
        let queries = slo_query_matrix(scenario.window, slo_start_day());

        let count_query = queries
            .iter()
            .find(|query| query.name == "sessions_count_day_dim2_grouped")
            .expect("dim2 session count query");
        assert_eq!(
            expected_total_for_slo_query(count_query, &scenario, &expectation),
            9_990
        );

        let duration_query = queries
            .iter()
            .find(|query| query.name == "sessions_duration_total_day_dim2_grouped")
            .expect("dim2 session duration query");
        assert_eq!(
            expected_total_for_slo_query(duration_query, &scenario, &expectation),
            17_382_600
        );
    }

    #[test]
    fn filtered_dim2_session_new_install_hard_gates_only_count_first_seen_matches() {
        let scenario = slo_scenario_definitions(&dummy_slo_run_config())
            .into_iter()
            .find(|scenario| scenario.key() == "stress-repair-30d")
            .expect("stress-repair-30d scenario");
        let expectation = slo_base_expectation(scenario.window);
        let query = slo_query_matrix(scenario.window, slo_start_day())
            .into_iter()
            .find(|query| query.name == "sessions_new_installs_day_dim2_grouped")
            .expect("dim2 session new-installs query");

        assert_eq!(
            expected_total_for_slo_query(&query, &scenario, &expectation),
            333
        );
    }

    #[test]
    fn burst_readiness_filtered_hard_gates_follow_the_actual_burst_distribution() {
        let scenario = slo_scenario_definitions(&dummy_slo_run_config())
            .into_iter()
            .find(|scenario| scenario.key() == "burst-readiness-100-installs-x3")
            .expect("burst-readiness-100-installs-x3 scenario");
        let expectation = live_slo_schedule(&scenario, &dummy_slo_run_config())
            .expect("burst schedule")
            .0
            .into_iter()
            .fold(SloExpectation::default(), |mut expectation, blob| {
                expectation.apply_delta(&blob.delta);
                expectation
            });
        let queries = slo_query_matrix(scenario.window, slo_start_day());

        let event_query = queries
            .iter()
            .find(|query| query.name == "events_count_day_dim2_grouped")
            .expect("burst dim2 event query");
        assert_eq!(
            expected_total_for_slo_query(event_query, &scenario, &expectation),
            99
        );

        let session_count_query = queries
            .iter()
            .find(|query| query.name == "sessions_count_day_dim2_grouped")
            .expect("burst dim2 session count query");
        assert_eq!(
            expected_total_for_slo_query(session_count_query, &scenario, &expectation),
            33
        );

        let duration_query = queries
            .iter()
            .find(|query| query.name == "sessions_duration_total_day_dim2_grouped")
            .expect("burst dim2 session duration query");
        assert_eq!(
            expected_total_for_slo_query(duration_query, &scenario, &expectation),
            3_960
        );

        let new_install_query = queries
            .iter()
            .find(|query| query.name == "sessions_new_installs_day_dim2_grouped")
            .expect("burst dim2 session new installs query");
        assert_eq!(
            expected_total_for_slo_query(new_install_query, &scenario, &expectation),
            33
        );
    }

    #[test]
    fn session_active_installs_dim2_queries_stay_visibility_only() {
        let queries = slo_query_matrix(
            SloWindow::Days30,
            NaiveDate::from_ymd_opt(2026, 1, 1).expect("start day"),
        );

        for query_name in [
            "sessions_active_installs_day_dim2_grouped",
            "sessions_active_installs_day_dim2_filtered",
            "sessions_active_installs_week_dim2_grouped",
            "sessions_active_installs_week_dim2_filtered",
            "sessions_active_installs_month_dim2_grouped",
            "sessions_active_installs_month_dim2_filtered",
            "sessions_active_installs_year_dim2_grouped",
            "sessions_active_installs_year_dim2_filtered",
        ] {
            let query = queries
                .iter()
                .find(|query| query.name == query_name)
                .unwrap_or_else(|| panic!("missing {query_name}"));
            assert!(!query.hard_gate, "{query_name} should stay visibility-only");
        }
    }

    #[test]
    fn session_active_installs_queries_use_exact_range_interval_contract() {
        let queries = slo_query_matrix(
            SloWindow::Days30,
            NaiveDate::from_ymd_opt(2026, 1, 1).expect("start day"),
        );

        let day = queries
            .iter()
            .find(|query| query.name == "sessions_active_installs_day_ungrouped")
            .expect("daily active-installs query");
        assert!(day.url.contains("start=2026-01-01"));
        assert!(day.url.contains("end=2026-01-30"));
        assert!(day.url.contains("interval=day"));
        assert!(!day.url.contains("granularity="));

        let week = queries
            .iter()
            .find(|query| query.name == "sessions_active_installs_week_ungrouped")
            .expect("weekly active-installs query");
        assert!(week.url.contains("start=2026-01-01"));
        assert!(week.url.contains("end=2026-01-30"));
        assert!(week.url.contains("interval=week"));
        assert!(!week.url.contains("granularity="));

        let month = queries
            .iter()
            .find(|query| query.name == "sessions_active_installs_month_ungrouped")
            .expect("monthly active-installs query");
        assert!(month.url.contains("start=2026-01-01"));
        assert!(month.url.contains("end=2026-01-30"));
        assert!(month.url.contains("interval=month"));
        assert!(!month.url.contains("granularity="));

        let year = queries
            .iter()
            .find(|query| query.name == "sessions_active_installs_year_ungrouped")
            .expect("yearly active-installs query");
        assert!(year.url.contains("start=2026-01-01"));
        assert!(year.url.contains("end=2026-01-30"));
        assert!(year.url.contains("interval=year"));
        assert!(!year.url.contains("granularity="));
    }

    #[test]
    fn slo_budget_evaluation_gates_representative_checkpoints_and_reads_visibility_queries() {
        let representative = SloScenarioResult {
            scenario: "live-append-small-blobs".to_owned(),
            run_config: dummy_slo_run_config(),
            phases: vec![],
            readiness: vec![
                ReadinessMeasurement {
                    name: "event_metrics_ready_ms".to_owned(),
                    elapsed_ms: 45_000,
                },
                ReadinessMeasurement {
                    name: "session_metrics_ready_ms".to_owned(),
                    elapsed_ms: 90_000,
                },
                ReadinessMeasurement {
                    name: "derived_metrics_ready_ms".to_owned(),
                    elapsed_ms: 90_000,
                },
            ],
            checkpoint_readiness: Some(CheckpointReadinessSummary {
                sample_count: 10,
                event_metrics_ready_p50_ms: 20_000,
                event_metrics_ready_p95_ms: 35_000,
                event_metrics_ready_max_ms: 40_000,
                session_metrics_ready_p50_ms: 30_000,
                session_metrics_ready_p95_ms: 65_000,
                session_metrics_ready_max_ms: 70_000,
                derived_metrics_ready_p50_ms: 30_000,
                derived_metrics_ready_p95_ms: 65_000,
                derived_metrics_ready_max_ms: 70_000,
            }),
            queries: vec![],
            budget: None,
            failure: None,
        };
        let reads_180 = SloScenarioResult {
            scenario: "reads-visibility-180d".to_owned(),
            run_config: dummy_slo_run_config(),
            phases: vec![],
            readiness: vec![],
            checkpoint_readiness: None,
            queries: vec![QueryMeasurement {
                name: "sessions_count_hour_grouped".to_owned(),
                iterations: 100,
                min_ms: 200,
                p50_ms: 300,
                p95_ms: 600,
                max_ms: 700,
            }],
            budget: None,
            failure: None,
        };

        let representative_budget = evaluate_slo_budget(
            &slo_scenario_definitions(&dummy_slo_run_config())
                .into_iter()
                .find(|scenario| scenario.key() == "live-append-small-blobs")
                .expect("live-append-small-blobs"),
            &representative,
        );
        let reads_180_budget = evaluate_slo_budget(
            &slo_scenario_definitions(&dummy_slo_run_config())
                .into_iter()
                .find(|scenario| scenario.key() == "reads-visibility-180d")
                .expect("reads-visibility-180d"),
            &reads_180,
        );

        assert!(!representative_budget.passed);
        assert!(!reads_180_budget.passed);
        assert_eq!(
            reads_180_budget.failures,
            vec!["sessions_count_hour_grouped p95 600ms exceeded budget 500ms".to_owned()]
        );
    }

    #[tokio::test]
    async fn poll_until_elapsed_anchors_timeout_to_reported_start_instant() {
        let started_at = Instant::now() - Duration::from_secs(1);
        let deadline = started_at + Duration::from_millis(10);
        let checks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let elapsed = poll_until_elapsed(
            {
                let checks = std::sync::Arc::clone(&checks);
                move || {
                    let checks = std::sync::Arc::clone(&checks);
                    async move {
                        checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok::<_, anyhow::Error>(false)
                    }
                }
            },
            deadline,
            "anchored readiness",
            started_at,
            true,
        )
        .await
        .expect("timeout publication succeeds");

        assert_eq!(elapsed, None);
        assert_eq!(checks.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_slo_phase_and_wait_for_readiness_starts_after_phase_completion() {
        let events = Arc::new(Mutex::new(Vec::new()));

        let (phase_output, readiness) = run_slo_phase_and_wait_for_readiness(
            {
                let events = Arc::clone(&events);
                move || {
                    let events = Arc::clone(&events);
                    async move {
                        events.lock().expect("lock events").push("phase");
                        Ok::<_, anyhow::Error>("phase-output")
                    }
                }
            },
            {
                let events = Arc::clone(&events);
                move |_started_at| {
                    let events = Arc::clone(&events);
                    async move {
                        let recorded = events.lock().expect("lock events").clone();
                        assert_eq!(recorded, vec!["phase"]);
                        events.lock().expect("lock events").push("readiness");
                        Ok::<_, anyhow::Error>(vec![ReadinessMeasurement {
                            name: "derived_metrics_ready_ms".to_owned(),
                            elapsed_ms: 123,
                        }])
                    }
                }
            },
        )
        .await
        .expect("measure readiness");

        assert_eq!(phase_output, "phase-output");
        assert_eq!(
            readiness,
            vec![ReadinessMeasurement {
                name: "derived_metrics_ready_ms".to_owned(),
                elapsed_ms: 123,
            }]
        );
        assert_eq!(
            events.lock().expect("lock events").as_slice(),
            ["phase", "readiness"]
        );
    }

    #[tokio::test]
    async fn run_slo_suite_writes_summary_even_when_a_scenario_fails() {
        let tempdir = tempdir().expect("create tempdir");
        let output_dir = tempdir.path().join("derived-metrics-slo");
        fs::create_dir_all(output_dir.join("stale-scenario")).expect("create stale subtree");
        fs::write(output_dir.join("summary.json"), "stale").expect("write stale summary");

        let error = run_slo_suite(
            &output_dir,
            dummy_slo_host(),
            dummy_slo_run_config(),
            |scenario: SloScenarioDefinition| async move {
                let scenario_key = scenario.key();
                Err::<SloScenarioResult, anyhow::Error>(anyhow!(
                    "intentional failure for {scenario_key}"
                ))
            },
        )
        .await
        .expect_err("suite should fail");

        assert!(error.to_string().contains(
            "scenario execution failed: intentional failure for live-append-small-blobs"
        ));
        assert!(host_metadata_output_path(&output_dir).exists());
        assert!(summary_output_path(&output_dir).exists());
        assert!(
            summary_output_path(&output_dir)
                .with_extension("md")
                .exists()
        );
        assert!(
            output_dir
                .join("live-append-small-blobs")
                .join("result.json")
                .exists()
        );
        assert!(
            output_dir
                .join("live-append-small-blobs")
                .join("result.md")
                .exists()
        );
        assert!(!output_dir.join("stale-scenario").exists());

        let summary: SloSummary = serde_json::from_str(
            &fs::read_to_string(summary_output_path(&output_dir)).expect("read summary json"),
        )
        .expect("parse summary json");
        assert_eq!(summary.scenarios.len(), 1);
        assert_eq!(summary.scenarios[0].scenario, "live-append-small-blobs");
        assert_eq!(summary.run_config, dummy_slo_run_config());
        assert_eq!(summary.scenarios[0].run_config, dummy_slo_run_config());
        assert_eq!(
            summary.scenarios[0].budget,
            Some(BudgetEvaluation {
                passed: false,
                failures: vec![
                    "scenario execution failed: intentional failure for live-append-small-blobs"
                        .to_owned()
                ],
            })
        );
        let host: HostMetadata = serde_json::from_str(
            &fs::read_to_string(host_metadata_output_path(&output_dir)).expect("read host json"),
        )
        .expect("parse host json");
        assert_eq!(host, dummy_slo_host());
        let scenario_json: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(
                output_dir
                    .join("live-append-small-blobs")
                    .join("result.json"),
            )
            .expect("read scenario json"),
        )
        .expect("parse scenario json");
        assert_eq!(
            scenario_json["run_config"]["worker_config"]["session_batch_size"],
            serde_json::json!(DEFAULT_SLO_SESSION_BATCH_SIZE)
        );
    }

    #[test]
    fn render_slo_markdown_summary_includes_family_readiness_and_budgets() {
        let summary: SloSummary = serde_json::from_value(serde_json::json!({
            "host": {
                "environment": "local"
            },
            "suites": ["representative", "mixed-repair"],
            "run_config": {
                "publication_repetitions": 1,
                "mode": "iterative",
                "worker_config": {
                    "session_batch_size": DEFAULT_SLO_SESSION_BATCH_SIZE,
                    "event_batch_size": DEFAULT_SLO_EVENT_BATCH_SIZE,
                    "session_incremental_concurrency": DEFAULT_SLO_SESSION_INCREMENTAL_CONCURRENCY,
                    "session_repair_concurrency": DEFAULT_SLO_SESSION_REPAIR_CONCURRENCY
                }
            },
            "verdict": {
                "baseline_source": "crates/fantasma-bench/baselines/slo-iterative-default.json",
                "representative_append": {
                    "overall": "better",
                    "checkpoint_derived": "better",
                    "final_derived": "flat"
                },
                "mixed_repair": {
                    "overall": "flat",
                    "checkpoint_derived": "flat",
                    "final_derived": "flat"
                },
                "dominant_stage_b_phase": "changed",
                "overall": "better"
            },
            "scenarios": [{
                "scenario": "live-append-small-blobs",
                "run_config": {
                    "publication_repetitions": 1,
                    "mode": "iterative",
                    "worker_config": {
                        "session_batch_size": DEFAULT_SLO_SESSION_BATCH_SIZE,
                        "event_batch_size": DEFAULT_SLO_EVENT_BATCH_SIZE,
                        "session_incremental_concurrency": DEFAULT_SLO_SESSION_INCREMENTAL_CONCURRENCY,
                        "session_repair_concurrency": DEFAULT_SLO_SESSION_REPAIR_CONCURRENCY
                    }
                },
                "phases": [{
                    "name": "ingest",
                    "events_sent": 900000,
                    "elapsed_ms": 15000,
                    "events_per_second": 60000.0
                }],
                "readiness": [
                    {"name": "event_metrics_ready_ms", "elapsed_ms": 20000},
                    {"name": "session_metrics_ready_ms", "elapsed_ms": 40000},
                    {"name": "derived_metrics_ready_ms", "elapsed_ms": 40000}
                ],
                "checkpoint_readiness": {
                    "sample_count": 10,
                    "event_metrics_ready_p50_ms": 10000,
                    "event_metrics_ready_p95_ms": 20000,
                    "event_metrics_ready_max_ms": 25000,
                    "session_metrics_ready_p50_ms": 20000,
                    "session_metrics_ready_p95_ms": 40000,
                    "session_metrics_ready_max_ms": 45000,
                    "derived_metrics_ready_p50_ms": 20000,
                    "derived_metrics_ready_p95_ms": 40000,
                    "derived_metrics_ready_max_ms": 45000
                },
                "queries": [{
                    "name": "events_count_day_grouped",
                    "iterations": 100,
                    "min_ms": 50,
                    "p50_ms": 60,
                    "p95_ms": 80,
                    "max_ms": 95
                }],
                "budget": {"passed": true, "failures": []}
            }]
        }))
        .expect("parse SLO summary fixture");

        let markdown = render_slo_markdown_summary(&summary);
        let rendered = serde_json::to_value(&summary).expect("serialize SLO summary");

        assert!(markdown.contains("# Fantasma Derived Metrics SLO Suite"));
        assert!(markdown.contains("- Published suites: representative, mixed-repair"));
        assert!(markdown.contains("- representative append verdict: better"));
        assert!(markdown.contains("- representative checkpoint derived verdict: better"));
        assert!(markdown.contains("- representative final derived verdict: flat"));
        assert!(markdown.contains("- mixed repair verdict: flat"));
        assert!(markdown.contains("- mixed repair checkpoint derived verdict: flat"));
        assert!(markdown.contains("- mixed repair final derived verdict: flat"));
        assert!(markdown.contains("- dominant Stage B phase: changed"));
        assert!(markdown.contains("- overall verdict: better"));
        assert!(markdown.contains("- Run config:"));
        assert!(markdown.contains("## live-append-small-blobs"));
        assert!(markdown.contains("- event_metrics_ready_ms: 20000ms"));
        assert!(markdown.contains("- session_metrics_ready_ms: 40000ms"));
        assert!(markdown.contains("- derived_metrics_ready_ms: 40000ms"));
        assert!(markdown.contains("- checkpoint derived p95: 40000ms"));
        assert!(markdown.contains("- Budget: PASS"));
        assert!(markdown.contains("| events_count_day_grouped | 60 | 80 | 50 | 95 |"));
        assert_eq!(
            rendered["run_config"]["mode"],
            serde_json::json!("iterative")
        );
        assert_eq!(rendered["verdict"]["overall"], serde_json::json!("better"));
        assert_eq!(
            rendered["verdict"]["representative_append"]["checkpoint_derived"],
            serde_json::json!("better")
        );
    }

    #[test]
    fn metric_verdict_uses_the_larger_of_one_second_or_five_percent_threshold() {
        assert_eq!(
            metric_verdict(Some(18_900), Some(20_000)),
            Some(SloVerdictStatus::Better)
        );
        assert_eq!(
            metric_verdict(Some(20_900), Some(20_000)),
            Some(SloVerdictStatus::Flat)
        );
        assert_eq!(
            metric_verdict(Some(21_000), Some(20_000)),
            Some(SloVerdictStatus::Worse)
        );
        assert_eq!(
            metric_verdict(Some(95_000), Some(100_000)),
            Some(SloVerdictStatus::Better)
        );
        assert_eq!(
            metric_verdict(Some(104_999), Some(100_000)),
            Some(SloVerdictStatus::Flat)
        );
        assert_eq!(
            metric_verdict(Some(105_000), Some(100_000)),
            Some(SloVerdictStatus::Worse)
        );
    }

    #[test]
    fn checkpoint_metric_requires_matching_sample_counts() {
        let baseline = SloIterativeBaselineScenario {
            checkpoint_sample_count: Some(10),
            checkpoint_derived_p95_ms: Some(400),
            final_derived_ready_ms: 100,
            dominant_stage_b_phase: None,
        };

        let matching = scenario_verdict(
            Some(&dummy_slo_scenario_result_with_checkpoint_samples(
                10, 400, 100,
            )),
            Some(&baseline),
        )
        .expect("matching sample-count verdict");
        assert_eq!(matching.checkpoint_sample_count_matches, Some(true));
        assert_eq!(matching.checkpoint_derived, Some(SloVerdictStatus::Flat));

        let mismatched = scenario_verdict(
            Some(&dummy_slo_scenario_result_with_checkpoint_samples(
                9, 400, 100,
            )),
            Some(&baseline),
        )
        .expect("mismatched sample-count verdict");
        assert_eq!(mismatched.checkpoint_sample_count_matches, Some(false));
        assert_eq!(mismatched.checkpoint_derived, None);
        assert_eq!(mismatched.final_derived, Some(SloVerdictStatus::Flat));
        assert_eq!(mismatched.overall, SloVerdictStatus::Flat);
    }

    #[test]
    fn overall_slo_verdict_prefers_worse_then_better_then_flat() {
        assert_eq!(
            overall_slo_verdict([Some(SloVerdictStatus::Better), Some(SloVerdictStatus::Flat)]),
            Some(SloVerdictStatus::Better)
        );
        assert_eq!(
            overall_slo_verdict([Some(SloVerdictStatus::Flat), Some(SloVerdictStatus::Flat)]),
            Some(SloVerdictStatus::Flat)
        );
        assert_eq!(
            overall_slo_verdict([
                Some(SloVerdictStatus::Better),
                Some(SloVerdictStatus::Worse)
            ]),
            Some(SloVerdictStatus::Worse)
        );
    }

    #[test]
    fn stage_b_phase_verdict_reports_changed_when_the_dominant_phase_differs() {
        let tempdir = tempdir().expect("create tempdir");
        let scenario_dir = tempdir.path().join("live-append-plus-light-repair");
        fs::create_dir_all(&scenario_dir).expect("create scenario dir");
        fs::write(
            scenario_dir.join(STAGE_B_SUMMARY_FILENAME),
            serde_json::to_vec(&serde_json::json!({
                "dominant_phase": "shared_bucket_finalize"
            }))
            .expect("serialize sidecar"),
        )
        .expect("write sidecar");

        let verdict = stage_b_phase_verdict(
            tempdir.path(),
            Some(&SloScenarioResult {
                scenario: "live-append-plus-light-repair".to_owned(),
                run_config: dummy_slo_run_config(),
                phases: vec![],
                readiness: vec![],
                checkpoint_readiness: None,
                queries: vec![],
                budget: None,
                failure: None,
            }),
            Some(&SloIterativeBaselineScenario {
                checkpoint_sample_count: Some(10),
                checkpoint_derived_p95_ms: Some(20_000),
                final_derived_ready_ms: 25_000,
                dominant_stage_b_phase: Some("session_rewrite".to_owned()),
            }),
        )
        .expect("derive stage-b verdict");

        assert_eq!(verdict, Some(SloStageBPhaseVerdict::Changed));
    }

    #[test]
    fn stage_b_phase_verdict_reports_unknown_when_current_sidecar_is_missing() {
        let tempdir = tempdir().expect("create tempdir");

        let verdict = stage_b_phase_verdict(
            tempdir.path(),
            Some(&dummy_slo_scenario_result_with_checkpoint_samples(
                10, 400, 100,
            )),
            Some(&SloIterativeBaselineScenario {
                checkpoint_sample_count: Some(10),
                checkpoint_derived_p95_ms: Some(400),
                final_derived_ready_ms: 100,
                dominant_stage_b_phase: Some("shared_bucket_finalize".to_owned()),
            }),
        )
        .expect("derive stage-b verdict");

        assert_eq!(verdict, Some(SloStageBPhaseVerdict::Unknown));
    }

    fn dummy_slo_scenario_result_with_checkpoint_samples(
        sample_count: usize,
        checkpoint_derived_p95_ms: u64,
        final_derived_ready_ms: u64,
    ) -> SloScenarioResult {
        SloScenarioResult {
            scenario: "live-append-plus-light-repair".to_owned(),
            run_config: dummy_slo_run_config(),
            phases: vec![],
            readiness: vec![ReadinessMeasurement {
                name: "derived_metrics_ready_ms".to_owned(),
                elapsed_ms: final_derived_ready_ms,
            }],
            checkpoint_readiness: Some(CheckpointReadinessSummary {
                sample_count,
                event_metrics_ready_p50_ms: checkpoint_derived_p95_ms,
                event_metrics_ready_p95_ms: checkpoint_derived_p95_ms,
                event_metrics_ready_max_ms: checkpoint_derived_p95_ms,
                session_metrics_ready_p50_ms: checkpoint_derived_p95_ms,
                session_metrics_ready_p95_ms: checkpoint_derived_p95_ms,
                session_metrics_ready_max_ms: checkpoint_derived_p95_ms,
                derived_metrics_ready_p50_ms: checkpoint_derived_p95_ms,
                derived_metrics_ready_p95_ms: checkpoint_derived_p95_ms,
                derived_metrics_ready_max_ms: checkpoint_derived_p95_ms,
            }),
            queries: vec![],
            budget: None,
            failure: None,
        }
    }

    #[test]
    fn slo_summary_json_rendering_preserves_family_readiness_fields() {
        let summary = SloSummary {
            host: HostMetadata {
                environment: "local".to_owned(),
            },
            suites: vec!["reads-visibility".to_owned()],
            run_config: dummy_slo_run_config(),
            verdict: None,
            scenarios: vec![SloScenarioResult {
                scenario: "reads-visibility-30d".to_owned(),
                run_config: dummy_slo_run_config(),
                phases: vec![],
                readiness: vec![
                    ReadinessMeasurement {
                        name: "event_metrics_ready_ms".to_owned(),
                        elapsed_ms: 18_000,
                    },
                    ReadinessMeasurement {
                        name: "session_metrics_ready_ms".to_owned(),
                        elapsed_ms: 35_000,
                    },
                    ReadinessMeasurement {
                        name: "derived_metrics_ready_ms".to_owned(),
                        elapsed_ms: 35_000,
                    },
                ],
                checkpoint_readiness: None,
                queries: vec![],
                budget: Some(BudgetEvaluation {
                    passed: true,
                    failures: vec![],
                }),
                failure: None,
            }],
        };

        let rendered = serde_json::to_value(&summary).expect("serialize SLO summary");

        assert_eq!(rendered["scenarios"][0]["scenario"], "reads-visibility-30d");
        assert_eq!(
            rendered["scenarios"][0]["readiness"][0]["name"],
            "event_metrics_ready_ms"
        );
        assert_eq!(
            rendered["scenarios"][0]["readiness"][1]["name"],
            "session_metrics_ready_ms"
        );
        assert_eq!(
            rendered["scenarios"][0]["readiness"][2]["name"],
            "derived_metrics_ready_ms"
        );
        assert_eq!(
            rendered["run_config"]["worker_config"]["session_batch_size"],
            serde_json::json!(DEFAULT_SLO_SESSION_BATCH_SIZE)
        );
    }

    #[test]
    fn heavy_profile_uses_publishable_local_workload_sizes() {
        let config = ProfileConfig::for_profile(Profile::Heavy);

        assert_eq!(config.hot_path_install_count, 3_000);
        assert_eq!(config.repair_group_count, 600);
        assert_eq!(config.scale_day_count, 60);
        assert_eq!(config.scale_install_count_per_day, 1_000);
        assert_eq!(config.warmup_queries, 10);
        assert_eq!(config.measured_queries, 100);
        assert_eq!(config.settle_timeout, Duration::from_secs(180));
    }

    #[test]
    fn aggregate_scenario_runs_uses_medians_for_each_measurement() {
        let aggregated = aggregate_scenario_runs(
            Scenario::Hot,
            Profile::Heavy,
            &[
                ScenarioResult {
                    scenario: Scenario::Hot,
                    profile: Profile::Heavy,
                    phases: vec![PhaseMeasurement {
                        name: "ingest".to_owned(),
                        events_sent: 9_000,
                        elapsed_ms: 1_100,
                        events_per_second: 8_181.82,
                    }],
                    readiness: vec![ReadinessMeasurement {
                        name: "derived_metrics_ready".to_owned(),
                        elapsed_ms: 420,
                    }],
                    queries: vec![QueryMeasurement {
                        name: "events_day_dim2".to_owned(),
                        iterations: 100,
                        min_ms: 4,
                        p50_ms: 7,
                        p95_ms: 10,
                        max_ms: 14,
                    }],
                    budget: None,
                },
                ScenarioResult {
                    scenario: Scenario::Hot,
                    profile: Profile::Heavy,
                    phases: vec![PhaseMeasurement {
                        name: "ingest".to_owned(),
                        events_sent: 9_000,
                        elapsed_ms: 1_000,
                        events_per_second: 9_000.0,
                    }],
                    readiness: vec![ReadinessMeasurement {
                        name: "derived_metrics_ready".to_owned(),
                        elapsed_ms: 400,
                    }],
                    queries: vec![QueryMeasurement {
                        name: "events_day_dim2".to_owned(),
                        iterations: 100,
                        min_ms: 3,
                        p50_ms: 6,
                        p95_ms: 9,
                        max_ms: 12,
                    }],
                    budget: None,
                },
                ScenarioResult {
                    scenario: Scenario::Hot,
                    profile: Profile::Heavy,
                    phases: vec![PhaseMeasurement {
                        name: "ingest".to_owned(),
                        events_sent: 9_000,
                        elapsed_ms: 900,
                        events_per_second: 10_000.0,
                    }],
                    readiness: vec![ReadinessMeasurement {
                        name: "derived_metrics_ready".to_owned(),
                        elapsed_ms: 390,
                    }],
                    queries: vec![QueryMeasurement {
                        name: "events_day_dim2".to_owned(),
                        iterations: 100,
                        min_ms: 2,
                        p50_ms: 5,
                        p95_ms: 8,
                        max_ms: 11,
                    }],
                    budget: None,
                },
                ScenarioResult {
                    scenario: Scenario::Hot,
                    profile: Profile::Heavy,
                    phases: vec![PhaseMeasurement {
                        name: "ingest".to_owned(),
                        events_sent: 9_000,
                        elapsed_ms: 1_050,
                        events_per_second: 8_571.43,
                    }],
                    readiness: vec![ReadinessMeasurement {
                        name: "derived_metrics_ready".to_owned(),
                        elapsed_ms: 410,
                    }],
                    queries: vec![QueryMeasurement {
                        name: "events_day_dim2".to_owned(),
                        iterations: 100,
                        min_ms: 3,
                        p50_ms: 6,
                        p95_ms: 9,
                        max_ms: 13,
                    }],
                    budget: None,
                },
                ScenarioResult {
                    scenario: Scenario::Hot,
                    profile: Profile::Heavy,
                    phases: vec![PhaseMeasurement {
                        name: "ingest".to_owned(),
                        events_sent: 9_000,
                        elapsed_ms: 980,
                        events_per_second: 9_183.67,
                    }],
                    readiness: vec![ReadinessMeasurement {
                        name: "derived_metrics_ready".to_owned(),
                        elapsed_ms: 405,
                    }],
                    queries: vec![QueryMeasurement {
                        name: "events_day_dim2".to_owned(),
                        iterations: 100,
                        min_ms: 3,
                        p50_ms: 6,
                        p95_ms: 9,
                        max_ms: 12,
                    }],
                    budget: None,
                },
            ],
        )
        .expect("aggregate scenario runs");

        assert_eq!(aggregated.scenario, Scenario::Hot);
        assert_eq!(aggregated.profile, Profile::Heavy);
        assert_eq!(aggregated.phases[0].elapsed_ms, 1_000);
        assert_eq!(aggregated.phases[0].events_per_second, 9_000.0);
        assert_eq!(aggregated.readiness[0].elapsed_ms, 405);
        assert_eq!(aggregated.queries[0].min_ms, 3);
        assert_eq!(aggregated.queries[0].p50_ms, 6);
        assert_eq!(aggregated.queries[0].p95_ms, 9);
        assert_eq!(aggregated.queries[0].max_ms, 12);
    }

    #[test]
    fn series_output_paths_match_the_published_layout() {
        let paths = series_paths(
            Path::new("artifacts/performance/2026-03-13-m3-pro-heavy"),
            Scenario::Repair,
        );

        assert_eq!(
            paths.run_output(1),
            PathBuf::from("artifacts/performance/2026-03-13-m3-pro-heavy/repair-path/run-01.json")
        );
        assert_eq!(
            paths.run_output(5),
            PathBuf::from("artifacts/performance/2026-03-13-m3-pro-heavy/repair-path/run-05.json")
        );
        assert_eq!(
            paths.median_output(),
            PathBuf::from("artifacts/performance/2026-03-13-m3-pro-heavy/repair-path/median.json")
        );
        assert_eq!(
            summary_output_path(Path::new("artifacts/performance/2026-03-13-m3-pro-heavy")),
            PathBuf::from("artifacts/performance/2026-03-13-m3-pro-heavy/summary.json")
        );
        assert_eq!(
            host_metadata_output_path(Path::new("artifacts/performance/2026-03-13-m3-pro-heavy")),
            PathBuf::from("artifacts/performance/2026-03-13-m3-pro-heavy/host.json")
        );
    }

    #[test]
    fn render_series_markdown_summary_includes_host_and_median_results() {
        let summary = SeriesSummary {
            profile: Profile::Heavy,
            repetitions: 5,
            host: HostMetadata {
                environment: "local".to_owned(),
            },
            scenarios: vec![ScenarioResult {
                scenario: Scenario::Scale,
                profile: Profile::Heavy,
                phases: vec![PhaseMeasurement {
                    name: "ingest".to_owned(),
                    events_sent: 180_000,
                    elapsed_ms: 19_000,
                    events_per_second: 9_473.68,
                }],
                readiness: vec![ReadinessMeasurement {
                    name: "derived_metrics_ready".to_owned(),
                    elapsed_ms: 18_500,
                }],
                queries: vec![QueryMeasurement {
                    name: "events_hour_dim2".to_owned(),
                    iterations: 100,
                    min_ms: 9,
                    p50_ms: 14,
                    p95_ms: 19,
                    max_ms: 31,
                }],
                budget: None,
            }],
        };

        let markdown = render_series_markdown_summary(&summary);

        assert!(markdown.contains("# Fantasma Heavy Benchmark Series"));
        assert!(markdown.contains("- Profile: heavy"));
        assert!(markdown.contains("- Repetitions per scenario: 5"));
        assert!(markdown.contains("- Environment: local"));
        assert!(markdown.contains("## scale-path"));
        assert!(markdown.contains("- ingest: 180000 events in 19000ms (9473.68 events/s)"));
        assert!(markdown.contains("| events_hour_dim2 | 14 | 19 | 9 | 31 |"));
    }

    #[test]
    fn scenario_serde_keeps_public_path_names() {
        assert_eq!(
            serde_json::to_string(&Scenario::Hot).expect("serialize hot scenario"),
            "\"hot-path\""
        );
        assert_eq!(
            serde_json::to_string(&Scenario::Repair).expect("serialize repair scenario"),
            "\"repair-path\""
        );
        assert_eq!(
            serde_json::to_string(&Scenario::Scale).expect("serialize scale scenario"),
            "\"scale-path\""
        );

        assert_eq!(
            serde_json::from_str::<Scenario>("\"hot-path\"").expect("deserialize hot-path"),
            Scenario::Hot
        );
        assert_eq!(
            serde_json::from_str::<Scenario>("\"repair-path\"").expect("deserialize repair-path"),
            Scenario::Repair
        );
        assert_eq!(
            serde_json::from_str::<Scenario>("\"scale-path\"").expect("deserialize scale-path"),
            Scenario::Scale
        );
    }

    #[test]
    fn ci_budget_flags_threshold_failures() {
        let budget = ScenarioBudget {
            min_phase_events_per_second: BTreeMap::from([("ingest".to_owned(), 500.0)]),
            max_readiness_ms: BTreeMap::from([("derived_metrics_ready".to_owned(), 1_000)]),
            max_query_p95_ms: BTreeMap::from([("events_day_dim2".to_owned(), 200)]),
        };
        let result = ScenarioResult {
            scenario: Scenario::Hot,
            profile: Profile::Ci,
            phases: vec![PhaseMeasurement {
                name: "ingest".to_owned(),
                events_sent: 200,
                elapsed_ms: 1_000,
                events_per_second: 200.0,
            }],
            readiness: vec![ReadinessMeasurement {
                name: "derived_metrics_ready".to_owned(),
                elapsed_ms: 1_500,
            }],
            queries: vec![QueryMeasurement {
                name: "events_day_dim2".to_owned(),
                iterations: 5,
                min_ms: 80,
                p50_ms: 120,
                p95_ms: 250,
                max_ms: 300,
            }],
            budget: None,
        };

        let evaluation = evaluate_budget(&budget, &result);

        assert!(!evaluation.passed);
        assert_eq!(
            evaluation.failures,
            vec![
                "ingest events_per_second 200.00 fell below budget 500.00".to_owned(),
                "derived_metrics_ready readiness 1500ms exceeded budget 1000ms".to_owned(),
                "events_day_dim2 p95 250ms exceeded budget 200ms".to_owned(),
            ]
        );
    }

    #[test]
    fn retained_ci_runner_samples_fit_committed_budgets() {
        for (budget, sample) in recent_main_runner_samples() {
            let evaluation = evaluate_budget(&budget, &sample);
            assert!(
                evaluation.passed,
                "committed CI budget rejected retained runner sample for {}: {:?}",
                sample.scenario.key(),
                evaluation.failures
            );
        }
    }

    #[test]
    fn percentile_uses_upper_rank() {
        let samples = vec![5, 10, 20, 30, 40];

        assert_eq!(percentile(&samples, 0.50), 20);
        assert_eq!(percentile(&samples, 0.95), 40);
    }

    #[test]
    fn prepare_stack_scopes_compose_commands_to_benchmark_project() {
        let compose_file = PathBuf::from("/tmp/compose.bench.yaml");
        let mut seen = Vec::<Vec<String>>::new();

        prepare_stack(&compose_file, |command| {
            seen.push(snapshot_command(command));
            Ok(())
        })
        .expect("prepare stack succeeds");

        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0], vec!["docker".to_owned(), "info".to_owned()]);
        assert_eq!(
            seen[1],
            vec![
                "docker".to_owned(),
                "compose".to_owned(),
                "-p".to_owned(),
                benchmark_compose_project_name().to_owned(),
                "-f".to_owned(),
                compose_file.display().to_string(),
                "down".to_owned(),
                "--volumes".to_owned(),
                "--remove-orphans".to_owned(),
            ]
        );
        assert_eq!(
            seen[2],
            vec![
                "docker".to_owned(),
                "compose".to_owned(),
                "-p".to_owned(),
                benchmark_compose_project_name().to_owned(),
                "-f".to_owned(),
                compose_file.display().to_string(),
                "up".to_owned(),
                "-d".to_owned(),
                "--build".to_owned(),
            ]
        );
        assert_eq!(benchmark_ingest_base_url(), "http://127.0.0.1:18081");
        assert_eq!(benchmark_api_base_url(), "http://127.0.0.1:18082");
    }

    #[test]
    fn render_benchmark_compose_template_rewrites_worker_env_values() {
        let rendered = render_benchmark_compose_template(
            include_str!("../../../infra/docker/compose.bench.yaml"),
            &BenchWorkerConfig {
                session_batch_size: 2_000,
                event_batch_size: 7_000,
                session_incremental_concurrency: 12,
                session_repair_concurrency: 4,
                idle_poll_interval_ms: 75,
            },
        );

        assert!(rendered.contains("FANTASMA_WORKER_IDLE_POLL_INTERVAL_MS: 75"));
        assert!(rendered.contains("FANTASMA_WORKER_SESSION_BATCH_SIZE: 2000"));
        assert!(rendered.contains("FANTASMA_WORKER_EVENT_BATCH_SIZE: 7000"));
        assert!(rendered.contains("FANTASMA_WORKER_SESSION_INCREMENTAL_CONCURRENCY: 12"));
        assert!(rendered.contains("FANTASMA_WORKER_SESSION_REPAIR_CONCURRENCY: 4"));
        assert!(rendered.contains("FANTASMA_WORKER_STAGE_B_TRACE_PATH: /tmp/stage-b-trace.jsonl"));
        assert!(
            rendered.contains("FANTASMA_WORKER_EVENT_LANE_TRACE_PATH: /tmp/event-lane-trace.jsonl")
        );
    }

    #[test]
    fn parse_docker_compose_port_output_accepts_ipv4_and_ipv6_bindings() {
        assert_eq!(
            parse_docker_compose_port_output("127.0.0.1:54321\n").expect("parse ipv4 binding"),
            54_321
        );
        assert_eq!(
            parse_docker_compose_port_output("[::1]:40123\n").expect("parse ipv6 binding"),
            40_123
        );
    }

    #[test]
    fn write_stage_b_sidecars_summarizes_trace_records_without_touching_main_artifacts() {
        let tempdir = tempdir().expect("create tempdir");
        let scenario_dir = tempdir.path().join("backfill-30d");

        let wrote_sidecars = write_stage_b_sidecars(
            &scenario_dir,
            "backfill-30d",
            &dummy_slo_run_config(),
            sample_stage_b_trace_contents(),
        )
        .expect("write stage-b sidecars");

        assert!(wrote_sidecars, "expected trace data to produce sidecars");
        assert!(!scenario_dir.join("result.json").exists());
        assert!(!scenario_dir.join("summary.json").exists());
        assert!(!scenario_dir.join("host.json").exists());

        let trace = fs::read_to_string(scenario_dir.join("stage-b-trace.jsonl"))
            .expect("read stage-b trace sidecar");
        assert_eq!(trace, sample_stage_b_trace_contents());

        let summary: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(scenario_dir.join("stage-b.json")).expect("read stage-b summary"),
        )
        .expect("parse stage-b summary");
        assert_eq!(summary["scenario"], "backfill-30d");
        assert_eq!(
            summary["run_config"]["worker_config"]["session_batch_size"],
            serde_json::json!(DEFAULT_SLO_SESSION_BATCH_SIZE)
        );
        assert_eq!(summary["dominant_phase"], "session_rewrite");
        assert_eq!(summary["record_type_counts"]["repair_job"], 2);
        assert_eq!(summary["record_type_counts"]["rebuild_finalize"], 1);
        assert_eq!(summary["total_counts"]["window_raw_events"], 15);
        assert_eq!(
            summary["slowest_records_by_type"]["repair_job"][0]["install_id"],
            "install-2"
        );
        assert_eq!(
            summary["slowest_records_by_type"]["rebuild_finalize"][0]["record_type"],
            "rebuild_finalize"
        );
    }

    #[test]
    fn write_stage_b_sidecars_skips_output_when_trace_records_are_empty() {
        let tempdir = tempdir().expect("create tempdir");
        let scenario_dir = tempdir.path().join("append-30d");

        let wrote_sidecars =
            write_stage_b_sidecars(&scenario_dir, "append-30d", &dummy_slo_run_config(), "\n")
                .expect("skip empty stage-b trace");

        assert!(!wrote_sidecars, "empty trace should not produce sidecars");
        assert!(!scenario_dir.join("stage-b-trace.jsonl").exists());
        assert!(!scenario_dir.join("stage-b.json").exists());
    }

    #[test]
    fn summarize_append_attribution_samples_uses_latest_lane_gap_for_visibility() {
        let summary = summarize_append_attribution_samples(&[
            AppendAttributionSample {
                sequence: 1,
                blob_kind: "append".to_owned(),
                events_in_blob: 5,
                cumulative_events: 5,
                checkpoint_target_event_id: 5,
                upload_complete_ms: 20,
                event_lane_expected_ready_ms: 45,
                session_lane_expected_ready_ms: 60,
                derived_ready_ms: 90,
            },
            AppendAttributionSample {
                sequence: 2,
                blob_kind: "append".to_owned(),
                events_in_blob: 5,
                cumulative_events: 10,
                checkpoint_target_event_id: 10,
                upload_complete_ms: 25,
                event_lane_expected_ready_ms: 80,
                session_lane_expected_ready_ms: 55,
                derived_ready_ms: 95,
            },
        ])
        .expect("append attribution summary");

        assert_eq!(summary.sample_count, 2);
        assert_eq!(summary.upload_to_event_lane_ready_p50_ms, 25);
        assert_eq!(summary.upload_to_event_lane_ready_p95_ms, 55);
        assert_eq!(summary.upload_to_session_lane_ready_p50_ms, 30);
        assert_eq!(summary.upload_to_session_lane_ready_p95_ms, 40);
        assert_eq!(summary.latest_lane_to_derived_visible_p50_ms, 15);
        assert_eq!(summary.latest_lane_to_derived_visible_p95_ms, 30);
        assert_eq!(summary.overall_derived_ready_p50_ms, 90);
        assert_eq!(summary.overall_derived_ready_p95_ms, 95);
        assert_eq!(
            summary.dominant_contributor,
            Some("upload_to_event_lane_ready".to_owned())
        );
    }

    #[test]
    fn write_append_attribution_sidecar_writes_summary_and_samples() {
        let tempdir = tempdir().expect("create tempdir");
        let scenario_dir = tempdir.path().join("live-append-small-blobs");
        let samples = vec![AppendAttributionSample {
            sequence: 1,
            blob_kind: "append".to_owned(),
            events_in_blob: 5,
            cumulative_events: 5,
            checkpoint_target_event_id: 5,
            upload_complete_ms: 20,
            event_lane_expected_ready_ms: 50,
            session_lane_expected_ready_ms: 65,
            derived_ready_ms: 90,
        }];
        let event_lane_trace = r#"{"record_type":"event_metrics_batch","last_processed_event_id":0,"next_offset":5,"processed_events":5,"phases_ms":{"build_rollups":17,"upsert_total":5,"upsert_dim1":7,"commit":3},"counts":{"total_delta_rows":2,"dim1_delta_rows":6}}
"#;

        write_append_attribution_sidecar(
            &scenario_dir,
            "live-append-small-blobs",
            &dummy_slo_run_config(),
            &samples,
            Some(event_lane_trace),
        )
        .expect("write append attribution sidecar");

        let sidecar: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(scenario_dir.join("append-attribution.json"))
                .expect("read append attribution sidecar"),
        )
        .expect("parse append attribution sidecar");

        assert_eq!(sidecar["scenario"], "live-append-small-blobs");
        assert_eq!(sidecar["samples"][0]["checkpoint_target_event_id"], 5);
        assert_eq!(sidecar["summary"]["sample_count"], 1);
        assert_eq!(sidecar["summary"]["upload_to_event_lane_ready_p95_ms"], 30);
        assert_eq!(
            sidecar["summary"]["latest_lane_to_derived_visible_p95_ms"],
            25
        );
        assert_eq!(
            sidecar["event_lane_summary"]["dominant_phase"],
            "build_rollups"
        );
        assert_eq!(
            sidecar["event_lane_summary"]["delta_rows_by_family"]["dim1_delta_rows"],
            6
        );
        assert_eq!(
            fs::read_to_string(scenario_dir.join("event-lane-trace.jsonl"))
                .expect("read event lane trace sidecar"),
            event_lane_trace
        );
    }

    #[test]
    fn summarize_event_lane_trace_records_surfaces_dominant_phase_and_delta_rows() {
        let records = parse_event_lane_trace_records(
            r#"{"record_type":"event_metrics_batch","last_processed_event_id":0,"next_offset":5,"processed_events":5,"phases_ms":{"build_rollups":17,"upsert_total":5,"upsert_dim1":7,"commit":3},"counts":{"total_delta_rows":2,"dim1_delta_rows":6,"dim2_delta_rows":8}}
{"record_type":"event_metrics_batch","last_processed_event_id":5,"next_offset":10,"processed_events":5,"phases_ms":{"build_rollups":19,"upsert_total":6,"upsert_dim1":8,"commit":4},"counts":{"total_delta_rows":2,"dim1_delta_rows":6,"dim2_delta_rows":8}}
"#,
        )
        .expect("parse event lane traces");
        let summary = summarize_event_lane_trace_records(&records).expect("event lane summary");

        assert_eq!(summary.record_count, 2);
        assert_eq!(summary.processed_events_total, 10);
        assert_eq!(summary.delta_rows_by_family["dim2_delta_rows"], 16);
        assert_eq!(summary.phases[0].name, "build_rollups");
        assert_eq!(summary.phases[0].distribution.p95_ms, 19);
        assert_eq!(summary.dominant_phase.as_deref(), Some("build_rollups"));
    }

    #[test]
    fn summarize_stage_b_trace_records_reports_null_when_no_phase_is_dominant() {
        let summary: serde_json::Value = serde_json::to_value(summarize_stage_b_trace_records(
            "repair-30d",
            &dummy_slo_run_config(),
            &parse_stage_b_trace_records(
                r#"{"record_type":"repair_job","install_id":"install-1","target_event_id":10,"phases_ms":{"row_mutation_lag_before_claim":40,"session_rewrite":45,"recompute_window_load":45},"counts":{"pending_frontier_events":2}}
{"record_type":"repair_job","install_id":"install-2","target_event_id":20,"phases_ms":{"row_mutation_lag_before_claim":40,"session_rewrite":45,"recompute_window_load":45},"counts":{"pending_frontier_events":3}}
"#,
            )
            .expect("parse stage-b trace"),
        ))
        .expect("serialize summary");

        assert_eq!(summary["dominant_phase"], serde_json::Value::Null);
        assert_eq!(
            summary["dominant_finalization_subphase"],
            serde_json::Value::Null
        );
        assert_eq!(summary["finalization_subphases"], serde_json::Value::Null);
    }

    #[test]
    fn summarize_stage_b_trace_records_separates_claim_lag_backlog_from_active_work() {
        let summary = summarize_stage_b_trace_records(
            "repair-30d",
            &dummy_slo_run_config(),
            &parse_stage_b_trace_records(
                r#"{"record_type":"repair_job","install_id":"install-1","target_event_id":10,"phases_ms":{"row_mutation_lag_before_claim":1000,"session_rewrite":25},"counts":{"pending_frontier_events":2}}
{"record_type":"rebuild_finalize","phases_ms":{"shared_bucket_finalize":50},"counts":{"claimed_rebuild_bucket_rows":3}}
"#,
            )
            .expect("parse stage-b trace"),
        );

        assert_eq!(summary.measured_stage_b_ms, 75);
        assert_eq!(
            summary.dominant_phase,
            Some("shared_bucket_finalize".to_owned())
        );
        assert_eq!(summary.backlog_context.len(), 1);
        assert_eq!(
            summary.backlog_context[0].name,
            "row_mutation_lag_before_claim"
        );
        assert_eq!(summary.backlog_context[0].total_ms, 1000);
        assert_eq!(summary.backlog_context[0].distribution.p95_ms, 1000);
    }

    #[test]
    fn summarize_stage_b_trace_records_reports_finalization_subphases_without_changing_top_level_accounting()
     {
        let summary: serde_json::Value = serde_json::to_value(summarize_stage_b_trace_records(
            "backfill-30d",
            &dummy_slo_run_config(),
            &parse_stage_b_trace_records(
                r#"{"record_type":"repair_job","install_id":"install-1","target_event_id":10,"phases_ms":{"session_rewrite":25},"counts":{"pending_frontier_events":2}}
{"record_type":"rebuild_finalize","phases_ms":{"shared_bucket_finalize":90},"finalization_subphases_ms":{"finalization_drain_setup":10,"finalization_session_daily_rebuild":15,"finalization_metric_total_rebuild":15,"finalization_metric_dim1_rebuild":40,"finalization_metric_dim2_rebuild":10},"session_daily_subphases_ms":{"session_daily_source_build":4,"session_daily_installs_write":5,"session_daily_aggregate_write":6},"counts":{"claimed_rebuild_bucket_rows":3}}
"#,
            )
            .expect("parse stage-b trace"),
        ))
        .expect("serialize summary");

        assert_eq!(summary["dominant_phase"], "shared_bucket_finalize");
        assert_eq!(
            summary["dominant_finalization_subphase"],
            "finalization_metric_dim1_rebuild"
        );
        assert_eq!(
            summary["finalization_subphases"][0]["name"],
            "finalization_metric_dim1_rebuild"
        );
        assert_eq!(
            summary["finalization_subphases"][0]["total_ms"],
            serde_json::json!(40)
        );
        assert_eq!(
            summary["finalization_subphases"][0]["distribution"]["p95_ms"],
            serde_json::json!(40)
        );
        assert_eq!(
            summary["session_daily_subphases"][0]["name"],
            "session_daily_aggregate_write"
        );
        assert_eq!(
            summary["dominant_session_daily_subphase"],
            serde_json::Value::Null
        );
        assert_eq!(summary["measured_stage_b_ms"], serde_json::json!(115));
    }

    #[test]
    fn summarize_stage_b_trace_records_reports_session_daily_subphases_without_changing_finalization_shape()
     {
        let summary: serde_json::Value = serde_json::to_value(summarize_stage_b_trace_records(
            "repair-30d",
            &dummy_slo_run_config(),
            &parse_stage_b_trace_records(
                r#"{"record_type":"rebuild_finalize","phases_ms":{"shared_bucket_finalize":50},"finalization_subphases_ms":{"finalization_session_daily_rebuild":50},"session_daily_subphases_ms":{"session_daily_source_build":5,"session_daily_installs_write":10,"session_daily_aggregate_write":35},"counts":{"claimed_rebuild_bucket_rows":2}}
"#,
            )
            .expect("parse stage-b trace"),
        ))
        .expect("serialize summary");

        assert_eq!(
            summary["dominant_finalization_subphase"],
            "finalization_session_daily_rebuild"
        );
        assert_eq!(
            summary["session_daily_subphases"][0]["name"],
            "session_daily_aggregate_write"
        );
        assert_eq!(
            summary["dominant_session_daily_subphase"],
            "session_daily_aggregate_write"
        );
        assert_eq!(
            summary["finalization_subphases"][0]["name"],
            "finalization_session_daily_rebuild"
        );
    }

    #[test]
    fn summarize_stage_b_trace_records_does_not_fabricate_session_daily_subphases_when_absent() {
        let summary: serde_json::Value = serde_json::to_value(summarize_stage_b_trace_records(
            "append-30d",
            &dummy_slo_run_config(),
            &parse_stage_b_trace_records(
                r#"{"record_type":"rebuild_finalize","phases_ms":{"shared_bucket_finalize":10},"finalization_subphases_ms":{"finalization_metric_total_rebuild":10},"counts":{"claimed_rebuild_bucket_rows":1}}
"#,
            )
            .expect("parse stage-b trace"),
        ))
        .expect("serialize summary");

        assert_eq!(summary["session_daily_subphases"], serde_json::Value::Null);
        assert_eq!(
            summary["dominant_session_daily_subphase"],
            serde_json::Value::Null
        );
    }

    fn sample_stage_b_trace_contents() -> &'static str {
        r#"{"record_type":"repair_job","install_id":"install-1","target_event_id":11,"phases_ms":{"queue_wait_claim":5,"frontier_fetch":7,"recompute_window_load":12,"session_rewrite":44,"touched_bucket_enqueue":3},"counts":{"pending_frontier_events":4,"window_raw_events":9}}
{"record_type":"repair_job","install_id":"install-2","target_event_id":22,"phases_ms":{"queue_wait_claim":6,"frontier_fetch":8,"recompute_window_load":14,"session_rewrite":51,"touched_bucket_enqueue":4},"counts":{"pending_frontier_events":5,"window_raw_events":6}}
{"record_type":"rebuild_finalize","phases_ms":{"shared_bucket_finalize":31},"finalization_subphases_ms":{"finalization_drain_setup":3,"finalization_session_daily_rebuild":4,"finalization_metric_total_rebuild":5,"finalization_metric_dim1_rebuild":14,"finalization_metric_dim2_rebuild":5},"session_daily_subphases_ms":{"session_daily_source_build":1,"session_daily_installs_write":1,"session_daily_aggregate_write":2},"counts":{"claimed_rebuild_bucket_rows":12,"touched_day_buckets":2}}
"#
    }

    #[test]
    fn checked_in_compose_defaults_match_worker_defaults() {
        for compose in [
            include_str!("../../../infra/docker/compose.yaml"),
            include_str!("../../../infra/docker/compose.bench.yaml"),
        ] {
            assert!(compose.contains("FANTASMA_WORKER_SESSION_BATCH_SIZE: 2000"));
            assert!(compose.contains("FANTASMA_WORKER_EVENT_BATCH_SIZE: 5000"));
            assert!(compose.contains("FANTASMA_WORKER_SESSION_INCREMENTAL_CONCURRENCY: 8"));
            assert!(compose.contains("FANTASMA_WORKER_SESSION_REPAIR_CONCURRENCY: 2"));
        }
    }

    #[test]
    fn prepare_stack_stops_before_up_when_cleanup_fails() {
        let compose_file = PathBuf::from("/tmp/compose.bench.yaml");
        let mut seen = Vec::<Vec<String>>::new();

        let error = prepare_stack(&compose_file, |command| {
            let snapshot = snapshot_command(command);
            let is_cleanup = snapshot.iter().any(|argument| argument == "down");
            seen.push(snapshot);
            if is_cleanup {
                return Err(anyhow!("cleanup failed"));
            }

            Ok(())
        })
        .expect_err("cleanup failure should abort startup");

        assert_eq!(seen.len(), 2, "startup must stop before docker compose up");
        assert_eq!(
            seen.last().expect("cleanup command recorded"),
            &vec![
                "docker".to_owned(),
                "compose".to_owned(),
                "-p".to_owned(),
                benchmark_compose_project_name().to_owned(),
                "-f".to_owned(),
                compose_file.display().to_string(),
                "down".to_owned(),
                "--volumes".to_owned(),
                "--remove-orphans".to_owned(),
            ]
        );
        assert!(
            error.to_string().contains("cleanup failed"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn benchmark_query_urls_do_not_embed_project_ids() {
        for url in hot_path_query_urls().values() {
            assert!(
                !url.contains("project_id="),
                "hot-path query url leaked project_id: {url}"
            );
        }

        for url in repair_path_query_urls().values() {
            assert!(
                !url.contains("project_id="),
                "repair-path query url leaked project_id: {url}"
            );
        }

        for url in scale_path_query_urls(
            NaiveDate::from_ymd_opt(2026, 1, 1).expect("start day"),
            NaiveDate::from_ymd_opt(2026, 1, 3).expect("end day"),
        )
        .values()
        {
            assert!(
                !url.contains("project_id="),
                "scale-path query url leaked project_id: {url}"
            );
        }
    }
}
