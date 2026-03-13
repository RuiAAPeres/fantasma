use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, TimeZone, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const DEFAULT_ADMIN_TOKEN: &str = "fg_pat_dev";
const BENCHMARK_COMPOSE_PROJECT_NAME: &str = "fantasma-bench";
const BENCHMARK_PROJECT_NAME: &str = "Fantasma Benchmark";
const INGEST_BASE_URL: &str = "http://127.0.0.1:18081";
const API_BASE_URL: &str = "http://127.0.0.1:18082";
const POST_BATCH_SIZE: usize = 100;
const HEALTH_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const PROVIDERS: [&str; 4] = ["strava", "garmin", "polar", "oura"];
const REGIONS: [&str; 4] = ["eu", "us", "apac", "latam"];
const PLANS: [&str; 3] = ["free", "pro", "team"];
const APP_VERSIONS: [&str; 3] = ["1.0.0", "1.1.0", "1.2.0"];
const OS_VERSIONS: [&str; 2] = ["18.3", "18.4"];

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BenchCommand {
    Stack(StackArgs),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StackArgs {
    scenario: Scenario,
    profile: Profile,
    output: PathBuf,
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
    id: String,
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
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    match parse_args(std::env::args_os())? {
        BenchCommand::Stack(args) => run_stack(args).await,
    }
}

async fn run_stack(args: StackArgs) -> Result<()> {
    let profile_config = ProfileConfig::for_profile(args.profile);
    let budget = load_budget(args.scenario, args.profile)?;
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("build HTTP client")?;

    let mut stack = StackGuard::new(compose_file_path());
    stack.start()?;
    wait_for_health(&client).await?;
    let project = provision_project(&client).await?;

    let mut result = match args.scenario {
        Scenario::Hot => run_hot_path(&client, &project, args.profile, profile_config).await?,
        Scenario::Repair => {
            run_repair_path(&client, &project, args.profile, profile_config).await?
        }
        Scenario::Scale => run_scale_path(&client, &project, args.profile, profile_config).await?,
    };
    result.budget = budget
        .as_ref()
        .map(|budget| evaluate_budget(budget, &result));

    write_result(&args.output, &result)?;
    println!("{}", render_markdown_summary(&result));

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
    }

    Ok(())
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
        let region = REGIONS[(install_index / PROVIDERS.len()) % REGIONS.len()];
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
                    ("region".to_owned(), region.to_owned()),
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
        let region = REGIONS[(group_index / PROVIDERS.len()) % REGIONS.len()];
        let plan = PLANS[group_index % PLANS.len()];
        let app_version = APP_VERSIONS[group_index % APP_VERSIONS.len()];
        let os_version = OS_VERSIONS[group_index % OS_VERSIONS.len()];
        let properties = BTreeMap::from([
            ("plan".to_owned(), plan.to_owned()),
            ("provider".to_owned(), provider.to_owned()),
            ("region".to_owned(), region.to_owned()),
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
            let region = REGIONS[(install_index / PROVIDERS.len()) % REGIONS.len()];
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
                        ("region".to_owned(), region.to_owned()),
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
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-01&platform=ios&group_by=provider&group_by=region",
                benchmark_api_base_url()
            ),
        ),
        (
            "events_hour_dim2",
            format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=hour&start=2026-03-01T00:00:00Z&end=2026-03-01T23:00:00Z&platform=ios&group_by=provider&group_by=region",
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
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-01-01&end=2026-01-03&platform=ios&group_by=provider&group_by=region",
                benchmark_api_base_url()
            ),
        ),
        (
            "events_hour_dim2",
            format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=hour&start=2026-01-01T00:00:00Z&end=2026-01-03T23:00:00Z&platform=ios&group_by=provider&group_by=region",
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
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=day&start={start_day}&end={end_day}&platform=ios&group_by=provider&group_by=region",
                benchmark_api_base_url()
            ),
        ),
        (
            "events_hour_dim2",
            format!(
                "{}/v1/metrics/events?event=app_open&metric=count&granularity=hour&start={start_day}T00:00:00Z&end={end_day}T23:00:00Z&platform=ios&group_by=provider&group_by=region",
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

fn write_result(output: &Path, result: &ScenarioResult) -> Result<()> {
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }

    fs::write(
        output,
        serde_json::to_string_pretty(result).context("serialize benchmark result")?,
    )
    .with_context(|| format!("write {}", output.display()))?;
    fs::write(output.with_extension("md"), render_markdown_summary(result))
        .with_context(|| format!("write {}", output.with_extension("md").display()))?;

    Ok(())
}

fn render_markdown_summary(result: &ScenarioResult) -> String {
    let mut lines = vec![
        format!(
            "# Fantasma Benchmark: {} ({})",
            result.scenario.key(),
            match result.profile {
                Profile::Ci => "ci",
                Profile::Extended => "extended",
            }
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

fn compose_file_path() -> PathBuf {
    repo_root().join("infra/docker/compose.bench.yaml")
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
    std::env::var("FANTASMA_ADMIN_TOKEN").unwrap_or_else(|_| DEFAULT_ADMIN_TOKEN.to_owned())
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
    started: bool,
}

impl StackGuard {
    fn new(compose_file: PathBuf) -> Self {
        Self {
            compose_file,
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
    args: impl IntoIterator<Item = &'static str>,
) -> Command {
    let mut command = Command::new("docker");
    command
        .arg("compose")
        .arg("-p")
        .arg(benchmark_compose_project_name())
        .arg("-f")
        .arg(compose_file);
    command.args(args);
    command
}

fn run_command(command: &mut Command) -> Result<()> {
    let output = command.output().context("run command")?;
    if output.status.success() {
        return Ok(());
    }

    bail!(
        "command failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn run_best_effort(command: &mut Command) {
    let _ = command.output();
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use std::fs;

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
