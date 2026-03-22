use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get},
};
use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, NaiveDate, SecondsFormat, Timelike, Utc,
    Weekday,
};
use fantasma_auth::StaticAdminAuthorizer;
use fantasma_core::{
    ActiveInstallsPoint, ActiveInstallsResponse, ActiveInstallsSeries, CurrentMetricResponse,
    EventMetric, EventMetricsQuery, MetricGranularity, MetricInterval, MetricsBucketWindow,
    MetricsPoint, MetricsResponse, MetricsSeries, SessionMetric, SessionMetricsQuery,
    UsageEventsResponse,
};
use fantasma_store::{
    ApiKeyKind, EventCatalogRecord, EventMetricsAggregateCubeRow, EventMetricsCubeRow, PgPool,
    ProjectDeletionRecord, ProjectRecord, ProjectState, ResolvedApiKey,
    SessionMetricsAggregateCubeRow, SessionMetricsCubeRow, StartProjectDeletionResult, StoreError,
    TopEventRecord, create_api_key, create_project_with_api_key, deserialize_active_install_bitmap,
    enqueue_project_purge, enqueue_range_deletion, fetch_event_metrics_aggregate_cube_rows_in_tx,
    fetch_event_metrics_cube_rows_in_tx, fetch_session_metrics_aggregate_cube_rows_in_tx,
    fetch_session_metrics_cube_rows_in_tx, generate_api_key_secret, list_api_keys,
    list_event_catalog, list_project_deletion_jobs, list_projects, list_top_events,
    list_usage_events, load_active_install_bitmap_totals_in_executor,
    load_live_install_count_in_tx, load_project, load_project_deletion_job, rename_project,
    resolve_api_key, revoke_api_key,
};
use roaring::RoaringTreemap;
use serde::{Deserialize, de::DeserializeOwned};
use sqlx::{Postgres, QueryBuilder, Row, Transaction};
use url::form_urlencoded;
use uuid::Uuid;

const PROJECT_KEY_HEADER: &str = "x-fantasma-key";

#[derive(Clone)]
struct AppState {
    authorizer: Arc<StaticAdminAuthorizer>,
    pool: PgPool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicEventMetricsQuery {
    metric: EventMetric,
    event: String,
    window: MetricsBucketWindow,
    filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicSessionMetricsQuery {
    metric: SessionMetric,
    window: MetricsBucketWindow,
    interval: Option<MetricInterval>,
    filters: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateProjectRequest {
    name: String,
    ingest_key_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateApiKeyRequest {
    name: String,
    kind: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchProjectRequest {
    name: String,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CreateRangeDeletionRequest {
    start_at: DateTime<Utc>,
    end_before: DateTime<Utc>,
    event: Option<String>,
    #[serde(default)]
    filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventDiscoveryQuery {
    start: DateTime<Utc>,
    end_exclusive: DateTime<Utc>,
    filters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TopEventsQuery {
    window: EventDiscoveryQuery,
    limit: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UsageEventsQuery {
    start: NaiveDate,
    end: NaiveDate,
}

const DEFAULT_TOP_EVENTS_LIMIT: i64 = 10;
const MAX_TOP_EVENTS_LIMIT: i64 = 50;
const LIVE_INSTALLS_WINDOW_SECONDS: i64 = 120;

pub fn app(pool: PgPool, authorizer: Arc<StaticAdminAuthorizer>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route(
            "/v1/projects",
            get(list_projects_route).post(create_project_route),
        )
        .route(
            "/v1/projects/{project_id}",
            get(get_project_route)
                .patch(patch_project_route)
                .delete(delete_project_route),
        )
        .route(
            "/v1/projects/{project_id}/keys",
            get(list_project_keys_route).post(create_project_key_route),
        )
        .route(
            "/v1/projects/{project_id}/keys/{key_id}",
            delete(revoke_project_key_route),
        )
        .route(
            "/v1/projects/{project_id}/deletions",
            get(list_project_deletions_route).post(create_range_deletion_route),
        )
        .route(
            "/v1/projects/{project_id}/deletions/{deletion_id}",
            get(get_project_deletion_route),
        )
        .route("/v1/usage/events", get(usage_events_route))
        .route("/v1/metrics/events", get(events_metrics))
        .route("/v1/metrics/events/catalog", get(event_catalog_route))
        .route("/v1/metrics/events/top", get(top_events_route))
        .route("/v1/metrics/live_installs", get(live_installs_route))
        .route("/v1/metrics/sessions", get(sessions_metrics))
        .with_state(AppState { authorizer, pool })
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok", "service": "fantasma-api" }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventMetricsQueryError(&'static str);

impl EventMetricsQueryError {
    fn error_code(&self) -> &'static str {
        self.0
    }
}

const MAX_FILTER_DIMENSIONS: usize = 4;
const MAX_METRIC_GROUPS: usize = 100;
const MAX_ACTIVE_INSTALL_POINTS: usize = 120;
const EMPTY_GROUP_BY: &[String] = &[];
type GroupKey = Vec<Option<String>>;
type CountsByBucket = BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>;
type SingletonCountsByBucket = (CountsByBucket, CountsByBucket);

fn parse_event_metrics_query(
    raw_query: &str,
) -> Result<PublicEventMetricsQuery, EventMetricsQueryError> {
    let mut event = None;
    let mut metric = None;
    let mut granularity = None;
    let mut start = None;
    let mut end = None;
    let mut filters = BTreeMap::new();

    for (raw_key, raw_value) in form_urlencoded::parse(raw_query.as_bytes()) {
        let key = raw_key.into_owned();
        let value = raw_value.into_owned();

        match key.as_str() {
            "event" => {
                if event.is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }

                if value.trim().is_empty() {
                    return Err(EventMetricsQueryError("invalid_event"));
                }

                event = Some(value);
            }
            "metric" => {
                if metric.is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }

                metric = Some(
                    parse_event_metric(&value).ok_or(EventMetricsQueryError("invalid_metric"))?,
                );
            }
            "granularity" => {
                if granularity.is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }

                granularity = Some(
                    parse_metric_granularity(&value)
                        .ok_or(EventMetricsQueryError("invalid_granularity"))?,
                );
            }
            "start" => {
                if start.is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }

                start = Some(value);
            }
            "end" => {
                if end.is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }

                end = Some(value);
            }
            "group_by" => return Err(EventMetricsQueryError("invalid_query_key")),
            "platform" | "app_version" | "os_version" | "locale" => {
                if filters.insert(key, value).is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }
            }
            "project_id" | "start_date" | "end_date" => {
                return Err(EventMetricsQueryError("invalid_query_key"));
            }
            _ => return Err(EventMetricsQueryError("invalid_query_key")),
        }
    }

    let metric = metric.ok_or(EventMetricsQueryError("invalid_metric"))?;
    let event = event.ok_or(EventMetricsQueryError("invalid_event"))?;
    let granularity = granularity.ok_or(EventMetricsQueryError("invalid_granularity"))?;
    let window = parse_metrics_window(
        granularity,
        start
            .as_deref()
            .ok_or(EventMetricsQueryError("invalid_time_range"))?,
        end.as_deref()
            .ok_or(EventMetricsQueryError("invalid_time_range"))?,
    )
    .map_err(EventMetricsQueryError)?;

    if filters.len() > MAX_FILTER_DIMENSIONS {
        return Err(EventMetricsQueryError("too_many_dimensions"));
    }

    Ok(PublicEventMetricsQuery {
        metric,
        event,
        window,
        filters,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UsageEventsQueryError(&'static str);

impl UsageEventsQueryError {
    fn error_code(&self) -> &'static str {
        self.0
    }
}

fn parse_usage_events_query(raw_query: &str) -> Result<UsageEventsQuery, UsageEventsQueryError> {
    let mut start = None;
    let mut end = None;

    for (raw_key, raw_value) in form_urlencoded::parse(raw_query.as_bytes()) {
        let key = raw_key.into_owned();
        let value = raw_value.into_owned();

        match key.as_str() {
            "start" => {
                if start.is_some() {
                    return Err(UsageEventsQueryError("duplicate_query_key"));
                }

                start = Some(value);
            }
            "end" => {
                if end.is_some() {
                    return Err(UsageEventsQueryError("duplicate_query_key"));
                }

                end = Some(value);
            }
            "project_id" | "start_date" | "end_date" => {
                return Err(UsageEventsQueryError("invalid_query_key"));
            }
            _ => return Err(UsageEventsQueryError("invalid_query_key")),
        }
    }

    let start = NaiveDate::parse_from_str(
        start
            .as_deref()
            .ok_or(UsageEventsQueryError("invalid_time_range"))?,
        "%Y-%m-%d",
    )
    .map_err(|_| UsageEventsQueryError("invalid_time_range"))?;
    let end = NaiveDate::parse_from_str(
        end.as_deref()
            .ok_or(UsageEventsQueryError("invalid_time_range"))?,
        "%Y-%m-%d",
    )
    .map_err(|_| UsageEventsQueryError("invalid_time_range"))?;

    if start > end {
        return Err(UsageEventsQueryError("invalid_time_range"));
    }

    Ok(UsageEventsQuery { start, end })
}

fn parse_session_metric_query(raw_query: &str) -> Result<PublicSessionMetricsQuery, &'static str> {
    let mut metric = None;
    let mut granularity = None;
    let mut interval = None;
    let mut start = None;
    let mut end = None;
    let mut filters = BTreeMap::new();

    for (raw_key, raw_value) in form_urlencoded::parse(raw_query.as_bytes()) {
        let key = raw_key.into_owned();
        let value = raw_value.into_owned();

        match key.as_str() {
            "metric" => {
                if metric.is_some() {
                    return Err("invalid_request");
                }

                metric = Some(parse_session_metric(&value).ok_or("invalid_request")?);
            }
            "granularity" => {
                if granularity.is_some() {
                    return Err("invalid_request");
                }

                granularity = Some(parse_metric_granularity(&value).ok_or("invalid_request")?);
            }
            "interval" => {
                if interval.is_some() {
                    return Err("invalid_request");
                }

                interval = Some(parse_metric_interval(&value).ok_or("invalid_request")?);
            }
            "start" => {
                if start.is_some() {
                    return Err("invalid_request");
                }

                start = Some(value);
            }
            "end" => {
                if end.is_some() {
                    return Err("invalid_request");
                }

                end = Some(value);
            }
            "group_by" => return Err("invalid_query_key"),
            "platform" | "app_version" | "os_version" | "locale" => {
                if filters.insert(key, value).is_some() {
                    return Err("duplicate_query_key");
                }
            }
            "project_id" | "start_date" | "end_date" => return Err("invalid_query_key"),
            _ => return Err("invalid_query_key"),
        }
    }

    if filters.len() > MAX_FILTER_DIMENSIONS {
        return Err("too_many_dimensions");
    }

    let metric = metric.ok_or("invalid_request")?;
    let window = match metric {
        SessionMetric::ActiveInstalls => {
            if granularity.is_some() {
                return Err("invalid_request");
            }

            parse_active_installs_window(
                start.as_deref().ok_or("invalid_request")?,
                end.as_deref().ok_or("invalid_request")?,
            )
            .map_err(|_| "invalid_request")?
        }
        SessionMetric::Count | SessionMetric::DurationTotal | SessionMetric::NewInstalls => {
            if interval.is_some() {
                return Err("invalid_request");
            }

            parse_metrics_window(
                granularity.ok_or("invalid_request")?,
                start.as_deref().ok_or("invalid_request")?,
                end.as_deref().ok_or("invalid_request")?,
            )
            .map_err(|_| "invalid_request")?
        }
    };

    Ok(PublicSessionMetricsQuery {
        metric,
        window,
        interval,
        filters,
    })
}

fn parse_event_catalog_query(raw_query: &str) -> Result<EventDiscoveryQuery, &'static str> {
    parse_event_discovery_query(raw_query, false).map(|(window, _)| window)
}

fn parse_live_installs_query(raw_query: &str) -> Result<(), &'static str> {
    if raw_query.is_empty() {
        return Ok(());
    }

    if form_urlencoded::parse(raw_query.as_bytes())
        .next()
        .is_some()
    {
        Err("invalid_query_key")
    } else {
        Ok(())
    }
}

fn parse_top_events_query(raw_query: &str) -> Result<TopEventsQuery, &'static str> {
    let (window, limit) = parse_event_discovery_query(raw_query, true)?;

    Ok(TopEventsQuery {
        window,
        limit: limit.expect("top-events parser always returns a limit"),
    })
}

fn parse_event_discovery_query(
    raw_query: &str,
    allow_limit: bool,
) -> Result<(EventDiscoveryQuery, Option<i64>), &'static str> {
    let mut start = None;
    let mut end = None;
    let mut filters = BTreeMap::new();
    let mut limit = None;

    for (raw_key, raw_value) in form_urlencoded::parse(raw_query.as_bytes()) {
        let key = raw_key.into_owned();
        let value = raw_value.into_owned();

        match key.as_str() {
            "start" => {
                if start.is_some() {
                    return Err("invalid_query_key");
                }

                start = Some(value);
            }
            "end" => {
                if end.is_some() {
                    return Err("invalid_query_key");
                }

                end = Some(value);
            }
            "limit" => {
                if !allow_limit {
                    return Err("invalid_query_key");
                }

                if limit.is_some() {
                    return Err("invalid_query_key");
                }

                limit = Some(parse_top_events_limit(&value)?);
            }
            "platform" | "app_version" | "os_version" | "locale" => {
                if filters.insert(key, value).is_some() {
                    return Err("invalid_query_key");
                }
            }
            "project_id" | "event" | "metric" | "granularity" | "group_by" => {
                return Err("invalid_query_key");
            }
            _ => return Err("invalid_query_key"),
        }
    }

    let (start, end_exclusive) = parse_event_discovery_window(
        start.as_deref().ok_or("invalid_time_range")?,
        end.as_deref().ok_or("invalid_time_range")?,
    )?;

    Ok((
        EventDiscoveryQuery {
            start,
            end_exclusive,
            filters,
        },
        Some(limit.unwrap_or(DEFAULT_TOP_EVENTS_LIMIT)).filter(|_| allow_limit),
    ))
}

fn parse_top_events_limit(raw: &str) -> Result<i64, &'static str> {
    let limit = raw.parse::<i64>().map_err(|_| "invalid_query_key")?;

    if !(1..=MAX_TOP_EVENTS_LIMIT).contains(&limit) {
        return Err("invalid_query_key");
    }

    Ok(limit)
}

fn parse_event_discovery_window(
    start: &str,
    end: &str,
) -> Result<(DateTime<Utc>, DateTime<Utc>), &'static str> {
    if let (Ok(start_day), Ok(end_day)) = (parse_day_bucket(start), parse_day_bucket(end)) {
        if start_day > end_day {
            return Err("invalid_time_range");
        }

        return Ok((
            start_day,
            end_day
                .checked_add_signed(ChronoDuration::days(1))
                .ok_or("invalid_time_range")?,
        ));
    }

    let start = DateTime::parse_from_rfc3339(start)
        .map_err(|_| "invalid_time_range")?
        .with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339(end)
        .map_err(|_| "invalid_time_range")?
        .with_timezone(&Utc);

    if start > end {
        return Err("invalid_time_range");
    }

    Ok((
        start,
        end.checked_add_signed(ChronoDuration::microseconds(1))
            .ok_or("invalid_time_range")?,
    ))
}

fn parse_event_metric(raw: &str) -> Option<EventMetric> {
    match raw {
        "count" => Some(EventMetric::Count),
        _ => None,
    }
}

fn parse_session_metric(raw: &str) -> Option<SessionMetric> {
    match raw {
        "count" => Some(SessionMetric::Count),
        "duration_total" => Some(SessionMetric::DurationTotal),
        "new_installs" => Some(SessionMetric::NewInstalls),
        "active_installs" => Some(SessionMetric::ActiveInstalls),
        _ => None,
    }
}

fn parse_metric_interval(raw: &str) -> Option<MetricInterval> {
    match raw {
        "day" => Some(MetricInterval::Day),
        "week" => Some(MetricInterval::Week),
        "month" => Some(MetricInterval::Month),
        "year" => Some(MetricInterval::Year),
        _ => None,
    }
}

fn parse_metric_granularity(raw: &str) -> Option<MetricGranularity> {
    match raw {
        "hour" => Some(MetricGranularity::Hour),
        "day" => Some(MetricGranularity::Day),
        "week" => Some(MetricGranularity::Week),
        "month" => Some(MetricGranularity::Month),
        "year" => Some(MetricGranularity::Year),
        _ => None,
    }
}

fn parse_active_installs_window(
    start: &str,
    end: &str,
) -> Result<MetricsBucketWindow, &'static str> {
    let start = parse_day_bucket(start)?;
    let end = parse_day_bucket(end)?;

    if start > end {
        return Err("invalid_time_range");
    }

    Ok(MetricsBucketWindow {
        granularity: MetricGranularity::Day,
        start,
        end,
    })
}

fn parse_metrics_window(
    granularity: MetricGranularity,
    start: &str,
    end: &str,
) -> Result<MetricsBucketWindow, &'static str> {
    let (start, end) = match granularity {
        MetricGranularity::Hour => (parse_hour_bucket(start)?, parse_hour_bucket(end)?),
        MetricGranularity::Day => (parse_day_bucket(start)?, parse_day_bucket(end)?),
        MetricGranularity::Week => (
            parse_aligned_day_bucket(start, granularity)?,
            parse_aligned_day_bucket(end, granularity)?,
        ),
        MetricGranularity::Month => (
            parse_aligned_day_bucket(start, granularity)?,
            parse_aligned_day_bucket(end, granularity)?,
        ),
        MetricGranularity::Year => (
            parse_aligned_day_bucket(start, granularity)?,
            parse_aligned_day_bucket(end, granularity)?,
        ),
    };

    if start > end {
        return Err("invalid_time_range");
    }

    Ok(MetricsBucketWindow {
        granularity,
        start,
        end,
    })
}

fn parse_aligned_day_bucket(
    raw: &str,
    granularity: MetricGranularity,
) -> Result<DateTime<Utc>, &'static str> {
    let bucket = parse_day_bucket(raw)?;

    let aligned = match granularity {
        MetricGranularity::Week => bucket.weekday() == Weekday::Mon,
        MetricGranularity::Month => bucket.day() == 1,
        MetricGranularity::Year => bucket.month() == 1 && bucket.day() == 1,
        MetricGranularity::Hour | MetricGranularity::Day => true,
    };

    if aligned {
        Ok(bucket)
    } else {
        Err("invalid_time_range")
    }
}

fn parse_day_bucket(raw: &str) -> Result<DateTime<Utc>, &'static str> {
    let day = NaiveDate::parse_from_str(raw, "%Y-%m-%d").map_err(|_| "invalid_time_range")?;
    Ok(DateTime::from_naive_utc_and_offset(
        day.and_hms_opt(0, 0, 0)
            .expect("midnight is a valid UTC datetime"),
        Utc,
    ))
}

fn parse_hour_bucket(raw: &str) -> Result<DateTime<Utc>, &'static str> {
    if !raw.ends_with('Z') {
        return Err("invalid_time_range");
    }

    let parsed = DateTime::parse_from_rfc3339(raw)
        .map_err(|_| "invalid_time_range")?
        .with_timezone(&Utc);

    if parsed.minute() != 0 || parsed.second() != 0 || parsed.nanosecond() != 0 {
        return Err("invalid_time_range");
    }

    Ok(parsed)
}

#[allow(clippy::result_large_err)]
fn parse_json_body<T: DeserializeOwned>(body: &Bytes) -> Result<T, axum::response::Response> {
    serde_json::from_slice(body).map_err(|_| query_error_response("invalid_request"))
}

async fn list_projects_route(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    match list_projects(&state.pool).await {
        Ok(projects) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "projects": projects
                    .iter()
                    .map(project_json)
                    .collect::<Vec<_>>()
            })),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(?error, "failed to list projects");
            query_error_response("internal_server_error")
        }
    }
}

async fn create_project_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    let payload = match parse_json_body::<CreateProjectRequest>(&body) {
        Ok(payload) => payload,
        Err(response) => return response,
    };

    let project_name = match validate_name(&payload.name) {
        Some(name) => name,
        None => return query_error_response("invalid_request"),
    };
    let ingest_key_name = match validate_name(&payload.ingest_key_name) {
        Some(name) => name,
        None => return query_error_response("invalid_request"),
    };

    let ingest_secret = generate_api_key_secret(ApiKeyKind::Ingest);
    let (project, ingest_key) = match create_project_with_api_key(
        &state.pool,
        &project_name,
        &ingest_key_name,
        ApiKeyKind::Ingest,
        &ingest_secret,
    )
    .await
    {
        Ok(records) => records,
        Err(error) => {
            tracing::error!(?error, "failed to create project with ingest key");
            return query_error_response("internal_server_error");
        }
    };

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "project": project_json(&project),
            "ingest_key": api_key_secret_json(&ingest_key, &ingest_secret),
        })),
    )
        .into_response()
}

async fn get_project_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    match load_project(&state.pool, project_id).await {
        Ok(Some(project)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "project": project_json(&project),
            })),
        )
            .into_response(),
        Ok(None) => query_error_response("not_found"),
        Err(error) => {
            tracing::error!(?error, project_id = %project_id, "failed to load project");
            query_error_response("internal_server_error")
        }
    }
}

async fn patch_project_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    body: Bytes,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    let payload = match parse_json_body::<PatchProjectRequest>(&body) {
        Ok(payload) => payload,
        Err(response) => return response,
    };

    let project_name = match validate_name(&payload.name) {
        Some(name) => name,
        None => return query_error_response("invalid_request"),
    };

    if let Err(response) =
        require_project_active_for_management_mutation(&state.pool, project_id).await
    {
        return response;
    }

    match rename_project(&state.pool, project_id, &project_name).await {
        Ok(Some(project)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "project": project_json(&project),
            })),
        )
            .into_response(),
        Ok(None) => query_error_response("not_found"),
        Err(error) => {
            if let Some(response) = project_mutation_error_response(&error) {
                return response;
            }
            tracing::error!(?error, project_id = %project_id, "failed to rename project");
            query_error_response("internal_server_error")
        }
    }
}

async fn list_project_keys_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    match list_api_keys(&state.pool, project_id).await {
        Ok(Some(keys)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "keys": keys.iter().map(api_key_metadata_json).collect::<Vec<_>>()
            })),
        )
            .into_response(),
        Ok(None) => query_error_response("not_found"),
        Err(error) => {
            tracing::error!(?error, project_id = %project_id, "failed to list project keys");
            query_error_response("internal_server_error")
        }
    }
}

async fn create_project_key_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    body: Bytes,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    let payload = match parse_json_body::<CreateApiKeyRequest>(&body) {
        Ok(payload) => payload,
        Err(response) => return response,
    };

    let key_name = match validate_name(&payload.name) {
        Some(name) => name,
        None => return query_error_response("invalid_request"),
    };
    let kind = match parse_api_key_kind(&payload.kind) {
        Some(kind) => kind,
        None => return query_error_response("invalid_request"),
    };

    if let Err(response) =
        require_project_active_for_management_mutation(&state.pool, project_id).await
    {
        return response;
    }

    let secret = generate_api_key_secret(kind);
    match create_api_key(&state.pool, project_id, &key_name, kind, &secret).await {
        Ok(Some(key)) => (
            StatusCode::CREATED,
            Json(serde_json::json!({
                "key": api_key_secret_json(&key, &secret),
            })),
        )
            .into_response(),
        Ok(None) => query_error_response("not_found"),
        Err(error) => {
            if let Some(response) = project_mutation_error_response(&error) {
                return response;
            }
            tracing::error!(?error, project_id = %project_id, "failed to create project key");
            query_error_response("internal_server_error")
        }
    }
}

async fn revoke_project_key_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((project_id, key_id)): Path<(Uuid, Uuid)>,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    if let Err(response) =
        require_project_active_for_management_mutation(&state.pool, project_id).await
    {
        return response;
    }

    match revoke_api_key(&state.pool, project_id, key_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => query_error_response("not_found"),
        Err(error) => {
            if let Some(response) = project_mutation_error_response(&error) {
                return response;
            }
            tracing::error!(?error, project_id = %project_id, key_id = %key_id, "failed to revoke project key");
            query_error_response("internal_server_error")
        }
    }
}

async fn delete_project_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    match enqueue_project_purge(&state.pool, project_id).await {
        Ok(StartProjectDeletionResult::Enqueued(job)) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "deletion": project_deletion_json(&job) })),
        )
            .into_response(),
        Ok(StartProjectDeletionResult::ProjectBusy) => query_error_response("project_busy"),
        Ok(StartProjectDeletionResult::ProjectPendingDeletion) => {
            query_error_response("project_pending_deletion")
        }
        Ok(StartProjectDeletionResult::NotFound) => query_error_response("not_found"),
        Err(error) => {
            tracing::error!(?error, project_id = %project_id, "failed to enqueue project purge");
            query_error_response("internal_server_error")
        }
    }
}

async fn create_range_deletion_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
    body: Bytes,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    let payload = match parse_json_body::<CreateRangeDeletionRequest>(&body) {
        Ok(payload) => payload,
        Err(response) => return response,
    };

    let scope = match validate_range_deletion_request(payload) {
        Ok(scope) => scope,
        Err(error) => return query_error_response(error),
    };

    match enqueue_range_deletion(&state.pool, project_id, &scope).await {
        Ok(StartProjectDeletionResult::Enqueued(job)) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "deletion": project_deletion_json(&job) })),
        )
            .into_response(),
        Ok(StartProjectDeletionResult::ProjectBusy) => query_error_response("project_busy"),
        Ok(StartProjectDeletionResult::ProjectPendingDeletion) => {
            query_error_response("project_pending_deletion")
        }
        Ok(StartProjectDeletionResult::NotFound) => query_error_response("not_found"),
        Err(error) => {
            tracing::error!(?error, project_id = %project_id, "failed to enqueue range deletion");
            query_error_response("internal_server_error")
        }
    }
}

async fn list_project_deletions_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(project_id): Path<Uuid>,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    match list_project_deletion_jobs(&state.pool, project_id).await {
        Ok(Some(deletions)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "deletions": deletions
                    .iter()
                    .map(project_deletion_json)
                    .collect::<Vec<_>>()
            })),
        )
            .into_response(),
        Ok(None) => query_error_response("not_found"),
        Err(error) => {
            tracing::error!(?error, project_id = %project_id, "failed to list project deletions");
            query_error_response("internal_server_error")
        }
    }
}

async fn get_project_deletion_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((project_id, deletion_id)): Path<(Uuid, Uuid)>,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    match load_project_deletion_job(&state.pool, project_id, deletion_id).await {
        Ok(Some(deletion)) => (
            StatusCode::OK,
            Json(serde_json::json!({ "deletion": project_deletion_json(&deletion) })),
        )
            .into_response(),
        Ok(None) => query_error_response("not_found"),
        Err(error) => {
            tracing::error!(?error, project_id = %project_id, deletion_id = %deletion_id, "failed to load project deletion");
            query_error_response("internal_server_error")
        }
    }
}

async fn usage_events_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> axum::response::Response {
    if let Err(response) = authorize_operator(&state.authorizer, &headers) {
        return response;
    }

    let query = match parse_usage_events_query(raw_query.as_deref().unwrap_or_default()) {
        Ok(query) => query,
        Err(error) => return query_error_response(error.error_code()),
    };

    match list_usage_events(&state.pool, query.start, query.end).await {
        Ok(projects) => (
            StatusCode::OK,
            Json(serde_json::json!(UsageEventsResponse {
                start: query.start.to_string(),
                end: query.end.to_string(),
                total_events_processed: projects
                    .iter()
                    .map(|project| project.events_processed)
                    .sum(),
                projects,
            })),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(?error, "failed to load usage events");
            query_error_response("internal_server_error")
        }
    }
}

#[allow(clippy::result_large_err)]
fn authorize_operator(
    authorizer: &StaticAdminAuthorizer,
    headers: &HeaderMap,
) -> Result<(), axum::response::Response> {
    authorizer.authorize(headers).map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response()
    })
}

async fn authorize_project_key(
    pool: &PgPool,
    headers: &HeaderMap,
    required_kind: ApiKeyKind,
) -> Result<ResolvedApiKey, axum::response::Response> {
    let Some(project_key) = headers.get(PROJECT_KEY_HEADER) else {
        return Err(query_error_response("unauthorized"));
    };
    let Ok(project_key) = project_key.to_str() else {
        return Err(query_error_response("unauthorized"));
    };

    match resolve_api_key(pool, project_key).await {
        Ok(Some(record)) if record.kind == required_kind => Ok(record),
        Ok(_) => Err(query_error_response("unauthorized")),
        Err(error) => {
            tracing::error!(?error, "failed to resolve project key");
            Err(query_error_response("internal_server_error"))
        }
    }
}

fn validate_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(trimmed.to_owned())
}

fn project_mutation_error_response(error: &StoreError) -> Option<axum::response::Response> {
    match error {
        StoreError::ProjectNotFound => Some(query_error_response("not_found")),
        StoreError::ProjectNotActive(ProjectState::RangeDeleting)
        | StoreError::ProjectFenceChanged => Some(query_error_response("project_busy")),
        StoreError::ProjectNotActive(ProjectState::PendingDeletion) => {
            Some(query_error_response("project_pending_deletion"))
        }
        StoreError::ProjectNotActive(ProjectState::Active) => None,
        _ => None,
    }
}

async fn require_project_active_for_management_mutation(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<(), axum::response::Response> {
    let project = load_project(pool, project_id).await.map_err(|error| {
        tracing::error!(?error, project_id = %project_id, "failed to load project for management state gate");
        query_error_response("internal_server_error")
    })?;

    match project {
        Some(project) => match project.state {
            ProjectState::Active => Ok(()),
            ProjectState::RangeDeleting => Err(query_error_response("project_busy")),
            ProjectState::PendingDeletion => Err(query_error_response("project_pending_deletion")),
        },
        None => Err(query_error_response("not_found")),
    }
}

async fn require_project_active_for_read_key_route(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<(), axum::response::Response> {
    let project = load_project(pool, project_id).await.map_err(|error| {
        tracing::error!(?error, project_id = %project_id, "failed to load project for read-key state gate");
        query_error_response("internal_server_error")
    })?;

    match project {
        Some(project) => match project.state {
            ProjectState::Active => Ok(()),
            ProjectState::RangeDeleting => Err(query_error_response("project_busy")),
            ProjectState::PendingDeletion => Err(query_error_response("project_pending_deletion")),
        },
        None => Err(query_error_response("unauthorized")),
    }
}

fn parse_api_key_kind(raw: &str) -> Option<ApiKeyKind> {
    match raw {
        "ingest" => Some(ApiKeyKind::Ingest),
        "read" => Some(ApiKeyKind::Read),
        _ => None,
    }
}

fn project_json(project: &ProjectRecord) -> serde_json::Value {
    serde_json::json!({
        "id": project.id,
        "name": project.name,
        "state": project.state.as_str(),
        "created_at": project.created_at,
    })
}

fn project_deletion_json(deletion: &ProjectDeletionRecord) -> serde_json::Value {
    serde_json::json!({
        "id": deletion.id,
        "project_id": deletion.project_id,
        "project_name": deletion.project_name,
        "kind": deletion.kind.as_str(),
        "status": deletion.status.as_str(),
        "scope": deletion.scope,
        "created_at": deletion.created_at,
        "started_at": deletion.started_at,
        "finished_at": deletion.finished_at,
        "error_code": deletion.error_code,
        "error_message": deletion.error_message,
        "deleted_raw_events": deletion.deleted_raw_events,
        "deleted_sessions": deletion.deleted_sessions,
        "rebuilt_installs": deletion.rebuilt_installs,
        "rebuilt_days": deletion.rebuilt_days,
        "rebuilt_hours": deletion.rebuilt_hours,
    })
}

fn event_catalog_json(event: &EventCatalogRecord) -> serde_json::Value {
    serde_json::json!({
        "name": event.event_name,
        "last_seen_at": event.last_seen_at.to_rfc3339_opts(SecondsFormat::AutoSi, true),
    })
}

fn top_event_json(event: &TopEventRecord) -> serde_json::Value {
    serde_json::json!({
        "name": event.event_name,
        "count": event.event_count,
    })
}

fn api_key_metadata_json(key: &fantasma_store::ApiKeyRecord) -> serde_json::Value {
    serde_json::json!({
        "id": key.id,
        "name": key.name,
        "kind": key.kind.as_str(),
        "prefix": key.key_prefix,
        "created_at": key.created_at,
        "revoked_at": key.revoked_at,
    })
}

fn api_key_secret_json(key: &fantasma_store::ApiKeyRecord, secret: &str) -> serde_json::Value {
    serde_json::json!({
        "id": key.id,
        "name": key.name,
        "kind": key.kind.as_str(),
        "prefix": key.key_prefix,
        "created_at": key.created_at,
        "revoked_at": key.revoked_at,
        "secret": secret,
    })
}

async fn events_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> impl IntoResponse {
    let auth = match authorize_project_key(&state.pool, &headers, ApiKeyKind::Read).await {
        Ok(auth) => auth,
        Err(response) => return response,
    };

    if let Err(response) =
        require_project_active_for_read_key_route(&state.pool, auth.project_id).await
    {
        return response;
    }

    let public_query = match parse_event_metrics_query(raw_query.as_deref().unwrap_or_default()) {
        Ok(query) => query,
        Err(error) => return query_error_response(error.error_code()),
    };
    let query = EventMetricsQuery {
        project_id: auth.project_id,
        metric: public_query.metric,
        event: public_query.event,
        window: public_query.window,
        filters: public_query.filters,
    };
    if let Err(error) = validate_event_metrics_query(&query) {
        return query_error_response(error);
    }

    match execute_event_metrics_query(&state.pool, &query).await {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).expect("serialize event metrics response")),
        )
            .into_response(),
        Err(MetricsResponseError::GroupLimitExceeded) => {
            query_error_response("group_limit_exceeded")
        }
        Err(MetricsResponseError::Store(error)) => {
            tracing::error!(?error, "failed to load event metrics");
            query_error_response("internal_server_error")
        }
    }
}

async fn sessions_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> impl IntoResponse {
    let auth = match authorize_project_key(&state.pool, &headers, ApiKeyKind::Read).await {
        Ok(auth) => auth,
        Err(response) => return response,
    };

    if let Err(response) =
        require_project_active_for_read_key_route(&state.pool, auth.project_id).await
    {
        return response;
    }

    let public_query = match parse_session_metric_query(raw_query.as_deref().unwrap_or_default()) {
        Ok(query) => query,
        Err(error) => return query_error_response(error),
    };
    let query = SessionMetricsQuery {
        project_id: auth.project_id,
        metric: public_query.metric,
        window: public_query.window,
        interval: public_query.interval,
        filters: public_query.filters,
    };

    if let Err(error) = validate_active_installs_query(&query) {
        return query_error_response(error);
    }

    if query.metric == SessionMetric::ActiveInstalls {
        match execute_active_installs_query(&state.pool, &query).await {
            Ok(response) => (
                StatusCode::OK,
                Json(serde_json::to_value(response).expect("serialize active installs response")),
            )
                .into_response(),
            Err(MetricsResponseError::GroupLimitExceeded) => {
                query_error_response("group_limit_exceeded")
            }
            Err(MetricsResponseError::Store(error)) => {
                tracing::error!(?error, "failed to load session metrics");
                query_error_response("internal_server_error")
            }
        }
    } else {
        match execute_session_metrics_query(&state.pool, &query).await {
            Ok(response) => (
                StatusCode::OK,
                Json(serde_json::to_value(response).expect("serialize session metrics response")),
            )
                .into_response(),
            Err(MetricsResponseError::GroupLimitExceeded) => {
                query_error_response("group_limit_exceeded")
            }
            Err(MetricsResponseError::Store(error)) => {
                tracing::error!(?error, "failed to load session metrics");
                query_error_response("internal_server_error")
            }
        }
    }
}

async fn live_installs_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> impl IntoResponse {
    let auth = match authorize_project_key(&state.pool, &headers, ApiKeyKind::Read).await {
        Ok(auth) => auth,
        Err(response) => return response,
    };

    if let Err(response) =
        require_project_active_for_read_key_route(&state.pool, auth.project_id).await
    {
        return response;
    }

    if let Err(error) = parse_live_installs_query(raw_query.as_deref().unwrap_or_default()) {
        return query_error_response(error);
    }

    match execute_live_installs_query(&state.pool, auth.project_id).await {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).expect("serialize live installs response")),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(?error, project_id = %auth.project_id, "failed to load live installs");
            query_error_response("internal_server_error")
        }
    }
}

async fn event_catalog_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> impl IntoResponse {
    let auth = match authorize_project_key(&state.pool, &headers, ApiKeyKind::Read).await {
        Ok(auth) => auth,
        Err(response) => return response,
    };

    let query = match parse_event_catalog_query(raw_query.as_deref().unwrap_or_default()) {
        Ok(query) => query,
        Err(error) => return query_error_response(error),
    };

    match list_event_catalog(
        &state.pool,
        auth.project_id,
        query.start,
        query.end_exclusive,
        &query.filters,
    )
    .await
    {
        Ok(events) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "events": events.iter().map(event_catalog_json).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(?error, project_id = %auth.project_id, "failed to list event catalog");
            query_error_response("internal_server_error")
        }
    }
}

async fn top_events_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> impl IntoResponse {
    let auth = match authorize_project_key(&state.pool, &headers, ApiKeyKind::Read).await {
        Ok(auth) => auth,
        Err(response) => return response,
    };

    let query = match parse_top_events_query(raw_query.as_deref().unwrap_or_default()) {
        Ok(query) => query,
        Err(error) => return query_error_response(error),
    };

    match list_top_events(
        &state.pool,
        auth.project_id,
        query.window.start,
        query.window.end_exclusive,
        &query.window.filters,
        query.limit,
    )
    .await
    {
        Ok(events) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "events": events.iter().map(top_event_json).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(?error, project_id = %auth.project_id, "failed to list top events");
            query_error_response("internal_server_error")
        }
    }
}

#[derive(Debug)]
enum MetricsResponseError {
    GroupLimitExceeded,
    Store(StoreError),
}

impl From<StoreError> for MetricsResponseError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetricsCubeValueRow {
    bucket_start: DateTime<Utc>,
    dimensions: BTreeMap<String, String>,
    value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetricsAggregateValueRow {
    dimensions: BTreeMap<String, String>,
    value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveInstallPointWindow {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

fn query_error_response(error: &'static str) -> axum::response::Response {
    let status = match error {
        "not_found" => StatusCode::NOT_FOUND,
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "project_busy" | "project_pending_deletion" => StatusCode::CONFLICT,
        "internal_server_error" => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    };

    (status, Json(serde_json::json!({ "error": error }))).into_response()
}

fn validate_range_deletion_request(
    payload: CreateRangeDeletionRequest,
) -> Result<serde_json::Value, &'static str> {
    if payload.start_at >= payload.end_before {
        return Err("invalid_request");
    }

    if payload
        .event
        .as_ref()
        .is_some_and(|event| event.trim().is_empty())
    {
        return Err("invalid_request");
    }

    if payload.filters.keys().any(|key| {
        !matches!(
            key.as_str(),
            "platform" | "app_version" | "os_version" | "locale"
        )
    }) {
        return Err("invalid_request");
    }

    serde_json::to_value(payload).map_err(|_| "invalid_request")
}

async fn execute_event_metrics_query(
    pool: &PgPool,
    query: &EventMetricsQuery,
) -> Result<MetricsResponse, MetricsResponseError> {
    let mut tx = begin_metrics_snapshot(pool).await?;
    precheck_event_metrics_group_limit(&mut tx, query).await?;
    let counts_by_bucket = load_event_group_counts_by_bucket(&mut tx, query).await?;
    tx.commit().await.map_err(StoreError::Database)?;

    Ok(build_metrics_response(
        query.metric.as_str(),
        &query.window,
        EMPTY_GROUP_BY,
        counts_by_bucket,
    ))
}

fn validate_event_metrics_query(query: &EventMetricsQuery) -> Result<(), &'static str> {
    match query.window.granularity {
        MetricGranularity::Hour | MetricGranularity::Day => Ok(()),
        MetricGranularity::Week | MetricGranularity::Month | MetricGranularity::Year => {
            Err("invalid_request")
        }
    }
}

fn validate_active_installs_query(query: &SessionMetricsQuery) -> Result<(), &'static str> {
    match query.metric {
        SessionMetric::ActiveInstalls => {
            if active_install_point_windows(query)?.len() > MAX_ACTIVE_INSTALL_POINTS {
                return Err("invalid_request");
            }

            Ok(())
        }
        SessionMetric::Count | SessionMetric::DurationTotal | SessionMetric::NewInstalls => {
            if query.interval.is_some() {
                return Err("invalid_request");
            }

            match query.window.granularity {
                MetricGranularity::Hour | MetricGranularity::Day => Ok(()),
                MetricGranularity::Week | MetricGranularity::Month | MetricGranularity::Year => {
                    Err("invalid_request")
                }
            }
        }
    }
}

async fn execute_active_installs_query(
    pool: &PgPool,
    query: &SessionMetricsQuery,
) -> Result<ActiveInstallsResponse, MetricsResponseError> {
    let mut tx = begin_metrics_snapshot(pool).await?;
    let windows = active_install_point_windows(query).map_err(|_| {
        MetricsResponseError::Store(StoreError::InvariantViolation(
            "active installs point windows must already be validated".to_owned(),
        ))
    })?;
    let mut counts_by_window = BTreeMap::new();
    for window in &windows {
        let counts = load_active_install_group_counts_for_window(&mut tx, query, window).await?;
        counts_by_window.insert(window.start, counts);
    }
    tx.commit().await.map_err(StoreError::Database)?;

    Ok(build_active_installs_response(
        query,
        &windows,
        counts_by_window,
    ))
}

fn active_install_point_windows(
    query: &SessionMetricsQuery,
) -> Result<Vec<ActiveInstallPointWindow>, &'static str> {
    let start = query.window.start.date_naive();
    let end = query.window.end.date_naive();
    let interval = query.interval;
    let mut windows = Vec::new();
    let mut current = start;

    loop {
        let point_end = match interval {
            None => end,
            Some(MetricInterval::Day) => current,
            Some(MetricInterval::Week) => end.min(end_of_week(current)),
            Some(MetricInterval::Month) => end.min(end_of_month(current)),
            Some(MetricInterval::Year) => end.min(end_of_year(current)),
        };

        windows.push(ActiveInstallPointWindow {
            start: day_start(current),
            end: day_start(point_end),
        });

        if point_end >= end || interval.is_none() {
            break;
        }

        current = point_end
            .succ_opt()
            .expect("valid successor within active installs range");
    }

    Ok(windows)
}

fn day_start(day: NaiveDate) -> DateTime<Utc> {
    DateTime::from_naive_utc_and_offset(day.and_hms_opt(0, 0, 0).expect("midnight is valid"), Utc)
}

fn end_of_week(day: NaiveDate) -> NaiveDate {
    day + ChronoDuration::days((6 - day.weekday().num_days_from_monday()) as i64)
}

fn end_of_month(day: NaiveDate) -> NaiveDate {
    let (year, month) = if day.month() == 12 {
        (day.year() + 1, 1)
    } else {
        (day.year(), day.month() + 1)
    };

    NaiveDate::from_ymd_opt(year, month, 1)
        .expect("valid first day of next month")
        .pred_opt()
        .expect("month has a valid predecessor")
}

fn end_of_year(day: NaiveDate) -> NaiveDate {
    NaiveDate::from_ymd_opt(day.year(), 12, 31).expect("valid last day of year")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveInstallReadStrategy {
    BitmapTotals,
    BitmapDim1,
    BitmapDim2,
    DistinctCount,
}

fn active_install_read_strategy(filters: &BTreeMap<String, String>) -> ActiveInstallReadStrategy {
    match cube_keys_with(filters, EMPTY_GROUP_BY).len() {
        0 => ActiveInstallReadStrategy::BitmapTotals,
        1 => ActiveInstallReadStrategy::BitmapDim1,
        2 => ActiveInstallReadStrategy::BitmapDim2,
        _ => ActiveInstallReadStrategy::DistinctCount,
    }
}

async fn load_active_install_group_counts_for_window(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
    window: &ActiveInstallPointWindow,
) -> Result<BTreeMap<GroupKey, u64>, MetricsResponseError> {
    let days = active_install_days(window);

    match active_install_read_strategy(&query.filters) {
        ActiveInstallReadStrategy::BitmapTotals => {
            let mut bitmap = RoaringTreemap::new();
            for row in
                load_active_install_bitmap_totals_in_executor(&mut **tx, query.project_id, &days)
                    .await?
            {
                bitmap |= row.bitmap;
            }

            Ok(BTreeMap::from([(Vec::new(), bitmap.len())]))
        }
        ActiveInstallReadStrategy::BitmapDim1 => {
            let referenced_keys = cube_keys_with(&query.filters, EMPTY_GROUP_BY);
            load_active_install_dim1_group_counts_for_window(tx, query, &days, &referenced_keys[0])
                .await
        }
        ActiveInstallReadStrategy::BitmapDim2 => {
            let referenced_keys = cube_keys_with(&query.filters, EMPTY_GROUP_BY);
            load_active_install_dim2_group_counts_for_window(
                tx,
                query,
                &days,
                &referenced_keys[0],
                &referenced_keys[1],
            )
            .await
        }
        ActiveInstallReadStrategy::DistinctCount => {
            let value = load_active_install_total_count_for_window(tx, query, &days).await?;
            Ok(BTreeMap::from([(Vec::new(), value)]))
        }
    }
}

async fn load_active_install_total_count_for_window(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
    days: &[NaiveDate],
) -> Result<u64, MetricsResponseError> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT COUNT(DISTINCT install_id)::BIGINT AS value \
         FROM session_active_install_slices \
         WHERE project_id = ",
    );
    builder.push_bind(query.project_id);
    builder.push(" AND day = ANY(");
    builder.push_bind(days);
    builder.push(")");

    if let Some(value) = query.filters.get("platform") {
        builder.push(" AND platform = ");
        builder.push_bind(value);
    }

    append_optional_slice_filter(&mut builder, &query.filters, "app_version");
    append_optional_slice_filter(&mut builder, &query.filters, "os_version");
    append_optional_slice_filter(&mut builder, &query.filters, "locale");

    let row = builder
        .build()
        .fetch_one(&mut **tx)
        .await
        .map_err(StoreError::from)?;

    Ok(row.try_get::<i64, _>("value").map_err(StoreError::from)? as u64)
}

fn append_optional_slice_filter(
    builder: &mut QueryBuilder<'_, Postgres>,
    filters: &BTreeMap<String, String>,
    key: &str,
) {
    if let Some(value) = filters.get(key) {
        builder.push(format!(" AND {key}_is_null = FALSE AND {key} = "));
        builder.push_bind(value.clone());
    }
}

fn active_install_days(window: &ActiveInstallPointWindow) -> Vec<NaiveDate> {
    let mut days = Vec::new();
    let mut day = window.start.date_naive();
    let end = window.end.date_naive();

    loop {
        days.push(day);
        if day >= end {
            break;
        }
        day = day
            .succ_opt()
            .expect("active install window days must stay in range");
    }

    days
}

async fn load_active_install_dim1_group_counts_for_window(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
    days: &[NaiveDate],
    key: &str,
) -> Result<BTreeMap<GroupKey, u64>, MetricsResponseError> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT CASE WHEN dim1_value_is_null THEN NULL ELSE dim1_value END AS dim_value, bitmap \
         FROM active_install_daily_bitmaps_dim1 WHERE project_id = ",
    );
    builder.push_bind(query.project_id);
    builder.push(" AND day = ANY(");
    builder.push_bind(days);
    builder.push(") AND dim1_key = ");
    builder.push_bind(key);

    if let Some(value) = query.filters.get(key) {
        builder.push(" AND dim1_value_is_null = FALSE AND dim1_value = ");
        builder.push_bind(value);
    }

    builder.push(" ORDER BY day ASC, dim_value ASC");
    let rows = builder
        .build()
        .fetch_all(&mut **tx)
        .await
        .map_err(StoreError::from)?;

    let mut bitmaps_by_group = BTreeMap::<GroupKey, RoaringTreemap>::new();
    for row in rows {
        let dim_value = row
            .try_get::<Option<String>, _>("dim_value")
            .map_err(StoreError::from)?;
        let bitmap = deserialize_active_install_bitmap(
            &row.try_get::<Vec<u8>, _>("bitmap")
                .map_err(StoreError::from)?,
        )?;
        let group_key = if EMPTY_GROUP_BY.is_empty() {
            Vec::new()
        } else {
            vec![dim_value]
        };
        let entry = bitmaps_by_group.entry(group_key).or_default();
        *entry |= bitmap;
    }

    Ok(active_install_group_counts(bitmaps_by_group))
}

async fn load_active_install_dim2_group_counts_for_window(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
    days: &[NaiveDate],
    key1: &str,
    key2: &str,
) -> Result<BTreeMap<GroupKey, u64>, MetricsResponseError> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT CASE WHEN dim1_value_is_null THEN NULL ELSE dim1_value END AS dim1_value, \
         CASE WHEN dim2_value_is_null THEN NULL ELSE dim2_value END AS dim2_value, bitmap \
         FROM active_install_daily_bitmaps_dim2 WHERE project_id = ",
    );
    builder.push_bind(query.project_id);
    builder.push(" AND day = ANY(");
    builder.push_bind(days);
    builder.push(") AND dim1_key = ");
    builder.push_bind(key1);
    builder.push(" AND dim2_key = ");
    builder.push_bind(key2);

    if let Some(value) = query.filters.get(key1) {
        builder.push(" AND dim1_value_is_null = FALSE AND dim1_value = ");
        builder.push_bind(value);
    }
    if let Some(value) = query.filters.get(key2) {
        builder.push(" AND dim2_value_is_null = FALSE AND dim2_value = ");
        builder.push_bind(value);
    }

    builder.push(" ORDER BY day ASC, dim1_value ASC, dim2_value ASC");
    let rows = builder
        .build()
        .fetch_all(&mut **tx)
        .await
        .map_err(StoreError::from)?;

    let mut bitmaps_by_group = BTreeMap::<GroupKey, RoaringTreemap>::new();
    for row in rows {
        let dim1_value = row
            .try_get::<Option<String>, _>("dim1_value")
            .map_err(StoreError::from)?;
        let dim2_value = row
            .try_get::<Option<String>, _>("dim2_value")
            .map_err(StoreError::from)?;
        let bitmap = deserialize_active_install_bitmap(
            &row.try_get::<Vec<u8>, _>("bitmap")
                .map_err(StoreError::from)?,
        )?;
        let group_key = EMPTY_GROUP_BY
            .iter()
            .map(|group_key| {
                if group_key == key1 {
                    dim1_value.clone()
                } else {
                    dim2_value.clone()
                }
            })
            .collect::<Vec<_>>();
        let entry = bitmaps_by_group.entry(group_key).or_default();
        *entry |= bitmap;
    }

    Ok(active_install_group_counts(bitmaps_by_group))
}

fn active_install_group_counts(
    bitmaps_by_group: BTreeMap<GroupKey, RoaringTreemap>,
) -> BTreeMap<GroupKey, u64> {
    bitmaps_by_group
        .into_iter()
        .map(|(group_key, bitmap)| (group_key, bitmap.len()))
        .collect()
}

fn build_active_installs_response(
    query: &SessionMetricsQuery,
    windows: &[ActiveInstallPointWindow],
    counts_by_window: BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>,
) -> ActiveInstallsResponse {
    let mut series_by_group = BTreeMap::<GroupKey, BTreeMap<DateTime<Utc>, u64>>::new();

    for (window_start, counts) in counts_by_window {
        for (group_key, value) in counts {
            series_by_group
                .entry(group_key)
                .or_default()
                .insert(window_start, value);
        }
    }

    let series = if EMPTY_GROUP_BY.is_empty() {
        let points_by_window = series_by_group.remove(&Vec::new()).unwrap_or_default();
        vec![ActiveInstallsSeries {
            dimensions: BTreeMap::new(),
            points: windows
                .iter()
                .map(|window| ActiveInstallsPoint {
                    start: format_active_install_day(window.start),
                    end: format_active_install_day(window.end),
                    value: points_by_window
                        .get(&window.start)
                        .copied()
                        .unwrap_or_default(),
                })
                .collect(),
        }]
    } else {
        let mut grouped = series_by_group.into_iter().collect::<Vec<_>>();
        grouped.sort_by(|(left, _), (right, _)| compare_group_keys(left, right));
        grouped
            .into_iter()
            .map(|(group_key, points_by_window)| ActiveInstallsSeries {
                dimensions: dimensions_for_group(EMPTY_GROUP_BY, &group_key),
                points: windows
                    .iter()
                    .map(|window| ActiveInstallsPoint {
                        start: format_active_install_day(window.start),
                        end: format_active_install_day(window.end),
                        value: points_by_window
                            .get(&window.start)
                            .copied()
                            .unwrap_or_default(),
                    })
                    .collect(),
            })
            .collect()
    };

    ActiveInstallsResponse {
        metric: query.metric.as_str().to_owned(),
        start: format_active_install_day(query.window.start),
        end: format_active_install_day(query.window.end),
        interval: query.interval,
        series,
    }
}

fn format_active_install_day(day: DateTime<Utc>) -> String {
    day.format("%Y-%m-%d").to_string()
}

async fn execute_session_metrics_query(
    pool: &PgPool,
    query: &SessionMetricsQuery,
) -> Result<MetricsResponse, MetricsResponseError> {
    let mut tx = begin_metrics_snapshot(pool).await?;
    precheck_session_metrics_group_limit(&mut tx, query).await?;
    let counts_by_bucket = load_session_group_counts_by_bucket(&mut tx, query).await?;
    tx.commit().await.map_err(StoreError::Database)?;

    Ok(build_metrics_response(
        query.metric.as_str(),
        &query.window,
        EMPTY_GROUP_BY,
        counts_by_bucket,
    ))
}

async fn execute_live_installs_query(
    pool: &PgPool,
    project_id: Uuid,
) -> Result<CurrentMetricResponse, StoreError> {
    let mut tx = pool.begin().await.map_err(StoreError::Database)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(StoreError::Database)?;
    let (as_of, value) =
        load_live_install_count_in_tx(&mut tx, project_id, LIVE_INSTALLS_WINDOW_SECONDS).await?;
    tx.commit().await.map_err(StoreError::Database)?;

    Ok(CurrentMetricResponse {
        metric: "live_installs".to_owned(),
        window_seconds: LIVE_INSTALLS_WINDOW_SECONDS as u64,
        as_of,
        value,
    })
}

async fn begin_metrics_snapshot(
    pool: &PgPool,
) -> Result<Transaction<'_, Postgres>, MetricsResponseError> {
    let mut tx = pool.begin().await.map_err(StoreError::Database)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(StoreError::Database)?;
    Ok(tx)
}

async fn precheck_event_metrics_group_limit(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
) -> Result<(), MetricsResponseError> {
    match EMPTY_GROUP_BY.len() {
        0 => Ok(()),
        1 => {
            let primary_rows = load_event_aggregate_rows(
                tx,
                query,
                &cube_keys_with(
                    &query.filters,
                    std::slice::from_ref(EMPTY_GROUP_BY.first().expect("group exists")),
                ),
                Some(MAX_METRIC_GROUPS + 1),
            )
            .await?;

            if primary_rows.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            let total_rows =
                load_event_aggregate_rows(tx, query, &filter_cube_keys(&query.filters), None)
                    .await?;
            let final_groups = synthesize_single_group_counts(
                aggregate_rows_to_groups(&primary_rows, EMPTY_GROUP_BY),
                sum_aggregate_rows(&total_rows),
            )?;

            if final_groups.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            Ok(())
        }
        2 => {
            let pair_rows = load_event_aggregate_rows(
                tx,
                query,
                &cube_keys_with(&query.filters, EMPTY_GROUP_BY),
                Some(MAX_METRIC_GROUPS + 1),
            )
            .await?;
            if pair_rows.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            let first_group = vec![EMPTY_GROUP_BY[0].clone()];
            let first_rows = load_event_aggregate_rows(
                tx,
                query,
                &cube_keys_with(&query.filters, &first_group),
                Some(MAX_METRIC_GROUPS + 1),
            )
            .await?;
            if first_rows.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            let second_group = vec![EMPTY_GROUP_BY[1].clone()];
            let second_rows = load_event_aggregate_rows(
                tx,
                query,
                &cube_keys_with(&query.filters, &second_group),
                Some(MAX_METRIC_GROUPS + 1),
            )
            .await?;
            if second_rows.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            let total_rows =
                load_event_aggregate_rows(tx, query, &filter_cube_keys(&query.filters), None)
                    .await?;
            let final_groups = synthesize_two_group_counts(
                aggregate_rows_to_groups(&pair_rows, EMPTY_GROUP_BY),
                aggregate_rows_to_groups(&first_rows, &first_group),
                aggregate_rows_to_groups(&second_rows, &second_group),
                sum_aggregate_rows(&total_rows),
            )?;

            if final_groups.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            Ok(())
        }
        other => Err(StoreError::InvariantViolation(format!(
            "event metrics group_by arity must be between 0 and 2, got {other}"
        ))
        .into()),
    }
}

async fn precheck_session_metrics_group_limit(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
) -> Result<(), MetricsResponseError> {
    match EMPTY_GROUP_BY.len() {
        0 => Ok(()),
        1 => {
            let primary_rows = load_session_aggregate_rows(
                tx,
                query,
                &cube_keys_with(
                    &query.filters,
                    std::slice::from_ref(EMPTY_GROUP_BY.first().expect("group exists")),
                ),
                Some(MAX_METRIC_GROUPS + 1),
            )
            .await?;

            if primary_rows.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            let total_rows =
                load_session_aggregate_rows(tx, query, &filter_cube_keys(&query.filters), None)
                    .await?;
            let final_groups = synthesize_single_group_counts(
                aggregate_rows_to_groups(&primary_rows, EMPTY_GROUP_BY),
                sum_aggregate_rows(&total_rows),
            )?;

            if final_groups.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            Ok(())
        }
        2 => {
            let pair_rows = load_session_aggregate_rows(
                tx,
                query,
                &cube_keys_with(&query.filters, EMPTY_GROUP_BY),
                Some(MAX_METRIC_GROUPS + 1),
            )
            .await?;
            if pair_rows.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            let first_group = vec![EMPTY_GROUP_BY[0].clone()];
            let first_rows = load_session_aggregate_rows(
                tx,
                query,
                &cube_keys_with(&query.filters, &first_group),
                Some(MAX_METRIC_GROUPS + 1),
            )
            .await?;
            if first_rows.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            let second_group = vec![EMPTY_GROUP_BY[1].clone()];
            let second_rows = load_session_aggregate_rows(
                tx,
                query,
                &cube_keys_with(&query.filters, &second_group),
                Some(MAX_METRIC_GROUPS + 1),
            )
            .await?;
            if second_rows.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            let total_rows =
                load_session_aggregate_rows(tx, query, &filter_cube_keys(&query.filters), None)
                    .await?;
            let final_groups = synthesize_two_group_counts(
                aggregate_rows_to_groups(&pair_rows, EMPTY_GROUP_BY),
                aggregate_rows_to_groups(&first_rows, &first_group),
                aggregate_rows_to_groups(&second_rows, &second_group),
                sum_aggregate_rows(&total_rows),
            )?;

            if final_groups.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            Ok(())
        }
        other => Err(StoreError::InvariantViolation(format!(
            "session metrics group_by arity must be between 0 and 2, got {other}"
        ))
        .into()),
    }
}

async fn load_event_group_counts_by_bucket(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
) -> Result<BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>, MetricsResponseError> {
    match EMPTY_GROUP_BY.len() {
        0 => {
            let total_rows =
                load_event_cube_rows(tx, query, &filter_cube_keys(&query.filters)).await?;
            Ok(sum_rows_by_bucket(&total_rows)
                .into_iter()
                .map(|(bucket_start, value)| (bucket_start, BTreeMap::from([(Vec::new(), value)])))
                .collect())
        }
        1 => {
            let primary_rows =
                load_event_cube_rows(tx, query, &cube_keys_with(&query.filters, EMPTY_GROUP_BY))
                    .await?;
            let total_rows =
                load_event_cube_rows(tx, query, &filter_cube_keys(&query.filters)).await?;

            synthesize_group_counts_by_bucket(
                group_rows_by_bucket(&primary_rows, EMPTY_GROUP_BY),
                sum_rows_by_bucket(&total_rows),
                None,
            )
            .map_err(Into::into)
        }
        2 => {
            let pair_rows =
                load_event_cube_rows(tx, query, &cube_keys_with(&query.filters, EMPTY_GROUP_BY))
                    .await?;
            let first_group = vec![EMPTY_GROUP_BY[0].clone()];
            let first_rows =
                load_event_cube_rows(tx, query, &cube_keys_with(&query.filters, &first_group))
                    .await?;
            let second_group = vec![EMPTY_GROUP_BY[1].clone()];
            let second_rows =
                load_event_cube_rows(tx, query, &cube_keys_with(&query.filters, &second_group))
                    .await?;
            let total_rows =
                load_event_cube_rows(tx, query, &filter_cube_keys(&query.filters)).await?;

            synthesize_group_counts_by_bucket(
                group_rows_by_bucket(&pair_rows, EMPTY_GROUP_BY),
                sum_rows_by_bucket(&total_rows),
                Some((
                    group_rows_by_bucket(&first_rows, &first_group),
                    group_rows_by_bucket(&second_rows, &second_group),
                )),
            )
            .map_err(Into::into)
        }
        other => Err(StoreError::InvariantViolation(format!(
            "event metrics group_by arity must be between 0 and 2, got {other}"
        ))
        .into()),
    }
}

async fn load_session_group_counts_by_bucket(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
) -> Result<BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>, MetricsResponseError> {
    match EMPTY_GROUP_BY.len() {
        0 => {
            let total_rows =
                load_session_cube_rows(tx, query, &filter_cube_keys(&query.filters)).await?;
            Ok(sum_rows_by_bucket(&total_rows)
                .into_iter()
                .map(|(bucket_start, value)| (bucket_start, BTreeMap::from([(Vec::new(), value)])))
                .collect())
        }
        1 => {
            let primary_rows =
                load_session_cube_rows(tx, query, &cube_keys_with(&query.filters, EMPTY_GROUP_BY))
                    .await?;
            let total_rows =
                load_session_cube_rows(tx, query, &filter_cube_keys(&query.filters)).await?;

            synthesize_group_counts_by_bucket(
                group_rows_by_bucket(&primary_rows, EMPTY_GROUP_BY),
                sum_rows_by_bucket(&total_rows),
                None,
            )
            .map_err(Into::into)
        }
        2 => {
            let pair_rows =
                load_session_cube_rows(tx, query, &cube_keys_with(&query.filters, EMPTY_GROUP_BY))
                    .await?;
            let first_group = vec![EMPTY_GROUP_BY[0].clone()];
            let first_rows =
                load_session_cube_rows(tx, query, &cube_keys_with(&query.filters, &first_group))
                    .await?;
            let second_group = vec![EMPTY_GROUP_BY[1].clone()];
            let second_rows =
                load_session_cube_rows(tx, query, &cube_keys_with(&query.filters, &second_group))
                    .await?;
            let total_rows =
                load_session_cube_rows(tx, query, &filter_cube_keys(&query.filters)).await?;

            synthesize_group_counts_by_bucket(
                group_rows_by_bucket(&pair_rows, EMPTY_GROUP_BY),
                sum_rows_by_bucket(&total_rows),
                Some((
                    group_rows_by_bucket(&first_rows, &first_group),
                    group_rows_by_bucket(&second_rows, &second_group),
                )),
            )
            .map_err(Into::into)
        }
        other => Err(StoreError::InvariantViolation(format!(
            "session metrics group_by arity must be between 0 and 2, got {other}"
        ))
        .into()),
    }
}

async fn load_event_cube_rows(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
    cube_keys: &[String],
) -> Result<Vec<MetricsCubeValueRow>, MetricsResponseError> {
    let rows = fetch_event_metrics_cube_rows_in_tx(
        tx,
        query.project_id,
        query.window.granularity,
        &query.event,
        query.window.start,
        query.window.end,
        cube_keys,
        &query.filters,
    )
    .await?;

    Ok(rows.into_iter().map(event_cube_row).collect())
}

async fn load_event_aggregate_rows(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
    cube_keys: &[String],
    limit: Option<usize>,
) -> Result<Vec<MetricsAggregateValueRow>, MetricsResponseError> {
    let rows = fetch_event_metrics_aggregate_cube_rows_in_tx(
        tx,
        query.project_id,
        query.window.granularity,
        &query.event,
        (query.window.start, query.window.end),
        cube_keys,
        &query.filters,
        limit,
    )
    .await?;

    Ok(rows.into_iter().map(event_aggregate_row).collect())
}

async fn load_session_cube_rows(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
    cube_keys: &[String],
) -> Result<Vec<MetricsCubeValueRow>, MetricsResponseError> {
    Ok(fetch_session_metrics_cube_rows_in_tx(
        tx,
        query.project_id,
        query.window.granularity,
        query.metric,
        query.window.start,
        query.window.end,
        cube_keys,
        &query.filters,
    )
    .await?
    .into_iter()
    .map(session_cube_row)
    .collect())
}

async fn load_session_aggregate_rows(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
    cube_keys: &[String],
    limit: Option<usize>,
) -> Result<Vec<MetricsAggregateValueRow>, MetricsResponseError> {
    Ok(fetch_session_metrics_aggregate_cube_rows_in_tx(
        tx,
        query.project_id,
        query.window.granularity,
        query.metric,
        (query.window.start, query.window.end),
        cube_keys,
        &query.filters,
        limit,
    )
    .await?
    .into_iter()
    .map(session_aggregate_row)
    .collect())
}

fn event_cube_row(row: EventMetricsCubeRow) -> MetricsCubeValueRow {
    MetricsCubeValueRow {
        bucket_start: row.bucket_start,
        dimensions: row.dimensions,
        value: row.event_count,
    }
}

fn event_aggregate_row(row: EventMetricsAggregateCubeRow) -> MetricsAggregateValueRow {
    MetricsAggregateValueRow {
        dimensions: row.dimensions,
        value: row.event_count,
    }
}

fn session_cube_row(row: SessionMetricsCubeRow) -> MetricsCubeValueRow {
    MetricsCubeValueRow {
        bucket_start: row.bucket_start,
        dimensions: row.dimensions,
        value: row.value,
    }
}

fn session_aggregate_row(row: SessionMetricsAggregateCubeRow) -> MetricsAggregateValueRow {
    MetricsAggregateValueRow {
        dimensions: row.dimensions,
        value: row.value,
    }
}

fn build_metrics_response(
    metric: &str,
    window: &MetricsBucketWindow,
    group_by: &[String],
    counts_by_bucket: BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>,
) -> MetricsResponse {
    let mut series_by_group = BTreeMap::<GroupKey, BTreeMap<DateTime<Utc>, u64>>::new();

    for (bucket_start, groups) in counts_by_bucket {
        for (group_key, value) in groups {
            series_by_group
                .entry(group_key)
                .or_default()
                .insert(bucket_start, value);
        }
    }

    let series = if group_by.is_empty() {
        vec![MetricsSeries {
            dimensions: BTreeMap::new(),
            points: zero_fill_metrics_points(
                window,
                series_by_group.remove(&Vec::new()).unwrap_or_default(),
            ),
        }]
    } else {
        let mut grouped = series_by_group.into_iter().collect::<Vec<_>>();
        grouped.sort_by(|(left, _), (right, _)| compare_group_keys(left, right));
        grouped
            .into_iter()
            .map(|(group_key, points_by_bucket)| MetricsSeries {
                dimensions: dimensions_for_group(group_by, &group_key),
                points: zero_fill_metrics_points(window, points_by_bucket),
            })
            .collect()
    };

    MetricsResponse {
        metric: metric.to_owned(),
        granularity: window.granularity,
        series,
    }
}

fn synthesize_group_counts_by_bucket(
    primary_by_bucket: CountsByBucket,
    totals_by_bucket: BTreeMap<DateTime<Utc>, u64>,
    singleton_by_bucket: Option<SingletonCountsByBucket>,
) -> Result<CountsByBucket, StoreError> {
    let mut buckets = totals_by_bucket.keys().copied().collect::<BTreeSet<_>>();
    buckets.extend(primary_by_bucket.keys().copied());

    if let Some((first_by_bucket, second_by_bucket)) = singleton_by_bucket.as_ref() {
        buckets.extend(first_by_bucket.keys().copied());
        buckets.extend(second_by_bucket.keys().copied());
    }

    let mut grouped = BTreeMap::new();

    for bucket_start in buckets {
        let total = totals_by_bucket
            .get(&bucket_start)
            .copied()
            .unwrap_or_default();
        let final_groups = match singleton_by_bucket.as_ref() {
            None => synthesize_single_group_counts(
                primary_by_bucket
                    .get(&bucket_start)
                    .cloned()
                    .unwrap_or_default(),
                total,
            )?,
            Some((first_by_bucket, second_by_bucket)) => synthesize_two_group_counts(
                primary_by_bucket
                    .get(&bucket_start)
                    .cloned()
                    .unwrap_or_default(),
                first_by_bucket
                    .get(&bucket_start)
                    .cloned()
                    .unwrap_or_default(),
                second_by_bucket
                    .get(&bucket_start)
                    .cloned()
                    .unwrap_or_default(),
                total,
            )?,
        };

        if !final_groups.is_empty() {
            grouped.insert(bucket_start, final_groups);
        }
    }

    Ok(grouped)
}

fn filter_cube_keys(filters: &BTreeMap<String, String>) -> Vec<String> {
    filters.keys().cloned().collect()
}

fn cube_keys_with(filters: &BTreeMap<String, String>, group_keys: &[String]) -> Vec<String> {
    let mut keys = filter_cube_keys(filters);
    keys.extend(group_keys.iter().cloned());
    keys.sort();
    keys
}

fn sum_rows_by_bucket(rows: &[MetricsCubeValueRow]) -> BTreeMap<DateTime<Utc>, u64> {
    let mut totals = BTreeMap::new();

    for row in rows {
        *totals.entry(row.bucket_start).or_default() += row.value;
    }

    totals
}

fn group_rows_by_bucket(
    rows: &[MetricsCubeValueRow],
    group_by: &[String],
) -> BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>> {
    let mut grouped = BTreeMap::<DateTime<Utc>, BTreeMap<GroupKey, u64>>::new();

    for row in rows {
        let group_key = group_by
            .iter()
            .map(|key| row.dimensions.get(key).cloned())
            .collect::<Vec<_>>();
        *grouped
            .entry(row.bucket_start)
            .or_default()
            .entry(group_key)
            .or_default() += row.value;
    }

    grouped
}

fn aggregate_rows_to_groups(
    rows: &[MetricsAggregateValueRow],
    group_by: &[String],
) -> BTreeMap<GroupKey, u64> {
    let mut grouped = BTreeMap::new();

    for row in rows {
        let group_key = group_by
            .iter()
            .map(|key| row.dimensions.get(key).cloned())
            .collect::<Vec<_>>();
        *grouped.entry(group_key).or_default() += row.value;
    }

    grouped
}

fn sum_aggregate_rows(rows: &[MetricsAggregateValueRow]) -> u64 {
    rows.iter().map(|row| row.value).sum()
}

fn synthesize_single_group_counts(
    mut non_null_groups: BTreeMap<GroupKey, u64>,
    total: u64,
) -> Result<BTreeMap<GroupKey, u64>, StoreError> {
    let non_null_total = non_null_groups.values().sum::<u64>();

    if non_null_total > total {
        // Session cuboids can be temporarily out of sync while bounded rebuild batches catch up.
        // Returning the visible non-null groups is better than failing the whole request.
        return Ok(non_null_groups);
    }

    let null_count = total - non_null_total;
    if null_count > 0 {
        non_null_groups.insert(vec![None], null_count);
    }

    Ok(non_null_groups)
}

fn synthesize_two_group_counts(
    pair_groups: BTreeMap<GroupKey, u64>,
    first_groups: BTreeMap<GroupKey, u64>,
    second_groups: BTreeMap<GroupKey, u64>,
    total: u64,
) -> Result<BTreeMap<GroupKey, u64>, StoreError> {
    let mut final_groups = BTreeMap::new();
    let mut pair_totals_by_first = BTreeMap::<String, u64>::new();
    let mut pair_totals_by_second = BTreeMap::<String, u64>::new();
    let mut returned_total = 0_u64;

    for (group_key, value) in pair_groups {
        let first_value = group_key.first().and_then(Clone::clone).ok_or_else(|| {
            StoreError::InvariantViolation(
                "missing first dimension value in non-null pair".to_owned(),
            )
        })?;
        let second_value = group_key.get(1).and_then(Clone::clone).ok_or_else(|| {
            StoreError::InvariantViolation(
                "missing second dimension value in non-null pair".to_owned(),
            )
        })?;
        *pair_totals_by_first.entry(first_value.clone()).or_default() += value;
        *pair_totals_by_second
            .entry(second_value.clone())
            .or_default() += value;
        returned_total += value;
        final_groups.insert(vec![Some(first_value), Some(second_value)], value);
    }

    for (group_key, total_for_first) in first_groups {
        let first_value = group_key.first().and_then(Clone::clone).ok_or_else(|| {
            StoreError::InvariantViolation(
                "missing first dimension value in single-dimension cube".to_owned(),
            )
        })?;
        let used = pair_totals_by_first
            .get(&first_value)
            .copied()
            .unwrap_or_default();
        let null_count = total_for_first.saturating_sub(used);
        if null_count > 0 {
            returned_total += null_count;
            final_groups.insert(vec![Some(first_value), None], null_count);
        }
    }

    for (group_key, total_for_second) in second_groups {
        let second_value = group_key.first().and_then(Clone::clone).ok_or_else(|| {
            StoreError::InvariantViolation(
                "missing second dimension value in single-dimension cube".to_owned(),
            )
        })?;
        let used = pair_totals_by_second
            .get(&second_value)
            .copied()
            .unwrap_or_default();
        let null_count = total_for_second.saturating_sub(used);
        if null_count > 0 {
            returned_total += null_count;
            final_groups.insert(vec![None, Some(second_value)], null_count);
        }
    }

    if returned_total > total {
        return Ok(final_groups);
    }

    let double_null = total - returned_total;
    if double_null > 0 {
        final_groups.insert(vec![None, None], double_null);
    }

    Ok(final_groups)
}

fn zero_fill_metrics_points(
    window: &MetricsBucketWindow,
    points_by_bucket: BTreeMap<DateTime<Utc>, u64>,
) -> Vec<MetricsPoint> {
    let mut filled = Vec::new();
    let mut current = window.start;

    while current <= window.end {
        filled.push(MetricsPoint {
            bucket: format_bucket(current, window.granularity),
            value: points_by_bucket.get(&current).copied().unwrap_or_default(),
        });
        current = next_bucket_start(current, window.granularity);
    }

    filled
}

fn next_bucket_start(bucket_start: DateTime<Utc>, granularity: MetricGranularity) -> DateTime<Utc> {
    match granularity {
        MetricGranularity::Hour => bucket_start + ChronoDuration::hours(1),
        MetricGranularity::Day => bucket_start + ChronoDuration::days(1),
        MetricGranularity::Week => bucket_start + ChronoDuration::weeks(1),
        MetricGranularity::Month => {
            let date = bucket_start.date_naive();
            let (year, month) = if date.month() == 12 {
                (date.year() + 1, 1)
            } else {
                (date.year(), date.month() + 1)
            };
            DateTime::from_naive_utc_and_offset(
                NaiveDate::from_ymd_opt(year, month, 1)
                    .expect("valid first day of next month")
                    .and_hms_opt(0, 0, 0)
                    .expect("midnight is valid"),
                Utc,
            )
        }
        MetricGranularity::Year => DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(bucket_start.year() + 1, 1, 1)
                .expect("valid first day of next year")
                .and_hms_opt(0, 0, 0)
                .expect("midnight is valid"),
            Utc,
        ),
    }
}

fn format_bucket(bucket_start: DateTime<Utc>, granularity: MetricGranularity) -> String {
    match granularity {
        MetricGranularity::Hour => bucket_start.to_rfc3339_opts(SecondsFormat::Secs, true),
        MetricGranularity::Day
        | MetricGranularity::Week
        | MetricGranularity::Month
        | MetricGranularity::Year => bucket_start.format("%Y-%m-%d").to_string(),
    }
}

fn dimensions_for_group(
    group_by: &[String],
    group_key: &[Option<String>],
) -> BTreeMap<String, Option<String>> {
    group_by
        .iter()
        .cloned()
        .zip(group_key.iter().cloned())
        .collect()
}

fn compare_group_keys(left: &[Option<String>], right: &[Option<String>]) -> Ordering {
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let ordering = match (left_value, right_value) {
            (Some(left_value), Some(right_value)) => left_value.cmp(right_value),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };

        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    left.len().cmp(&right.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Request, StatusCode};
    use chrono::TimeZone;
    use tower::ServiceExt;

    fn bucket(day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, day, hour, 0, 0)
            .single()
            .expect("valid bucket")
    }

    fn day_window() -> MetricsBucketWindow {
        MetricsBucketWindow {
            granularity: MetricGranularity::Day,
            start: bucket(1, 0),
            end: bucket(3, 0),
        }
    }

    fn hour_window() -> MetricsBucketWindow {
        MetricsBucketWindow {
            granularity: MetricGranularity::Hour,
            start: bucket(1, 0),
            end: bucket(1, 2),
        }
    }

    #[tokio::test]
    async fn build_metrics_response_zero_fills_ungrouped_day_series() {
        let response = build_metrics_response(
            "count",
            &day_window(),
            &[],
            BTreeMap::from([
                (bucket(1, 0), BTreeMap::from([(Vec::new(), 2_u64)])),
                (bucket(3, 0), BTreeMap::from([(Vec::new(), 4_u64)])),
            ]),
        );

        assert_eq!(
            serde_json::to_value(response).expect("serialize response"),
            serde_json::json!({
                "metric": "count",
                "granularity": "day",
                "series": [
                    {
                        "dimensions": {},
                        "points": [
                            { "bucket": "2026-01-01", "value": 2 },
                            { "bucket": "2026-01-02", "value": 0 },
                            { "bucket": "2026-01-03", "value": 4 }
                        ]
                    }
                ]
            })
        );
    }

    #[tokio::test]
    async fn build_metrics_response_zero_fills_grouped_hour_series() {
        let response = build_metrics_response(
            "count",
            &hour_window(),
            &["platform".to_owned()],
            BTreeMap::from([(
                bucket(1, 1),
                BTreeMap::from([(vec![Some("ios".to_owned())], 3_u64)]),
            )]),
        );

        assert_eq!(
            serde_json::to_value(response).expect("serialize response"),
            serde_json::json!({
                "metric": "count",
                "granularity": "hour",
                "series": [
                    {
                        "dimensions": { "platform": "ios" },
                        "points": [
                            { "bucket": "2026-01-01T00:00:00Z", "value": 0 },
                            { "bucket": "2026-01-01T01:00:00Z", "value": 3 },
                            { "bucket": "2026-01-01T02:00:00Z", "value": 0 }
                        ]
                    }
                ]
            })
        );
    }

    #[tokio::test]
    async fn app_exposes_bucketed_metrics_routes_and_live_installs() {
        let pool = PgPool::connect_lazy("postgres://localhost/fantasma").expect("lazy pool");
        let app = super::app(
            pool,
            Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
        );

        let events_family_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/metrics/events")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("events route response");
        let sessions_family_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/metrics/sessions")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("sessions route response");
        let live_installs_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/metrics/live_installs")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("live installs route response");
        let usage_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/usage/events?start=2026-03-01&end=2026-03-01")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("usage route response");
        let legacy_paths = [
            "/v1/metrics/events/daily",
            "/v1/metrics/events/aggregate",
            "/v1/metrics/sessions/count/daily",
            "/v1/metrics/sessions/duration",
            "/v1/metrics/active-installs",
        ];

        assert_ne!(events_family_response.status(), StatusCode::NOT_FOUND);
        assert_ne!(sessions_family_response.status(), StatusCode::NOT_FOUND);
        assert_ne!(live_installs_response.status(), StatusCode::NOT_FOUND);
        assert_ne!(usage_response.status(), StatusCode::NOT_FOUND);
        for path in legacy_paths {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .body(axum::body::Body::empty())
                        .expect("request"),
                )
                .await
                .expect("legacy route response");

            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "{path} must be removed"
            );
        }
    }

    #[test]
    fn parse_event_metrics_query_accepts_all_four_built_in_filters() {
        let query = parse_event_metrics_query(
            "event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&platform=ios&app_version=1.0.0&os_version=18.3&locale=pt-PT",
        )
        .expect("query parses");

        assert_eq!(query.metric, EventMetric::Count);
        assert_eq!(query.filters.len(), 4);
        assert_eq!(query.filters.get("locale"), Some(&"pt-PT".to_owned()));
    }

    #[test]
    fn parse_event_metrics_query_accepts_requests_without_project_id() {
        let query = parse_event_metrics_query(
            "event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02",
        )
        .expect("query parses");

        assert_eq!(query.event, "app_open");
        assert_eq!(
            query.window,
            MetricsBucketWindow {
                granularity: MetricGranularity::Day,
                start: Utc
                    .with_ymd_and_hms(2026, 3, 1, 0, 0, 0)
                    .single()
                    .expect("start"),
                end: Utc
                    .with_ymd_and_hms(2026, 3, 2, 0, 0, 0)
                    .single()
                    .expect("end"),
            }
        );
    }

    #[test]
    fn parse_usage_events_query_accepts_exact_date_bounds() {
        let query =
            parse_usage_events_query("start=2026-03-01&end=2026-03-31").expect("query parses");

        assert_eq!(
            query.start,
            NaiveDate::from_ymd_opt(2026, 3, 1).expect("start")
        );
        assert_eq!(
            query.end,
            NaiveDate::from_ymd_opt(2026, 3, 31).expect("end")
        );
    }

    #[test]
    fn parse_usage_events_query_rejects_duplicate_start() {
        let error = parse_usage_events_query("start=2026-03-01&start=2026-03-02&end=2026-03-31")
            .unwrap_err();

        assert_eq!(error.error_code(), "duplicate_query_key");
    }

    #[test]
    fn parse_usage_events_query_rejects_duplicate_end() {
        let error =
            parse_usage_events_query("start=2026-03-01&end=2026-03-31&end=2026-04-01").unwrap_err();

        assert_eq!(error.error_code(), "duplicate_query_key");
    }

    #[test]
    fn parse_usage_events_query_rejects_legacy_and_unknown_keys() {
        for raw_query in [
            "project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start=2026-03-01&end=2026-03-31",
            "start_date=2026-03-01&start=2026-03-01&end=2026-03-31",
            "end_date=2026-03-31&start=2026-03-01&end=2026-03-31",
            "start=2026-03-01&end=2026-03-31&plan=pro",
        ] {
            let error = parse_usage_events_query(raw_query).unwrap_err();
            assert_eq!(error.error_code(), "invalid_query_key");
        }
    }

    #[test]
    fn parse_usage_events_query_rejects_invalid_time_range() {
        let error = parse_usage_events_query("start=2026-03-31&end=2026-03-01").unwrap_err();

        assert_eq!(error.error_code(), "invalid_time_range");
    }

    #[test]
    fn parse_event_metrics_query_rejects_invalid_property_filter_keys() {
        let error = parse_event_metrics_query(
            "event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&plan-name=pro",
        )
        .unwrap_err();

        assert_eq!(error.error_code(), "invalid_query_key");
    }

    #[test]
    fn parse_event_metrics_query_rejects_group_by() {
        let error = parse_event_metrics_query(
            "event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&group_by=provider",
        )
        .unwrap_err();

        assert_eq!(error.error_code(), "invalid_query_key");
    }

    #[test]
    fn parse_event_metrics_query_treats_metric_as_required_public_key() {
        let query = parse_event_metrics_query(
            "event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02",
        )
        .expect("query parses");

        assert_eq!(query.metric, EventMetric::Count);
        assert!(!query.filters.contains_key("metric"));
    }

    #[test]
    fn parse_event_metrics_query_treats_start_and_end_as_required_public_keys() {
        let query = parse_event_metrics_query(
            "event=app_open&metric=count&granularity=hour&start=2026-03-01T00:00:00Z&end=2026-03-01T01:00:00Z",
        )
        .expect("query parses");

        assert_eq!(query.window.granularity, MetricGranularity::Hour);
        assert!(!query.filters.contains_key("start"));
        assert!(!query.filters.contains_key("end"));
    }

    #[test]
    fn parse_event_metrics_query_rejects_count_without_event() {
        let error = parse_event_metrics_query(
            "metric=count&granularity=day&start=2026-03-01&end=2026-03-02",
        )
        .unwrap_err();

        assert_eq!(error.error_code(), "invalid_event");
    }

    #[test]
    fn parse_event_metrics_query_rejects_total_count() {
        let error = parse_event_metrics_query(
            "metric=total_count&granularity=day&start=2026-03-01&end=2026-03-02",
        )
        .unwrap_err();

        assert_eq!(error.error_code(), "invalid_metric");
    }

    #[test]
    fn parse_session_metric_query_accepts_exact_public_keys_only() {
        let query = parse_session_metric_query(
            "metric=count&granularity=hour&start=2026-03-01T00:00:00Z&end=2026-03-02T00:00:00Z",
        )
        .expect("query parses");

        assert_eq!(query.metric, SessionMetric::Count);
        assert_eq!(
            query.window.start,
            chrono::DateTime::parse_from_rfc3339("2026-03-01T00:00:00Z")
                .expect("start")
                .with_timezone(&chrono::Utc)
        );
        assert_eq!(
            query.window.end,
            chrono::DateTime::parse_from_rfc3339("2026-03-02T00:00:00Z")
                .expect("end")
                .with_timezone(&chrono::Utc)
        );
    }

    #[test]
    fn parse_session_metric_query_accepts_all_four_built_in_filters() {
        let query = parse_session_metric_query(
            "metric=count&granularity=day&start=2026-03-01&end=2026-03-02&platform=ios&app_version=1.0.0&os_version=18.3&locale=en-GB",
        )
        .expect("query parses");

        assert_eq!(query.filters.len(), 4);
        assert_eq!(query.filters.get("locale"), Some(&"en-GB".to_owned()));
    }

    #[test]
    fn parse_session_metric_query_accepts_exact_range_active_installs() {
        let query =
            parse_session_metric_query("metric=active_installs&start=2026-03-01&end=2026-03-17")
                .expect("query parses");

        assert_eq!(query.metric, SessionMetric::ActiveInstalls);
        assert_eq!(
            query.window.start,
            Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0)
                .single()
                .expect("start")
        );
        assert_eq!(
            query.window.end,
            Utc.with_ymd_and_hms(2026, 3, 17, 0, 0, 0)
                .single()
                .expect("end")
        );
    }

    #[test]
    fn parse_session_metric_query_accepts_intervaled_active_installs() {
        let query = parse_session_metric_query(
            "metric=active_installs&start=2026-03-01&end=2026-03-17&interval=week",
        )
        .expect("query parses");

        assert_eq!(query.metric, SessionMetric::ActiveInstalls);
        assert_eq!(
            query.window.start,
            Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0)
                .single()
                .expect("start")
        );
        assert_eq!(
            query.window.end,
            Utc.with_ymd_and_hms(2026, 3, 17, 0, 0, 0)
                .single()
                .expect("end")
        );
    }

    #[test]
    fn parse_session_metric_query_rejects_group_by() {
        let error = parse_session_metric_query(
            "metric=count&granularity=day&start=2026-03-01&end=2026-03-02&group_by=provider",
        )
        .unwrap_err();

        assert_eq!(error, "invalid_query_key");
    }

    #[test]
    fn parse_live_installs_query_rejects_locale_filter() {
        let error = parse_live_installs_query("locale=en-GB").unwrap_err();

        assert_eq!(error, "invalid_query_key");
    }

    #[test]
    fn parse_event_catalog_query_rejects_limit() {
        let error =
            parse_event_catalog_query("start=2026-03-01&end=2026-03-02&limit=1").unwrap_err();

        assert_eq!(error, "invalid_query_key");
    }

    #[test]
    fn parse_session_metric_query_rejects_legacy_project_id() {
        let error = parse_session_metric_query(
            "project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&metric=count&granularity=day&start=2026-03-01&end=2026-03-02",
        )
        .unwrap_err();

        assert_eq!(error, "invalid_query_key");
    }

    #[test]
    fn parse_session_metric_query_rejects_legacy_date_keys() {
        let error = parse_session_metric_query(
            "metric=count&granularity=day&start_date=2026-03-01&end_date=2026-03-02",
        )
        .unwrap_err();

        assert_eq!(error, "invalid_query_key");
    }

    #[test]
    fn parse_session_metric_query_rejects_active_installs_granularity() {
        let error = parse_session_metric_query(
            "metric=active_installs&granularity=day&start=2026-03-01&end=2026-03-02",
        )
        .unwrap_err();

        assert_eq!(error, "invalid_request");
    }

    #[test]
    fn parse_session_metric_query_rejects_interval_for_bucket_metrics() {
        let error = parse_session_metric_query(
            "metric=count&granularity=day&start=2026-03-01&end=2026-03-02&interval=day",
        )
        .unwrap_err();

        assert_eq!(error, "invalid_request");
    }

    #[test]
    fn validate_active_installs_query_rejects_point_count_over_limit() {
        let public_query = parse_session_metric_query(
            "metric=active_installs&start=2026-01-01&end=2026-05-20&interval=day",
        )
        .expect("query parses");
        let query = SessionMetricsQuery {
            project_id: Uuid::nil(),
            metric: public_query.metric,
            window: public_query.window,
            interval: public_query.interval,
            filters: public_query.filters,
        };

        assert_eq!(
            validate_active_installs_query(&query),
            Err("invalid_request")
        );
    }

    #[test]
    fn active_install_read_strategy_uses_bitmaps_for_up_to_two_filters() {
        assert_eq!(
            active_install_read_strategy(&BTreeMap::new()),
            ActiveInstallReadStrategy::BitmapTotals
        );
        assert_eq!(
            active_install_read_strategy(&BTreeMap::from([(
                "platform".to_owned(),
                "ios".to_owned()
            )])),
            ActiveInstallReadStrategy::BitmapDim1
        );
        assert_eq!(
            active_install_read_strategy(&BTreeMap::from([
                ("platform".to_owned(), "ios".to_owned()),
                ("locale".to_owned(), "en-US".to_owned()),
            ])),
            ActiveInstallReadStrategy::BitmapDim2
        );
    }

    #[test]
    fn active_install_read_strategy_falls_back_after_two_filters() {
        assert_eq!(
            active_install_read_strategy(&BTreeMap::from([
                ("platform".to_owned(), "ios".to_owned()),
                ("app_version".to_owned(), "1.0.0".to_owned()),
                ("locale".to_owned(), "en-US".to_owned()),
            ])),
            ActiveInstallReadStrategy::DistinctCount
        );
        assert_eq!(
            active_install_read_strategy(&BTreeMap::from([
                ("platform".to_owned(), "ios".to_owned()),
                ("app_version".to_owned(), "1.0.0".to_owned()),
                ("os_version".to_owned(), "18.3".to_owned()),
                ("locale".to_owned(), "en-US".to_owned()),
            ])),
            ActiveInstallReadStrategy::DistinctCount
        );
    }

    #[test]
    fn next_bucket_start_advances_week_month_and_year() {
        assert_eq!(
            next_bucket_start(
                Utc.with_ymd_and_hms(2026, 3, 2, 0, 0, 0)
                    .single()
                    .expect("week start"),
                MetricGranularity::Week,
            ),
            Utc.with_ymd_and_hms(2026, 3, 9, 0, 0, 0)
                .single()
                .expect("next week")
        );
        assert_eq!(
            next_bucket_start(
                Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0)
                    .single()
                    .expect("month start"),
                MetricGranularity::Month,
            ),
            Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0)
                .single()
                .expect("next month")
        );
        assert_eq!(
            next_bucket_start(
                Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
                    .single()
                    .expect("year start"),
                MetricGranularity::Year,
            ),
            Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0)
                .single()
                .expect("next year")
        );
    }

    #[test]
    fn format_bucket_uses_date_labels_for_non_daily_range_buckets() {
        assert_eq!(
            format_bucket(
                Utc.with_ymd_and_hms(2026, 3, 2, 0, 0, 0)
                    .single()
                    .expect("week start"),
                MetricGranularity::Week,
            ),
            "2026-03-02"
        );
        assert_eq!(
            format_bucket(
                Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0)
                    .single()
                    .expect("month start"),
                MetricGranularity::Month,
            ),
            "2026-03-01"
        );
        assert_eq!(
            format_bucket(
                Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
                    .single()
                    .expect("year start"),
                MetricGranularity::Year,
            ),
            "2026-01-01"
        );
    }

    #[test]
    fn zero_fill_metrics_points_includes_empty_week_buckets() {
        let window = MetricsBucketWindow {
            granularity: MetricGranularity::Week,
            start: Utc
                .with_ymd_and_hms(2026, 3, 2, 0, 0, 0)
                .single()
                .expect("start"),
            end: Utc
                .with_ymd_and_hms(2026, 3, 16, 0, 0, 0)
                .single()
                .expect("end"),
        };
        let points = zero_fill_metrics_points(
            &window,
            BTreeMap::from([(
                Utc.with_ymd_and_hms(2026, 3, 9, 0, 0, 0)
                    .single()
                    .expect("middle"),
                5,
            )]),
        );

        assert_eq!(
            points,
            vec![
                MetricsPoint {
                    bucket: "2026-03-02".to_owned(),
                    value: 0,
                },
                MetricsPoint {
                    bucket: "2026-03-09".to_owned(),
                    value: 5,
                },
                MetricsPoint {
                    bucket: "2026-03-16".to_owned(),
                    value: 0,
                },
            ]
        );
    }

    #[test]
    fn query_error_response_returns_not_found_status() {
        let response = query_error_response("not_found");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn openapi_documents_scoped_auth_and_event_metrics_contract() {
        let spec: serde_yaml::Value =
            serde_yaml::from_str(include_str!("../../../schemas/openapi/fantasma.yaml"))
                .expect("openapi parses");

        let ingest_422 = &spec["paths"]["/v1/events"]["post"]["responses"]["422"]["content"]["application/json"]
            ["schema"]["oneOf"];
        let event_metrics_500 = &spec["paths"]["/v1/metrics/events"]["get"]["responses"]["500"];
        let catalog_security = spec["paths"]["/v1/metrics/events/catalog"]["get"]["security"]
            .as_sequence()
            .expect("catalog security");
        let top_parameters = spec["paths"]["/v1/metrics/events/top"]["get"]["parameters"]
            .as_sequence()
            .expect("top-events parameters");
        let usage_route = &spec["paths"]["/v1/usage/events"]["get"];
        let usage_200 = &usage_route["responses"]["200"];
        let usage_security = usage_route["security"]
            .as_sequence()
            .expect("usage security");
        let project_route_security = spec["paths"]["/v1/projects/{project_id}"]["get"]["security"]
            .as_sequence()
            .expect("project route security");
        let live_installs_route = &spec["paths"]["/v1/metrics/live_installs"]["get"];
        let live_installs_200 = &live_installs_route["responses"]["200"];
        let session_metrics_route = &spec["paths"]["/v1/metrics/sessions"]["get"];
        let session_metrics_200 = &session_metrics_route["responses"]["200"]["content"]["application/json"]
            ["schema"]["oneOf"];
        let session_metrics_500 = &spec["paths"]["/v1/metrics/sessions"]["get"]["responses"]["500"];
        let session_metrics_parameters = session_metrics_route["parameters"]
            .as_sequence()
            .expect("session metrics parameters");
        let platform_filter_schema = &spec["components"]["parameters"]["PlatformFilter"]["schema"];
        let locale_filter_schema = &spec["components"]["parameters"]["LocaleFilter"]["schema"];
        let operator_auth = &spec["components"]["securitySchemes"]["OperatorBearerAuth"];
        let project_key_auth = &spec["components"]["securitySchemes"]["ProjectKeyHeader"];
        let usage_response_schema = &spec["components"]["schemas"]["UsageEventsResponse"];
        let usage_project_schema = &spec["components"]["schemas"]["UsageProjectEvents"];
        let project_schema = &spec["components"]["schemas"]["ProjectSummary"];
        let live_installs_schema = &spec["components"]["schemas"]["LiveInstallsResponse"];
        let event_metrics_description = &spec["paths"]["/v1/metrics/events"]["get"]["description"];
        let event_metrics_parameters = spec["paths"]["/v1/metrics/events"]["get"]["parameters"]
            .as_sequence()
            .expect("event metrics parameters");
        let event_metric_granularity = &spec["components"]["parameters"]["EventMetricGranularity"];
        let session_metric_granularity =
            &spec["components"]["parameters"]["SessionMetricGranularity"];
        let event_name_description = &spec["components"]["parameters"]["EventName"]["description"];
        let management_security = spec["paths"]["/v1/projects"]["get"]["security"]
            .as_sequence()
            .expect("management security");
        let metrics_security = spec["paths"]["/v1/metrics/events"]["get"]["security"]
            .as_sequence()
            .expect("metrics security");

        assert!(
            ingest_422.is_sequence(),
            "ingest 422 must document both structural and indexed validation errors"
        );
        assert!(
            ingest_422
                .as_sequence()
                .expect("sequence")
                .iter()
                .any(|schema| schema["$ref"] == "#/components/schemas/ErrorResponse"),
            "ingest 422 must include invalid_request in the published envelope"
        );
        assert!(
            ingest_422
                .as_sequence()
                .expect("sequence")
                .iter()
                .any(|schema| schema["$ref"] == "#/components/schemas/EventValidationResponse"),
            "ingest 422 must include indexed validation errors in the published envelope"
        );
        assert!(
            event_metrics_500.is_mapping(),
            "event metrics route must document internal_server_error"
        );
        assert!(
            usage_route.is_mapping(),
            "openapi must publish the operator usage route"
        );
        assert!(
            usage_200.is_mapping(),
            "usage route must document a success response"
        );
        assert!(
            spec["paths"]["/v1/projects/{project_id}"]["patch"].is_mapping(),
            "openapi must publish the project metadata patch route"
        );
        assert!(
            spec["paths"]["/v1/metrics/events/total"].is_null(),
            "openapi must remove the total-event route"
        );
        assert!(
            spec["paths"]["/v1/metrics/events/catalog"]["get"].is_mapping(),
            "openapi must publish the event catalog route"
        );
        assert!(
            spec["paths"]["/v1/metrics/events/top"]["get"].is_mapping(),
            "openapi must publish the top-events route"
        );
        assert!(
            live_installs_route.is_mapping(),
            "openapi must publish the live-installs route"
        );
        assert!(
            live_installs_200.is_mapping(),
            "live-installs route must document a success response"
        );
        assert!(
            session_metrics_500.is_mapping(),
            "session metrics route must document internal_server_error"
        );
        assert!(
            session_metrics_200
                .as_sequence()
                .expect("session metrics oneOf")
                .iter()
                .any(|schema| schema["$ref"] == "#/components/schemas/ActiveInstallsResponse"),
            "session metrics route must publish the exact-range active_installs response shape"
        );
        assert!(
            session_metrics_parameters
                .iter()
                .any(|parameter| parameter["$ref"] == "#/components/parameters/MetricInterval"),
            "session metrics route must document the active_installs interval parameter"
        );
        assert!(
            event_metrics_parameters
                .iter()
                .any(|parameter| parameter["$ref"]
                    == "#/components/parameters/EventMetricGranularity"),
            "event metrics route must require the event-specific granularity parameter"
        );
        assert!(
            session_metrics_parameters
                .iter()
                .any(|parameter| parameter["$ref"]
                    == "#/components/parameters/SessionMetricGranularity"),
            "session metrics route must use the conditional granularity parameter"
        );
        assert_eq!(
            event_metric_granularity["required"].as_bool(),
            Some(true),
            "event metric granularity must be marked required in OpenAPI"
        );
        assert_eq!(
            session_metric_granularity["required"].as_bool(),
            Some(false),
            "session metric granularity must remain conditional in OpenAPI"
        );
        assert_eq!(locale_filter_schema["type"], "string");
        assert!(
            event_metrics_description
                .as_str()
                .expect("event metrics description")
                .contains("built-in equality filters"),
            "openapi must document the narrowed event metric contract"
        );
        assert!(
            event_name_description
                .as_str()
                .expect("event name description")
                .contains("Required when `metric=count`."),
            "the event query parameter description must explain the conditional event contract"
        );
        assert!(
            !event_name_description
                .as_str()
                .expect("event name description")
                .contains("total_count"),
            "the event query parameter description must not mention removed total_count support"
        );
        assert_eq!(platform_filter_schema["type"], "string");
        assert_eq!(operator_auth["type"], "http");
        assert_eq!(operator_auth["scheme"], "bearer");
        assert!(
            operator_auth["description"]
                .as_str()
                .expect("operator auth description")
                .contains("/v1/usage/*"),
            "operator auth description must include usage routes"
        );
        assert_eq!(project_key_auth["type"], "apiKey");
        assert_eq!(project_key_auth["in"], "header");
        assert_eq!(project_key_auth["name"], "X-Fantasma-Key");
        assert_eq!(
            usage_response_schema["required"]
                .as_sequence()
                .map(|required| required.len()),
            Some(4)
        );
        assert_eq!(project_schema["type"], "object");
        assert!(
            usage_project_schema["properties"]["project"]["$ref"]
                == "#/components/schemas/ProjectSummary"
        );
        assert_eq!(
            live_installs_schema["properties"]["metric"]["const"],
            "live_installs"
        );
        assert_eq!(
            live_installs_schema["properties"]["window_seconds"]["const"],
            120
        );
        assert!(
            event_metrics_parameters
                .iter()
                .all(|parameter| parameter["$ref"] != "#/components/parameters/ProjectId"),
            "metrics must not document a public project_id query parameter"
        );
        assert!(
            management_security
                .iter()
                .any(|entry| entry["OperatorBearerAuth"].is_sequence()),
            "management routes must document operator bearer auth"
        );
        assert!(
            project_route_security
                .iter()
                .any(|entry| entry["OperatorBearerAuth"].is_sequence()),
            "project metadata routes must document operator bearer auth"
        );
        assert!(
            metrics_security
                .iter()
                .any(|entry| entry["ProjectKeyHeader"].is_sequence()),
            "metrics routes must document project-key auth"
        );
        assert!(
            usage_security
                .iter()
                .any(|entry| entry["OperatorBearerAuth"].is_sequence()),
            "usage routes must document operator bearer auth"
        );
        assert!(
            catalog_security
                .iter()
                .any(|entry| entry["ProjectKeyHeader"].is_sequence()),
            "event catalog must document project-key auth"
        );
        assert!(
            top_parameters
                .iter()
                .any(|parameter| parameter["$ref"] == "#/components/parameters/TopEventsLimit"),
            "top-events must document the limit parameter"
        );
    }

    #[test]
    fn synthesize_single_group_counts_adds_null_bucket_to_final_group_count() {
        let non_null = (0..100)
            .map(|index| (vec![Some(format!("provider_{index:03}"))], 1_u64))
            .collect::<BTreeMap<_, _>>();

        let final_groups =
            synthesize_single_group_counts(non_null, 101).expect("groups synthesize");

        assert_eq!(final_groups.len(), 101);
        assert_eq!(final_groups.get(&vec![None]), Some(&1));
    }

    #[test]
    fn synthesize_two_group_counts_handles_single_null_buckets_after_filtering() {
        let pair_groups = BTreeMap::from([(
            vec![Some("strava".to_owned()), Some("true".to_owned())],
            1_u64,
        )]);
        let first_groups = BTreeMap::from([(vec![Some("strava".to_owned())], 2_u64)]);
        let second_groups = BTreeMap::from([
            (vec![Some("true".to_owned())], 1_u64),
            (vec![Some("false".to_owned())], 1_u64),
        ]);

        let final_groups = synthesize_two_group_counts(pair_groups, first_groups, second_groups, 3)
            .expect("groups synthesize");

        assert_eq!(final_groups.len(), 3);
        assert_eq!(
            final_groups.get(&vec![Some("strava".to_owned()), Some("true".to_owned())]),
            Some(&1)
        );
        assert_eq!(
            final_groups.get(&vec![Some("strava".to_owned()), None]),
            Some(&1)
        );
        assert_eq!(
            final_groups.get(&vec![None, Some("false".to_owned())]),
            Some(&1)
        );
    }

    #[test]
    fn synthesize_single_group_counts_tolerates_temporarily_inconsistent_totals() {
        let non_null = BTreeMap::from([(vec![Some("ios".to_owned())], 3_u64)]);

        let final_groups =
            synthesize_single_group_counts(non_null.clone(), 2).expect("groups synthesize");

        assert_eq!(final_groups, non_null);
    }

    #[test]
    fn synthesize_two_group_counts_tolerates_temporarily_inconsistent_totals() {
        let pair_groups = BTreeMap::from([(
            vec![Some("ios".to_owned()), Some("1.0.0".to_owned())],
            3_u64,
        )]);
        let first_groups = BTreeMap::from([(vec![Some("ios".to_owned())], 2_u64)]);
        let second_groups = BTreeMap::from([(vec![Some("1.0.0".to_owned())], 2_u64)]);

        let final_groups =
            synthesize_two_group_counts(pair_groups.clone(), first_groups, second_groups, 2)
                .expect("groups synthesize");

        assert_eq!(final_groups, pair_groups);
    }
}
