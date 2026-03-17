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
    CurrentMetricResponse, EventMetric, EventMetricsQuery, MetricGranularity, MetricsBucketWindow,
    MetricsPoint, MetricsResponse, MetricsSeries, SessionMetric, SessionMetricsQuery,
    is_reserved_event_property_key, is_valid_event_property_key,
};
use fantasma_store::{
    ApiKeyKind, EventCatalogRecord, EventMetricsAggregateCubeRow, EventMetricsCubeRow, PgPool,
    ProjectRecord, ResolvedApiKey, SessionMetricsAggregateCubeRow, SessionMetricsCubeRow,
    StoreError, TopEventRecord, create_api_key, create_project_with_api_key,
    fetch_event_metrics_aggregate_cube_rows_in_tx, fetch_event_metrics_cube_rows_in_tx,
    fetch_session_metrics_aggregate_cube_rows_in_tx, fetch_session_metrics_cube_rows_in_tx,
    generate_api_key_secret, list_api_keys, list_event_catalog, list_projects, list_top_events,
    load_live_install_count_in_tx, load_project, rename_project, resolve_api_key, revoke_api_key,
};
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
    group_by: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicSessionMetricsQuery {
    metric: SessionMetric,
    window: MetricsBucketWindow,
    filters: BTreeMap<String, String>,
    group_by: Vec<String>,
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
            get(get_project_route).patch(patch_project_route),
        )
        .route(
            "/v1/projects/{project_id}/keys",
            get(list_project_keys_route).post(create_project_key_route),
        )
        .route(
            "/v1/projects/{project_id}/keys/{key_id}",
            delete(revoke_project_key_route),
        )
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

const MAX_GROUP_BY_DIMENSIONS: usize = 2;
const MAX_EVENT_REFERENCED_DIMENSIONS: usize = 2;
const MAX_METRIC_GROUPS: usize = 100;
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
    let mut group_by = Vec::new();
    let mut group_by_seen = BTreeSet::new();

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
            "group_by" => {
                if value.is_empty() {
                    return Err(EventMetricsQueryError("invalid_group_by"));
                }

                if !matches!(value.as_str(), "platform" | "app_version" | "os_version")
                    && (is_reserved_event_property_key(&value)
                        || !is_valid_event_property_key(&value))
                {
                    return Err(EventMetricsQueryError("invalid_group_by"));
                }

                if !group_by_seen.insert(value.clone()) {
                    return Err(EventMetricsQueryError("duplicate_group_by"));
                }

                group_by.push(value);
            }
            "platform" | "app_version" | "os_version" => {
                if filters.insert(key, value).is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }
            }
            "project_id" | "start_date" | "end_date" => {
                return Err(EventMetricsQueryError("invalid_query_key"));
            }
            _ => {
                if is_reserved_event_property_key(&key) || !is_valid_event_property_key(&key) {
                    return Err(EventMetricsQueryError("invalid_query_key"));
                }

                if filters.insert(key, value).is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }
            }
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

    if group_by.len() > MAX_GROUP_BY_DIMENSIONS {
        return Err(EventMetricsQueryError("too_many_group_by_dimensions"));
    }

    let filter_keys = filters.keys().cloned().collect::<BTreeSet<_>>();
    if group_by.iter().any(|key| filter_keys.contains(key)) {
        return Err(EventMetricsQueryError("conflicting_dimension_usage"));
    }

    let mut referenced_dimensions = filter_keys;
    referenced_dimensions.extend(group_by.iter().cloned());
    if referenced_dimensions.len() > MAX_EVENT_REFERENCED_DIMENSIONS {
        return Err(EventMetricsQueryError("too_many_dimensions"));
    }

    Ok(PublicEventMetricsQuery {
        metric,
        event,
        window,
        filters,
        group_by,
    })
}

fn parse_session_metric_query(raw_query: &str) -> Result<PublicSessionMetricsQuery, &'static str> {
    let mut metric = None;
    let mut granularity = None;
    let mut start = None;
    let mut end = None;
    let mut filters = BTreeMap::new();
    let mut group_by = Vec::new();
    let mut group_by_seen = BTreeSet::new();

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
            "group_by" => {
                if value.is_empty() {
                    return Err("invalid_group_by");
                }

                if !is_supported_session_dimension(&value)
                    && (is_reserved_event_property_key(&value)
                        || !is_valid_event_property_key(&value))
                {
                    return Err("invalid_group_by");
                }

                if !group_by_seen.insert(value.clone()) {
                    return Err("duplicate_group_by");
                }

                group_by.push(value);
            }
            "platform" | "app_version" | "os_version" => {
                if filters.insert(key, value).is_some() {
                    return Err("duplicate_query_key");
                }
            }
            "project_id" | "start_date" | "end_date" => return Err("invalid_query_key"),
            _ => {
                if is_reserved_event_property_key(&key) || !is_valid_event_property_key(&key) {
                    return Err("invalid_query_key");
                }

                if filters.insert(key, value).is_some() {
                    return Err("duplicate_query_key");
                }
            }
        }
    }

    if group_by.len() > MAX_GROUP_BY_DIMENSIONS {
        return Err("too_many_group_by_dimensions");
    }

    let filter_keys = filters.keys().cloned().collect::<BTreeSet<_>>();
    if group_by.iter().any(|key| filter_keys.contains(key)) {
        return Err("conflicting_dimension_usage");
    }

    let mut referenced_dimensions = filter_keys;
    referenced_dimensions.extend(group_by.iter().cloned());
    if referenced_dimensions.len() > MAX_EVENT_REFERENCED_DIMENSIONS {
        return Err("too_many_dimensions");
    }

    Ok(PublicSessionMetricsQuery {
        metric: metric.ok_or("invalid_request")?,
        window: parse_metrics_window(
            granularity.ok_or("invalid_request")?,
            start.as_deref().ok_or("invalid_request")?,
            end.as_deref().ok_or("invalid_request")?,
        )
        .map_err(|_| "invalid_request")?,
        filters,
        group_by,
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
                    if filters.insert(key, value).is_some() {
                        return Err("invalid_query_key");
                    }
                    continue;
                }

                if limit.is_some() {
                    return Err("invalid_query_key");
                }

                limit = Some(parse_top_events_limit(&value)?);
            }
            "platform" | "app_version" | "os_version" => {
                if filters.insert(key, value).is_some() {
                    return Err("invalid_query_key");
                }
            }
            "project_id" | "event" | "metric" | "granularity" | "group_by" => {
                return Err("invalid_query_key");
            }
            _ => {
                if is_reserved_event_property_key(&key) || !is_valid_event_property_key(&key) {
                    return Err("invalid_query_key");
                }

                if filters.insert(key, value).is_some() {
                    return Err("invalid_query_key");
                }
            }
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

fn is_supported_session_dimension(key: &str) -> bool {
    matches!(key, "platform" | "app_version" | "os_version")
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

    match revoke_api_key(&state.pool, project_id, key_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => query_error_response("not_found"),
        Err(error) => {
            tracing::error!(?error, project_id = %project_id, key_id = %key_id, "failed to revoke project key");
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
        "created_at": project.created_at,
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
        group_by: public_query.group_by,
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

    let public_query = match parse_session_metric_query(raw_query.as_deref().unwrap_or_default()) {
        Ok(query) => query,
        Err(error) => return query_error_response(error),
    };
    let query = SessionMetricsQuery {
        project_id: auth.project_id,
        metric: public_query.metric,
        window: public_query.window,
        filters: public_query.filters,
        group_by: public_query.group_by,
    };

    if let Err(error) = validate_active_installs_query(&query) {
        return query_error_response(error);
    }

    let response = if query.metric == SessionMetric::ActiveInstalls {
        execute_active_installs_query(&state.pool, &query).await
    } else {
        execute_session_metrics_query(&state.pool, &query).await
    };

    match response {
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

async fn live_installs_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> impl IntoResponse {
    let auth = match authorize_project_key(&state.pool, &headers, ApiKeyKind::Read).await {
        Ok(auth) => auth,
        Err(response) => return response,
    };

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

fn query_error_response(error: &'static str) -> axum::response::Response {
    let status = match error {
        "not_found" => StatusCode::NOT_FOUND,
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "internal_server_error" => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    };

    (status, Json(serde_json::json!({ "error": error }))).into_response()
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
        &query.group_by,
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
        SessionMetric::ActiveInstalls => match query.window.granularity {
            MetricGranularity::Day
            | MetricGranularity::Week
            | MetricGranularity::Month
            | MetricGranularity::Year => Ok(()),
            MetricGranularity::Hour => Err("invalid_request"),
        },
        SessionMetric::Count | SessionMetric::DurationTotal | SessionMetric::NewInstalls => {
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
) -> Result<MetricsResponse, MetricsResponseError> {
    let mut tx = begin_metrics_snapshot(pool).await?;
    let counts_by_bucket = load_active_install_counts_by_bucket(&mut tx, query).await?;
    let group_count = counts_by_bucket
        .values()
        .flat_map(|groups| groups.keys())
        .collect::<BTreeSet<_>>()
        .len();
    if group_count > MAX_METRIC_GROUPS {
        return Err(MetricsResponseError::GroupLimitExceeded);
    }
    tx.commit().await.map_err(StoreError::Database)?;

    Ok(build_metrics_response(
        query.metric.as_str(),
        &query.window,
        &query.group_by,
        counts_by_bucket,
    ))
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
        &query.group_by,
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
    match query.group_by.len() {
        0 => Ok(()),
        1 => {
            let primary_rows = load_event_aggregate_rows(
                tx,
                query,
                &cube_keys_with(
                    &query.filters,
                    std::slice::from_ref(query.group_by.first().expect("group exists")),
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
                aggregate_rows_to_groups(&primary_rows, &query.group_by),
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
                &cube_keys_with(&query.filters, &query.group_by),
                Some(MAX_METRIC_GROUPS + 1),
            )
            .await?;
            if pair_rows.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            let first_group = vec![query.group_by[0].clone()];
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

            let second_group = vec![query.group_by[1].clone()];
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
                aggregate_rows_to_groups(&pair_rows, &query.group_by),
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
    match query.group_by.len() {
        0 => Ok(()),
        1 => {
            let primary_rows = load_session_aggregate_rows(
                tx,
                query,
                &cube_keys_with(
                    &query.filters,
                    std::slice::from_ref(query.group_by.first().expect("group exists")),
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
                aggregate_rows_to_groups(&primary_rows, &query.group_by),
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
                &cube_keys_with(&query.filters, &query.group_by),
                Some(MAX_METRIC_GROUPS + 1),
            )
            .await?;
            if pair_rows.len() > MAX_METRIC_GROUPS {
                return Err(MetricsResponseError::GroupLimitExceeded);
            }

            let first_group = vec![query.group_by[0].clone()];
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

            let second_group = vec![query.group_by[1].clone()];
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
                aggregate_rows_to_groups(&pair_rows, &query.group_by),
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
    match query.group_by.len() {
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
                load_event_cube_rows(tx, query, &cube_keys_with(&query.filters, &query.group_by))
                    .await?;
            let total_rows =
                load_event_cube_rows(tx, query, &filter_cube_keys(&query.filters)).await?;

            synthesize_group_counts_by_bucket(
                group_rows_by_bucket(&primary_rows, &query.group_by),
                sum_rows_by_bucket(&total_rows),
                None,
            )
            .map_err(Into::into)
        }
        2 => {
            let pair_rows =
                load_event_cube_rows(tx, query, &cube_keys_with(&query.filters, &query.group_by))
                    .await?;
            let first_group = vec![query.group_by[0].clone()];
            let first_rows =
                load_event_cube_rows(tx, query, &cube_keys_with(&query.filters, &first_group))
                    .await?;
            let second_group = vec![query.group_by[1].clone()];
            let second_rows =
                load_event_cube_rows(tx, query, &cube_keys_with(&query.filters, &second_group))
                    .await?;
            let total_rows =
                load_event_cube_rows(tx, query, &filter_cube_keys(&query.filters)).await?;

            synthesize_group_counts_by_bucket(
                group_rows_by_bucket(&pair_rows, &query.group_by),
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

async fn load_active_install_counts_by_bucket(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
) -> Result<BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>, MetricsResponseError> {
    match query.window.granularity {
        MetricGranularity::Day => load_daily_active_install_counts_by_bucket(tx, query).await,
        MetricGranularity::Week | MetricGranularity::Month | MetricGranularity::Year => {
            load_range_active_install_counts_by_bucket(tx, query).await
        }
        MetricGranularity::Hour => Err(StoreError::InvariantViolation(
            "active_installs does not support hour granularity".to_owned(),
        )
        .into()),
    }
}

async fn load_daily_active_install_counts_by_bucket(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
) -> Result<BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>, MetricsResponseError> {
    let bucket_expr = "day::timestamp AT TIME ZONE 'UTC'";
    let mut builder = QueryBuilder::<Postgres>::new("WITH session_slices AS (SELECT ");
    builder.push(bucket_expr);
    builder.push(" AS bucket_start, install_id, platform, app_version, os_version, properties");

    builder.push(" FROM session_active_install_slices WHERE project_id = ");
    builder.push_bind(query.project_id);
    builder.push(" AND day BETWEEN ");
    builder.push_bind(query.window.start.date_naive());
    builder.push(" AND ");
    builder.push_bind(query.window.end.date_naive());

    for (key, value) in &query.filters {
        builder.push(" AND ");
        push_active_install_filter_predicate(&mut builder, key, value);
    }

    builder.push(") SELECT bucket_start");
    for key in &query.group_by {
        builder.push(", ");
        push_active_install_dimension_expr(&mut builder, key);
        builder.push(" AS ");
        builder.push(active_install_dimension_alias(key));
    }
    builder.push(", COUNT(*)::BIGINT AS value FROM session_slices GROUP BY bucket_start");
    for key in &query.group_by {
        builder.push(", ");
        builder.push(active_install_dimension_alias(key));
    }
    builder.push(" ORDER BY bucket_start ASC");
    for key in &query.group_by {
        builder.push(", ");
        builder.push(active_install_dimension_alias(key));
        builder.push(" ASC");
    }

    let rows = builder
        .build()
        .fetch_all(&mut **tx)
        .await
        .map_err(StoreError::from)?;
    let mut counts_by_bucket = BTreeMap::<DateTime<Utc>, BTreeMap<GroupKey, u64>>::new();

    for row in rows {
        let bucket_start = row
            .try_get::<DateTime<Utc>, _>("bucket_start")
            .expect("bucket_start column exists");
        let group_key = query
            .group_by
            .iter()
            .map(|key| {
                row.try_get::<Option<String>, _>(active_install_dimension_alias(key))
                    .expect("group dimension column exists")
            })
            .collect::<Vec<_>>();
        counts_by_bucket.entry(bucket_start).or_default().insert(
            group_key,
            row.try_get::<i64, _>("value").expect("value column exists") as u64,
        );
    }

    Ok(counts_by_bucket)
}

async fn load_range_active_install_counts_by_bucket(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
) -> Result<BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>, MetricsResponseError> {
    let referenced_keys = cube_keys_with(&query.filters, &query.group_by);

    match referenced_keys.len() {
        0 => load_range_active_install_total_counts_by_bucket(tx, query).await,
        1 => load_range_active_install_dim1_counts_by_bucket(tx, query, &referenced_keys[0]).await,
        2 => {
            load_range_active_install_dim2_counts_by_bucket(
                tx,
                query,
                &referenced_keys[0],
                &referenced_keys[1],
            )
            .await
        }
        other => Err(StoreError::InvariantViolation(format!(
            "active_installs referenced dimension arity must be between 0 and 2, got {other}"
        ))
        .into()),
    }
}

async fn load_range_active_install_total_counts_by_bucket(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
) -> Result<BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>, MetricsResponseError> {
    let rows = sqlx::query(
        r#"
        SELECT bucket_start, active_installs
        FROM active_install_metric_buckets_total
        WHERE project_id = $1
          AND granularity = $2
          AND bucket_start BETWEEN $3 AND $4
        ORDER BY bucket_start ASC
        "#,
    )
    .bind(query.project_id)
    .bind(query.window.granularity.as_str())
    .bind(query.window.start)
    .bind(query.window.end)
    .fetch_all(&mut **tx)
    .await
    .map_err(StoreError::from)?;

    let mut counts_by_bucket = BTreeMap::<DateTime<Utc>, BTreeMap<GroupKey, u64>>::new();
    for row in rows {
        counts_by_bucket
            .entry(
                row.try_get("bucket_start")
                    .expect("bucket_start column exists"),
            )
            .or_default()
            .insert(
                Vec::new(),
                row.try_get::<i64, _>("active_installs")
                    .expect("active_installs column exists") as u64,
            );
    }

    Ok(counts_by_bucket)
}

async fn load_range_active_install_dim1_counts_by_bucket(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
    key: &str,
) -> Result<BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>, MetricsResponseError> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT bucket_start, CASE WHEN dim1_value_is_null THEN NULL ELSE dim1_value END AS dim_value, active_installs \
         FROM active_install_metric_buckets_dim1 WHERE project_id = ",
    );
    builder.push_bind(query.project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(query.window.granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(query.window.start);
    builder.push(" AND ");
    builder.push_bind(query.window.end);
    builder.push(" AND dim1_key = ");
    builder.push_bind(key);

    if let Some(value) = query.filters.get(key) {
        builder.push(" AND dim1_value_is_null = FALSE AND dim1_value = ");
        builder.push_bind(value);
    }

    builder.push(" ORDER BY bucket_start ASC, dim_value ASC");

    let rows = builder
        .build()
        .fetch_all(&mut **tx)
        .await
        .map_err(StoreError::from)?;
    let mut counts_by_bucket = BTreeMap::<DateTime<Utc>, BTreeMap<GroupKey, u64>>::new();

    for row in rows {
        let group_key = if query.group_by.is_empty() {
            Vec::new()
        } else {
            vec![
                row.try_get::<Option<String>, _>("dim_value")
                    .expect("dim_value column exists"),
            ]
        };
        counts_by_bucket
            .entry(
                row.try_get("bucket_start")
                    .expect("bucket_start column exists"),
            )
            .or_default()
            .insert(
                group_key,
                row.try_get::<i64, _>("active_installs")
                    .expect("active_installs column exists") as u64,
            );
    }

    Ok(counts_by_bucket)
}

async fn load_range_active_install_dim2_counts_by_bucket(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
    key1: &str,
    key2: &str,
) -> Result<BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>, MetricsResponseError> {
    let mut builder = QueryBuilder::<Postgres>::new("SELECT bucket_start");
    for (index, key) in query.group_by.iter().enumerate() {
        builder.push(", ");
        if key == key1 {
            builder.push("CASE WHEN dim1_value_is_null THEN NULL ELSE dim1_value END");
        } else {
            builder.push("CASE WHEN dim2_value_is_null THEN NULL ELSE dim2_value END");
        }
        builder.push(" AS group_");
        builder.push(index.to_string());
    }
    builder.push(", active_installs FROM active_install_metric_buckets_dim2 WHERE project_id = ");
    builder.push_bind(query.project_id);
    builder.push(" AND granularity = ");
    builder.push_bind(query.window.granularity.as_str());
    builder.push(" AND bucket_start BETWEEN ");
    builder.push_bind(query.window.start);
    builder.push(" AND ");
    builder.push_bind(query.window.end);
    builder.push(" AND dim1_key = ");
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

    builder.push(" ORDER BY bucket_start ASC");
    for index in 0..query.group_by.len() {
        builder.push(", group_");
        builder.push(index.to_string());
        builder.push(" ASC");
    }

    let rows = builder
        .build()
        .fetch_all(&mut **tx)
        .await
        .map_err(StoreError::from)?;
    let mut counts_by_bucket = BTreeMap::<DateTime<Utc>, BTreeMap<GroupKey, u64>>::new();

    for row in rows {
        let group_key = (0..query.group_by.len())
            .map(|index| {
                row.try_get::<Option<String>, _>(format!("group_{index}").as_str())
                    .expect("group column exists")
            })
            .collect::<Vec<_>>();
        counts_by_bucket
            .entry(
                row.try_get("bucket_start")
                    .expect("bucket_start column exists"),
            )
            .or_default()
            .insert(
                group_key,
                row.try_get::<i64, _>("active_installs")
                    .expect("active_installs column exists") as u64,
            );
    }

    Ok(counts_by_bucket)
}

async fn load_session_group_counts_by_bucket(
    tx: &mut Transaction<'_, Postgres>,
    query: &SessionMetricsQuery,
) -> Result<BTreeMap<DateTime<Utc>, BTreeMap<GroupKey, u64>>, MetricsResponseError> {
    match query.group_by.len() {
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
                load_session_cube_rows(tx, query, &cube_keys_with(&query.filters, &query.group_by))
                    .await?;
            let total_rows =
                load_session_cube_rows(tx, query, &filter_cube_keys(&query.filters)).await?;

            synthesize_group_counts_by_bucket(
                group_rows_by_bucket(&primary_rows, &query.group_by),
                sum_rows_by_bucket(&total_rows),
                None,
            )
            .map_err(Into::into)
        }
        2 => {
            let pair_rows =
                load_session_cube_rows(tx, query, &cube_keys_with(&query.filters, &query.group_by))
                    .await?;
            let first_group = vec![query.group_by[0].clone()];
            let first_rows =
                load_session_cube_rows(tx, query, &cube_keys_with(&query.filters, &first_group))
                    .await?;
            let second_group = vec![query.group_by[1].clone()];
            let second_rows =
                load_session_cube_rows(tx, query, &cube_keys_with(&query.filters, &second_group))
                    .await?;
            let total_rows =
                load_session_cube_rows(tx, query, &filter_cube_keys(&query.filters)).await?;

            synthesize_group_counts_by_bucket(
                group_rows_by_bucket(&pair_rows, &query.group_by),
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
        group_by: group_by.to_vec(),
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

fn push_active_install_dimension_expr(builder: &mut QueryBuilder<'_, Postgres>, key: &str) {
    match key {
        "platform" => {
            builder.push("platform");
        }
        "app_version" => {
            builder.push("CASE WHEN app_version_is_null THEN NULL ELSE app_version END");
        }
        "os_version" => {
            builder.push("CASE WHEN os_version_is_null THEN NULL ELSE os_version END");
        }
        _ => {
            builder.push("properties ->> ");
            builder.push_bind(key.to_owned());
        }
    }
}

fn push_active_install_filter_predicate(
    builder: &mut QueryBuilder<'_, Postgres>,
    key: &str,
    value: &str,
) {
    match key {
        "platform" => {
            builder.push("platform = ");
            builder.push_bind(value.to_owned());
        }
        "app_version" => {
            builder.push("app_version_is_null = FALSE AND app_version = ");
            builder.push_bind(value.to_owned());
        }
        "os_version" => {
            builder.push("os_version_is_null = FALSE AND os_version = ");
            builder.push_bind(value.to_owned());
        }
        _ => {
            builder.push("properties ->> ");
            builder.push_bind(key.to_owned());
            builder.push(" = ");
            builder.push_bind(value.to_owned());
        }
    }
}

fn active_install_dimension_alias(key: &str) -> &str {
    match key {
        "platform" => "platform",
        "app_version" => "app_version",
        "os_version" => "os_version",
        _ => key,
    }
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
                "group_by": [],
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
                "group_by": ["platform"],
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
    fn parse_event_metrics_query_preserves_group_by_request_order() {
        let query = parse_event_metrics_query(
            "event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&group_by=provider&group_by=is_paying",
        )
        .expect("query parses");

        assert_eq!(query.metric, EventMetric::Count);
        assert_eq!(
            query.group_by,
            vec!["provider".to_owned(), "is_paying".to_owned()]
        );
        assert!(!query.filters.contains_key("provider"));
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
    fn parse_event_metrics_query_rejects_invalid_property_filter_keys() {
        let error = parse_event_metrics_query(
            "event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&plan-name=pro",
        )
        .unwrap_err();

        assert_eq!(error.error_code(), "invalid_query_key");
    }

    #[test]
    fn parse_event_metrics_query_rejects_duplicate_group_by() {
        let error = parse_event_metrics_query(
            "event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&group_by=provider&group_by=provider",
        )
        .unwrap_err();

        assert_eq!(error.error_code(), "duplicate_group_by");
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
    fn parse_event_metrics_query_accepts_two_referenced_dimensions() {
        let query = parse_event_metrics_query(
            "event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&plan=pro&group_by=provider",
        )
        .expect("query parses");

        assert_eq!(query.group_by, vec!["provider".to_owned()]);
        assert_eq!(query.filters.get("plan"), Some(&"pro".to_owned()));
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
    fn parse_event_metrics_query_rejects_three_referenced_dimensions() {
        let error = parse_event_metrics_query(
            "event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&app_version=1.4.0&plan=pro&group_by=provider",
        )
        .unwrap_err();

        assert_eq!(error.error_code(), "too_many_dimensions");
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
    fn parse_session_metric_query_accepts_two_referenced_dimensions() {
        let query = parse_session_metric_query(
            "metric=count&granularity=day&start=2026-03-01&end=2026-03-02&plan=pro&group_by=provider",
        )
        .expect("query parses");

        assert_eq!(query.group_by, vec!["provider".to_owned()]);
        assert_eq!(query.filters.get("plan"), Some(&"pro".to_owned()));
    }

    #[test]
    fn parse_session_metric_query_accepts_weekly_active_installs() {
        let query = parse_session_metric_query(
            "metric=active_installs&granularity=week&start=2026-03-02&end=2026-03-09",
        )
        .expect("query parses");

        assert_eq!(query.metric, SessionMetric::ActiveInstalls);
        assert_eq!(query.window.granularity, MetricGranularity::Week);
        assert_eq!(
            query.window.start,
            Utc.with_ymd_and_hms(2026, 3, 2, 0, 0, 0)
                .single()
                .expect("start")
        );
        assert_eq!(
            query.window.end,
            Utc.with_ymd_and_hms(2026, 3, 9, 0, 0, 0)
                .single()
                .expect("end")
        );
    }

    #[test]
    fn parse_session_metric_query_rejects_three_referenced_dimensions() {
        let error = parse_session_metric_query(
            "metric=count&granularity=day&start=2026-03-01&end=2026-03-02&app_version=1.4.0&plan=pro&group_by=provider",
        )
        .unwrap_err();

        assert_eq!(error, "too_many_dimensions");
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
        let project_route_security = spec["paths"]["/v1/projects/{project_id}"]["get"]["security"]
            .as_sequence()
            .expect("project route security");
        let live_installs_route = &spec["paths"]["/v1/metrics/live_installs"]["get"];
        let live_installs_200 = &live_installs_route["responses"]["200"];
        let session_metrics_500 = &spec["paths"]["/v1/metrics/sessions"]["get"]["responses"]["500"];
        let explicit_filters =
            &spec["paths"]["/v1/metrics/events"]["get"]["x-fantasma-explicit-property-filters"];
        let platform_filter_schema = &spec["components"]["parameters"]["PlatformFilter"]["schema"];
        let operator_auth = &spec["components"]["securitySchemes"]["OperatorBearerAuth"];
        let project_key_auth = &spec["components"]["securitySchemes"]["ProjectKeyHeader"];
        let live_installs_schema = &spec["components"]["schemas"]["LiveInstallsResponse"];
        let event_metrics_description = &spec["paths"]["/v1/metrics/events"]["get"]["description"];
        let event_metrics_parameters = spec["paths"]["/v1/metrics/events"]["get"]["parameters"]
            .as_sequence()
            .expect("event metrics parameters");
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
            explicit_filters.is_mapping(),
            "event metrics routes must document the additional explicit-property filter namespace"
        );
        assert!(
            event_metrics_description
                .as_str()
                .expect("event metrics description")
                .contains("`metric=count` is the only public event metric"),
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
        assert_eq!(project_key_auth["type"], "apiKey");
        assert_eq!(project_key_auth["in"], "header");
        assert_eq!(project_key_auth["name"], "X-Fantasma-Key");
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
