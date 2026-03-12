use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::{Query, RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use fantasma_auth::StaticAdminAuthorizer;
use fantasma_core::{
    DailyMetricQuery, EventMetricsAggregateResponse, EventMetricsAggregateRow,
    EventMetricsDailyResponse, EventMetricsDailySeries, EventMetricsDateWindow, EventMetricsPoint,
    EventMetricsQuery, MetricResponse, MetricSeriesPoint, SessionMetricQuery,
    is_reserved_event_property_key, is_valid_event_property_key,
};
use fantasma_store::{
    EventMetricsAggregateCubeRow, EventMetricsCubeRow, PgPool, SessionDailyRecord, StoreError,
    average_session_duration_seconds, count_active_installs, count_sessions,
    fetch_event_metrics_aggregate_cube_rows_in_tx, fetch_event_metrics_cube_rows_in_tx,
    fetch_session_daily_range,
};
use sqlx::{Postgres, Transaction};
use url::form_urlencoded;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    authorizer: Arc<StaticAdminAuthorizer>,
    pool: PgPool,
}

pub fn app(pool: PgPool, authorizer: Arc<StaticAdminAuthorizer>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/metrics/events/aggregate", get(events_aggregate))
        .route("/v1/metrics/events/daily", get(events_daily))
        .route("/v1/metrics/sessions/count", get(sessions_count))
        .route(
            "/v1/metrics/sessions/count/daily",
            get(sessions_count_daily),
        )
        .route("/v1/metrics/sessions/duration", get(sessions_duration))
        .route(
            "/v1/metrics/sessions/duration/total/daily",
            get(session_duration_total_daily),
        )
        .route("/v1/metrics/active-installs", get(active_installs))
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

fn parse_event_metrics_query(raw_query: &str) -> Result<EventMetricsQuery, EventMetricsQueryError> {
    let mut project_id = None;
    let mut event = None;
    let mut start_date = None;
    let mut end_date = None;
    let mut filters = BTreeMap::new();
    let mut group_by = Vec::new();
    let mut group_by_seen = BTreeSet::new();

    for (raw_key, raw_value) in form_urlencoded::parse(raw_query.as_bytes()) {
        let key = raw_key.into_owned();
        let value = raw_value.into_owned();

        match key.as_str() {
            "project_id" => {
                if project_id.is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }

                project_id = Some(
                    Uuid::parse_str(&value)
                        .map_err(|_| EventMetricsQueryError("invalid_project_id"))?,
                );
            }
            "event" => {
                if event.is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }

                if value.trim().is_empty() {
                    return Err(EventMetricsQueryError("invalid_event"));
                }

                event = Some(value);
            }
            "start_date" => {
                if start_date.is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }

                start_date = Some(
                    chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                        .map_err(|_| EventMetricsQueryError("invalid_date_range"))?,
                );
            }
            "end_date" => {
                if end_date.is_some() {
                    return Err(EventMetricsQueryError("duplicate_query_key"));
                }

                end_date = Some(
                    chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                        .map_err(|_| EventMetricsQueryError("invalid_date_range"))?,
                );
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

    let project_id = project_id.ok_or(EventMetricsQueryError("invalid_project_id"))?;
    let event = event.ok_or(EventMetricsQueryError("invalid_event"))?;
    let start_date = start_date.ok_or(EventMetricsQueryError("invalid_date_range"))?;
    let end_date = end_date.ok_or(EventMetricsQueryError("invalid_date_range"))?;

    if start_date > end_date {
        return Err(EventMetricsQueryError("invalid_date_range"));
    }

    if group_by.len() > 2 {
        return Err(EventMetricsQueryError("too_many_group_by_dimensions"));
    }

    let filter_keys = filters.keys().cloned().collect::<BTreeSet<_>>();
    if group_by.iter().any(|key| filter_keys.contains(key)) {
        return Err(EventMetricsQueryError("conflicting_dimension_usage"));
    }

    let mut referenced_dimensions = filter_keys;
    referenced_dimensions.extend(group_by.iter().cloned());
    if referenced_dimensions.len() > 3 {
        return Err(EventMetricsQueryError("too_many_dimensions"));
    }

    Ok(EventMetricsQuery {
        project_id,
        event,
        window: EventMetricsDateWindow {
            start_date,
            end_date,
        },
        filters,
        group_by,
    })
}

async fn events_aggregate(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> impl IntoResponse {
    if state.authorizer.authorize(&headers).is_err() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response();
    }

    let query = match parse_event_metrics_query(raw_query.as_deref().unwrap_or_default()) {
        Ok(query) => query,
        Err(error) => return query_error_response(error.error_code()),
    };

    match execute_event_metrics_aggregate_query(&state.pool, &query).await {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).expect("serialize event metric response")),
        )
            .into_response(),
        Err(EventMetricsResponseError::GroupLimitExceeded) => {
            query_error_response("group_limit_exceeded")
        }
        Err(EventMetricsResponseError::Store(error)) => {
            tracing::error!(?error, "failed to compute event metrics aggregate");
            query_error_response("internal_server_error")
        }
    }
}

async fn events_daily(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> impl IntoResponse {
    if state.authorizer.authorize(&headers).is_err() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response();
    }

    let query = match parse_event_metrics_query(raw_query.as_deref().unwrap_or_default()) {
        Ok(query) => query,
        Err(error) => return query_error_response(error.error_code()),
    };

    match execute_event_metrics_daily_query(&state.pool, &query).await {
        Ok(response) => (
            StatusCode::OK,
            Json(serde_json::to_value(response).expect("serialize event metrics response")),
        )
            .into_response(),
        Err(EventMetricsResponseError::GroupLimitExceeded) => {
            query_error_response("group_limit_exceeded")
        }
        Err(EventMetricsResponseError::Store(error)) => {
            tracing::error!(?error, "failed to compute daily event metrics");
            query_error_response("internal_server_error")
        }
    }
}

#[derive(Debug)]
enum EventMetricsResponseError {
    GroupLimitExceeded,
    Store(StoreError),
}

impl From<StoreError> for EventMetricsResponseError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

type GroupKey = Vec<Option<String>>;
const MAX_EVENT_METRIC_GROUPS: usize = 100;

fn query_error_response(error: &'static str) -> axum::response::Response {
    let status = match error {
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "internal_server_error" => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    };

    (status, Json(serde_json::json!({ "error": error }))).into_response()
}

async fn execute_event_metrics_aggregate_query(
    pool: &PgPool,
    query: &EventMetricsQuery,
) -> Result<EventMetricsAggregateResponse, EventMetricsResponseError> {
    let mut tx = begin_event_metrics_snapshot(pool).await?;
    precheck_event_metrics_group_limit(&mut tx, query).await?;
    let response = load_event_metrics_aggregate_response(&mut tx, query).await?;
    tx.commit().await.map_err(StoreError::Database)?;
    Ok(response)
}

async fn execute_event_metrics_daily_query(
    pool: &PgPool,
    query: &EventMetricsQuery,
) -> Result<EventMetricsDailyResponse, EventMetricsResponseError> {
    let mut tx = begin_event_metrics_snapshot(pool).await?;
    precheck_event_metrics_group_limit(&mut tx, query).await?;
    let response = load_event_metrics_daily_response(&mut tx, query).await?;
    tx.commit().await.map_err(StoreError::Database)?;
    Ok(response)
}

async fn begin_event_metrics_snapshot(
    pool: &PgPool,
) -> Result<Transaction<'_, Postgres>, EventMetricsResponseError> {
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
) -> Result<(), EventMetricsResponseError> {
    match query.group_by.len() {
        0 => Ok(()),
        1 => precheck_single_group_limit(tx, query).await,
        2 => precheck_two_group_limit(tx, query).await,
        other => Err(StoreError::InvariantViolation(format!(
            "event metrics group_by arity must be between 0 and 2, got {other}"
        ))
        .into()),
    }
}

async fn precheck_single_group_limit(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
) -> Result<(), EventMetricsResponseError> {
    let group_key = query
        .group_by
        .first()
        .expect("single-group query has one group key");
    let primary_rows = fetch_event_metrics_aggregate_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        (query.window.start_date, query.window.end_date),
        &cube_keys_with(query, group_key),
        &query.filters,
        Some(MAX_EVENT_METRIC_GROUPS + 1),
    )
    .await?;

    if primary_rows.len() > MAX_EVENT_METRIC_GROUPS {
        return Err(EventMetricsResponseError::GroupLimitExceeded);
    }

    let total_rows = fetch_event_metrics_aggregate_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        (query.window.start_date, query.window.end_date),
        &filter_cube_keys(query),
        &query.filters,
        None,
    )
    .await?;

    let final_groups = synthesize_single_group_counts(
        aggregate_rows_to_groups(&primary_rows, &query.group_by),
        sum_aggregate_rows(&total_rows),
    )?;

    if final_groups.len() > MAX_EVENT_METRIC_GROUPS {
        return Err(EventMetricsResponseError::GroupLimitExceeded);
    }

    Ok(())
}

async fn precheck_two_group_limit(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
) -> Result<(), EventMetricsResponseError> {
    let first_group = query
        .group_by
        .first()
        .expect("two-group query has first group");
    let second_group = query
        .group_by
        .get(1)
        .expect("two-group query has second group");
    let pair_rows = fetch_event_metrics_aggregate_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        (query.window.start_date, query.window.end_date),
        &cube_keys_with_many(query, [&first_group[..], &second_group[..]].as_slice()),
        &query.filters,
        Some(MAX_EVENT_METRIC_GROUPS + 1),
    )
    .await?;

    if pair_rows.len() > MAX_EVENT_METRIC_GROUPS {
        return Err(EventMetricsResponseError::GroupLimitExceeded);
    }

    let first_rows = fetch_event_metrics_aggregate_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        (query.window.start_date, query.window.end_date),
        &cube_keys_with(query, first_group),
        &query.filters,
        Some(MAX_EVENT_METRIC_GROUPS + 1),
    )
    .await?;

    if first_rows.len() > MAX_EVENT_METRIC_GROUPS {
        return Err(EventMetricsResponseError::GroupLimitExceeded);
    }

    let second_rows = fetch_event_metrics_aggregate_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        (query.window.start_date, query.window.end_date),
        &cube_keys_with(query, second_group),
        &query.filters,
        Some(MAX_EVENT_METRIC_GROUPS + 1),
    )
    .await?;

    if second_rows.len() > MAX_EVENT_METRIC_GROUPS {
        return Err(EventMetricsResponseError::GroupLimitExceeded);
    }

    let total_rows = fetch_event_metrics_aggregate_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        (query.window.start_date, query.window.end_date),
        &filter_cube_keys(query),
        &query.filters,
        None,
    )
    .await?;

    let final_groups = synthesize_two_group_counts(
        aggregate_rows_to_groups(&pair_rows, &query.group_by),
        aggregate_rows_to_groups(&first_rows, std::slice::from_ref(first_group)),
        aggregate_rows_to_groups(&second_rows, std::slice::from_ref(second_group)),
        sum_aggregate_rows(&total_rows),
    )?;

    if final_groups.len() > MAX_EVENT_METRIC_GROUPS {
        return Err(EventMetricsResponseError::GroupLimitExceeded);
    }

    Ok(())
}

async fn load_event_metrics_aggregate_response(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
) -> Result<EventMetricsAggregateResponse, EventMetricsResponseError> {
    let counts_by_day = load_group_counts_by_day(tx, query).await?;
    let mut totals_by_group = BTreeMap::<GroupKey, u64>::new();

    for groups in counts_by_day.values() {
        for (group_key, value) in groups {
            *totals_by_group.entry(group_key.clone()).or_default() += *value;
        }
    }

    let rows = if query.group_by.is_empty() {
        vec![EventMetricsAggregateRow {
            dimensions: BTreeMap::new(),
            value: totals_by_group.remove(&Vec::new()).unwrap_or_default(),
        }]
    } else {
        let mut grouped = totals_by_group.into_iter().collect::<Vec<_>>();
        grouped.sort_by(|(left_key, _), (right_key, _)| compare_group_keys(left_key, right_key));
        grouped
            .into_iter()
            .map(|(group_key, value)| EventMetricsAggregateRow {
                dimensions: dimensions_for_group(&query.group_by, &group_key),
                value,
            })
            .collect()
    };

    Ok(EventMetricsAggregateResponse {
        metric: "event_count".to_owned(),
        group_by: query.group_by.clone(),
        rows,
    })
}

async fn load_event_metrics_daily_response(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
) -> Result<EventMetricsDailyResponse, EventMetricsResponseError> {
    let counts_by_day = load_group_counts_by_day(tx, query).await?;
    let mut series_by_group = BTreeMap::<GroupKey, BTreeMap<chrono::NaiveDate, u64>>::new();

    for (day, groups) in counts_by_day {
        for (group_key, value) in groups {
            series_by_group
                .entry(group_key)
                .or_default()
                .insert(day, value);
        }
    }

    let series = if query.group_by.is_empty() {
        vec![EventMetricsDailySeries {
            dimensions: BTreeMap::new(),
            points: zero_fill_event_metrics_points(
                &query.window,
                series_by_group.remove(&Vec::new()).unwrap_or_default(),
            ),
        }]
    } else {
        let mut grouped = series_by_group.into_iter().collect::<Vec<_>>();
        grouped.sort_by(|(left_key, _), (right_key, _)| compare_group_keys(left_key, right_key));
        grouped
            .into_iter()
            .map(|(group_key, points_by_day)| EventMetricsDailySeries {
                dimensions: dimensions_for_group(&query.group_by, &group_key),
                points: zero_fill_event_metrics_points(&query.window, points_by_day),
            })
            .collect()
    };

    Ok(EventMetricsDailyResponse {
        metric: "event_count_daily".to_owned(),
        group_by: query.group_by.clone(),
        series,
    })
}

async fn load_group_counts_by_day(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
) -> Result<BTreeMap<chrono::NaiveDate, BTreeMap<GroupKey, u64>>, EventMetricsResponseError> {
    match query.group_by.len() {
        0 => load_ungrouped_counts_by_day(tx, query).await,
        1 => load_single_group_counts_by_day(tx, query).await,
        2 => load_two_group_counts_by_day(tx, query).await,
        other => Err(StoreError::InvariantViolation(format!(
            "event metrics group_by arity must be between 0 and 2, got {other}"
        ))
        .into()),
    }
}

async fn load_ungrouped_counts_by_day(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
) -> Result<BTreeMap<chrono::NaiveDate, BTreeMap<GroupKey, u64>>, EventMetricsResponseError> {
    let total_rows = fetch_event_metrics_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        query.window.start_date,
        query.window.end_date,
        &filter_cube_keys(query),
        &query.filters,
    )
    .await?;
    let totals_by_day = sum_rows_by_day(&total_rows);

    Ok(totals_by_day
        .into_iter()
        .map(|(day, value)| (day, BTreeMap::from([(Vec::new(), value)])))
        .collect())
}

async fn load_single_group_counts_by_day(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
) -> Result<BTreeMap<chrono::NaiveDate, BTreeMap<GroupKey, u64>>, EventMetricsResponseError> {
    let group_key = query
        .group_by
        .first()
        .expect("single-group query has one group key");
    let primary_rows = fetch_event_metrics_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        query.window.start_date,
        query.window.end_date,
        &cube_keys_with(query, group_key),
        &query.filters,
    )
    .await?;
    let total_rows = fetch_event_metrics_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        query.window.start_date,
        query.window.end_date,
        &filter_cube_keys(query),
        &query.filters,
    )
    .await?;

    let non_null_by_day = group_rows_by_day(&primary_rows, &query.group_by);
    let totals_by_day = sum_rows_by_day(&total_rows);
    let mut days = totals_by_day.keys().copied().collect::<BTreeSet<_>>();
    days.extend(non_null_by_day.keys().copied());
    let mut grouped = BTreeMap::new();

    for day in days {
        let total = totals_by_day.get(&day).copied().unwrap_or_default();
        let groups = synthesize_single_group_counts(
            non_null_by_day.get(&day).cloned().unwrap_or_default(),
            total,
        )
        .map_err(EventMetricsResponseError::from)?;

        if !groups.is_empty() {
            grouped.insert(day, groups);
        }
    }

    Ok(grouped)
}

async fn load_two_group_counts_by_day(
    tx: &mut Transaction<'_, Postgres>,
    query: &EventMetricsQuery,
) -> Result<BTreeMap<chrono::NaiveDate, BTreeMap<GroupKey, u64>>, EventMetricsResponseError> {
    let first_group = query
        .group_by
        .first()
        .expect("two-group query has first group");
    let second_group = query
        .group_by
        .get(1)
        .expect("two-group query has second group");
    let primary_rows = fetch_event_metrics_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        query.window.start_date,
        query.window.end_date,
        &cube_keys_with_many(query, [&first_group[..], &second_group[..]].as_slice()),
        &query.filters,
    )
    .await?;
    let first_rows = fetch_event_metrics_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        query.window.start_date,
        query.window.end_date,
        &cube_keys_with(query, first_group),
        &query.filters,
    )
    .await?;
    let second_rows = fetch_event_metrics_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        query.window.start_date,
        query.window.end_date,
        &cube_keys_with(query, second_group),
        &query.filters,
    )
    .await?;
    let total_rows = fetch_event_metrics_cube_rows_in_tx(
        tx,
        query.project_id,
        &query.event,
        query.window.start_date,
        query.window.end_date,
        &filter_cube_keys(query),
        &query.filters,
    )
    .await?;

    let non_null_by_day = group_rows_by_day(&primary_rows, &query.group_by);
    let first_by_day = group_rows_by_day(&first_rows, std::slice::from_ref(first_group));
    let second_by_day = group_rows_by_day(&second_rows, std::slice::from_ref(second_group));
    let totals_by_day = sum_rows_by_day(&total_rows);
    let mut days = totals_by_day.keys().copied().collect::<BTreeSet<_>>();
    days.extend(non_null_by_day.keys().copied());
    days.extend(first_by_day.keys().copied());
    days.extend(second_by_day.keys().copied());
    let mut grouped = BTreeMap::new();

    for day in days {
        let total = totals_by_day.get(&day).copied().unwrap_or_default();
        let final_groups = synthesize_two_group_counts(
            non_null_by_day.get(&day).cloned().unwrap_or_default(),
            first_by_day.get(&day).cloned().unwrap_or_default(),
            second_by_day.get(&day).cloned().unwrap_or_default(),
            total,
        )
        .map_err(EventMetricsResponseError::from)?;

        if !final_groups.is_empty() {
            grouped.insert(day, final_groups);
        }
    }

    Ok(grouped)
}

fn filter_cube_keys(query: &EventMetricsQuery) -> Vec<String> {
    query.filters.keys().cloned().collect()
}

fn cube_keys_with(query: &EventMetricsQuery, group_key: &str) -> Vec<String> {
    cube_keys_with_many(query, &[group_key])
}

fn cube_keys_with_many(query: &EventMetricsQuery, group_keys: &[&str]) -> Vec<String> {
    let mut keys = filter_cube_keys(query);
    keys.extend(group_keys.iter().map(|key| (*key).to_owned()));
    keys.sort();
    keys
}

fn sum_rows_by_day(rows: &[EventMetricsCubeRow]) -> BTreeMap<chrono::NaiveDate, u64> {
    let mut totals = BTreeMap::new();

    for row in rows {
        *totals.entry(row.day).or_default() += row.event_count;
    }

    totals
}

fn sum_aggregate_rows(rows: &[EventMetricsAggregateCubeRow]) -> u64 {
    rows.iter().map(|row| row.event_count).sum()
}

fn aggregate_rows_to_groups(
    rows: &[EventMetricsAggregateCubeRow],
    group_by: &[String],
) -> BTreeMap<GroupKey, u64> {
    let mut grouped = BTreeMap::new();

    for row in rows {
        let group_key = group_by
            .iter()
            .map(|group_key| row.dimensions.get(group_key).cloned())
            .collect::<Vec<_>>();
        *grouped.entry(group_key).or_default() += row.event_count;
    }

    grouped
}

fn synthesize_single_group_counts(
    mut non_null_groups: BTreeMap<GroupKey, u64>,
    total: u64,
) -> Result<BTreeMap<GroupKey, u64>, StoreError> {
    let non_null_total = non_null_groups.values().sum::<u64>();

    if non_null_total > total {
        return Err(StoreError::InvariantViolation(
            "event metric non-null buckets exceeded total".to_owned(),
        ));
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
        if used > total_for_first {
            return Err(StoreError::InvariantViolation(
                "event metric pair totals exceeded first-dimension total".to_owned(),
            ));
        }

        let null_count = total_for_first - used;
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
        if used > total_for_second {
            return Err(StoreError::InvariantViolation(
                "event metric pair totals exceeded second-dimension total".to_owned(),
            ));
        }

        let null_count = total_for_second - used;
        if null_count > 0 {
            returned_total += null_count;
            final_groups.insert(vec![None, Some(second_value)], null_count);
        }
    }

    if returned_total > total {
        return Err(StoreError::InvariantViolation(
            "event metric grouped totals exceeded total".to_owned(),
        ));
    }

    let double_null = total - returned_total;
    if double_null > 0 {
        final_groups.insert(vec![None, None], double_null);
    }

    Ok(final_groups)
}

fn group_rows_by_day(
    rows: &[EventMetricsCubeRow],
    group_by: &[String],
) -> BTreeMap<chrono::NaiveDate, BTreeMap<GroupKey, u64>> {
    let mut grouped = BTreeMap::<chrono::NaiveDate, BTreeMap<GroupKey, u64>>::new();

    for row in rows {
        let group_key = group_by
            .iter()
            .map(|group_key| row.dimensions.get(group_key).cloned())
            .collect::<Vec<_>>();
        *grouped
            .entry(row.day)
            .or_default()
            .entry(group_key)
            .or_default() += row.event_count;
    }

    grouped
}

fn zero_fill_event_metrics_points(
    window: &EventMetricsDateWindow,
    points_by_day: BTreeMap<chrono::NaiveDate, u64>,
) -> Vec<EventMetricsPoint> {
    let mut filled = Vec::new();
    let mut current = window.start_date;

    while current <= window.end_date {
        filled.push(EventMetricsPoint {
            date: current,
            value: points_by_day.get(&current).copied().unwrap_or_default(),
        });
        current = current
            .succ_opt()
            .expect("event metrics daily range stays within chrono bounds");
    }

    filled
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

async fn sessions_count(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SessionMetricQuery>,
) -> impl IntoResponse {
    metric_value_response(
        &state.authorizer,
        &headers,
        &query,
        "sessions_count",
        async { count_sessions(&state.pool, query.project_id, query.start, query.end).await },
    )
    .await
}

async fn sessions_count_daily(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DailyMetricQuery>,
) -> impl IntoResponse {
    daily_metric_response(
        &state.authorizer,
        &headers,
        &query,
        "sessions_count_daily",
        async {
            load_daily_metric_series(&state.pool, &query, |record| record.sessions_count as u64)
                .await
        },
    )
    .await
}

async fn sessions_duration(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SessionMetricQuery>,
) -> impl IntoResponse {
    metric_value_response(
        &state.authorizer,
        &headers,
        &query,
        "sessions_duration",
        async {
            average_session_duration_seconds(&state.pool, query.project_id, query.start, query.end)
                .await
        },
    )
    .await
}

async fn session_duration_total_daily(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DailyMetricQuery>,
) -> impl IntoResponse {
    daily_metric_response(
        &state.authorizer,
        &headers,
        &query,
        "session_duration_total_daily",
        async {
            load_daily_metric_series(&state.pool, &query, |record| {
                record.total_duration_seconds as u64
            })
            .await
        },
    )
    .await
}

async fn active_installs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SessionMetricQuery>,
) -> impl IntoResponse {
    metric_value_response(
        &state.authorizer,
        &headers,
        &query,
        "active_installs",
        async {
            count_active_installs(&state.pool, query.project_id, query.start, query.end).await
        },
    )
    .await
}

async fn metric_value_response<F>(
    authorizer: &StaticAdminAuthorizer,
    headers: &HeaderMap,
    query: &SessionMetricQuery,
    metric_name: &'static str,
    compute: F,
) -> axum::response::Response
where
    F: std::future::Future<Output = Result<u64, StoreError>>,
{
    if authorizer.authorize(headers).is_err() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response();
    }

    if query.start > query.end {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": "invalid_time_range" })),
        )
            .into_response();
    }

    match compute.await {
        Ok(value) => (
            StatusCode::OK,
            Json(
                serde_json::to_value(MetricResponse {
                    metric: metric_name.to_owned(),
                    points: vec![MetricSeriesPoint {
                        date: query.end.date_naive(),
                        value,
                    }],
                })
                .expect("serialize metric response"),
            ),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(?error, "failed to compute derived metric");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal_server_error" })),
            )
                .into_response()
        }
    }
}

async fn daily_metric_response<F>(
    authorizer: &StaticAdminAuthorizer,
    headers: &HeaderMap,
    query: &DailyMetricQuery,
    metric_name: &'static str,
    compute: F,
) -> axum::response::Response
where
    F: std::future::Future<Output = Result<Vec<MetricSeriesPoint>, StoreError>>,
{
    if authorizer.authorize(headers).is_err() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response();
    }

    if query.start_date > query.end_date {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({ "error": "invalid_date_range" })),
        )
            .into_response();
    }

    match compute.await {
        Ok(points) => (
            StatusCode::OK,
            Json(
                serde_json::to_value(MetricResponse {
                    metric: metric_name.to_owned(),
                    points: zero_fill_daily_points(query, points),
                })
                .expect("serialize metric response"),
            ),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(?error, "failed to compute daily metric");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal_server_error" })),
            )
                .into_response()
        }
    }
}

async fn load_daily_metric_series<F>(
    pool: &PgPool,
    query: &DailyMetricQuery,
    value: F,
) -> Result<Vec<MetricSeriesPoint>, StoreError>
where
    F: Fn(&SessionDailyRecord) -> u64,
{
    let rows =
        fetch_session_daily_range(pool, query.project_id, query.start_date, query.end_date).await?;

    Ok(rows
        .into_iter()
        .map(|record| MetricSeriesPoint {
            date: record.day,
            value: value(&record),
        })
        .collect())
}

fn zero_fill_daily_points(
    query: &DailyMetricQuery,
    points: Vec<MetricSeriesPoint>,
) -> Vec<MetricSeriesPoint> {
    let values_by_day = points
        .into_iter()
        .map(|point| (point.date, point.value))
        .collect::<BTreeMap<_, _>>();
    let mut filled = Vec::new();
    let mut current = query.start_date;

    while current <= query.end_date {
        filled.push(MetricSeriesPoint {
            date: current,
            value: values_by_day.get(&current).copied().unwrap_or(0),
        });
        current = current
            .succ_opt()
            .expect("valid daily metric range stays within chrono bounds");
    }

    filled
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use serde_json::Value;
    use tower::ServiceExt;

    fn query() -> SessionMetricQuery {
        serde_json::from_value(serde_json::json!({
            "project_id": "9bad8b88-5e7a-44ed-98ce-4cf9ddde713a",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z"
        }))
        .expect("valid query")
    }

    fn daily_query() -> DailyMetricQuery {
        serde_json::from_value(serde_json::json!({
            "project_id": "9bad8b88-5e7a-44ed-98ce-4cf9ddde713a",
            "start_date": "2026-01-01",
            "end_date": "2026-01-03"
        }))
        .expect("valid daily query")
    }

    fn authorizer() -> StaticAdminAuthorizer {
        StaticAdminAuthorizer::new("fg_pat_dev")
    }

    fn authorized_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            "Bearer fg_pat_dev".parse().expect("header"),
        );
        headers
    }

    async fn response_json(response: Response) -> Value {
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        serde_json::from_slice(&body).expect("deserialize response body")
    }

    #[tokio::test]
    async fn metric_value_response_rejects_unauthorized_requests() {
        let response = metric_value_response(
            &authorizer(),
            &HeaderMap::new(),
            &query(),
            "sessions_count",
            async { Ok(7) },
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn metric_value_response_rejects_invalid_time_ranges() {
        let response = metric_value_response(
            &authorizer(),
            &authorized_headers(),
            &SessionMetricQuery {
                start: query().end,
                end: query().start,
                ..query()
            },
            "sessions_count",
            async { Ok(7) },
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn metric_value_response_returns_session_count_payload() {
        let response = metric_value_response(
            &authorizer(),
            &authorized_headers(),
            &query(),
            "sessions_count",
            async { Ok(7) },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({
                "metric": "sessions_count",
                "points": [
                    { "date": "2026-01-02", "value": 7 }
                ]
            })
        );
    }

    #[tokio::test]
    async fn metric_value_response_returns_average_duration_payload() {
        let response = metric_value_response(
            &authorizer(),
            &authorized_headers(),
            &query(),
            "sessions_duration",
            async { Ok(1800) },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({
                "metric": "sessions_duration",
                "points": [
                    { "date": "2026-01-02", "value": 1800 }
                ]
            })
        );
    }

    #[tokio::test]
    async fn metric_value_response_returns_active_installs_payload() {
        let response = metric_value_response(
            &authorizer(),
            &authorized_headers(),
            &query(),
            "active_installs",
            async { Ok(3) },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({
                "metric": "active_installs",
                "points": [
                    { "date": "2026-01-02", "value": 3 }
                ]
            })
        );
    }

    #[tokio::test]
    async fn daily_metric_response_rejects_invalid_date_ranges() {
        let response = daily_metric_response(
            &authorizer(),
            &authorized_headers(),
            &DailyMetricQuery {
                start_date: daily_query().end_date,
                end_date: daily_query().start_date,
                ..daily_query()
            },
            "sessions_count_daily",
            async { Ok(vec![]) },
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn daily_metric_response_returns_zero_filled_series_payload() {
        let response = daily_metric_response(
            &authorizer(),
            &authorized_headers(),
            &daily_query(),
            "sessions_count_daily",
            async {
                Ok(vec![
                    MetricSeriesPoint {
                        date: serde_json::from_value(serde_json::json!("2026-01-01"))
                            .expect("date"),
                        value: 2,
                    },
                    MetricSeriesPoint {
                        date: serde_json::from_value(serde_json::json!("2026-01-03"))
                            .expect("date"),
                        value: 4,
                    },
                ])
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({
                "metric": "sessions_count_daily",
                "points": [
                    { "date": "2026-01-01", "value": 2 },
                    { "date": "2026-01-02", "value": 0 },
                    { "date": "2026-01-03", "value": 4 }
                ]
            })
        );
    }

    #[tokio::test]
    async fn app_exposes_only_supported_daily_metric_routes() {
        let pool = PgPool::connect_lazy("postgres://localhost/fantasma").expect("lazy pool");
        let app = super::app(pool, Arc::new(authorizer()));

        let sessions_count_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/metrics/sessions/count/daily")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("sessions count route response");
        let duration_total_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/metrics/sessions/duration/total/daily")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("duration total route response");
        let active_installs_response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/metrics/active-installs/daily")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("active installs route response");

        assert_ne!(sessions_count_response.status(), StatusCode::NOT_FOUND);
        assert_ne!(duration_total_response.status(), StatusCode::NOT_FOUND);
        assert_eq!(active_installs_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn app_rejects_malformed_event_metrics_queries() {
        let pool = PgPool::connect_lazy("postgres://localhost/fantasma").expect("lazy pool");
        let app = super::app(pool, Arc::new(authorizer()));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/metrics/events/aggregate?event=app_open&start_date=2026-03-01&end_date=2026-03-02")
                    .header("authorization", "Bearer fg_pat_dev")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("event metrics response");

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({
                "error": "invalid_project_id"
            })
        );
    }

    #[test]
    fn parse_event_metrics_query_preserves_group_by_request_order() {
        let query = parse_event_metrics_query(
            "project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-03-01&end_date=2026-03-02&platform=ios&group_by=provider&group_by=is_paying",
        )
        .expect("query parses");

        assert_eq!(
            query.group_by,
            vec!["provider".to_owned(), "is_paying".to_owned()]
        );
        assert_eq!(query.filters.get("platform"), Some(&"ios".to_owned()));
        assert!(!query.filters.contains_key("provider"));
    }

    #[test]
    fn parse_event_metrics_query_rejects_missing_project_id() {
        let error =
            parse_event_metrics_query("event=app_open&start_date=2026-03-01&end_date=2026-03-02")
                .unwrap_err();

        assert_eq!(error.error_code(), "invalid_project_id");
    }

    #[test]
    fn parse_event_metrics_query_rejects_invalid_property_filter_keys() {
        let error = parse_event_metrics_query(
            "project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-03-01&end_date=2026-03-02&plan-name=pro",
        )
        .unwrap_err();

        assert_eq!(error.error_code(), "invalid_query_key");
    }

    #[test]
    fn parse_event_metrics_query_rejects_duplicate_group_by() {
        let error = parse_event_metrics_query(
            "project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-03-01&end_date=2026-03-02&group_by=provider&group_by=provider",
        )
        .unwrap_err();

        assert_eq!(error.error_code(), "duplicate_group_by");
    }

    #[test]
    fn openapi_documents_structural_ingest_errors_and_event_metrics_500s() {
        let spec: serde_yaml::Value =
            serde_yaml::from_str(include_str!("../../../schemas/openapi/fantasma.yaml"))
                .expect("openapi parses");

        let ingest_422 = &spec["paths"]["/v1/events"]["post"]["responses"]["422"]["content"]["application/json"]
            ["schema"]["oneOf"];
        let event_metrics_aggregate_500 =
            &spec["paths"]["/v1/metrics/events/aggregate"]["get"]["responses"]["500"];
        let event_metrics_daily_500 =
            &spec["paths"]["/v1/metrics/events/daily"]["get"]["responses"]["500"];
        let explicit_filters = &spec["paths"]["/v1/metrics/events/aggregate"]["get"]["x-fantasma-explicit-property-filters"];
        let platform_filter_schema = &spec["components"]["parameters"]["PlatformFilter"]["schema"];

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
            event_metrics_aggregate_500.is_mapping(),
            "aggregate route must document internal_server_error"
        );
        assert!(
            event_metrics_daily_500.is_mapping(),
            "daily route must document internal_server_error"
        );
        assert!(
            explicit_filters.is_mapping(),
            "event metrics routes must document the additional explicit-property filter namespace"
        );
        assert_eq!(platform_filter_schema["type"], "string");
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
}
