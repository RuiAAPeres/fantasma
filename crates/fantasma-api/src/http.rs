use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use fantasma_auth::StaticAdminAuthorizer;
use fantasma_core::{
    DailyMetricQuery, EventCountQuery, MetricResponse, MetricSeriesPoint, SessionMetricQuery,
};
use fantasma_store::{
    PgPool, SessionDailyRecord, StoreError, average_session_duration_seconds,
    count_active_installs, count_events, count_sessions, fetch_session_daily_range,
};

#[derive(Clone)]
struct AppState {
    authorizer: Arc<StaticAdminAuthorizer>,
    pool: PgPool,
}

pub fn app(pool: PgPool, authorizer: Arc<StaticAdminAuthorizer>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/metrics/events/count", get(events_count))
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
        .route(
            "/v1/metrics/active-installs/daily",
            get(active_installs_daily),
        )
        .with_state(AppState { authorizer, pool })
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok", "service": "fantasma-api" }))
}

async fn events_count(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<EventCountQuery>,
) -> impl IntoResponse {
    if state.authorizer.authorize(&headers).is_err() {
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

    match count_events(&state.pool, &query).await {
        Ok(count) => (
            StatusCode::OK,
            Json(
                serde_json::to_value(MetricResponse {
                    metric: "events_count".to_owned(),
                    points: vec![MetricSeriesPoint {
                        date: query.end.date_naive(),
                        value: count,
                    }],
                })
                .expect("serialize metric response"),
            ),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(?error, "failed to count raw events");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal_server_error" })),
            )
                .into_response()
        }
    }
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

async fn active_installs_daily(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DailyMetricQuery>,
) -> impl IntoResponse {
    daily_metric_response(
        &state.authorizer,
        &headers,
        &query,
        "active_installs_daily",
        async {
            load_daily_metric_series(&state.pool, &query, |record| record.active_installs as u64)
                .await
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
    use axum::response::Response;
    use serde_json::Value;

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
    async fn daily_metric_response_returns_active_installs_daily_payload() {
        let response = daily_metric_response(
            &authorizer(),
            &authorized_headers(),
            &daily_query(),
            "active_installs_daily",
            async {
                Ok(vec![MetricSeriesPoint {
                    date: serde_json::from_value(serde_json::json!("2026-01-01")).expect("date"),
                    value: 3,
                }])
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({
                "metric": "active_installs_daily",
                "points": [
                    { "date": "2026-01-01", "value": 3 },
                    { "date": "2026-01-02", "value": 0 },
                    { "date": "2026-01-03", "value": 0 }
                ]
            })
        );
    }
}
