use std::{env, sync::Arc};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use fantasma_auth::StaticAdminAuthorizer;
use fantasma_core::{EventCountQuery, MetricResponse, MetricSeriesPoint, SessionMetricQuery};
use fantasma_store::{
    BootstrapConfig, DatabaseConfig, PgPool, StoreError, average_session_duration_seconds,
    bootstrap, connect, count_active_installs, count_events, count_sessions,
};
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    authorizer: Arc<StaticAdminAuthorizer>,
    pool: PgPool,
}

#[tokio::main]
async fn main() {
    init_tracing();

    let bind_address =
        env::var("FANTASMA_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8082".to_owned());
    let database_url = env::var("FANTASMA_DATABASE_URL").expect("FANTASMA_DATABASE_URL");
    let project_id = env::var("FANTASMA_PROJECT_ID")
        .ok()
        .and_then(|value| Uuid::parse_str(&value).ok())
        .unwrap_or_else(default_project_id);
    let project_name = env::var("FANTASMA_PROJECT_NAME")
        .unwrap_or_else(|_| "Local Development Project".to_owned());
    let ingest_key = env::var("FANTASMA_INGEST_KEY").ok();
    let pool = connect(&DatabaseConfig::new(database_url))
        .await
        .expect("connect database");
    bootstrap(
        &pool,
        &BootstrapConfig {
            project_id,
            project_name,
            ingest_key,
        },
    )
    .await
    .expect("bootstrap database");
    let state = AppState {
        authorizer: Arc::new(load_authorizer()),
        pool,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/metrics/events/count", get(events_count))
        .route("/v1/metrics/sessions/count", get(sessions_count))
        .route("/v1/metrics/sessions/duration", get(sessions_duration))
        .route("/v1/metrics/active-installs", get(active_installs))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind_address)
        .await
        .expect("bind api listener");
    let local_addr = listener.local_addr().expect("read local address");
    info!("fantasma-api listening on {local_addr}");

    axum::serve(listener, app)
        .await
        .expect("serve api application");
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            env::var("FANTASMA_LOG_LEVEL")
                .unwrap_or_else(|_| "fantasma_api=info,tower_http=info".to_owned()),
        )
        .try_init();
}

fn default_project_id() -> Uuid {
    Uuid::from_u128(0x9bad8b88_5e7a_44ed_98ce_4cf9ddde713a)
}

fn load_authorizer() -> StaticAdminAuthorizer {
    let admin_token = env::var("FANTASMA_ADMIN_TOKEN").unwrap_or_else(|_| "fg_pat_dev".to_owned());

    StaticAdminAuthorizer::new(admin_token)
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
}
