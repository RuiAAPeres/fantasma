use std::{env, sync::Arc};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use fantasma_auth::StaticAdminAuthorizer;
use fantasma_core::{EventCountQuery, MetricQuery, MetricResponse, MetricSeriesPoint};
use fantasma_store::{BootstrapConfig, DatabaseConfig, PgPool, bootstrap, connect, count_events};
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
        .route("/v1/metrics/active-users", get(active_users))
        .route("/v1/metrics/sessions", get(sessions))
        .route("/v1/metrics/retention", get(retention))
        .route("/v1/metrics/screens", get(screens))
        .route("/v1/metrics/releases", get(releases))
        .route("/v1/metrics/events", get(custom_events))
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

async fn active_users(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MetricQuery>,
) -> impl IntoResponse {
    metric_response(&state, &headers, query, "active_users")
}

async fn sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MetricQuery>,
) -> impl IntoResponse {
    metric_response(&state, &headers, query, "sessions")
}

async fn retention(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MetricQuery>,
) -> impl IntoResponse {
    metric_response(&state, &headers, query, "retention")
}

async fn screens(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MetricQuery>,
) -> impl IntoResponse {
    metric_response(&state, &headers, query, "screens")
}

async fn releases(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MetricQuery>,
) -> impl IntoResponse {
    metric_response(&state, &headers, query, "releases")
}

async fn custom_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MetricQuery>,
) -> impl IntoResponse {
    metric_response(&state, &headers, query, "events")
}

fn metric_response(
    state: &AppState,
    headers: &HeaderMap,
    _query: MetricQuery,
    metric_name: &'static str,
) -> axum::response::Response {
    if state.authorizer.authorize(headers).is_err() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        Json(
            serde_json::to_value(MetricResponse {
                metric: metric_name.to_owned(),
                points: Vec::new(),
            })
            .expect("serialize metric response"),
        ),
    )
        .into_response()
}
