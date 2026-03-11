use std::{env, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use fantasma_auth::StaticKeyAuthorizer;
use fantasma_core::{EventBatchRequest, EventBatchResponse};
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    authorizer: Arc<StaticKeyAuthorizer>,
}

#[tokio::main]
async fn main() {
    init_tracing();

    let bind_address =
        env::var("FANTASMA_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8081".to_owned());
    let state = AppState {
        authorizer: Arc::new(load_authorizer()),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/events", post(ingest_events))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind_address)
        .await
        .expect("bind ingest listener");
    let local_addr = listener.local_addr().expect("read local address");
    info!("fantasma-ingest listening on {local_addr}");

    axum::serve(listener, app)
        .await
        .expect("serve ingest application");
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            env::var("FANTASMA_LOG_LEVEL")
                .unwrap_or_else(|_| "fantasma_ingest=info,tower_http=info".to_owned()),
        )
        .try_init();
}

fn load_authorizer() -> StaticKeyAuthorizer {
    let project_id = env::var("FANTASMA_PROJECT_ID")
        .ok()
        .and_then(|value| Uuid::parse_str(&value).ok())
        .unwrap_or_else(|| StaticKeyAuthorizer::default().project_id());
    let ingest_key = env::var("FANTASMA_INGEST_KEY").unwrap_or_else(|_| "fg_ing_dev".to_owned());
    let admin_token = env::var("FANTASMA_ADMIN_TOKEN").unwrap_or_else(|_| "fg_pat_dev".to_owned());

    StaticKeyAuthorizer::new(project_id, ingest_key, admin_token)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok", "service": "fantasma-ingest" }))
}

async fn ingest_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<EventBatchRequest>,
) -> impl IntoResponse {
    if state.authorizer.authorize_ingest(&headers).is_err() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response();
    }

    let issues = payload.validate();
    if !issues.is_empty() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(
                serde_json::to_value(EventBatchResponse {
                    accepted: 0,
                    rejected: issues.len(),
                    errors: issues,
                })
                .expect("serialize validation response"),
            ),
        )
            .into_response();
    }

    let accepted = payload.events.len();
    let body = EventBatchResponse {
        accepted,
        rejected: 0,
        errors: Vec::new(),
    };

    (
        StatusCode::ACCEPTED,
        Json(serde_json::to_value(body).expect("serialize response")),
    )
        .into_response()
}
