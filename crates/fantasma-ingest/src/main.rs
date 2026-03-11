use std::env;

use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use fantasma_core::{EventAcceptedResponse, EventBatchRequest, EventValidationResponse};
use fantasma_store::{
    BootstrapConfig, DatabaseConfig, PgPool, bootstrap, connect, insert_events,
    resolve_project_id_for_ingest_key,
};
use tracing::info;
use uuid::Uuid;

const INGEST_HEADER: &str = "x-fantasma-key";
const MAX_PAYLOAD_BYTES: usize = 512 * 1024;

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

#[tokio::main]
async fn main() {
    init_tracing();

    let bind_address =
        env::var("FANTASMA_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8081".to_owned());
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
    let state = AppState { pool };

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

fn default_project_id() -> Uuid {
    Uuid::from_u128(0x9bad8b88_5e7a_44ed_98ce_4cf9ddde713a)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok", "service": "fantasma-ingest" }))
}

async fn ingest_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Request,
) -> impl IntoResponse {
    let Some(ingest_key) = headers.get(INGEST_HEADER) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response();
    };
    let Ok(ingest_key) = ingest_key.to_str() else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "unauthorized" })),
        )
            .into_response();
    };
    let project_id = match resolve_project_id_for_ingest_key(&state.pool, ingest_key).await {
        Ok(Some(project_id)) => project_id,
        Ok(None) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "unauthorized" })),
            )
                .into_response();
        }
        Err(error) => {
            tracing::error!(?error, "failed to resolve ingest key");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal_server_error" })),
            )
                .into_response();
        }
    };

    let body = match to_bytes(request.into_body(), MAX_PAYLOAD_BYTES).await {
        Ok(body) => body,
        Err(_) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(serde_json::json!({ "error": "payload_too_large" })),
            )
                .into_response();
        }
    };
    let payload: EventBatchRequest = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::debug!(?error, "failed to parse event batch request");
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "error": "invalid_request" })),
            )
                .into_response();
        }
    };

    let issues = payload.validate();
    if !issues.is_empty() {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(
                serde_json::to_value(EventValidationResponse { errors: issues })
                    .expect("serialize validation response"),
            ),
        )
            .into_response();
    }

    if let Err(error) = insert_events(&state.pool, project_id, &payload.events).await {
        tracing::error!(?error, "failed to insert accepted events");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "internal_server_error" })),
        )
            .into_response();
    }

    let body = EventAcceptedResponse {
        accepted: payload.events.len(),
    };

    (
        StatusCode::ACCEPTED,
        Json(serde_json::to_value(body).expect("serialize response")),
    )
        .into_response()
}
