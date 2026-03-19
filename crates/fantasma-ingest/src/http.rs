use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use fantasma_core::{EventAcceptedResponse, EventValidationResponse, RawEventBatchRequest};
use fantasma_store::{
    PgPool, ProjectState, StoreError, insert_events, load_project,
    resolve_project_id_for_ingest_key,
};

const INGEST_HEADER: &str = "x-fantasma-key";
const MAX_PAYLOAD_BYTES: usize = 512 * 1024;

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

pub fn app(pool: PgPool) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/events", post(ingest_events))
        .with_state(AppState { pool })
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

    match load_project(&state.pool, project_id).await {
        Ok(Some(project)) => match project.state {
            ProjectState::Active => {}
            ProjectState::RangeDeleting => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "project_busy" })),
                )
                    .into_response();
            }
            ProjectState::PendingDeletion => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "project_pending_deletion" })),
                )
                    .into_response();
            }
        },
        Ok(None) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "unauthorized" })),
            )
                .into_response();
        }
        Err(error) => {
            tracing::error!(?error, project_id = %project_id, "failed to load project for ingest state gate");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "internal_server_error" })),
            )
                .into_response();
        }
    }

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
    let payload: RawEventBatchRequest = match serde_json::from_slice(&body) {
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

    let payload = match payload.normalize() {
        Ok(payload) => payload,
        Err(issues) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(
                    serde_json::to_value(EventValidationResponse { errors: issues })
                        .expect("serialize validation response"),
                ),
            )
                .into_response();
        }
    };

    if let Err(error) = insert_events(&state.pool, project_id, &payload.events).await {
        let response = match error {
            StoreError::ProjectNotActive(ProjectState::RangeDeleting)
            | StoreError::ProjectFenceChanged => Some((
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "project_busy" })),
            )),
            StoreError::ProjectNotActive(ProjectState::PendingDeletion) => Some((
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "project_pending_deletion" })),
            )),
            StoreError::ProjectNotFound => Some((
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "unauthorized" })),
            )),
            _ => None,
        };
        if let Some(response) = response {
            return response.into_response();
        }
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
