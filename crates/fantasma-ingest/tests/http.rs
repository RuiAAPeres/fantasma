use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header::CONTENT_TYPE},
    response::Response,
};
use fantasma_store::{
    ApiKeyKind, PgPool, create_api_key, create_project, generate_api_key_secret, run_migrations,
};
use tower::util::ServiceExt;

async fn response_json(response: Response) -> serde_json::Value {
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body"),
    )
    .expect("decode response body")
}

fn event_batch_body(count: usize) -> String {
    serde_json::json!({
        "events": (0..count)
            .map(|index| serde_json::json!({
                "event": "app_open",
                "timestamp": "2026-01-01T00:00:00Z",
                "install_id": format!("install-{index}"),
                "platform": "ios"
            }))
            .collect::<Vec<_>>()
    })
    .to_string()
}

struct ProvisionedProject {
    ingest_key: String,
    read_key: String,
}

async fn provision_project(pool: &PgPool) -> ProvisionedProject {
    let project = create_project(pool, "Ingest Test Project")
        .await
        .expect("create project");

    let ingest_key = generate_api_key_secret(ApiKeyKind::Ingest);
    create_api_key(
        pool,
        project.id,
        "ingest-test",
        ApiKeyKind::Ingest,
        &ingest_key,
    )
    .await
    .expect("create ingest key")
    .expect("project exists");

    let read_key = generate_api_key_secret(ApiKeyKind::Read);
    create_api_key(pool, project.id, "read-test", ApiKeyKind::Read, &read_key)
        .await
        .expect("create read key")
        .expect("project exists");

    ProvisionedProject {
        ingest_key,
        read_key,
    }
}

#[sqlx::test]
async fn ingest_requires_ingest_key_header(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    let app = fantasma_ingest::app(pool);

    let response = app
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from("{\"events\":[]}"))
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({ "error": "unauthorized" })
    );
}

#[sqlx::test]
async fn ingest_rejects_non_ingest_project_keys(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    let provisioned = provision_project(&pool).await;
    let app = fantasma_ingest::app(pool);

    let response = app
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", provisioned.read_key)
                .body(Body::from("{\"events\":[]}"))
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({ "error": "unauthorized" })
    );
}

#[sqlx::test]
async fn ingest_rejects_malformed_json(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    let provisioned = provision_project(&pool).await;
    let app = fantasma_ingest::app(pool);

    let response = app
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", provisioned.ingest_key)
                .body(Body::from("not-json"))
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({ "error": "invalid_request" })
    );
}

#[sqlx::test]
async fn ingest_returns_validation_error_envelope(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    let provisioned = provision_project(&pool).await;
    let app = fantasma_ingest::app(pool);

    let response = app
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    let body = response_json(response).await;
    assert_eq!(body["errors"][0]["index"], serde_json::json!(0));
    assert_eq!(
        body["errors"][0]["message"],
        serde_json::json!("event name must not be empty")
    );
}

#[sqlx::test]
async fn ingest_rejects_payloads_above_limit(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    let provisioned = provision_project(&pool).await;
    let app = fantasma_ingest::app(pool);
    let oversized = "x".repeat(513 * 1024);

    let response = app
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", provisioned.ingest_key)
                .body(Body::from(oversized))
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({ "error": "payload_too_large" })
    );
}

#[sqlx::test]
async fn ingest_accepts_valid_batches(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    let provisioned = provision_project(&pool).await;
    let app = fantasma_ingest::app(pool);

    let response = app
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({ "accepted": 1 })
    );
}

#[sqlx::test]
async fn ingest_returns_project_busy_for_range_deleting_projects(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    let provisioned = provision_project(&pool).await;
    sqlx::query("UPDATE projects SET state = 'range_deleting' WHERE name = 'Ingest Test Project'")
        .execute(&pool)
        .await
        .expect("update project state");
    let app = fantasma_ingest::app(pool);

    let response = app
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", provisioned.ingest_key)
                .body(Body::from(event_batch_body(1)))
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({ "error": "project_busy" })
    );
}

#[sqlx::test]
async fn ingest_returns_project_pending_deletion_for_pending_projects(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    let provisioned = provision_project(&pool).await;
    sqlx::query(
        "UPDATE projects SET state = 'pending_deletion' WHERE name = 'Ingest Test Project'",
    )
    .execute(&pool)
    .await
    .expect("update project state");
    let app = fantasma_ingest::app(pool);

    let response = app
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", provisioned.ingest_key)
                .body(Body::from(event_batch_body(1)))
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({ "error": "project_pending_deletion" })
    );
}

#[sqlx::test]
async fn ingest_accepts_batches_up_to_two_hundred_events(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    let provisioned = provision_project(&pool).await;
    let app = fantasma_ingest::app(pool);

    let response = app
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", provisioned.ingest_key)
                .body(Body::from(event_batch_body(200)))
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({ "accepted": 200 })
    );
}

#[sqlx::test]
async fn ingest_rejects_batches_above_two_hundred_events(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    let provisioned = provision_project(&pool).await;
    let app = fantasma_ingest::app(pool);

    let response = app
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", provisioned.ingest_key)
                .body(Body::from(event_batch_body(201)))
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({
            "errors": [
                {
                    "index": 0,
                    "message": "batch may contain at most 200 events"
                }
            ]
        })
    );
}
