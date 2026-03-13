use std::sync::Arc;

use axum::{
    body::{Body, to_bytes},
    http::{
        Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
    response::Response,
};
use fantasma_auth::StaticAdminAuthorizer;
use fantasma_store::{PgPool, run_migrations};
use tower::ServiceExt;
use uuid::Uuid;

async fn response_json(response: Response) -> serde_json::Value {
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body"),
    )
    .expect("decode response body")
}

async fn provision_project(api: axum::Router) -> (String, String, String) {
    let create_project_response = api
        .clone()
        .oneshot(
            Request::post("/v1/projects")
                .header(AUTHORIZATION, "Bearer fg_pat_dev")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "API Test Project",
                        "ingest_key_name": "ios-sdk"
                    })
                    .to_string(),
                ))
                .expect("valid create-project request"),
        )
        .await
        .expect("create-project request succeeds");
    assert_eq!(create_project_response.status(), StatusCode::CREATED);

    let created_project = response_json(create_project_response).await;
    assert_eq!(
        created_project["ingest_key"]["revoked_at"],
        serde_json::Value::Null
    );
    assert!(
        created_project["ingest_key"].get("project_id").is_none(),
        "create-project responses must not expose project_id in key payloads"
    );
    let project_id = created_project["project"]["id"]
        .as_str()
        .expect("project id")
        .to_owned();
    let ingest_key = created_project["ingest_key"]["secret"]
        .as_str()
        .expect("ingest key secret")
        .to_owned();

    let create_read_key_response = api
        .oneshot(
            Request::post(format!("/v1/projects/{project_id}/keys"))
                .header(AUTHORIZATION, "Bearer fg_pat_dev")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "dashboard",
                        "kind": "read"
                    })
                    .to_string(),
                ))
                .expect("valid create-key request"),
        )
        .await
        .expect("create-key request succeeds");
    assert_eq!(create_read_key_response.status(), StatusCode::CREATED);

    let created_read_key = response_json(create_read_key_response).await;
    assert_eq!(
        created_read_key["key"]["revoked_at"],
        serde_json::Value::Null
    );
    assert!(
        created_read_key["key"].get("project_id").is_none(),
        "create-key responses must not expose project_id in key payloads"
    );
    let read_key = created_read_key["key"]["secret"]
        .as_str()
        .expect("read key secret")
        .to_owned();

    (project_id, ingest_key, read_key)
}

fn event_payload() -> Body {
    Body::from(
        serde_json::json!({
            "events": [
                {
                    "event": "app_open",
                    "timestamp": "2026-01-01T00:00:00Z",
                    "install_id": "api-auth-test-install",
                    "platform": "ios"
                }
            ]
        })
        .to_string(),
    )
}

#[sqlx::test]
async fn management_routes_require_operator_bearer_auth(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(pool, Arc::new(StaticAdminAuthorizer::new("fg_pat_dev")));

    let unauthorized_response = api
        .clone()
        .oneshot(
            Request::get("/v1/projects")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(unauthorized_response.status(), StatusCode::UNAUTHORIZED);

    let authorized_response = api
        .oneshot(
            Request::get("/v1/projects")
                .header(AUTHORIZATION, "Bearer fg_pat_dev")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(authorized_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(authorized_response).await,
        serde_json::json!({ "projects": [] })
    );
}

#[sqlx::test]
async fn ingest_and_metrics_routes_enforce_project_key_kinds(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(pool, Arc::new(StaticAdminAuthorizer::new("fg_pat_dev")));
    let (_, ingest_key, read_key) = provision_project(api.clone()).await;

    let ingest_with_read_key = ingest
        .clone()
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &read_key)
                .body(event_payload())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(ingest_with_read_key.status(), StatusCode::UNAUTHORIZED);

    let ingest_with_ingest_key = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &ingest_key)
                .body(event_payload())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(ingest_with_ingest_key.status(), StatusCode::ACCEPTED);

    let metrics_with_ingest_key = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-01",
            )
            .header("x-fantasma-key", &ingest_key)
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(metrics_with_ingest_key.status(), StatusCode::UNAUTHORIZED);

    let metrics_with_operator_token = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-01",
            )
            .header(AUTHORIZATION, "Bearer fg_pat_dev")
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(
        metrics_with_operator_token.status(),
        StatusCode::UNAUTHORIZED
    );

    let metrics_with_read_key = api
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-01",
            )
            .header("x-fantasma-key", &read_key)
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(metrics_with_read_key.status(), StatusCode::OK);
}

#[sqlx::test]
async fn event_metrics_reject_public_project_id_query_params(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(pool, Arc::new(StaticAdminAuthorizer::new("fg_pat_dev")));
    let (project_id, _, read_key) = provision_project(api.clone()).await;

    let response = api
        .oneshot(
            Request::get(format!(
                "/v1/metrics/events?project_id={project_id}&event=app_open&metric=count&granularity=day&start=2026-01-01&end=2026-01-01"
            ))
            .header("x-fantasma-key", &read_key)
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({ "error": "invalid_query_key" })
    );
}

#[sqlx::test]
async fn management_missing_resources_return_not_found(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(pool, Arc::new(StaticAdminAuthorizer::new("fg_pat_dev")));
    let missing_project_id = Uuid::new_v4();
    let missing_key_id = Uuid::new_v4();

    let list_response = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{missing_project_id}/keys"))
                .header(AUTHORIZATION, "Bearer fg_pat_dev")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(list_response.status(), StatusCode::NOT_FOUND);

    let create_response = api
        .clone()
        .oneshot(
            Request::post(format!("/v1/projects/{missing_project_id}/keys"))
                .header(AUTHORIZATION, "Bearer fg_pat_dev")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "dashboard",
                        "kind": "read"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(create_response.status(), StatusCode::NOT_FOUND);

    let revoke_response = api
        .oneshot(
            Request::delete(format!(
                "/v1/projects/{missing_project_id}/keys/{missing_key_id}"
            ))
            .header(AUTHORIZATION, "Bearer fg_pat_dev")
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(revoke_response.status(), StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn management_post_routes_authorize_before_body_validation(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(pool, Arc::new(StaticAdminAuthorizer::new("fg_pat_dev")));
    let missing_project_id = Uuid::new_v4();

    let create_project_response = api
        .clone()
        .oneshot(
            Request::post("/v1/projects")
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("not-json"))
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(create_project_response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response_json(create_project_response).await,
        serde_json::json!({ "error": "unauthorized" })
    );

    let create_key_response = api
        .oneshot(
            Request::post(format!("/v1/projects/{missing_project_id}/keys"))
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("not-json"))
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(create_key_response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response_json(create_key_response).await,
        serde_json::json!({ "error": "unauthorized" })
    );
}

#[sqlx::test]
async fn session_metrics_reject_public_project_id_query_params(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(pool, Arc::new(StaticAdminAuthorizer::new("fg_pat_dev")));
    let (project_id, _, read_key) = provision_project(api.clone()).await;

    let response = api
        .oneshot(
            Request::get(format!(
                "/v1/metrics/sessions?project_id={project_id}&metric=count&granularity=day&start=2026-01-01&end=2026-01-01"
            ))
            .header("x-fantasma-key", &read_key)
            .body(Body::empty())
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
async fn management_create_routes_reject_unknown_json_fields(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(pool, Arc::new(StaticAdminAuthorizer::new("fg_pat_dev")));

    let create_project_response = api
        .clone()
        .oneshot(
            Request::post("/v1/projects")
                .header(AUTHORIZATION, "Bearer fg_pat_dev")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "API Test Project",
                        "ingest_key_name": "ios-sdk",
                        "extra": "nope"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(
        create_project_response.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        response_json(create_project_response).await,
        serde_json::json!({ "error": "invalid_request" })
    );

    let (project_id, _, _) = provision_project(api.clone()).await;

    let create_key_response = api
        .oneshot(
            Request::post(format!("/v1/projects/{project_id}/keys"))
                .header(AUTHORIZATION, "Bearer fg_pat_dev")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "dashboard",
                        "kind": "read",
                        "extra": "nope"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(
        create_key_response.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        response_json(create_key_response).await,
        serde_json::json!({ "error": "invalid_request" })
    );
}
