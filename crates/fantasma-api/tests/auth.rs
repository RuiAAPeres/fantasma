use std::sync::Arc;

use axum::{
    body::{Body, to_bytes},
    http::{
        Method, Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
    response::Response,
};
use chrono::{DateTime, Utc};
use fantasma_auth::StaticAdminAuthorizer;
use fantasma_store::{PgPool, create_project, run_migrations};
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
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
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
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "local-read",
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

async fn insert_usage_event(
    pool: &PgPool,
    project_id: Uuid,
    event_name: &str,
    timestamp: &str,
    received_at: &str,
    install_id: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO events_raw (
            project_id,
            event_name,
            timestamp,
            received_at,
            install_id,
            platform,
            app_version,
            os_version,
            locale
        )
        VALUES ($1, $2, $3, $4, $5, 'ios', NULL, NULL, NULL)
        "#,
    )
    .bind(project_id)
    .bind(event_name)
    .bind(
        DateTime::parse_from_rfc3339(timestamp)
            .expect("timestamp")
            .with_timezone(&Utc),
    )
    .bind(
        DateTime::parse_from_rfc3339(received_at)
            .expect("received_at")
            .with_timezone(&Utc),
    )
    .bind(install_id)
    .execute(pool)
    .await
    .expect("insert usage event");
}

#[sqlx::test]
async fn management_routes_require_operator_bearer_auth(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );

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
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
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
    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
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
            .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
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
async fn usage_events_route_requires_operator_auth_and_counts_received_at(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (project_a, _, read_key_a) = provision_project(api.clone()).await;
    let (project_b, _, _) = provision_project(api.clone()).await;

    let project_a = Uuid::parse_str(&project_a).expect("project a");
    let project_b = Uuid::parse_str(&project_b).expect("project b");

    insert_usage_event(
        &pool,
        project_a,
        "app_open",
        "2026-03-01T00:00:00Z",
        "2026-03-01T00:00:00Z",
        "install-a",
    )
    .await;
    insert_usage_event(
        &pool,
        project_a,
        "app_open",
        "2026-03-01T01:00:00Z",
        "2026-03-05T00:00:00Z",
        "install-b",
    )
    .await;
    insert_usage_event(
        &pool,
        project_b,
        "app_open",
        "2026-02-28T23:00:00Z",
        "2026-03-01T12:00:00Z",
        "install-c",
    )
    .await;

    let project_key_response = api
        .clone()
        .oneshot(
            Request::get("/v1/usage/events?start=2026-03-01&end=2026-03-01")
                .header("x-fantasma-key", &read_key_a)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(project_key_response.status(), StatusCode::UNAUTHORIZED);

    let operator_response = api
        .oneshot(
            Request::get("/v1/usage/events?start=2026-03-01&end=2026-03-01")
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(operator_response.status(), StatusCode::OK);

    let usage = response_json(operator_response).await;
    assert_eq!(usage["start"], serde_json::json!("2026-03-01"));
    assert_eq!(usage["end"], serde_json::json!("2026-03-01"));
    assert_eq!(usage["total_events_processed"], serde_json::json!(2));
    assert_eq!(
        usage["projects"].as_array().map(|projects| projects.len()),
        Some(2)
    );
    assert_eq!(
        usage["projects"][0]["project"]["id"],
        serde_json::json!(project_a)
    );
    assert_eq!(
        usage["projects"][0]["events_processed"],
        serde_json::json!(1)
    );
    assert_eq!(
        usage["projects"][1]["project"]["id"],
        serde_json::json!(project_b)
    );
    assert_eq!(
        usage["projects"][1]["events_processed"],
        serde_json::json!(1)
    );
}

#[sqlx::test]
async fn usage_events_route_rejects_missing_invalid_and_project_key_auth(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (_, ingest_key, read_key) = provision_project(api.clone()).await;
    let uri = "/v1/usage/events?start=2026-03-01&end=2026-03-01";

    let cases = [
        (
            "missing auth",
            Request::get(uri).body(Body::empty()).expect("request"),
        ),
        (
            "invalid bearer",
            Request::get(uri)
                .header(AUTHORIZATION, "Bearer fg_pat_wrong")
                .body(Body::empty())
                .expect("request"),
        ),
        (
            "ingest key",
            Request::get(uri)
                .header("x-fantasma-key", &ingest_key)
                .body(Body::empty())
                .expect("request"),
        ),
        (
            "read key",
            Request::get(uri)
                .header("x-fantasma-key", &read_key)
                .body(Body::empty())
                .expect("request"),
        ),
    ];

    for (label, request) in cases {
        let response = api
            .clone()
            .oneshot(request)
            .await
            .expect("response succeeds");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{label}");
        assert_eq!(
            response_json(response).await,
            serde_json::json!({ "error": "unauthorized" }),
            "{label}"
        );
    }
}

#[sqlx::test]
async fn usage_events_route_rejects_duplicate_and_invalid_query_keys(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let cases = [
        (
            "/v1/usage/events?start=2026-03-01&start=2026-03-02&end=2026-03-31",
            "duplicate_query_key",
        ),
        (
            "/v1/usage/events?start=2026-03-01&end=2026-03-31&end=2026-04-01",
            "duplicate_query_key",
        ),
        (
            "/v1/usage/events?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start=2026-03-01&end=2026-03-31",
            "invalid_query_key",
        ),
        (
            "/v1/usage/events?start=2026-03-01&end=2026-03-31&plan=pro",
            "invalid_query_key",
        ),
    ];

    for (uri, expected_error) in cases {
        let response = api
            .clone()
            .oneshot(
                Request::get(uri)
                    .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response succeeds");

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY, "{uri}");
        assert_eq!(
            response_json(response).await,
            serde_json::json!({ "error": expected_error }),
            "{uri}"
        );
    }
}

#[sqlx::test]
async fn usage_events_route_rejects_invalid_time_ranges(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let cases = [
        "/v1/usage/events?end=2026-03-31",
        "/v1/usage/events?start=2026-03-01",
        "/v1/usage/events?start=not-a-date&end=2026-03-31",
        "/v1/usage/events?start=2026-03-01&end=not-a-date",
        "/v1/usage/events?start=2026-03-31&end=2026-03-01",
    ];

    for uri in cases {
        let response = api
            .clone()
            .oneshot(
                Request::get(uri)
                    .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response succeeds");

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY, "{uri}");
        assert_eq!(
            response_json(response).await,
            serde_json::json!({ "error": "invalid_time_range" }),
            "{uri}"
        );
    }
}

#[sqlx::test]
async fn live_installs_route_requires_read_key_auth(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (_, ingest_key, read_key) = provision_project(api.clone()).await;

    let missing_auth = api
        .clone()
        .oneshot(
            Request::get("/v1/metrics/live_installs")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);

    let ingest_auth = api
        .clone()
        .oneshot(
            Request::get("/v1/metrics/live_installs")
                .header("x-fantasma-key", &ingest_key)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(ingest_auth.status(), StatusCode::UNAUTHORIZED);

    let operator_auth = api
        .clone()
        .oneshot(
            Request::get("/v1/metrics/live_installs")
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(operator_auth.status(), StatusCode::UNAUTHORIZED);

    let read_auth = api
        .oneshot(
            Request::get("/v1/metrics/live_installs")
                .header("x-fantasma-key", &read_key)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(read_auth.status(), StatusCode::OK);
}

#[sqlx::test]
async fn event_metrics_reject_public_project_id_query_params(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
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

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let missing_project_id = Uuid::new_v4();
    let missing_key_id = Uuid::new_v4();

    let list_response = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{missing_project_id}/keys"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
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
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "local-read",
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
            .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(revoke_response.status(), StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn management_project_responses_include_lifecycle_state(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (project_id, _, _) = provision_project(api.clone()).await;

    let list_response = api
        .clone()
        .oneshot(
            Request::get("/v1/projects")
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(list_response.status(), StatusCode::OK);
    let listed = response_json(list_response).await;
    assert_eq!(listed["projects"][0]["state"], serde_json::json!("active"));

    let get_response = api
        .oneshot(
            Request::get(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(get_response.status(), StatusCode::OK);
    let fetched = response_json(get_response).await;
    assert_eq!(fetched["project"]["state"], serde_json::json!("active"));
}

#[sqlx::test]
async fn delete_project_enqueues_pending_deletion_job_and_is_idempotent(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (project_id, _, _) = provision_project(api.clone()).await;

    let first_response = api
        .clone()
        .oneshot(
            Request::delete(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(first_response.status(), StatusCode::ACCEPTED);
    let first = response_json(first_response).await;
    assert_eq!(
        first["deletion"]["project_id"],
        serde_json::json!(project_id)
    );
    assert_eq!(
        first["deletion"]["kind"],
        serde_json::json!("project_purge")
    );
    assert_eq!(first["deletion"]["status"], serde_json::json!("queued"));

    let project_response = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    let project = response_json(project_response).await;
    assert_eq!(
        project["project"]["state"],
        serde_json::json!("pending_deletion")
    );

    let second_response = api
        .oneshot(
            Request::delete(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(second_response.status(), StatusCode::ACCEPTED);
    let second = response_json(second_response).await;
    assert_eq!(second["deletion"]["id"], first["deletion"]["id"]);
}

#[sqlx::test]
async fn delete_project_returns_latest_successful_historical_purge(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let project = create_project(&pool, "Historical Purge Project")
        .await
        .expect("create project");
    let older_job_id = Uuid::new_v4();
    let latest_job_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO project_deletions (
            id,
            project_id,
            project_name,
            kind,
            status,
            scope,
            created_at,
            finished_at
        )
        VALUES
            ($1, $2, $3, 'project_purge', 'succeeded', NULL, '2026-03-01T00:00:00Z', '2026-03-01T00:05:00Z'),
            ($4, $2, $3, 'project_purge', 'succeeded', NULL, '2026-03-02T00:00:00Z', '2026-03-02T00:05:00Z')
        "#,
    )
    .bind(older_job_id)
    .bind(project.id)
    .bind(project.name.clone())
    .bind(latest_job_id)
    .execute(&pool)
    .await
    .expect("insert historical purge jobs");

    sqlx::query("DELETE FROM projects WHERE id = $1")
        .bind(project.id)
        .execute(&pool)
        .await
        .expect("delete project row");

    let response = api
        .oneshot(
            Request::delete(format!("/v1/projects/{}", project.id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = response_json(response).await;
    assert_eq!(body["deletion"]["id"], serde_json::json!(latest_job_id));
    assert_eq!(body["deletion"]["status"], serde_json::json!("succeeded"));
    assert_eq!(body["deletion"]["kind"], serde_json::json!("project_purge"));
}

#[sqlx::test]
async fn range_delete_conflicts_when_project_is_pending_deletion(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (project_id, _, _) = provision_project(api.clone()).await;

    let delete_response = api
        .clone()
        .oneshot(
            Request::delete(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(delete_response.status(), StatusCode::ACCEPTED);

    let range_response = api
        .oneshot(
            Request::post(format!("/v1/projects/{project_id}/deletions"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "start_at": "2026-03-01T00:00:00Z",
                        "end_before": "2026-03-08T00:00:00Z",
                        "event": "app_open",
                        "filters": {
                            "platform": "ios"
                        }
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(range_response.status(), StatusCode::CONFLICT);
    assert_eq!(
        response_json(range_response).await,
        serde_json::json!({ "error": "project_pending_deletion" })
    );
}

#[sqlx::test]
async fn deletion_list_and_get_return_jobs_for_project(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (project_id, _, _) = provision_project(api.clone()).await;

    let delete_response = api
        .clone()
        .oneshot(
            Request::delete(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    let deletion = response_json(delete_response).await;
    let deletion_id = deletion["deletion"]["id"]
        .as_str()
        .expect("deletion id")
        .to_owned();

    let list_response = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{project_id}/deletions"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(list_response.status(), StatusCode::OK);
    let listed = response_json(list_response).await;
    assert_eq!(listed["deletions"][0]["id"], serde_json::json!(deletion_id));

    let get_response = api
        .oneshot(
            Request::get(format!("/v1/projects/{project_id}/deletions/{deletion_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(get_response.status(), StatusCode::OK);
    let fetched = response_json(get_response).await;
    assert_eq!(fetched["deletion"]["id"], serde_json::json!(deletion_id));
    assert_eq!(
        fetched["deletion"]["kind"],
        serde_json::json!("project_purge")
    );
}

#[sqlx::test]
async fn historical_keys_return_not_found_after_project_row_removal(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let project = create_project(&pool, "Historical Keys Project")
        .await
        .expect("create project");
    let deletion_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO project_deletions (
            id,
            project_id,
            project_name,
            kind,
            status,
            scope,
            created_at,
            finished_at
        )
        VALUES ($1, $2, $3, 'project_purge', 'succeeded', NULL, now(), now())
        "#,
    )
    .bind(deletion_id)
    .bind(project.id)
    .bind(project.name.clone())
    .execute(&pool)
    .await
    .expect("insert historical deletion");

    sqlx::query("DELETE FROM projects WHERE id = $1")
        .bind(project.id)
        .execute(&pool)
        .await
        .expect("delete project row");

    let keys_response = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{}/keys", project.id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(keys_response.status(), StatusCode::NOT_FOUND);

    let deletions_response = api
        .oneshot(
            Request::get(format!("/v1/projects/{}/deletions", project.id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(deletions_response.status(), StatusCode::OK);
    let body = response_json(deletions_response).await;
    assert_eq!(body["deletions"][0]["id"], serde_json::json!(deletion_id));
}

#[sqlx::test]
async fn deletion_get_returns_not_found_for_mismatched_project(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (project_a_id, _, _) = provision_project(api.clone()).await;
    let (project_b_id, _, _) = provision_project(api.clone()).await;

    let delete_response = api
        .clone()
        .oneshot(
            Request::delete(format!("/v1/projects/{project_a_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(delete_response.status(), StatusCode::ACCEPTED);
    let deletion = response_json(delete_response).await;
    let deletion_id = deletion["deletion"]["id"].as_str().expect("deletion id");

    let mismatched_response = api
        .oneshot(
            Request::get(format!(
                "/v1/projects/{project_b_id}/deletions/{deletion_id}"
            ))
            .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response succeeds");

    assert_eq!(mismatched_response.status(), StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn management_post_routes_authorize_before_body_validation(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
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

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
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
        serde_json::json!({ "error": "invalid_query_key" })
    );
}

#[sqlx::test]
async fn live_installs_rejects_public_query_params(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (project_id, _, read_key) = provision_project(api.clone()).await;
    let cases = [
        format!("/v1/metrics/live_installs?project_id={project_id}"),
        "/v1/metrics/live_installs?metric=live_installs".to_owned(),
        "/v1/metrics/live_installs?granularity=day".to_owned(),
        "/v1/metrics/live_installs?start=2026-01-01".to_owned(),
        "/v1/metrics/live_installs?end=2026-01-01".to_owned(),
        "/v1/metrics/live_installs?platform=ios".to_owned(),
        "/v1/metrics/live_installs?locale=en-US".to_owned(),
        "/v1/metrics/live_installs?group_by=platform".to_owned(),
    ];

    for uri in cases {
        let response = api
            .clone()
            .oneshot(
                Request::get(&uri)
                    .header("x-fantasma-key", &read_key)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response succeeds");

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY, "{uri}");
        assert_eq!(
            response_json(response).await,
            serde_json::json!({ "error": "invalid_query_key" }),
            "{uri}"
        );
    }
}

#[sqlx::test]
async fn management_create_routes_reject_unknown_json_fields(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );

    let create_project_response = api
        .clone()
        .oneshot(
            Request::post("/v1/projects")
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
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
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "local-read",
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

#[sqlx::test]
async fn project_metadata_routes_require_operator_bearer_auth(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (project_id, _, read_key) = provision_project(api.clone()).await;

    let unauthorized_get = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{project_id}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(unauthorized_get.status(), StatusCode::UNAUTHORIZED);

    let read_key_get = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{project_id}"))
                .header("x-fantasma-key", &read_key)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(read_key_get.status(), StatusCode::UNAUTHORIZED);

    let operator_get = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(operator_get.status(), StatusCode::OK);

    let read_key_patch = api
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/v1/projects/{project_id}"))
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &read_key)
                .body(Body::from(
                    serde_json::json!({
                        "name": "Renamed Project"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(read_key_patch.status(), StatusCode::UNAUTHORIZED);

    let operator_patch = api
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "Renamed Project"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(operator_patch.status(), StatusCode::OK);
}

#[sqlx::test]
async fn project_metadata_get_and_patch_return_project_payload(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (project_id, _, _) = provision_project(api.clone()).await;

    let get_response = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(get_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(get_response).await["project"]["name"],
        serde_json::json!("API Test Project")
    );

    let patch_response = api
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "Renamed API Test Project"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(patch_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(patch_response).await["project"]["name"],
        serde_json::json!("Renamed API Test Project")
    );

    let get_after_patch = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(get_after_patch.status(), StatusCode::OK);
    let project = response_json(get_after_patch).await["project"].clone();
    assert_eq!(project["id"], serde_json::json!(project_id));
    assert_eq!(
        project["name"],
        serde_json::json!("Renamed API Test Project")
    );
    assert!(project["created_at"].is_string());
}

#[sqlx::test]
async fn project_metadata_patch_rejects_unknown_fields_and_missing_projects(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let missing_project_id = Uuid::new_v4();
    let (project_id, _, _) = provision_project(api.clone()).await;

    let invalid_patch = api
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/v1/projects/{project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "Still Valid",
                        "slug": "not-allowed"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(invalid_patch.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_json(invalid_patch).await,
        serde_json::json!({ "error": "invalid_request" })
    );

    let missing_get = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{missing_project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(missing_get.status(), StatusCode::NOT_FOUND);

    let missing_patch = api
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/v1/projects/{missing_project_id}"))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "Renamed Project"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(missing_patch.status(), StatusCode::NOT_FOUND);
}

#[sqlx::test]
async fn event_discovery_routes_require_read_keys(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool,
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let (project_id, ingest_key, read_key) = provision_project(api.clone()).await;

    let operator_catalog = api
        .clone()
        .oneshot(
            Request::get("/v1/metrics/events/catalog?start=2026-01-01&end=2026-01-02")
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(operator_catalog.status(), StatusCode::UNAUTHORIZED);

    let ingest_top = api
        .clone()
        .oneshot(
            Request::get("/v1/metrics/events/top?start=2026-01-01&end=2026-01-02")
                .header("x-fantasma-key", &ingest_key)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(ingest_top.status(), StatusCode::UNAUTHORIZED);

    let operator_total = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-01-01&end=2026-01-02",
            )
            .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(operator_total.status(), StatusCode::UNAUTHORIZED);

    let read_catalog = api
        .clone()
        .oneshot(
            Request::get("/v1/metrics/events/catalog?start=2026-01-01&end=2026-01-02")
                .header("x-fantasma-key", &read_key)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(read_catalog.status(), StatusCode::OK);

    let read_total = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-01-01&end=2026-01-02",
            )
            .header("x-fantasma-key", &read_key)
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(read_total.status(), StatusCode::OK);

    let invalid_project_query = api
        .clone()
        .oneshot(
            Request::get(format!(
                "/v1/metrics/events/catalog?project_id={project_id}&start=2026-01-01&end=2026-01-02"
            ))
            .header("x-fantasma-key", &read_key)
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(
        invalid_project_query.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let invalid_total_query = api
        .clone()
        .oneshot(
            Request::get(format!(
                "/v1/metrics/events?project_id={project_id}&event=app_open&metric=count&granularity=day&start=2026-01-01&end=2026-01-02"
            ))
            .header("x-fantasma-key", &read_key)
            .body(Body::empty())
            .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(
        invalid_total_query.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let invalid_limit = api
        .oneshot(
            Request::get("/v1/metrics/events/top?start=2026-01-01&end=2026-01-02&limit=0")
                .header("x-fantasma-key", &read_key)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response succeeds");
    assert_eq!(invalid_limit.status(), StatusCode::UNPROCESSABLE_ENTITY);
}
