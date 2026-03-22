use std::{sync::Arc, time::Duration};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use fantasma_auth::StaticAdminAuthorizer;
use fantasma_store::{
    PgPool, ProjectProcessingActorKind, claim_project_processing_lease,
    release_project_processing_lease, run_migrations,
};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use tower::ServiceExt;

fn scale_like_events(day: &str, install_count: usize) -> Vec<serde_json::Value> {
    let app_versions = ["1.0.0", "1.1.0", "1.2.0"];
    let os_versions = ["18.3", "18.4"];
    let locales = ["en-US", "pt-PT", "fr-FR", "de-DE"];
    let mut events = Vec::with_capacity(install_count * 3);

    for install_index in 0..install_count {
        let app_version = app_versions[install_index % app_versions.len()];
        let os_version = os_versions[install_index % os_versions.len()];
        let locale = locales[install_index % locales.len()];

        for timestamp in ["00:00:00Z", "00:10:00Z", "00:20:00Z"] {
            events.push(serde_json::json!({
                "event": "app_open",
                "timestamp": format!("{day}T{timestamp}"),
                "install_id": format!("scale-{day}-{install_index}"),
                "platform": "ios",
                "app_version": app_version,
                "os_version": os_version,
                "locale": locale,
            }));
        }
    }

    events
}

#[sqlx::test]
async fn range_delete_worker_rebuilds_state_and_reactivates_project(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "install-a",
                                "platform": "ios"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:10:00Z",
                                "install_id": "install-a",
                                "platform": "ios"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T01:00:00Z",
                                "install_id": "install-b",
                                "platform": "ios"
                            },
                            {
                                "event": "purchase_completed",
                                "timestamp": "2026-01-02T00:00:00Z",
                                "install_id": "install-a",
                                "platform": "ios"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");
    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_session_batch(&pool, 100)
        .await
        .expect("session worker batch succeeds");
    fantasma_worker::process_event_metrics_batch(&pool, 100)
        .await
        .expect("event metrics worker batch succeeds");

    let create_deletion_response = api
        .clone()
        .oneshot(
            Request::post(format!("/v1/projects/{}/deletions", provisioned.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "start_at": "2026-01-01T00:00:00Z",
                        "end_before": "2026-01-02T00:00:00Z",
                        "event": "app_open"
                    })
                    .to_string(),
                ))
                .expect("valid range-delete request"),
        )
        .await
        .expect("range-delete request succeeds");
    assert_eq!(create_deletion_response.status(), StatusCode::ACCEPTED);
    let created_deletion = response_json(create_deletion_response).await;
    let deletion_id = created_deletion["deletion"]["id"]
        .as_str()
        .expect("deletion id")
        .to_owned();

    let pending_project = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{}", provisioned.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("valid get-project request"),
        )
        .await
        .expect("project get succeeds");
    assert_eq!(pending_project.status(), StatusCode::OK);
    assert_eq!(
        response_json(pending_project).await["project"]["state"],
        serde_json::json!("range_deleting")
    );

    assert_eq!(
        fantasma_worker::process_project_deletion_batch(&pool)
            .await
            .expect("project deletion worker succeeds"),
        1
    );

    let reloaded_project = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{}", provisioned.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("valid get-project request"),
        )
        .await
        .expect("project get succeeds after range delete");
    assert_eq!(reloaded_project.status(), StatusCode::OK);
    assert_eq!(
        response_json(reloaded_project).await["project"]["state"],
        serde_json::json!("active")
    );

    let deletion_response = api
        .clone()
        .oneshot(
            Request::get(format!(
                "/v1/projects/{}/deletions/{}",
                provisioned.project_id, deletion_id
            ))
            .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
            .body(Body::empty())
            .expect("valid deletion get request"),
        )
        .await
        .expect("deletion get succeeds");
    assert_eq!(deletion_response.status(), StatusCode::OK);
    let deletion = response_json(deletion_response).await;
    assert_eq!(
        deletion["deletion"]["status"],
        serde_json::json!("succeeded")
    );
    assert_eq!(
        deletion["deletion"]["deleted_raw_events"],
        serde_json::json!(3)
    );

    let project_id = provisioned
        .project_id
        .parse::<uuid::Uuid>()
        .expect("project id parses");
    let raw_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events_raw WHERE project_id = $1")
            .bind(project_id)
            .fetch_one(&pool)
            .await
            .expect("count raw events");
    let session_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sessions WHERE project_id = $1")
            .bind(project_id)
            .fetch_one(&pool)
            .await
            .expect("count sessions");

    assert_eq!(raw_count, 1);
    assert_eq!(session_count, 1);
}

#[sqlx::test]
async fn project_purge_worker_removes_project_and_keeps_history(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let delete_response = api
        .clone()
        .oneshot(
            Request::delete(format!("/v1/projects/{}", provisioned.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("valid delete-project request"),
        )
        .await
        .expect("delete-project request succeeds");
    assert_eq!(delete_response.status(), StatusCode::ACCEPTED);

    assert_eq!(
        fantasma_worker::process_project_deletion_batch(&pool)
            .await
            .expect("project purge worker succeeds"),
        1
    );

    let missing_project = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{}", provisioned.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("valid get-project request"),
        )
        .await
        .expect("project get succeeds");
    assert_eq!(missing_project.status(), StatusCode::NOT_FOUND);

    let history_response = api
        .oneshot(
            Request::get(format!("/v1/projects/{}/deletions", provisioned.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("valid deletions list request"),
        )
        .await
        .expect("deletions list succeeds");
    assert_eq!(history_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(history_response).await["deletions"][0]["status"],
        serde_json::json!("succeeded")
    );
}

#[sqlx::test]
async fn project_deletion_waits_for_older_processing_leases(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;
    let project_id = provisioned
        .project_id
        .parse::<uuid::Uuid>()
        .expect("project id parses");

    let mut leases = Vec::new();
    for actor_kind in [
        ProjectProcessingActorKind::Ingest,
        ProjectProcessingActorKind::SessionApply,
        ProjectProcessingActorKind::SessionRepair,
        ProjectProcessingActorKind::EventMetrics,
        ProjectProcessingActorKind::ProjectPatch,
        ProjectProcessingActorKind::ApiKeyCreate,
        ProjectProcessingActorKind::ApiKeyRevoke,
    ] {
        let lease = claim_project_processing_lease(
            &pool,
            project_id,
            actor_kind,
            &format!("held-{}", actor_kind.as_str()),
        )
        .await
        .expect("claim processing lease");
        leases.push(lease);
    }

    let create_deletion_response = api
        .clone()
        .oneshot(
            Request::post(format!("/v1/projects/{}/deletions", provisioned.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "start_at": "2026-01-01T00:00:00Z",
                        "end_before": "2026-01-02T00:00:00Z"
                    })
                    .to_string(),
                ))
                .expect("valid range-delete request"),
        )
        .await
        .expect("range-delete request succeeds");
    assert_eq!(create_deletion_response.status(), StatusCode::ACCEPTED);

    assert_eq!(
        fantasma_worker::process_project_deletion_batch(&pool)
            .await
            .expect("project deletion worker returns without running while leases remain"),
        0
    );

    let blocked_project = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{}", provisioned.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("valid get-project request"),
        )
        .await
        .expect("project get succeeds while deletion is blocked");
    assert_eq!(blocked_project.status(), StatusCode::OK);
    assert_eq!(
        response_json(blocked_project).await["project"]["state"],
        serde_json::json!("range_deleting")
    );

    let blocked_jobs = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{}/deletions", provisioned.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("valid deletions list request"),
        )
        .await
        .expect("deletions list succeeds");
    assert_eq!(blocked_jobs.status(), StatusCode::OK);
    assert_eq!(
        response_json(blocked_jobs).await["deletions"][0]["status"],
        serde_json::json!("queued")
    );

    for lease in &leases {
        release_project_processing_lease(&pool, lease)
            .await
            .expect("release processing lease");
    }

    assert_eq!(
        fantasma_worker::process_project_deletion_batch(&pool)
            .await
            .expect("project deletion worker runs once older leases clear"),
        1
    );
}

#[sqlx::test]
async fn project_deletion_waits_while_open_writer_transaction_holds_visible_lease(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;
    let project_id = provisioned
        .project_id
        .parse::<uuid::Uuid>()
        .expect("project id parses");

    let lease = claim_project_processing_lease(
        &pool,
        project_id,
        ProjectProcessingActorKind::Ingest,
        "inflight-ingest",
    )
    .await
    .expect("claim ingest processing lease");

    let mut inflight_tx = pool.begin().await.expect("begin inflight writer tx");
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&mut *inflight_tx)
        .await
        .expect("keep writer transaction open");

    let create_deletion_response = api
        .clone()
        .oneshot(
            Request::post(format!("/v1/projects/{}/deletions", provisioned.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "start_at": "2026-01-01T00:00:00Z",
                        "end_before": "2026-01-02T00:00:00Z"
                    })
                    .to_string(),
                ))
                .expect("valid range-delete request"),
        )
        .await
        .expect("range-delete request succeeds");
    assert_eq!(create_deletion_response.status(), StatusCode::ACCEPTED);

    assert_eq!(
        fantasma_worker::process_project_deletion_batch(&pool)
            .await
            .expect("project deletion worker returns without running while inflight lease remains"),
        0
    );

    inflight_tx
        .rollback()
        .await
        .expect("rollback inflight writer tx");
    release_project_processing_lease(&pool, &lease)
        .await
        .expect("release ingest processing lease");

    assert_eq!(
        fantasma_worker::process_project_deletion_batch(&pool)
            .await
            .expect("project deletion worker runs once inflight lease clears"),
        1
    );
}

#[sqlx::test]
async fn blocked_deletion_does_not_head_of_line_block_other_projects(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let blocked_project = provision_project(api.clone()).await;
    let ready_project = provision_project(api.clone()).await;
    let blocked_project_id = blocked_project
        .project_id
        .parse::<uuid::Uuid>()
        .expect("blocked project id parses");

    let lease = claim_project_processing_lease(
        &pool,
        blocked_project_id,
        ProjectProcessingActorKind::Ingest,
        "blocked-range-delete",
    )
    .await
    .expect("claim blocking lease");

    let blocked_delete_response = api
        .clone()
        .oneshot(
            Request::post(format!(
                "/v1/projects/{}/deletions",
                blocked_project.project_id
            ))
            .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::json!({
                    "start_at": "2026-01-01T00:00:00Z",
                    "end_before": "2026-01-02T00:00:00Z"
                })
                .to_string(),
            ))
            .expect("valid blocked range-delete request"),
        )
        .await
        .expect("blocked range-delete request succeeds");
    assert_eq!(blocked_delete_response.status(), StatusCode::ACCEPTED);

    let ready_delete_response = api
        .clone()
        .oneshot(
            Request::delete(format!("/v1/projects/{}", ready_project.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("valid ready project delete request"),
        )
        .await
        .expect("ready project delete request succeeds");
    assert_eq!(ready_delete_response.status(), StatusCode::ACCEPTED);

    assert_eq!(
        fantasma_worker::process_project_deletion_batch(&pool)
            .await
            .expect("project deletion worker should process ready project behind blocked one"),
        1
    );

    let ready_project_get = api
        .clone()
        .oneshot(
            Request::get(format!("/v1/projects/{}", ready_project.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("valid get ready project request"),
        )
        .await
        .expect("ready project get succeeds");
    assert_eq!(ready_project_get.status(), StatusCode::NOT_FOUND);

    let blocked_jobs = api
        .clone()
        .oneshot(
            Request::get(format!(
                "/v1/projects/{}/deletions",
                blocked_project.project_id
            ))
            .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
            .body(Body::empty())
            .expect("valid blocked project deletions list request"),
        )
        .await
        .expect("blocked project deletions list succeeds");
    assert_eq!(blocked_jobs.status(), StatusCode::OK);
    assert_eq!(
        response_json(blocked_jobs).await["deletions"][0]["status"],
        serde_json::json!("queued")
    );

    release_project_processing_lease(&pool, &lease)
        .await
        .expect("release blocking lease");
}

#[sqlx::test]
async fn stale_processing_leases_do_not_block_deletion_forever(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;
    let project_id = provisioned
        .project_id
        .parse::<uuid::Uuid>()
        .expect("project id parses");

    let lease = claim_project_processing_lease(
        &pool,
        project_id,
        ProjectProcessingActorKind::Ingest,
        "stale-ingest",
    )
    .await
    .expect("claim stale lease");

    sqlx::query(
        r#"
        UPDATE project_processing_leases
        SET claimed_at = now() - interval '2 hours',
            heartbeat_at = now() - interval '2 hours'
        WHERE project_id = $1
          AND actor_kind = $2
          AND actor_id = $3
        "#,
    )
    .bind(project_id)
    .bind(ProjectProcessingActorKind::Ingest.as_str())
    .bind(&lease.actor_id)
    .execute(&pool)
    .await
    .expect("age lease heartbeat");

    let delete_response = api
        .clone()
        .oneshot(
            Request::delete(format!("/v1/projects/{}", provisioned.project_id))
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .body(Body::empty())
                .expect("valid purge request"),
        )
        .await
        .expect("purge request succeeds");
    assert_eq!(delete_response.status(), StatusCode::ACCEPTED);

    assert_eq!(
        fantasma_worker::process_project_deletion_batch(&pool)
            .await
            .expect("project deletion worker should ignore stale lease blockers"),
        1
    );
}

fn benchmark_like_session_events(
    day: &str,
    day_offset: usize,
    install_count: usize,
) -> Vec<serde_json::Value> {
    let app_versions = ["1.0.0", "1.1.0", "1.2.0", "2.0.0"];
    let os_versions = ["18.3", "18.4", "18.5", "19.0"];
    let locales = ["en-US", "pt-PT", "fr-FR", "de-DE"];
    let mut events = Vec::with_capacity(install_count * 4);

    for install_index in 0..install_count {
        let app_version = app_versions[(day_offset + install_index) % app_versions.len()];
        let os_version = os_versions[(day_offset + install_index) % os_versions.len()];
        let locale = locales[(day_offset + install_index) % locales.len()];
        let platform = if install_index % 2 == 0 {
            "ios"
        } else {
            "android"
        };

        for (hour, minute) in [(0, 10), (0, 11), (0, 12), (0, 13)] {
            events.push(serde_json::json!({
                "event": "app_open",
                "timestamp": format!("{day}T{hour:02}:{minute:02}:00Z"),
                "install_id": format!("bench-install-{install_index:04}"),
                "platform": platform,
                "app_version": app_version,
                "os_version": os_version,
                "locale": locale,
            }));
        }
    }

    events
}

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body"),
    )
    .expect("decode response body")
}

fn sum_metric_series(series: &serde_json::Value) -> u64 {
    series
        .as_array()
        .expect("metrics series is an array")
        .iter()
        .map(|entry| {
            entry["points"]
                .as_array()
                .expect("series points is an array")
                .iter()
                .map(|point| point["value"].as_u64().expect("point value is u64"))
                .sum::<u64>()
        })
        .sum()
}

async fn session_count_response(
    api: axum::Router,
    read_key: &str,
    start: &str,
    end: &str,
) -> axum::response::Response {
    api.oneshot(
        Request::get(format!(
            "/v1/metrics/sessions?metric=count&granularity=day&start={start}&end={end}"
        ))
        .header("x-fantasma-key", read_key)
        .body(Body::empty())
        .expect("valid session request"),
    )
    .await
    .expect("session request succeeds")
}

#[derive(Debug, Clone)]
struct ProvisionedProject {
    project_id: String,
    ingest_key: String,
    read_key: String,
}

async fn provision_project(api: axum::Router) -> ProvisionedProject {
    let create_project_response = api
        .clone()
        .oneshot(
            Request::post("/v1/projects")
                .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "Pipeline Project",
                        "ingest_key_name": "ios-demo"
                    })
                    .to_string(),
                ))
                .expect("valid create-project request"),
        )
        .await
        .expect("create-project request succeeds");
    assert_eq!(create_project_response.status(), StatusCode::CREATED);

    let created_project = response_json(create_project_response).await;
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
    let read_key = created_read_key["key"]["secret"]
        .as_str()
        .expect("read key secret")
        .to_owned();

    ProvisionedProject {
        project_id,
        ingest_key,
        read_key,
    }
}

#[sqlx::test]
async fn pipeline_provisions_project_and_scopes_ingest_and_read_keys(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:10:00Z",
                                "install_id": "install-1",
                                "platform": "ios"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");

    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_session_batch(&pool, 100)
        .await
        .expect("worker batch succeeds");

    let read_metrics_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-02",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid read metrics request"),
        )
        .await
        .expect("read metrics request succeeds");

    assert_eq!(read_metrics_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(read_metrics_response).await,
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "group_by": [],
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 },
                        { "bucket": "2026-01-02", "value": 0 }
                    ]
                }
            ]
        })
    );

    let ingest_key_metrics_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-02",
            )
            .header("x-fantasma-key", &provisioned.ingest_key)
            .body(Body::empty())
            .expect("valid ingest-key metrics request"),
        )
        .await
        .expect("ingest-key metrics request succeeds");

    assert_eq!(
        ingest_key_metrics_response.status(),
        StatusCode::UNAUTHORIZED
    );

    let operator_metrics_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-02",
            )
            .header(AUTHORIZATION, "Bearer fg_pat_test_admin")
            .body(Body::empty())
            .expect("valid operator metrics request"),
        )
        .await
        .expect("operator metrics request succeeds");

    assert_eq!(operator_metrics_response.status(), StatusCode::UNAUTHORIZED);
}

#[sqlx::test]
async fn event_discovery_routes_return_catalog_and_top_events_for_real_ingest(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "install-pro-1",
                                "platform": "ios",
                                "app_version": "1.0.0",
                                "os_version": "18.3",
                                "locale": "en-US"
                            },
                            {
                                "event": "purchase_completed",
                                "timestamp": "2026-01-01T00:05:00.123Z",
                                "install_id": "install-pro-1",
                                "platform": "ios",
                                "app_version": "1.0.0",
                                "os_version": "18.3",
                                "locale": "en-US"
                            },
                            {
                                "event": "screen_view",
                                "timestamp": "2026-01-01T00:10:00Z",
                                "install_id": "install-free-1",
                                "platform": "ios",
                                "app_version": "1.0.1",
                                "os_version": "18.4",
                                "locale": "pt-PT"
                            },
                            {
                                "event": "screen_view",
                                "timestamp": "2026-01-01T00:15:00Z",
                                "install_id": "install-free-2",
                                "platform": "ios",
                                "app_version": "1.0.1",
                                "os_version": "18.4",
                                "locale": "pt-PT"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");
    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    let catalog_response = api
        .clone()
        .oneshot(
            Request::get("/v1/metrics/events/catalog?start=2026-01-01&end=2026-01-01")
                .header("x-fantasma-key", &provisioned.read_key)
                .body(Body::empty())
                .expect("valid catalog request"),
        )
        .await
        .expect("catalog request succeeds");
    assert_eq!(catalog_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(catalog_response).await,
        serde_json::json!({
            "events": [
                { "name": "screen_view", "last_seen_at": "2026-01-01T00:15:00Z" },
                { "name": "purchase_completed", "last_seen_at": "2026-01-01T00:05:00.123Z" },
                { "name": "app_open", "last_seen_at": "2026-01-01T00:00:00Z" }
            ]
        })
    );

    let top_response = api
        .clone()
        .oneshot(
            Request::get("/v1/metrics/events/top?start=2026-01-01&end=2026-01-01&limit=3")
                .header("x-fantasma-key", &provisioned.read_key)
                .body(Body::empty())
                .expect("valid top request"),
        )
        .await
        .expect("top request succeeds");
    assert_eq!(top_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(top_response).await,
        serde_json::json!({
            "events": [
                { "name": "screen_view", "count": 2 },
                { "name": "app_open", "count": 1 },
                { "name": "purchase_completed", "count": 1 }
            ]
        })
    );

    let filtered_top_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/events/top?start=2026-01-01&end=2026-01-01&locale=en-US&limit=3",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid filtered top request"),
        )
        .await
        .expect("filtered top request succeeds");
    assert_eq!(filtered_top_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(filtered_top_response).await,
        serde_json::json!({
            "events": [
                { "name": "app_open", "count": 1 },
                { "name": "purchase_completed", "count": 1 }
            ]
        })
    );
}

#[sqlx::test]
async fn event_catalog_uses_limit_as_reserved_parameter(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "trial_started",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "install-soft-limit",
                                "platform": "ios",
                                "locale": "en-US"
                            },
                            {
                                "event": "trial_blocked",
                                "timestamp": "2026-01-01T00:05:00Z",
                                "install_id": "install-hard-limit",
                                "platform": "ios",
                                "locale": "en-US"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");
    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    let catalog_response = api
        .oneshot(
            Request::get("/v1/metrics/events/catalog?start=2026-01-01&end=2026-01-01&limit=1")
                .header("x-fantasma-key", &provisioned.read_key)
                .body(Body::empty())
                .expect("valid catalog request"),
        )
        .await
        .expect("catalog request succeeds");
    assert_eq!(catalog_response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_json(catalog_response).await,
        serde_json::json!({ "error": "invalid_query_key" })
    );
}

#[sqlx::test]
async fn pipeline_exposes_bucketed_session_metrics_after_worker_batch(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:10:00Z",
                                "install_id": "install-1",
                                "platform": "ios"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");

    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_session_batch(&pool, 100)
        .await
        .expect("worker batch succeeds");

    let count_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-02",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("api request succeeds");

    assert_eq!(count_response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(count_response.into_body(), usize::MAX)
                .await
                .expect("read response body"),
        )
        .expect("decode response"),
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "group_by": [],
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 },
                        { "bucket": "2026-01-02", "value": 0 }
                    ]
                }
            ]
        })
    );

    let duration_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=duration_total&granularity=day&start=2026-01-01&end=2026-01-02",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("api request succeeds");

    assert_eq!(duration_response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(duration_response.into_body(), usize::MAX)
                .await
                .expect("read response body"),
        )
        .expect("decode response"),
        serde_json::json!({
            "metric": "duration_total",
            "granularity": "day",
            "group_by": [],
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-01-01", "value": 600 },
                        { "bucket": "2026-01-02", "value": 0 }
                    ]
                }
            ]
        })
    );

    let new_installs_response = api.clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=new_installs&granularity=day&start=2026-01-01&end=2026-01-02",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("api request succeeds");

    assert_eq!(new_installs_response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(new_installs_response.into_body(), usize::MAX)
                .await
                .expect("read response body"),
        )
        .expect("decode response"),
        serde_json::json!({
            "metric": "new_installs",
            "granularity": "day",
            "group_by": [],
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 },
                        { "bucket": "2026-01-02", "value": 0 }
                    ]
                }
            ]
        })
    );

    let active_installs_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=active_installs&start=2026-01-01&end=2026-01-02",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("api request succeeds");

    assert_eq!(active_installs_response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(active_installs_response.into_body(), usize::MAX)
                .await
                .expect("read response body"),
        )
        .expect("decode response"),
        serde_json::json!({
            "metric": "active_installs",
            "start": "2026-01-01",
            "end": "2026-01-02",
            "group_by": [],
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "start": "2026-01-01", "end": "2026-01-02", "value": 1 }
                    ]
                }
            ]
        })
    );
}

#[sqlx::test]
async fn pipeline_exposes_event_metrics_after_worker_batch(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-03-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.4.0",
                                "os_version": "18.3",
                                "locale": "en-US"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-03-01T01:00:00Z",
                                "install_id": "install-2",
                                "platform": "ios",
                                "app_version": "1.4.0",
                                "os_version": "18.3",
                                "locale": "pt-PT"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");

    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_event_metrics_batch(&pool, 100)
        .await
        .expect("event metrics worker batch succeeds");

    let hourly_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/events?event=app_open&metric=count&granularity=hour&start=2026-03-01T00:00:00Z&end=2026-03-01T01:00:00Z&platform=ios&app_version=1.4.0&os_version=18.3&locale=en-US",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("hourly request succeeds");

    assert_eq!(hourly_response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(hourly_response.into_body(), usize::MAX)
                .await
                .expect("read response body"),
        )
        .expect("decode response"),
        serde_json::json!({
            "metric": "count",
            "granularity": "hour",
            "group_by": [],
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-03-01T00:00:00Z", "value": 1 },
                        { "bucket": "2026-03-01T01:00:00Z", "value": 0 }
                    ]
                }
            ]
        })
    );

    let daily_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&platform=ios&app_version=1.4.0&os_version=18.3&locale=pt-PT",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("daily request succeeds");

    assert_eq!(daily_response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(daily_response.into_body(), usize::MAX)
                .await
                .expect("read response body"),
        )
        .expect("decode response"),
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "group_by": [],
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-03-01", "value": 1 },
                        { "bucket": "2026-03-02", "value": 0 }
                    ]
                }
            ]
        })
    );
}

#[sqlx::test]
async fn pipeline_exposes_live_installs_after_event_worker_batch(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "live-install-1",
                                "platform": "ios"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "live-install-2",
                                "platform": "ios"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");
    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_event_metrics_batch(&pool, 100)
        .await
        .expect("event worker batch succeeds");

    let live_installs_response = api
        .oneshot(
            Request::get("/v1/metrics/live_installs")
                .header("x-fantasma-key", &provisioned.read_key)
                .body(Body::empty())
                .expect("valid live installs request"),
        )
        .await
        .expect("live installs request succeeds");
    assert_eq!(live_installs_response.status(), StatusCode::OK);

    let body = response_json(live_installs_response).await;
    assert_eq!(body["metric"], serde_json::json!("live_installs"));
    assert_eq!(body["window_seconds"], serde_json::json!(120));
    assert_eq!(body["value"], serde_json::json!(2));
    assert!(body["as_of"].is_string());
}

#[sqlx::test]
async fn pipeline_exposes_dim2_event_metrics_with_null_buckets(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-03-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.4.0",
                                "os_version": "18.3",
                                "locale": "en-US"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-03-01T01:00:00Z",
                                "install_id": "install-2",
                                "platform": "ios",
                                "app_version": "1.4.0",
                                "os_version": "18.3",
                                "locale": "pt-PT"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");

    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_event_metrics_batch(&pool, 100)
        .await
        .expect("event metrics worker batch succeeds");

    let response = api
        .oneshot(
            Request::get(
                "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&platform=ios&app_version=1.4.0&os_version=18.3&locale=en-US",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("daily request succeeds");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "group_by": [],
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-03-01", "value": 1 },
                        { "bucket": "2026-03-02", "value": 0 }
                    ]
                }
            ]
        })
    );
}

#[sqlx::test]
async fn pipeline_keeps_active_installs_visible_while_session_backlog_remains(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-03-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.4.0",
                                "os_version": "18.3"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-03-01T01:00:00Z",
                                "install_id": "install-2",
                                "platform": "ios",
                                "app_version": "1.4.0",
                                "os_version": "18.3"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");

    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    let processed = fantasma_worker::process_session_batch(&pool, 1)
        .await
        .expect("session worker batch succeeds");
    assert_eq!(processed, 1, "test needs a remaining session backlog");

    let count_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-03-01&end=2026-03-01",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid count request"),
        )
        .await
        .expect("count request succeeds");

    assert_eq!(count_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(count_response).await,
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "group_by": [],
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-03-01", "value": 1 }
                    ]
                }
            ]
        })
    );

    let active_installs_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=active_installs&start=2026-03-01&end=2026-03-01",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid active installs request"),
        )
        .await
        .expect("active installs request succeeds");

    assert_eq!(active_installs_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(active_installs_response).await,
        serde_json::json!({
            "metric": "active_installs",
            "start": "2026-03-01",
            "end": "2026-03-01",
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "start": "2026-03-01", "end": "2026-03-01", "value": 1 }
                    ]
                }
            ]
        }),
        "exact-range active_installs should reflect committed session batches even while later raw events remain queued"
    );

    let remaining = fantasma_worker::process_session_batch(&pool, 1)
        .await
        .expect("second session worker batch succeeds");
    assert_eq!(
        remaining, 1,
        "the query should have run while a later raw session batch was still pending"
    );
}

#[sqlx::test]
async fn pipeline_rejects_active_installs_invalid_combinations(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    for path in [
        "/v1/metrics/sessions?metric=active_installs&granularity=day&start=2026-01-01&end=2026-01-01",
        "/v1/metrics/sessions?metric=count&granularity=day&interval=day&start=2026-01-01&end=2026-01-01",
        "/v1/metrics/sessions?metric=active_installs&start=2026-01-01&end=2026-05-20&interval=day",
    ] {
        let response = api
            .clone()
            .oneshot(
                Request::get(path)
                    .header("x-fantasma-key", &provisioned.read_key)
                    .body(Body::empty())
                    .expect("valid api request"),
            )
            .await
            .expect("api request succeeds");

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            response_json(response).await,
            serde_json::json!({
                "error": "invalid_request"
            })
        );
    }
}

#[sqlx::test]
async fn pipeline_exposes_weekly_active_installs_with_zero_fill(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-03-02T10:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.1.0",
                                "os_version": "18.3",
                                "locale": "en-US"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-03-03T12:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.1.0",
                                "os_version": "18.3",
                                "locale": "en-US"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-03-10T09:00:00Z",
                                "install_id": "install-2",
                                "platform": "ios",
                                "app_version": "1.1.0",
                                "os_version": "18.3",
                                "locale": "en-US"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");

    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_session_batch(&pool, 100)
        .await
        .expect("worker batch succeeds");

    let response = api
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=active_installs&start=2026-03-01&end=2026-03-17&interval=week",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("api request succeeds");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({
            "metric": "active_installs",
            "start": "2026-03-01",
            "end": "2026-03-17",
            "interval": "week",
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "start": "2026-03-01", "end": "2026-03-01", "value": 0 },
                        { "start": "2026-03-02", "end": "2026-03-08", "value": 1 },
                        { "start": "2026-03-09", "end": "2026-03-15", "value": 1 },
                        { "start": "2026-03-16", "end": "2026-03-17", "value": 0 }
                    ]
                }
            ]
        })
    );
}

#[sqlx::test]
async fn pipeline_exposes_dim2_session_metrics_and_active_install_overlaps(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.1.0",
                                "os_version": "18.3",
                                "locale": "en-US"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:10:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.1.0",
                                "os_version": "18.4",
                                "locale": "en-US"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T01:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.1.0",
                                "os_version": "18.4",
                                "locale": "en-US"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T01:10:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.1.0",
                                "os_version": "18.4",
                                "locale": "en-US"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T02:00:00Z",
                                "install_id": "install-2",
                                "platform": "ios",
                                "app_version": "1.1.0",
                                "os_version": "18.3",
                                "locale": "en-US"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T02:10:00Z",
                                "install_id": "install-2",
                                "platform": "ios",
                                "app_version": "1.1.0",
                                "os_version": "18.3",
                                "locale": "en-US"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");

    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_session_batch(&pool, 100)
        .await
        .expect("worker batch succeeds");

    let filtered_count_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-01&platform=ios&locale=en-US",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("filtered count request succeeds");

    assert_eq!(filtered_count_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(filtered_count_response).await,
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "group_by": [],
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-01-01", "value": 3 }
                    ]
                }
            ]
        })
    );

    let filtered_active_installs_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=active_installs&start=2026-01-01&end=2026-01-01&platform=ios&locale=en-US",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("filtered active installs request succeeds");

    assert_eq!(filtered_active_installs_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(filtered_active_installs_response).await,
        serde_json::json!({
            "metric": "active_installs",
            "start": "2026-01-01",
            "end": "2026-01-01",
            "group_by": [],
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "start": "2026-01-01", "end": "2026-01-01", "value": 2 }
                    ]
                }
            ]
        })
    );

    let grouped_active_installs_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=active_installs&start=2026-01-01&end=2026-01-01&group_by=platform&group_by=locale",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("grouped active installs request succeeds");

    assert_eq!(grouped_active_installs_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(grouped_active_installs_response).await,
        serde_json::json!({
            "metric": "active_installs",
            "start": "2026-01-01",
            "end": "2026-01-01",
            "group_by": ["platform", "locale"],
            "series": [
                {
                    "dimensions": {
                        "platform": "ios",
                        "locale": "en-US"
                    },
                    "points": [
                        { "start": "2026-01-01", "end": "2026-01-01", "value": 2 }
                    ]
                }
            ]
        })
    );

    let invalid_grouped_active_installs_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=active_installs&start=2026-01-01&end=2026-01-01&platform=ios&group_by=app_version&group_by=locale",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("invalid grouped active installs request succeeds");

    assert_eq!(
        invalid_grouped_active_installs_response.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        response_json(invalid_grouped_active_installs_response).await,
        serde_json::json!({
            "error": "too_many_dimensions"
        })
    );
}

#[sqlx::test]
async fn pipeline_rejects_event_metrics_queries_that_exceed_group_limit(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;
    let events = (0..101)
        .map(|index| {
            serde_json::json!({
                "event": "app_open",
                "timestamp": "2026-03-01T00:00:00Z",
                "install_id": format!("install-{index}"),
                "platform": "ios",
                "app_version": format!("1.4.{index}"),
                "os_version": "18.3",
                "locale": "en-US"
            })
        })
        .collect::<Vec<_>>();

    let first_ingest_response = ingest
        .clone()
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({ "events": events[..100].to_vec() }).to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("first ingest request succeeds");
    let second_ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({ "events": events[100..].to_vec() }).to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("second ingest request succeeds");

    assert_eq!(first_ingest_response.status(), StatusCode::ACCEPTED);
    assert_eq!(second_ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_event_metrics_batch(&pool, 200)
        .await
        .expect("event metrics worker batch succeeds");

    let response = api
        .oneshot(
            Request::get(
                "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-01&group_by=app_version",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("aggregate request succeeds");

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("read response body"),
        )
        .expect("decode response"),
        serde_json::json!({
            "error": "group_limit_exceeded"
        })
    );
}

#[sqlx::test]
async fn pipeline_exposes_grouped_event_metrics_up_to_four_built_in_dimensions(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.0.0",
                                "os_version": "18.3",
                                "locale": "en-US"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T01:00:00Z",
                                "install_id": "install-2",
                                "platform": "ios",
                                "app_version": "1.0.0",
                                "os_version": "18.4",
                                "locale": "pt-PT"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T02:00:00Z",
                                "install_id": "install-3",
                                "platform": "android",
                                "app_version": "2.0.0",
                                "os_version": "15",
                                "locale": "en-US"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");

    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_event_metrics_batch(&pool, 100)
        .await
        .expect("event worker batch succeeds");

    let response = api
        .oneshot(
            Request::get(
                "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-01-01&end=2026-01-01&group_by=platform&group_by=app_version&group_by=os_version&group_by=locale",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("grouped event request succeeds");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "group_by": ["platform", "app_version", "os_version", "locale"],
            "series": [
                {
                    "dimensions": {
                        "platform": "android",
                        "app_version": "2.0.0",
                        "os_version": "15",
                        "locale": "en-US"
                    },
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 }
                    ]
                },
                {
                    "dimensions": {
                        "platform": "ios",
                        "app_version": "1.0.0",
                        "os_version": "18.3",
                        "locale": "en-US"
                    },
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 }
                    ]
                },
                {
                    "dimensions": {
                        "platform": "ios",
                        "app_version": "1.0.0",
                        "os_version": "18.4",
                        "locale": "pt-PT"
                    },
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 }
                    ]
                }
            ]
        })
    );
}

#[sqlx::test]
async fn pipeline_rejects_event_metrics_queries_that_exceed_d2_dimension_budget(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;
    let events = (0..101)
        .map(|index| {
            serde_json::json!({
                "event": "app_open",
                "timestamp": "2026-03-01T00:00:00Z",
                "install_id": format!("install-{index}"),
                "platform": "ios",
                "app_version": "1.4.0",
                "os_version": "18.3",
                "locale": "en-US"
            })
        })
        .collect::<Vec<_>>();

    let first_ingest_response = ingest
        .clone()
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({ "events": events[..100].to_vec() }).to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("first ingest request succeeds");
    let second_ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({ "events": events[100..].to_vec() }).to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("second ingest request succeeds");

    assert_eq!(first_ingest_response.status(), StatusCode::ACCEPTED);
    assert_eq!(second_ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_event_metrics_batch(&pool, 200)
        .await
        .expect("event metrics worker batch succeeds");

    let response = api
        .oneshot(
            Request::get(
                "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-01&platform=ios&app_version=1.4.0&os_version=18.3&locale=en-US",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("aggregate request succeeds");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-03-01", "value": 101 }
                    ]
                }
            ]
        })
    );
}

#[sqlx::test]
async fn pipeline_keeps_grouped_event_metrics_day_queries_200_during_worker_catchup(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;
    let events = scale_like_events("2026-04-01", 120);

    for batch in events.chunks(100) {
        let ingest_response = ingest
            .clone()
            .oneshot(
                Request::post("/v1/events")
                    .header(CONTENT_TYPE, "application/json")
                    .header("x-fantasma-key", &provisioned.ingest_key)
                    .body(Body::from(
                        serde_json::json!({ "events": batch.to_vec() }).to_string(),
                    ))
                    .expect("valid ingest request"),
            )
            .await
            .expect("ingest request succeeds");

        assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);
    }

    let worker_pool = pool.clone();
    let worker = tokio::spawn(async move {
        loop {
            let processed = fantasma_worker::process_event_metrics_batch(&worker_pool, 1)
                .await
                .expect("event metrics worker batch succeeds");
            if processed == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    let query = "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-04-01&end=2026-04-01&platform=ios&locale=en-US";
    let mut saw_internal_server_error = false;
    let mut query_count = 0;

    while !worker.is_finished() {
        let response = api
            .clone()
            .oneshot(
                Request::get(query)
                    .header("x-fantasma-key", &provisioned.read_key)
                    .body(Body::empty())
                    .expect("valid api request"),
            )
            .await
            .expect("daily query succeeds");
        query_count += 1;

        if response.status() == StatusCode::INTERNAL_SERVER_ERROR {
            saw_internal_server_error = true;
            break;
        }

        tokio::task::yield_now().await;
    }

    worker.await.expect("worker task joins");

    assert!(
        query_count > 0,
        "the regression must issue at least one metrics request during worker catch-up"
    );
    assert!(
        !saw_internal_server_error,
        "daily event metrics queries should never return internal_server_error while the worker commits bounded batches"
    );
}

#[sqlx::test]
async fn pipeline_returns_grouped_session_platform_app_version_counts(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    for day in ["2026-01-01", "2026-01-02", "2026-01-03"] {
        let events = scale_like_events(day, 120);
        for batch in events.chunks(100) {
            let ingest_response = ingest
                .clone()
                .oneshot(
                    Request::post("/v1/events")
                        .header(CONTENT_TYPE, "application/json")
                        .header("x-fantasma-key", &provisioned.ingest_key)
                        .body(Body::from(
                            serde_json::json!({ "events": batch.to_vec() }).to_string(),
                        ))
                        .expect("valid ingest request"),
                )
                .await
                .expect("ingest request succeeds");

            assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);
        }
    }

    loop {
        let event_processed = fantasma_worker::process_event_metrics_batch(&pool, 200)
            .await
            .expect("event worker batch succeeds");
        let session_processed = fantasma_worker::process_session_batch(&pool, 200)
            .await
            .expect("session worker batch succeeds");
        if event_processed == 0 && session_processed == 0 {
            break;
        }
    }

    let response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-03",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("session request succeeds");

    assert_eq!(response.status(), StatusCode::OK);
}

#[sqlx::test]
async fn pipeline_returns_grouped_session_platform_app_version_counts_for_benchmark_like_history(
    pool: PgPool,
) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    for day_offset in 0..30 {
        let day = format!("2026-01-{:02}", day_offset + 1);
        let events = benchmark_like_session_events(&day, day_offset, 120);
        for batch in events.chunks(100) {
            let ingest_response = ingest
                .clone()
                .oneshot(
                    Request::post("/v1/events")
                        .header(CONTENT_TYPE, "application/json")
                        .header("x-fantasma-key", &provisioned.ingest_key)
                        .body(Body::from(
                            serde_json::json!({ "events": batch.to_vec() }).to_string(),
                        ))
                        .expect("valid ingest request"),
                )
                .await
                .expect("ingest request succeeds");

            assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);
        }
    }

    loop {
        let event_processed = fantasma_worker::process_event_metrics_batch(&pool, 1000)
            .await
            .expect("event worker batch succeeds");
        let session_processed = fantasma_worker::process_session_batch(&pool, 1000)
            .await
            .expect("session worker batch succeeds");
        if event_processed == 0 && session_processed == 0 {
            break;
        }
    }

    let response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-30",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("session request succeeds");

    assert_eq!(response.status(), StatusCode::OK);
}

#[sqlx::test]
async fn pipeline_keeps_grouped_session_metrics_day_queries_200_during_worker_catchup(
    pool: PgPool,
) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    for day_offset in 0..10 {
        let day = format!("2026-01-{:02}", day_offset + 1);
        let events = benchmark_like_session_events(&day, day_offset, 120);
        for batch in events.chunks(100) {
            let ingest_response = ingest
                .clone()
                .oneshot(
                    Request::post("/v1/events")
                        .header(CONTENT_TYPE, "application/json")
                        .header("x-fantasma-key", &provisioned.ingest_key)
                        .body(Body::from(
                            serde_json::json!({ "events": batch.to_vec() }).to_string(),
                        ))
                        .expect("valid ingest request"),
                )
                .await
                .expect("ingest request succeeds");

            assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);
        }
    }

    let worker_pool = pool.clone();
    let worker = tokio::spawn(async move {
        loop {
            let processed = fantasma_worker::process_session_batch(&worker_pool, 1)
                .await
                .expect("session worker batch succeeds");
            if processed == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    let query = "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-10";
    let mut saw_internal_server_error = false;
    let mut query_count = 0;

    while !worker.is_finished() {
        let response = api
            .clone()
            .oneshot(
                Request::get(query)
                    .header("x-fantasma-key", &provisioned.read_key)
                    .body(Body::empty())
                    .expect("valid api request"),
            )
            .await
            .expect("session query succeeds");
        query_count += 1;

        if response.status() == StatusCode::INTERNAL_SERVER_ERROR {
            saw_internal_server_error = true;
            break;
        }

        tokio::task::yield_now().await;
    }

    worker.await.expect("worker task joins");

    assert!(
        query_count > 0,
        "the regression must issue at least one session query during worker catch-up"
    );
    assert!(
        !saw_internal_server_error,
        "daily session metrics queries should never return internal_server_error while the session worker commits bounded batches"
    );
}

#[sqlx::test]
async fn pipeline_readiness_poll_for_grouped_session_counts_stays_200_and_converges(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;
    // The adjacent catch-up regression already proves session queries stay 200 while the
    // worker advances bounded batches. Keep this readiness-convergence case smaller so it remains
    // inside the CI timeout budget under workspace-level contention.
    let days = 4_u64;
    let installs_per_day = 24_u64;
    let expected_total = days * installs_per_day;

    for day_offset in 0..days as usize {
        let day = format!("2026-01-{:02}", day_offset + 1);
        let events = benchmark_like_session_events(&day, day_offset, installs_per_day as usize);
        for batch in events.chunks(100) {
            let ingest_response = ingest
                .clone()
                .oneshot(
                    Request::post("/v1/events")
                        .header(CONTENT_TYPE, "application/json")
                        .header("x-fantasma-key", &provisioned.ingest_key)
                        .body(Body::from(
                            serde_json::json!({ "events": batch.to_vec() }).to_string(),
                        ))
                        .expect("valid ingest request"),
                )
                .await
                .expect("ingest request succeeds");

            assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);
        }
    }

    let worker_pool = pool.clone();
    let worker = tokio::spawn(async move {
        loop {
            let processed = fantasma_worker::process_session_batch(&worker_pool, 1)
                .await
                .expect("session worker batch succeeds");
            if processed == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });

    let mut saw_internal_server_error = false;
    let mut saw_expected_total = false;
    let poll_completed = tokio::time::timeout(Duration::from_secs(300), async {
        loop {
            let response = session_count_response(
                api.clone(),
                &provisioned.read_key,
                "2026-01-01",
                "2026-01-10",
            )
            .await;

            if response.status() == StatusCode::INTERNAL_SERVER_ERROR {
                saw_internal_server_error = true;
                break;
            }

            assert_eq!(response.status(), StatusCode::OK);
            let body = response_json(response).await;
            if sum_metric_series(&body["series"]) == expected_total {
                saw_expected_total = true;
                break;
            }

            if worker.is_finished() {
                let final_poll = session_count_response(
                    api.clone(),
                    &provisioned.read_key,
                    "2026-01-01",
                    "2026-01-10",
                )
                .await;
                assert_eq!(final_poll.status(), StatusCode::OK);
                let final_poll_body = response_json(final_poll).await;
                if sum_metric_series(&final_poll_body["series"]) == expected_total {
                    saw_expected_total = true;
                }
                break;
            }

            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert!(
        poll_completed.is_ok(),
        "readiness polling should converge before the bounded worker timeout"
    );

    worker.await.expect("worker task joins");

    let final_response = session_count_response(
        api.clone(),
        &provisioned.read_key,
        "2026-01-01",
        "2026-01-10",
    )
    .await;
    assert_eq!(final_response.status(), StatusCode::OK);
    let final_body = response_json(final_response).await;

    assert!(
        !saw_internal_server_error,
        "readiness polling must never surface internal_server_error for session queries"
    );
    assert_eq!(sum_metric_series(&final_body["series"]), expected_total);
    assert!(
        saw_expected_total,
        "readiness polling must observe the expected session total"
    );
}

#[sqlx::test]
async fn pipeline_exposes_grouped_session_metrics_and_rejects_unsupported_dimensions(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:10:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.0.0"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T01:00:00Z",
                                "install_id": "install-2",
                                "platform": "ios",
                                "app_version": "2.0.0"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("ingest request succeeds");

    assert_eq!(ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_session_batch(&pool, 100)
        .await
        .expect("worker batch succeeds");

    let grouped_count_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-01&app_version=2.0.0",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("filtered count request succeeds");

    assert_eq!(grouped_count_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(grouped_count_response).await,
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 }
                    ]
                }
            ]
        })
    );

    let grouped_new_installs_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=new_installs&granularity=day&start=2026-01-01&end=2026-01-01&app_version=2.0.0",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("filtered new installs request succeeds");

    assert_eq!(grouped_new_installs_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(grouped_new_installs_response).await,
        serde_json::json!({
            "metric": "new_installs",
            "granularity": "day",
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 }
                    ]
                }
            ]
        })
    );

    let unsupported_dimension_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-01&plan-name=pro",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("unsupported dimension request succeeds");

    assert_eq!(
        unsupported_dimension_response.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[sqlx::test]
async fn pipeline_keeps_new_installs_fixed_on_first_seen_bucket_after_late_events(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let first_ingest_response = ingest
        .clone()
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T01:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.0.0"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("first ingest request succeeds");

    assert_eq!(first_ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_session_batch(&pool, 100)
        .await
        .expect("first worker batch succeeds");

    let second_ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "0.9.0"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("late ingest request succeeds");

    assert_eq!(second_ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_session_batch(&pool, 100)
        .await
        .expect("late worker batch succeeds");

    let hourly_new_installs_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=new_installs&granularity=hour&start=2026-01-01T00:00:00Z&end=2026-01-01T01:00:00Z",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("hourly new installs request succeeds");

    assert_eq!(hourly_new_installs_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(hourly_new_installs_response).await,
        serde_json::json!({
            "metric": "new_installs",
            "granularity": "hour",
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-01-01T00:00:00Z", "value": 0 },
                        { "bucket": "2026-01-01T01:00:00Z", "value": 1 }
                    ]
                }
            ]
        })
    );

    let grouped_new_installs_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=new_installs&granularity=day&start=2026-01-01&end=2026-01-01&app_version=1.0.0",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("filtered new installs request succeeds");

    assert_eq!(grouped_new_installs_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(grouped_new_installs_response).await,
        serde_json::json!({
            "metric": "new_installs",
            "granularity": "day",
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 }
                    ]
                }
            ]
        })
    );
}

#[sqlx::test]
async fn pipeline_moves_grouped_session_app_version_after_late_event_repair(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_test_admin")),
    );
    let provisioned = provision_project(api.clone()).await;

    let first_ingest_response = ingest
        .clone()
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T01:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios"
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T01:10:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.0.0"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("first ingest request succeeds");

    assert_eq!(first_ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_session_batch(&pool, 100)
        .await
        .expect("first worker batch succeeds");

    let late_ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", &provisioned.ingest_key)
                .body(Body::from(
                    serde_json::json!({
                        "events": [
                            {
                                "event": "app_open",
                                "timestamp": "2026-01-01T00:50:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "0.9.0"
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("valid ingest request"),
        )
        .await
        .expect("late ingest request succeeds");

    assert_eq!(late_ingest_response.status(), StatusCode::ACCEPTED);

    fantasma_worker::process_session_batch(&pool, 100)
        .await
        .expect("late worker batch succeeds");

    let grouped_count_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-01&app_version=0.9.0",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("filtered count request succeeds");

    assert_eq!(grouped_count_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(grouped_count_response).await,
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 }
                    ]
                }
            ]
        })
    );

    let grouped_duration_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/sessions?metric=duration_total&granularity=day&start=2026-01-01&end=2026-01-01&app_version=0.9.0",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("filtered duration request succeeds");

    assert_eq!(grouped_duration_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(grouped_duration_response).await,
        serde_json::json!({
            "metric": "duration_total",
            "granularity": "day",
            "series": [
                {
                    "dimensions": {},
                    "points": [
                        { "bucket": "2026-01-01", "value": 20 * 60 }
                    ]
                }
            ]
        })
    );
}
