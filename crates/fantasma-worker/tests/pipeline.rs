use std::sync::Arc;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use fantasma_auth::StaticAdminAuthorizer;
use fantasma_store::{BootstrapConfig, PgPool, ensure_local_project, run_migrations};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use tower::ServiceExt;
use uuid::Uuid;

fn project_id() -> Uuid {
    Uuid::from_u128(0x9bad8b88_5e7a_44ed_98ce_4cf9ddde713a)
}

fn bootstrap_config() -> BootstrapConfig {
    BootstrapConfig {
        project_id: project_id(),
        project_name: "Pipeline Test Project".to_owned(),
        ingest_key: Some("fg_ing_test".to_owned()),
    }
}

#[sqlx::test]
async fn pipeline_exposes_daily_metrics_after_worker_batch(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    ensure_local_project(&pool, Some(&bootstrap_config()))
        .await
        .expect("seed local project");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_dev")),
    );

    let ingest_response = ingest
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", "fg_ing_test")
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
                "/v1/metrics/sessions/count/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start_date=2026-01-01&end_date=2026-01-02",
            )
            .header(AUTHORIZATION, "Bearer fg_pat_dev")
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
            "metric": "sessions_count_daily",
            "points": [
                { "date": "2026-01-01", "value": 1 },
                { "date": "2026-01-02", "value": 0 }
            ]
        })
    );

    let duration_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/sessions/duration/total/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start_date=2026-01-01&end_date=2026-01-02",
            )
            .header(AUTHORIZATION, "Bearer fg_pat_dev")
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
            "metric": "session_duration_total_daily",
            "points": [
                { "date": "2026-01-01", "value": 600 },
                { "date": "2026-01-02", "value": 0 }
            ]
        })
    );

    let active_installs_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/active-installs/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start_date=2026-01-01&end_date=2026-01-02",
            )
            .header(AUTHORIZATION, "Bearer fg_pat_dev")
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("api request succeeds");

    assert_eq!(active_installs_response.status(), StatusCode::NOT_FOUND);
}
