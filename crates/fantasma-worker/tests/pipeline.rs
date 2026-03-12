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

fn scale_like_events(day: &str, install_count: usize) -> Vec<serde_json::Value> {
    let providers = ["strava", "garmin", "polar", "oura"];
    let regions = ["eu", "us", "apac", "latam"];
    let plans = ["free", "pro", "team"];
    let app_versions = ["1.0.0", "1.1.0", "1.2.0"];
    let os_versions = ["18.3", "18.4"];
    let mut events = Vec::with_capacity(install_count * 3);

    for install_index in 0..install_count {
        let provider = providers[install_index % providers.len()];
        let region = regions[(install_index / providers.len()) % regions.len()];
        let plan = plans[install_index % plans.len()];
        let app_version = app_versions[install_index % app_versions.len()];
        let os_version = os_versions[install_index % os_versions.len()];

        for timestamp in ["00:00:00Z", "00:10:00Z", "00:20:00Z"] {
            events.push(serde_json::json!({
                "event": "app_open",
                "timestamp": format!("{day}T{timestamp}"),
                "install_id": format!("scale-{day}-{install_index}"),
                "platform": "ios",
                "app_version": app_version,
                "os_version": os_version,
                "properties": {
                    "plan": plan,
                    "provider": provider,
                    "region": region
                }
            }));
        }
    }

    events
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

#[sqlx::test]
async fn pipeline_exposes_event_metrics_after_worker_batch(pool: PgPool) {
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
                                "timestamp": "2026-03-01T00:00:00Z",
                                "install_id": "install-1",
                                "platform": "ios",
                                "app_version": "1.4.0",
                                "os_version": "18.3",
                                "properties": {
                                    "provider": "strava",
                                    "is_paying": "true"
                                }
                            },
                            {
                                "event": "app_open",
                                "timestamp": "2026-03-01T01:00:00Z",
                                "install_id": "install-2",
                                "platform": "ios",
                                "app_version": "1.4.0",
                                "os_version": "18.3",
                                "properties": {
                                    "provider": "strava"
                                }
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

    let aggregate_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/events/aggregate?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-03-01&end_date=2026-03-01&platform=ios&group_by=provider",
            )
            .header(AUTHORIZATION, "Bearer fg_pat_dev")
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("aggregate request succeeds");

    assert_eq!(aggregate_response.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(aggregate_response.into_body(), usize::MAX)
                .await
                .expect("read response body"),
        )
        .expect("decode response"),
        serde_json::json!({
            "metric": "event_count",
            "group_by": ["provider"],
            "rows": [
                {
                    "dimensions": {
                        "provider": "strava"
                    },
                    "value": 2
                }
            ]
        })
    );

    let daily_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/events/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-03-01&end_date=2026-03-02&os_version=18.3&group_by=provider&group_by=is_paying",
            )
            .header(AUTHORIZATION, "Bearer fg_pat_dev")
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
            "metric": "event_count_daily",
            "group_by": ["provider", "is_paying"],
            "series": [
                {
                    "dimensions": {
                        "provider": "strava",
                        "is_paying": "true"
                    },
                    "points": [
                        { "date": "2026-03-01", "value": 1 },
                        { "date": "2026-03-02", "value": 0 }
                    ]
                },
                {
                    "dimensions": {
                        "provider": "strava",
                        "is_paying": null
                    },
                    "points": [
                        { "date": "2026-03-01", "value": 1 },
                        { "date": "2026-03-02", "value": 0 }
                    ]
                }
            ]
        })
    );
}

#[sqlx::test]
async fn pipeline_rejects_event_metrics_queries_that_exceed_group_limit(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    ensure_local_project(&pool, Some(&bootstrap_config()))
        .await
        .expect("seed local project");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_dev")),
    );
    let events = (0..101)
        .map(|index| {
            serde_json::json!({
                "event": "app_open",
                "timestamp": "2026-03-01T00:00:00Z",
                "install_id": format!("install-{index}"),
                "platform": "ios",
                "app_version": "1.4.0",
                "os_version": "18.3",
                "properties": {
                    "provider": format!("provider-{index:03}")
                }
            })
        })
        .collect::<Vec<_>>();

    let first_ingest_response = ingest
        .clone()
        .oneshot(
            Request::post("/v1/events")
                .header(CONTENT_TYPE, "application/json")
                .header("x-fantasma-key", "fg_ing_test")
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
                .header("x-fantasma-key", "fg_ing_test")
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
                "/v1/metrics/events/aggregate?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-03-01&end_date=2026-03-01&group_by=provider",
            )
            .header(AUTHORIZATION, "Bearer fg_pat_dev")
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
async fn pipeline_keeps_grouped_event_metrics_daily_queries_200_during_worker_catchup(pool: PgPool) {
    run_migrations(&pool).await.expect("migrations succeed");
    ensure_local_project(&pool, Some(&bootstrap_config()))
        .await
        .expect("seed local project");

    let ingest = fantasma_ingest::app(pool.clone());
    let api = fantasma_api::app(
        pool.clone(),
        Arc::new(StaticAdminAuthorizer::new("fg_pat_dev")),
    );
    let events = scale_like_events("2026-04-01", 120);

    for batch in events.chunks(100) {
        let ingest_response = ingest
            .clone()
            .oneshot(
                Request::post("/v1/events")
                    .header(CONTENT_TYPE, "application/json")
                    .header("x-fantasma-key", "fg_ing_test")
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
            tokio::task::yield_now().await;
        }
    });

    let query = "/v1/metrics/events/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-04-01&end_date=2026-04-01&platform=ios&group_by=provider&group_by=region";
    let mut saw_internal_server_error = false;

    while !worker.is_finished() {
        let response = api
            .clone()
            .oneshot(
                Request::get(query)
                    .header(AUTHORIZATION, "Bearer fg_pat_dev")
                    .body(Body::empty())
                    .expect("valid api request"),
            )
            .await
            .expect("daily query succeeds");

        if response.status() == StatusCode::INTERNAL_SERVER_ERROR {
            saw_internal_server_error = true;
            break;
        }

        tokio::task::yield_now().await;
    }

    worker.await.expect("worker task joins");

    assert!(
        !saw_internal_server_error,
        "grouped daily event metrics queries should never return internal_server_error while the worker commits bounded batches"
    );
}
