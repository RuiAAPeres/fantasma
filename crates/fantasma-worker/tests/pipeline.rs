use std::{sync::Arc, time::Duration};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use fantasma_auth::StaticAdminAuthorizer;
use fantasma_store::{PgPool, run_migrations};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use tower::ServiceExt;

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

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body"),
    )
    .expect("decode response body")
}

#[derive(Debug, Clone)]
struct ProvisionedProject {
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

    let new_installs_response = api
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

    let hourly_response = api
        .clone()
        .oneshot(
            Request::get(
                "/v1/metrics/events?event=app_open&metric=count&granularity=hour&start=2026-03-01T00:00:00Z&end=2026-03-01T01:00:00Z&platform=ios&group_by=provider",
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
            "group_by": ["provider"],
            "series": [
                {
                    "dimensions": {
                        "provider": "strava"
                    },
                    "points": [
                        { "bucket": "2026-03-01T00:00:00Z", "value": 1 },
                        { "bucket": "2026-03-01T01:00:00Z", "value": 1 }
                    ]
                }
            ]
        })
    );

    let daily_response = api
        .oneshot(
            Request::get(
                "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&os_version=18.3&group_by=provider&group_by=is_paying",
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
            "group_by": ["provider", "is_paying"],
            "series": [
                {
                    "dimensions": {
                        "provider": "strava",
                        "is_paying": "true"
                    },
                    "points": [
                        { "bucket": "2026-03-01", "value": 1 },
                        { "bucket": "2026-03-02", "value": 0 }
                    ]
                },
                {
                    "dimensions": {
                        "provider": "strava",
                        "is_paying": null
                    },
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
async fn pipeline_exposes_dim4_event_metrics_with_null_buckets(pool: PgPool) {
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
                                "properties": {
                                    "plan": "pro",
                                    "provider": "strava",
                                    "region": "eu"
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
                                    "plan": "pro",
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

    let response = api
        .oneshot(
            Request::get(
                "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-02&app_version=1.4.0&plan=pro&group_by=provider&group_by=region",
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
            "group_by": ["provider", "region"],
            "series": [
                {
                    "dimensions": {
                        "provider": "strava",
                        "region": "eu"
                    },
                    "points": [
                        { "bucket": "2026-03-01", "value": 1 },
                        { "bucket": "2026-03-02", "value": 0 }
                    ]
                },
                {
                    "dimensions": {
                        "provider": "strava",
                        "region": null
                    },
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
                "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-01&group_by=provider",
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
async fn pipeline_rejects_dim4_event_metrics_queries_that_exceed_group_limit(pool: PgPool) {
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
                "properties": {
                    "plan": "pro",
                    "provider": format!("provider-{index:03}"),
                    "region": format!("region-{index:03}")
                }
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
                "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-03-01&end=2026-03-01&app_version=1.4.0&plan=pro&group_by=provider&group_by=region",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("aggregate request succeeds");

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_json(response).await,
        serde_json::json!({
            "error": "group_limit_exceeded"
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

    let query = "/v1/metrics/events?event=app_open&metric=count&granularity=day&start=2026-04-01&end=2026-04-01&platform=ios&group_by=provider&group_by=region";
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
        "the regression must issue at least one grouped metrics request during worker catch-up"
    );
    assert!(
        !saw_internal_server_error,
        "grouped daily event metrics queries should never return internal_server_error while the worker commits bounded batches"
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
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-01&group_by=app_version",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("grouped count request succeeds");

    assert_eq!(grouped_count_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(grouped_count_response).await,
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "group_by": ["app_version"],
            "series": [
                {
                    "dimensions": {
                        "app_version": "2.0.0"
                    },
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 }
                    ]
                },
                {
                    "dimensions": {
                        "app_version": null
                    },
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
                "/v1/metrics/sessions?metric=new_installs&granularity=day&start=2026-01-01&end=2026-01-01&group_by=app_version",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("grouped new installs request succeeds");

    assert_eq!(grouped_new_installs_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(grouped_new_installs_response).await,
        serde_json::json!({
            "metric": "new_installs",
            "granularity": "day",
            "group_by": ["app_version"],
            "series": [
                {
                    "dimensions": {
                        "app_version": "2.0.0"
                    },
                    "points": [
                        { "bucket": "2026-01-01", "value": 1 }
                    ]
                },
                {
                    "dimensions": {
                        "app_version": null
                    },
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
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-01&group_by=os_version",
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
            "group_by": [],
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
}

#[sqlx::test]
async fn pipeline_keeps_grouped_session_app_version_fixed_after_late_event_repair(pool: PgPool) {
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
                "/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-01&group_by=app_version",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("grouped count request succeeds");

    assert_eq!(grouped_count_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(grouped_count_response).await,
        serde_json::json!({
            "metric": "count",
            "granularity": "day",
            "group_by": ["app_version"],
            "series": [
                {
                    "dimensions": {
                        "app_version": null
                    },
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
                "/v1/metrics/sessions?metric=duration_total&granularity=day&start=2026-01-01&end=2026-01-01&group_by=app_version",
            )
            .header("x-fantasma-key", &provisioned.read_key)
            .body(Body::empty())
            .expect("valid api request"),
        )
        .await
        .expect("grouped duration request succeeds");

    assert_eq!(grouped_duration_response.status(), StatusCode::OK);
    assert_eq!(
        response_json(grouped_duration_response).await,
        serde_json::json!({
            "metric": "duration_total",
            "granularity": "day",
            "group_by": ["app_version"],
            "series": [
                {
                    "dimensions": {
                        "app_version": null
                    },
                    "points": [
                        { "bucket": "2026-01-01", "value": 20 * 60 }
                    ]
                }
            ]
        })
    );
}
