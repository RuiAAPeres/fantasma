use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use axum::{
    Json, Router,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get},
};
use clap::Parser;
use fantasma_cli::{
    app::App,
    cli::Cli,
    config::{CliConfig, load_config, save_config},
};
use fantasma_core::{ProjectSummary, UsageProjectEvents};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct RequestRecord {
    method: String,
    path: String,
    query: Option<String>,
    authorization: Option<String>,
    fantasma_key: Option<String>,
    body: Option<Value>,
}

#[derive(Clone)]
struct ServerState {
    records: Arc<Mutex<Vec<RequestRecord>>>,
    project_id: Uuid,
    read_key_id: Uuid,
    deletion_id: Uuid,
    operator_token: String,
    read_key_secret: String,
}

struct TestServer {
    _temp: TempDir,
    config_path: PathBuf,
    app: App,
    base_url: Url,
    records: Arc<Mutex<Vec<RequestRecord>>>,
    project_id: Uuid,
    read_key_id: Uuid,
    deletion_id: Uuid,
    operator_token: String,
    read_key_secret: String,
}

impl TestServer {
    async fn spawn() -> Self {
        Self::spawn_with_credentials("/", "secret-token", "fg_rd_created").await
    }

    async fn spawn_with_base_path(base_path: &str) -> Self {
        Self::spawn_with_credentials(base_path, "secret-token", "fg_rd_created").await
    }

    async fn spawn_with_credentials(
        base_path: &str,
        operator_token: &str,
        read_key_secret: &str,
    ) -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("fantasma.toml");
        let records = Arc::new(Mutex::new(Vec::new()));
        let project_id = Uuid::new_v4();
        let read_key_id = Uuid::new_v4();
        let deletion_id = Uuid::new_v4();
        let state = ServerState {
            records: Arc::clone(&records),
            project_id,
            read_key_id,
            deletion_id,
            operator_token: operator_token.to_owned(),
            read_key_secret: read_key_secret.to_owned(),
        };

        let api_router = Router::new()
            .route("/v1/projects", get(list_projects).post(create_project))
            .route(
                "/v1/projects/{project_id}/keys",
                get(list_keys).post(create_key),
            )
            .route(
                "/v1/projects/{project_id}/keys/{key_id}",
                delete(revoke_key),
            )
            .route("/v1/projects/{project_id}", delete(delete_project))
            .route(
                "/v1/projects/{project_id}/deletions",
                get(list_project_deletions).post(create_range_deletion),
            )
            .route(
                "/v1/projects/{project_id}/deletions/{deletion_id}",
                get(get_project_deletion),
            )
            .route("/v1/usage/events", get(usage_events))
            .route("/v1/metrics/events", get(event_metrics))
            .route("/v1/metrics/events/top", get(top_events))
            .route("/v1/metrics/events/catalog", get(event_catalog))
            .route("/v1/metrics/sessions", get(session_metrics))
            .route("/v1/metrics/live_installs", get(live_installs))
            .with_state(state);
        let router = if base_path == "/" {
            api_router
        } else {
            Router::new().nest(base_path, api_router)
        };

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve router");
        });

        let base_url = Url::parse(&format!("http://{address}{base_path}")).expect("base url");
        let mut config = CliConfig::default();
        config.ensure_instance_mut("prod", base_url.clone());
        config.active_instance = Some("prod".to_owned());
        save_config(&config_path, &config).expect("seed config");

        Self {
            _temp: temp,
            config_path: config_path.clone(),
            app: App::new(config_path),
            base_url,
            records,
            project_id,
            read_key_id,
            deletion_id,
            operator_token: operator_token.to_owned(),
            read_key_secret: read_key_secret.to_owned(),
        }
    }

    fn parse(&self, args: &[&str]) -> Cli {
        let mut argv = vec!["fantasma"];
        argv.extend_from_slice(args);
        Cli::parse_from(argv)
    }

    fn read_config(&self) -> CliConfig {
        load_config(&self.config_path).expect("load config")
    }

    fn records(&self) -> Vec<RequestRecord> {
        self.records.lock().expect("records lock").clone()
    }
}

#[tokio::test]
async fn auth_login_validates_operator_token_and_persists_it() {
    let server = TestServer::spawn().await;

    let output = server
        .app
        .run(server.parse(&[
            "auth",
            "login",
            "--instance",
            "prod",
            "--token",
            "secret-token",
        ]))
        .await
        .expect("login succeeds");

    let config = server.read_config();
    let profile = config.instances.get("prod").expect("prod profile");

    assert!(output.stdout.contains("saved operator token"));
    assert_eq!(profile.operator_token.as_deref(), Some("secret-token"));

    let records = server.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].method, "GET");
    assert_eq!(records[0].path, "/v1/projects");
    assert_eq!(
        records[0].authorization.as_deref(),
        Some("Bearer secret-token")
    );
}

#[tokio::test]
async fn auth_logout_with_instance_clears_only_that_profile_token() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let prod_profile = config.instances.get_mut("prod").unwrap();
    prod_profile.operator_token = Some("prod-token".to_owned());
    prod_profile.active_project_id = Some(server.project_id);
    prod_profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "prod-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    let staging_project_id = Uuid::new_v4();
    let staging_profile = config.ensure_instance_mut(
        "staging",
        Url::parse("https://staging.example.com").unwrap(),
    );
    staging_profile.operator_token = Some("staging-token".to_owned());
    staging_profile.active_project_id = Some(staging_project_id);
    staging_profile.store_read_key(
        staging_project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: Uuid::new_v4(),
            name: "staging-read".to_owned(),
            secret: "staging-read-secret".to_owned(),
            created_at: "2026-03-13T12:30:00Z".to_owned(),
        },
    );
    config.active_instance = Some("prod".to_owned());
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["auth", "logout", "--instance", "staging"]))
        .await
        .expect("logout succeeds");

    let config = server.read_config();
    let prod_profile = config.instances.get("prod").expect("prod profile");
    let staging_profile = config.instances.get("staging").expect("staging profile");

    assert!(output.stdout.contains("logged out instance staging"));
    assert_eq!(prod_profile.operator_token.as_deref(), Some("prod-token"));
    assert_eq!(prod_profile.active_project_id, Some(server.project_id));
    assert!(prod_profile.read_keys.contains_key(&server.project_id));
    assert_eq!(staging_profile.operator_token, None);
    assert_eq!(staging_profile.active_project_id, Some(staging_project_id));
    assert!(staging_profile.read_keys.contains_key(&staging_project_id));
}

#[tokio::test]
async fn auth_logout_without_instance_uses_active_profile_only() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let prod_profile = config.instances.get_mut("prod").unwrap();
    prod_profile.operator_token = Some("prod-token".to_owned());
    prod_profile.active_project_id = Some(server.project_id);
    prod_profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "prod-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    let staging_project_id = Uuid::new_v4();
    let staging_profile = config.ensure_instance_mut(
        "staging",
        Url::parse("https://staging.example.com").unwrap(),
    );
    staging_profile.operator_token = Some("staging-token".to_owned());
    staging_profile.active_project_id = Some(staging_project_id);
    staging_profile.store_read_key(
        staging_project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: Uuid::new_v4(),
            name: "staging-read".to_owned(),
            secret: "staging-read-secret".to_owned(),
            created_at: "2026-03-13T12:30:00Z".to_owned(),
        },
    );
    config.active_instance = Some("prod".to_owned());
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["auth", "logout"]))
        .await
        .expect("logout succeeds");

    let config = server.read_config();
    let prod_profile = config.instances.get("prod").expect("prod profile");
    let staging_profile = config.instances.get("staging").expect("staging profile");

    assert!(output.stdout.contains("logged out instance prod"));
    assert_eq!(config.active_instance.as_deref(), Some("prod"));
    assert_eq!(prod_profile.operator_token, None);
    assert_eq!(prod_profile.active_project_id, Some(server.project_id));
    assert!(prod_profile.read_keys.contains_key(&server.project_id));
    assert_eq!(
        staging_profile.operator_token.as_deref(),
        Some("staging-token")
    );
    assert_eq!(staging_profile.active_project_id, Some(staging_project_id));
    assert!(staging_profile.read_keys.contains_key(&staging_project_id));
}

#[tokio::test]
async fn projects_create_prints_initial_ingest_key_without_persisting_it() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    config.instances.get_mut("prod").unwrap().operator_token = Some("secret-token".to_owned());
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&[
            "projects",
            "create",
            "--name",
            "CLI Project",
            "--ingest-key-name",
            "ios-sdk",
        ]))
        .await
        .expect("project creation succeeds");

    let config = server.read_config();
    let profile = config.instances.get("prod").expect("prod profile");

    assert!(output.stdout.contains("fg_ing_created"));
    assert!(output.stdout.contains(&server.project_id.to_string()));
    assert!(
        profile.read_keys.is_empty(),
        "ingest keys must not be stored locally"
    );

    let records = server.records();
    let create = records
        .iter()
        .find(|record| record.method == "POST" && record.path == "/v1/projects")
        .expect("create project request");
    assert_eq!(create.authorization.as_deref(), Some("Bearer secret-token"));
    assert_eq!(
        create.body.as_ref(),
        Some(&json!({
            "name": "CLI Project",
            "ingest_key_name": "ios-sdk"
        }))
    );
}

#[tokio::test]
async fn projects_use_persists_active_project_for_later_keys_and_metrics_commands() {
    let server = TestServer::spawn().await;
    let previous_project_id = Uuid::new_v4();
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some(server.operator_token.clone());
    profile.active_project_id = Some(previous_project_id);
    profile.store_read_key(
        previous_project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: Uuid::new_v4(),
            name: "old-read".to_owned(),
            secret: "old-read-secret".to_owned(),
            created_at: "2026-03-13T11:00:00Z".to_owned(),
        },
    );
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "new-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    server
        .app
        .run(server.parse(&["projects", "use", &server.project_id.to_string()]))
        .await
        .expect("project use succeeds");

    let config = server.read_config();
    assert_eq!(
        config.instances["prod"].active_project_id,
        Some(server.project_id)
    );

    server
        .app
        .run(server.parse(&["keys", "list"]))
        .await
        .expect("keys list succeeds");
    server
        .app
        .run(server.parse(&[
            "metrics",
            "sessions",
            "--metric",
            "count",
            "--granularity",
            "day",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-02",
        ]))
        .await
        .expect("metrics succeeds");

    let records = server.records();
    assert!(records.iter().any(|record| {
        record.method == "GET"
            && record.path == format!("/v1/projects/{}/keys", server.project_id)
            && record.authorization.as_deref() == Some("Bearer secret-token")
    }));
    assert!(records.iter().any(|record| {
        record.method == "GET"
            && record.path == "/v1/metrics/sessions"
            && record.fantasma_key.as_deref() == Some(server.read_key_secret.as_str())
    }));
}

#[tokio::test]
async fn projects_delete_uses_active_project() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some(server.operator_token.clone());
    profile.active_project_id = Some(server.project_id);
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["projects", "delete"]))
        .await
        .expect("project delete succeeds");

    assert!(output.stdout.contains("project purge queued"));
    assert!(output.stdout.contains(&server.deletion_id.to_string()));

    let records = server.records();
    let delete = records
        .iter()
        .find(|record| {
            record.method == "DELETE"
                && record.path == format!("/v1/projects/{}", server.project_id)
        })
        .expect("delete project request");
    assert_eq!(
        delete.authorization.as_deref(),
        Some(format!("Bearer {}", server.operator_token).as_str())
    );
}

#[tokio::test]
async fn project_deletions_list_uses_active_project() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some(server.operator_token.clone());
    profile.active_project_id = Some(server.project_id);
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["projects", "deletions", "list"]))
        .await
        .expect("project deletions list succeeds");

    assert!(output.stdout.contains(&server.deletion_id.to_string()));
    assert!(output.stdout.contains("project_purge"));
    assert!(output.stdout.contains("CLI Project"));

    let records = server.records();
    let list = records
        .iter()
        .find(|record| {
            record.method == "GET"
                && record.path == format!("/v1/projects/{}/deletions", server.project_id)
        })
        .expect("list project deletions request");
    assert_eq!(
        list.authorization.as_deref(),
        Some(format!("Bearer {}", server.operator_token).as_str())
    );
}

#[tokio::test]
async fn project_deletions_get_uses_active_project() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some(server.operator_token.clone());
    profile.active_project_id = Some(server.project_id);
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&[
            "projects",
            "deletions",
            "get",
            &server.deletion_id.to_string(),
        ]))
        .await
        .expect("project deletion get succeeds");

    assert!(
        output
            .stdout
            .contains(&format!("deletion id: {}", server.deletion_id))
    );
    assert!(
        output
            .stdout
            .contains(&format!("project id: {}", server.project_id))
    );
    assert!(output.stdout.contains("kind: project_purge"));

    let records = server.records();
    let get = records
        .iter()
        .find(|record| {
            record.method == "GET"
                && record.path
                    == format!(
                        "/v1/projects/{}/deletions/{}",
                        server.project_id, server.deletion_id
                    )
        })
        .expect("get project deletion request");
    assert_eq!(
        get.authorization.as_deref(),
        Some(format!("Bearer {}", server.operator_token).as_str())
    );
}

#[tokio::test]
async fn project_deletions_range_sends_expected_scope() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some(server.operator_token.clone());
    profile.active_project_id = Some(server.project_id);
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&[
            "projects",
            "deletions",
            "range",
            "--start-at",
            "2026-03-01T00:00:00Z",
            "--end-before",
            "2026-03-08T00:00:00Z",
            "--event",
            "app_open",
            "--filter",
            "platform=ios",
            "--filter",
            "app_version=1.2.3",
        ]))
        .await
        .expect("range deletion succeeds");

    assert!(output.stdout.contains("range deletion queued"));
    assert!(output.stdout.contains("kind: range_delete"));

    let records = server.records();
    let range = records
        .iter()
        .find(|record| {
            record.method == "POST"
                && record.path == format!("/v1/projects/{}/deletions", server.project_id)
        })
        .expect("range deletion request");
    assert_eq!(
        range.authorization.as_deref(),
        Some(format!("Bearer {}", server.operator_token).as_str())
    );
    assert_eq!(
        range.body.as_ref(),
        Some(&json!({
            "start_at": "2026-03-01T00:00:00Z",
            "end_before": "2026-03-08T00:00:00Z",
            "event": "app_open",
            "filters": {
                "app_version": "1.2.3",
                "platform": "ios"
            }
        }))
    );
}

#[test]
fn usage_events_subcommand_parses() {
    let cli = Cli::try_parse_from([
        "fantasma",
        "usage",
        "events",
        "--start",
        "2026-03-01",
        "--end",
        "2026-03-31",
        "--json",
    ])
    .expect("usage events should parse");

    assert!(matches!(
        cli.command,
        fantasma_cli::cli::Command::Usage(fantasma_cli::cli::UsageCommand {
            command: fantasma_cli::cli::UsageSubcommand::Events(_),
        })
    ));
}

#[tokio::test]
async fn usage_events_json_uses_operator_token_and_shared_response_shape() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    config.instances.get_mut("prod").unwrap().operator_token = Some("secret-token".to_owned());
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&[
            "usage",
            "events",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-31",
            "--json",
        ]))
        .await
        .expect("usage events succeeds");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["start"], json!("2026-03-01"));
    assert_eq!(body["total_events_processed"], json!(12));
    assert_eq!(body["projects"][0]["events_processed"], json!(12));
    assert_eq!(
        body["projects"][0]["project"]["id"],
        json!(server.project_id)
    );

    let records = server.records();
    let usage_request = records
        .iter()
        .find(|record| record.path == "/v1/usage/events")
        .expect("usage request");
    assert_eq!(
        usage_request.authorization.as_deref(),
        Some("Bearer secret-token")
    );
    assert_eq!(
        usage_request.query.as_deref(),
        Some("start=2026-03-01&end=2026-03-31")
    );
}

#[tokio::test]
async fn usage_events_json_preserves_raw_api_payload() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    config.instances.get_mut("prod").unwrap().operator_token = Some("secret-token".to_owned());
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&[
            "usage",
            "events",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-31",
            "--json",
        ]))
        .await
        .expect("usage events succeeds");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["api_revision"], json!("2026-03-19"));
}

#[tokio::test]
async fn usage_events_text_renders_total_and_project_rows_without_project_context() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    config.instances.get_mut("prod").unwrap().operator_token = Some("secret-token".to_owned());
    config.instances.get_mut("prod").unwrap().active_project_id = None;
    config.instances.get_mut("prod").unwrap().read_keys.clear();
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&[
            "usage",
            "events",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-31",
        ]))
        .await
        .expect("usage events succeeds");

    assert_eq!(
        output.stdout,
        format!(
            "events processed\t12\n{}\tUsage Project\t2026-03-10T12:00:00Z\t12",
            server.project_id
        )
    );
}

#[tokio::test]
async fn usage_events_uses_operator_token_even_without_active_project() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    config.instances.get_mut("prod").unwrap().operator_token = Some("secret-token".to_owned());
    config.instances.get_mut("prod").unwrap().active_project_id = None;
    config.instances.get_mut("prod").unwrap().read_keys.clear();
    save_config(&server.config_path, &config).expect("save config");

    server
        .app
        .run(server.parse(&[
            "usage",
            "events",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-31",
            "--json",
        ]))
        .await
        .expect("usage events succeeds");

    let records = server.records();
    assert!(
        records
            .iter()
            .any(|record| record.path == "/v1/usage/events")
    );
    assert!(
        records
            .iter()
            .all(|record| record.path != "/v1/metrics/sessions"),
        "usage should not require the worker-derived metrics read path"
    );
}

#[tokio::test]
async fn read_key_creation_uses_active_project_and_replaces_local_key() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some("secret-token".to_owned());
    profile.active_project_id = Some(server.project_id);
    save_config(&server.config_path, &config).expect("save config");

    server
        .app
        .run(server.parse(&["keys", "create", "--kind", "read", "--name", "cli-a"]))
        .await
        .expect("first read key succeeds");

    server
        .app
        .run(server.parse(&["keys", "create", "--kind", "read", "--name", "cli-b"]))
        .await
        .expect("second read key succeeds");

    let config = server.read_config();
    let profile = config.instances.get("prod").expect("prod profile");
    let stored = profile
        .read_keys
        .get(&server.project_id)
        .expect("stored read key");

    assert_eq!(profile.read_keys.len(), 1);
    assert_eq!(stored.name, "cli-b");
    assert_eq!(stored.secret, "fg_rd_created");

    let records = server.records();
    assert!(
        records
            .iter()
            .filter(|record| record.path.ends_with("/keys"))
            .count()
            >= 2,
        "expected both read-key creation requests"
    );
}

#[tokio::test]
async fn ingest_key_creation_prints_secret_without_touching_local_read_keys() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some(server.operator_token.clone());
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "existing-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["keys", "create", "--kind", "ingest", "--name", "ios-sdk"]))
        .await
        .expect("ingest key creation succeeds");

    let config = server.read_config();
    let profile = config.instances.get("prod").expect("prod profile");
    let stored = profile
        .read_keys
        .get(&server.project_id)
        .expect("existing read key remains");

    assert!(output.stdout.contains("fg_ing_created"));
    assert_eq!(profile.read_keys.len(), 1);
    assert_eq!(stored.name, "existing-read");
    assert_eq!(stored.secret, server.read_key_secret);

    let records = server.records();
    let create = records
        .iter()
        .find(|record| record.method == "POST" && record.path.ends_with("/keys"))
        .expect("key create request");
    assert_eq!(
        create.body.as_ref(),
        Some(&json!({
            "name": "ios-sdk",
            "kind": "ingest"
        }))
    );
}

#[tokio::test]
async fn status_reports_local_and_remote_validation() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some("secret-token".to_owned());
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "fg_rd_created".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["status"]))
        .await
        .expect("status succeeds");

    assert!(output.stdout.contains("active instance: prod"));
    assert!(output.stdout.contains("operator token: valid"));
    assert!(output.stdout.contains("read key: valid"));

    let records = server.records();
    assert!(
        records
            .iter()
            .any(|record| record.method == "GET" && record.path == "/v1/projects")
    );
    assert!(
        records
            .iter()
            .any(|record| record.method == "GET" && record.path == "/v1/metrics/sessions")
    );
}

#[tokio::test]
async fn status_reports_missing_local_configuration_without_failing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let config_path = temp.path().join("fantasma.toml");
    let app = App::new(config_path);
    let cli = Cli::parse_from(["fantasma", "status"]);

    let output = app.run(cli).await.expect("status should not fail");

    assert!(output.stdout.contains("active instance: missing"));
    assert!(output.stdout.contains("api base url: missing"));
    assert!(output.stdout.contains("active project: missing"));
    assert!(output.stdout.contains("operator token: missing"));
    assert!(output.stdout.contains("read key: not checked"));
}

#[tokio::test]
async fn status_json_reports_local_only_state() {
    let temp = tempfile::tempdir().expect("tempdir");
    let config_path = temp.path().join("fantasma.toml");
    let app = App::new(config_path);

    let output = app
        .run(Cli::parse_from(["fantasma", "status", "--json"]))
        .await
        .expect("status should not fail");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["active_instance"], Value::Null);
    assert_eq!(body["api_base_url"], Value::Null);
    assert_eq!(body["active_project_id"], Value::Null);
    assert_eq!(body["operator_token_status"], json!("missing"));
    assert_eq!(body["read_key_status"], json!("not checked"));
}

#[tokio::test]
async fn projects_list_supports_json_output() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    config.instances.get_mut("prod").unwrap().operator_token = Some("secret-token".to_owned());
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["projects", "list", "--json"]))
        .await
        .expect("projects list succeeds");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["projects"][0]["id"], json!(server.project_id));
    assert_eq!(body["projects"][0]["name"], json!("CLI Project"));
}

#[tokio::test]
async fn keys_list_supports_json_output() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some(server.operator_token.clone());
    profile.active_project_id = Some(server.project_id);
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["keys", "list", "--json"]))
        .await
        .expect("key list succeeds");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["keys"][0]["id"], json!(server.read_key_id));
    assert_eq!(body["keys"][0]["kind"], json!("read"));
    assert_eq!(body["keys"][0]["prefix"], json!("fg_rd_"));
}

#[tokio::test]
async fn keys_revoke_clears_stored_read_key() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some("secret-token".to_owned());
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "fg_rd_created".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["keys", "revoke", &server.read_key_id.to_string()]))
        .await
        .expect("revoke succeeds");

    let config = server.read_config();
    let profile = config.instances.get("prod").expect("prod profile");

    assert!(output.stdout.contains("revoked key"));
    assert!(
        !profile.read_keys.contains_key(&server.project_id),
        "matching local read key should be removed after revoke"
    );
}

#[tokio::test]
async fn metrics_sessions_json_uses_stored_read_key() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "fg_rd_created".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&[
            "metrics",
            "sessions",
            "--metric",
            "count",
            "--granularity",
            "day",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-02",
            "--json",
        ]))
        .await
        .expect("metrics succeeds");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["metric"], json!("count"));
    assert_eq!(body["series"][0]["points"][0]["value"], json!(9));

    let records = server.records();
    let metrics_request = records
        .iter()
        .find(|record| record.path == "/v1/metrics/sessions")
        .expect("metrics request");
    assert_eq!(
        metrics_request.fantasma_key.as_deref(),
        Some("fg_rd_created")
    );
}

#[tokio::test]
async fn metrics_sessions_active_installs_json_uses_stored_read_key() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "fg_rd_created".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let cli = Cli::try_parse_from([
        "fantasma",
        "metrics",
        "sessions",
        "--metric",
        "active_installs",
        "--start",
        "2026-03-01",
        "--end",
        "2026-03-02",
        "--json",
    ])
    .expect("exact-range active_installs should parse");

    let output = server.app.run(cli).await.expect("metrics succeeds");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["metric"], json!("active_installs"));
    assert_eq!(body["series"][0]["points"][0]["value"], json!(9));

    let records = server.records();
    let metrics_request = records
        .iter()
        .find(|record| record.path == "/v1/metrics/sessions")
        .expect("metrics request");
    assert_eq!(
        metrics_request.fantasma_key.as_deref(),
        Some("fg_rd_created")
    );
}

#[test]
fn metrics_live_installs_subcommand_parses() {
    let cli = Cli::try_parse_from(["fantasma", "metrics", "live-installs", "--json"])
        .expect("live-installs subcommand should parse");

    assert!(matches!(
        cli.command,
        fantasma_cli::cli::Command::Metrics(fantasma_cli::cli::MetricsCommand {
            command: fantasma_cli::cli::MetricsSubcommand::LiveInstalls(_),
        })
    ));
}

#[tokio::test]
async fn metrics_live_installs_json_uses_stored_read_key() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "fg_rd_created".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["metrics", "live-installs", "--json"]))
        .await
        .expect("live installs succeeds");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["metric"], json!("live_installs"));
    assert_eq!(body["value"], json!(9));
    assert_eq!(body["window_seconds"], json!(120));

    let records = server.records();
    let metrics_request = records
        .iter()
        .find(|record| record.path == "/v1/metrics/live_installs")
        .expect("live installs request");
    assert_eq!(
        metrics_request.fantasma_key.as_deref(),
        Some("fg_rd_created")
    );
    assert_eq!(metrics_request.query, None);
}

#[tokio::test]
async fn metrics_live_installs_text_renders_scalar_response() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "fg_rd_created".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["metrics", "live-installs"]))
        .await
        .expect("live installs succeeds");

    assert_eq!(output.stdout, "live_installs\t9\t2026-03-17T12:34:56Z\t120");
}

#[test]
fn metrics_sessions_active_installs_subcommand_parses_exact_range_without_granularity() {
    Cli::try_parse_from([
        "fantasma",
        "metrics",
        "sessions",
        "--metric",
        "active_installs",
        "--start",
        "2026-03-01",
        "--end",
        "2026-03-17",
        "--json",
    ])
    .expect("exact-range active_installs should parse without granularity");
}

#[test]
fn metrics_sessions_active_installs_subcommand_accepts_interval() {
    Cli::try_parse_from([
        "fantasma",
        "metrics",
        "sessions",
        "--metric",
        "active_installs",
        "--start",
        "2026-03-01",
        "--end",
        "2026-03-17",
        "--interval",
        "week",
        "--json",
    ])
    .expect("active_installs should accept --interval");
}

#[test]
fn metrics_sessions_active_installs_rejects_granularity_flag() {
    let temp = tempfile::tempdir().expect("tempdir");
    let app = App::new(temp.path().join("fantasma.toml"));
    let cli = Cli::try_parse_from([
        "fantasma",
        "metrics",
        "sessions",
        "--metric",
        "active_installs",
        "--granularity",
        "day",
        "--start",
        "2026-03-01",
        "--end",
        "2026-03-17",
    ])
    .expect("active_installs arguments still parse");

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let error = runtime
        .block_on(app.run(cli))
        .expect_err("active_installs should reject --granularity locally");

    let rendered = error.to_string();
    assert!(
        rendered.contains("--granularity")
            || rendered.contains("active_installs")
            || rendered.contains("interval"),
        "error should mention the incompatible active_installs granularity flag: {rendered}"
    );
}

#[tokio::test]
async fn metrics_sessions_active_installs_serializes_exact_range_query() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let cli = Cli::try_parse_from([
        "fantasma",
        "metrics",
        "sessions",
        "--metric",
        "active_installs",
        "--start",
        "2026-03-01",
        "--end",
        "2026-03-17",
        "--interval",
        "week",
        "--filter",
        "locale=en-US",
        "--json",
    ])
    .expect("exact-range active_installs should parse");

    server
        .app
        .run(cli)
        .await
        .expect("active installs metrics succeed");

    let records = server.records();
    let request = records
        .iter()
        .find(|record| record.path == "/v1/metrics/sessions")
        .expect("metrics request");
    assert_eq!(
        request.query.as_deref(),
        Some("metric=active_installs&start=2026-03-01&end=2026-03-17&interval=week&locale=en-US")
    );
}

#[tokio::test]
async fn metrics_sessions_active_installs_text_renders_exact_range_points() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let cli = Cli::try_parse_from([
        "fantasma",
        "metrics",
        "sessions",
        "--metric",
        "active_installs",
        "--start",
        "2026-03-01",
        "--end",
        "2026-03-17",
    ])
    .expect("exact-range active_installs should parse");

    let output = server
        .app
        .run(cli)
        .await
        .expect("active installs text output succeeds");

    assert_eq!(
        output.stdout,
        "dimensions\tstart\tend\tvalue\n{}\t2026-03-01\t2026-03-17\t9"
    );
}

#[tokio::test]
async fn metrics_sessions_active_installs_serializes_builtin_filters_exactly() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let cli = Cli::try_parse_from([
        "fantasma",
        "metrics",
        "sessions",
        "--metric",
        "active_installs",
        "--start",
        "2026-03-01",
        "--end",
        "2026-03-17",
        "--interval",
        "week",
        "--filter",
        "locale=en-US",
        "--json",
    ])
    .expect("exact-range active_installs should parse");

    server
        .app
        .run(cli)
        .await
        .expect("active installs metrics succeed");

    let records = server.records();
    let request = records
        .iter()
        .find(|record| record.path == "/v1/metrics/sessions")
        .expect("metrics request");
    assert_eq!(
        request.query.as_deref(),
        Some("metric=active_installs&start=2026-03-01&end=2026-03-17&interval=week&locale=en-US")
    );
}

#[tokio::test]
async fn metrics_sessions_serializes_builtin_filters_exactly() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    server
        .app
        .run(server.parse(&[
            "metrics",
            "sessions",
            "--metric",
            "count",
            "--granularity",
            "day",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-02",
            "--filter",
            "platform=ios",
            "--filter",
            "locale=en-US",
            "--json",
        ]))
        .await
        .expect("session metrics succeed");

    let records = server.records();
    let request = records
        .iter()
        .find(|record| record.path == "/v1/metrics/sessions")
        .expect("metrics request");
    assert_eq!(
        request.query.as_deref(),
        Some(
            "metric=count&granularity=day&start=2026-03-01&end=2026-03-02&locale=en-US&platform=ios"
        )
    );
}

#[tokio::test]
async fn instances_add_use_remove_updates_local_profiles() {
    let server = TestServer::spawn().await;

    server
        .app
        .run(server.parse(&[
            "instances",
            "add",
            "staging",
            "--url",
            "https://staging.example.com",
        ]))
        .await
        .expect("instance add succeeds");
    server
        .app
        .run(server.parse(&["instances", "use", "staging"]))
        .await
        .expect("instance use succeeds");
    server
        .app
        .run(server.parse(&["instances", "remove", "prod"]))
        .await
        .expect("instance remove succeeds");

    let config = server.read_config();

    assert_eq!(config.active_instance.as_deref(), Some("staging"));
    assert!(config.instances.contains_key("staging"));
    assert!(!config.instances.contains_key("prod"));
}

#[tokio::test]
async fn removing_active_instance_leaves_status_in_local_only_state() {
    let server = TestServer::spawn().await;

    server
        .app
        .run(server.parse(&["instances", "remove", "prod"]))
        .await
        .expect("instance remove succeeds");

    let output = server
        .app
        .run(server.parse(&["status"]))
        .await
        .expect("status succeeds");

    let config = server.read_config();
    assert_eq!(config.active_instance, None);
    assert!(config.instances.is_empty());
    assert!(output.stdout.contains("active instance: missing"));
    assert!(output.stdout.contains("operator token: missing"));
}

#[tokio::test]
async fn removing_non_active_instance_preserves_active_profile_state() {
    let server = TestServer::spawn().await;

    server
        .app
        .run(server.parse(&[
            "instances",
            "add",
            "staging",
            "--url",
            "https://staging.example.com",
        ]))
        .await
        .expect("instance add succeeds");

    let mut config = server.read_config();
    let staging_project_id = Uuid::new_v4();
    let staging_profile = config.instances.get_mut("staging").unwrap();
    staging_profile.operator_token = Some("staging-token".to_owned());
    staging_profile.active_project_id = Some(staging_project_id);
    staging_profile.store_read_key(
        staging_project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: Uuid::new_v4(),
            name: "staging-read".to_owned(),
            secret: "staging-read-secret".to_owned(),
            created_at: "2026-03-13T12:30:00Z".to_owned(),
        },
    );
    config.active_instance = Some("staging".to_owned());
    save_config(&server.config_path, &config).expect("save config");

    server
        .app
        .run(server.parse(&["instances", "remove", "prod"]))
        .await
        .expect("instance remove succeeds");

    let output = server
        .app
        .run(server.parse(&["status"]))
        .await
        .expect("status succeeds");

    let config = server.read_config();
    assert_eq!(config.active_instance.as_deref(), Some("staging"));
    assert!(config.instances.contains_key("staging"));
    assert!(!config.instances.contains_key("prod"));
    assert!(output.stdout.contains("active instance: staging"));
    assert!(output.stdout.contains("active project:"));
}

#[tokio::test]
async fn instances_add_rejects_different_url_for_existing_profile() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some("secret-token".to_owned());
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "fg_rd_created".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let error = server
        .app
        .run(server.parse(&[
            "instances",
            "add",
            "prod",
            "--url",
            "https://new.example.com",
        ]))
        .await
        .expect_err("instance update should fail");

    let config = server.read_config();
    let profile = config.instances.get("prod").expect("prod profile");

    assert!(error.to_string().contains("already exists"));
    assert_eq!(profile.api_base_url.as_str(), server.base_url.as_str());
    assert_eq!(profile.operator_token.as_deref(), Some("secret-token"));
    assert_eq!(profile.active_project_id, Some(server.project_id));
    assert!(profile.read_keys.contains_key(&server.project_id));
}

#[tokio::test]
async fn instances_list_json_redacts_secret_material() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some("secret-token".to_owned());
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "fg_rd_created".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["instances", "list", "--json"]))
        .await
        .expect("instance list succeeds");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["instances"]["prod"]["has_operator_token"], json!(true));
    assert_eq!(
        body["instances"]["prod"]["stored_read_key_projects"],
        json!([server.project_id])
    );
    assert_eq!(body["instances"]["prod"].get("operator_token"), None);
    assert_eq!(body["instances"]["prod"].get("read_keys"), None);
}

#[tokio::test]
async fn status_reports_invalid_remote_credentials_without_exiting() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some("bad-token".to_owned());
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "bad-read".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["status"]))
        .await
        .expect("status should report invalid state instead of failing");

    assert!(output.stdout.contains("operator token: invalid"));
    assert!(output.stdout.contains("read key: invalid"));
}

#[tokio::test]
async fn status_json_reports_valid_state() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some(server.operator_token.clone());
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["status", "--json"]))
        .await
        .expect("status succeeds");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["active_instance"], json!("prod"));
    assert_eq!(body["api_base_url"], json!(server.base_url));
    assert_eq!(body["active_project_id"], json!(server.project_id));
    assert_eq!(body["operator_token_status"], json!("valid"));
    assert_eq!(body["read_key_status"], json!("valid"));
}

#[tokio::test]
async fn status_json_reports_invalid_credentials() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some("bad-token".to_owned());
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "bad-read".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["status", "--json"]))
        .await
        .expect("status succeeds");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["operator_token_status"], json!("invalid"));
    assert_eq!(body["read_key_status"], json!("invalid"));
}

#[tokio::test]
async fn status_json_reports_missing_read_key_for_active_project() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some(server.operator_token.clone());
    profile.active_project_id = Some(server.project_id);
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["status", "--json"]))
        .await
        .expect("status succeeds");

    let body: Value = serde_json::from_str(&output.stdout).expect("json output");
    assert_eq!(body["operator_token_status"], json!("valid"));
    assert_eq!(body["read_key_status"], json!("missing"));
}

#[tokio::test]
async fn status_distinguishes_route_and_server_failures_from_invalid_credentials() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some("missing-route".to_owned());
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "server-error".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["status"]))
        .await
        .expect("status should report endpoint failures");

    assert!(output.stdout.contains("operator token: missing_route"));
    assert!(output.stdout.contains("read key: server_error"));
}

#[tokio::test]
async fn status_preserves_instance_base_path_prefix() {
    let server = TestServer::spawn_with_base_path("/fantasma/").await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some("secret-token".to_owned());
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "fg_rd_created".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["status"]))
        .await
        .expect("status succeeds");

    assert!(output.stdout.contains("operator token: valid"));
    assert!(output.stdout.contains("read key: valid"));

    let records = server.records();
    assert!(
        records
            .iter()
            .any(|record| record.method == "GET" && record.path == "/fantasma/v1/projects")
    );
    assert!(
        records
            .iter()
            .any(|record| record.method == "GET" && record.path == "/fantasma/v1/metrics/sessions")
    );
}

#[tokio::test]
async fn keys_list_text_uses_active_project() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.operator_token = Some("secret-token".to_owned());
    profile.active_project_id = Some(server.project_id);
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&["keys", "list"]))
        .await
        .expect("key list succeeds");

    assert!(output.stdout.contains(&server.read_key_id.to_string()));
    assert!(output.stdout.contains("cli-read"));
    assert!(output.stdout.contains("fg_rd_"));
}

#[tokio::test]
async fn metrics_events_rejects_malformed_filters_before_request() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let error = server
        .app
        .run(server.parse(&[
            "metrics",
            "events",
            "--event",
            "app_open",
            "--metric",
            "count",
            "--granularity",
            "day",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-02",
            "--filter",
            "broken-filter",
        ]))
        .await
        .expect_err("invalid filter should fail");

    assert!(
        error
            .to_string()
            .contains("invalid filter 'broken-filter', expected key=value")
    );
    assert!(server.records().is_empty(), "request should not be sent");
}

#[tokio::test]
async fn metrics_events_rejects_count_without_event_before_request() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let error = server
        .app
        .run(server.parse(&[
            "metrics",
            "events",
            "--metric",
            "count",
            "--granularity",
            "day",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-02",
        ]))
        .await
        .expect_err("missing event should fail");

    assert!(error.to_string().contains("count requires --event"));
    assert!(server.records().is_empty(), "request should not be sent");
}

#[tokio::test]
async fn metrics_events_rejects_unknown_metric_before_request() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let error = Cli::try_parse_from([
        "fantasma",
        "metrics",
        "events",
        "--event",
        "app_open",
        "--metric",
        "total_count",
        "--granularity",
        "day",
        "--start",
        "2026-03-01",
        "--end",
        "2026-03-02",
    ])
    .expect_err("unknown metric should fail");

    assert!(error.to_string().contains("invalid value 'total_count'"));
    assert!(server.records().is_empty(), "request should not be sent");
}

#[tokio::test]
async fn metrics_events_text_renders_series_rows() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: "fg_rd_created".to_owned(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&[
            "metrics",
            "events",
            "--event",
            "app_open",
            "--metric",
            "count",
            "--granularity",
            "day",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-02",
            "--filter",
            "platform=ios",
        ]))
        .await
        .expect("event metrics succeed");

    assert!(output.stdout.contains("\"platform\":\"ios\""));
    assert!(output.stdout.contains("2026-03-01"));
    assert!(output.stdout.contains("4"));
}

#[tokio::test]
async fn metrics_events_serializes_builtin_filters_exactly() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    server
        .app
        .run(server.parse(&[
            "metrics",
            "events",
            "--event",
            "app_open",
            "--metric",
            "count",
            "--granularity",
            "day",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-02",
            "--filter",
            "platform=ios",
            "--filter",
            "locale=en-US",
            "--json",
        ]))
        .await
        .expect("event metrics succeed");

    let records = server.records();
    let request = records
        .iter()
        .find(|record| record.path == "/v1/metrics/events")
        .expect("metrics request");
    assert_eq!(
        request.query.as_deref(),
        Some(
            "metric=count&granularity=day&start=2026-03-01&end=2026-03-02&event=app_open&locale=en-US&platform=ios"
        )
    );
}

#[tokio::test]
async fn metrics_events_top_text_renders_ranked_rows_and_serializes_limit() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&[
            "metrics",
            "events-top",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-02",
            "--limit",
            "5",
            "--filter",
            "platform=ios",
        ]))
        .await
        .expect("top events succeed");

    assert!(output.stdout.contains("app_open\t12"));
    assert!(output.stdout.contains("button_pressed\t4"));

    let records = server.records();
    let request = records
        .iter()
        .find(|record| record.path == "/v1/metrics/events/top")
        .expect("top events request");
    assert_eq!(
        request.query.as_deref(),
        Some("start=2026-03-01&end=2026-03-02&limit=5&platform=ios")
    );
}

#[tokio::test]
async fn metrics_events_catalog_text_renders_last_seen_rows() {
    let server = TestServer::spawn().await;
    let mut config = server.read_config();
    let profile = config.instances.get_mut("prod").unwrap();
    profile.active_project_id = Some(server.project_id);
    profile.store_read_key(
        server.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: server.read_key_id,
            name: "cli-read".to_owned(),
            secret: server.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    save_config(&server.config_path, &config).expect("save config");

    let output = server
        .app
        .run(server.parse(&[
            "metrics",
            "events-catalog",
            "--start",
            "2026-03-01",
            "--end",
            "2026-03-02",
            "--filter",
            "platform=ios",
        ]))
        .await
        .expect("event catalog succeeds");

    assert!(output.stdout.contains("app_open\t2026-03-02T10:15:00Z"));
    assert!(
        output
            .stdout
            .contains("button_pressed\t2026-03-01T08:00:00Z")
    );

    let records = server.records();
    let request = records
        .iter()
        .find(|record| record.path == "/v1/metrics/events/catalog")
        .expect("event catalog request");
    assert_eq!(
        request.query.as_deref(),
        Some("start=2026-03-01&end=2026-03-02&platform=ios")
    );
}

#[tokio::test]
async fn login_surfaces_401_404_and_422_guidance() {
    let server = TestServer::spawn().await;

    for (token, expected) in [
        (
            "bad-token",
            "validate operator token failed with 401 unauthorized; check the configured operator token or read key",
        ),
        (
            "missing-route",
            "validate operator token failed with 404 not found; verify the API base URL and target project/key identifiers",
        ),
        (
            "unprocessable-token",
            "validate operator token failed with 422 invalid request; review the command arguments",
        ),
    ] {
        let error = server
            .app
            .run(server.parse(&["auth", "login", "--instance", "prod", "--token", token]))
            .await
            .expect_err("login should fail");
        assert!(
            error.to_string().contains(expected),
            "missing guidance for token {token}: {error}"
        );
    }
}

#[tokio::test]
async fn project_and_key_management_surface_401_404_and_422_guidance() {
    let server = TestServer::spawn().await;

    for (token, expected) in [
        (
            "bad-token",
            "list projects failed with 401 unauthorized; check the configured operator token or read key",
        ),
        (
            "missing-route",
            "list projects failed with 404 not found; verify the API base URL and target project/key identifiers",
        ),
        (
            "unprocessable-token",
            "list projects failed with 422 invalid request; review the command arguments",
        ),
    ] {
        let mut config = server.read_config();
        config.instances.get_mut("prod").unwrap().operator_token = Some(token.to_owned());
        save_config(&server.config_path, &config).expect("save config");

        let error = server
            .app
            .run(server.parse(&["projects", "list"]))
            .await
            .expect_err("projects list should fail");
        assert!(
            error.to_string().contains(expected),
            "missing guidance for projects list token {token}: {error}"
        );
    }

    for (token, expected) in [
        (
            "bad-token",
            "create project key failed with 401 unauthorized; check the configured operator token or read key",
        ),
        (
            "missing-route",
            "create project key failed with 404 not found; verify the API base URL and target project/key identifiers",
        ),
        (
            "unprocessable-token",
            "create project key failed with 422 invalid request; review the command arguments",
        ),
    ] {
        let mut config = server.read_config();
        let profile = config.instances.get_mut("prod").unwrap();
        profile.operator_token = Some(token.to_owned());
        profile.active_project_id = Some(server.project_id);
        save_config(&server.config_path, &config).expect("save config");

        let error = server
            .app
            .run(server.parse(&["keys", "create", "--kind", "read", "--name", "cli-read"]))
            .await
            .expect_err("key create should fail");
        assert!(
            error.to_string().contains(expected),
            "missing guidance for key create token {token}: {error}"
        );
    }
}

#[tokio::test]
async fn revoke_and_metrics_surface_401_404_and_422_guidance() {
    let server = TestServer::spawn().await;

    for (token, expected) in [
        (
            "bad-token",
            "revoke project key failed with 401 unauthorized; check the configured operator token or read key",
        ),
        (
            "missing-route",
            "revoke project key failed with 404 not found; verify the API base URL and target project/key identifiers",
        ),
        (
            "unprocessable-token",
            "revoke project key failed with 422 invalid request; review the command arguments",
        ),
    ] {
        let mut config = server.read_config();
        let profile = config.instances.get_mut("prod").unwrap();
        profile.operator_token = Some(token.to_owned());
        profile.active_project_id = Some(server.project_id);
        profile.store_read_key(
            server.project_id,
            fantasma_cli::config::StoredReadKey {
                key_id: server.read_key_id,
                name: "cli-read".to_owned(),
                secret: server.read_key_secret.clone(),
                created_at: "2026-03-13T12:00:00Z".to_owned(),
            },
        );
        save_config(&server.config_path, &config).expect("save config");

        let error = server
            .app
            .run(server.parse(&["keys", "revoke", &server.read_key_id.to_string()]))
            .await
            .expect_err("key revoke should fail");
        assert!(
            error.to_string().contains(expected),
            "missing guidance for key revoke token {token}: {error}"
        );
    }

    for (secret, expected) in [
        (
            "bad-read",
            "load metrics failed with 401 unauthorized; check the configured operator token or read key",
        ),
        (
            "missing-route",
            "load metrics failed with 404 not found; verify the API base URL and target project/key identifiers",
        ),
        (
            "unprocessable-key",
            "load metrics failed with 422 invalid request; review the command arguments",
        ),
    ] {
        let mut config = server.read_config();
        let profile = config.instances.get_mut("prod").unwrap();
        profile.active_project_id = Some(server.project_id);
        profile.store_read_key(
            server.project_id,
            fantasma_cli::config::StoredReadKey {
                key_id: server.read_key_id,
                name: "cli-read".to_owned(),
                secret: secret.to_owned(),
                created_at: "2026-03-13T12:00:00Z".to_owned(),
            },
        );
        save_config(&server.config_path, &config).expect("save config");

        let error = server
            .app
            .run(server.parse(&[
                "metrics",
                "sessions",
                "--metric",
                "count",
                "--granularity",
                "day",
                "--start",
                "2026-03-01",
                "--end",
                "2026-03-02",
            ]))
            .await
            .expect_err("metrics should fail");
        assert!(
            error.to_string().contains(expected),
            "missing guidance for metrics key {secret}: {error}"
        );
    }
}

#[tokio::test]
async fn multi_profile_switching_uses_the_active_profiles_credentials_and_project_state() {
    let prod = TestServer::spawn_with_credentials("/", "prod-token", "prod-read-secret").await;
    let staging =
        TestServer::spawn_with_credentials("/fantasma/", "staging-token", "staging-read-secret")
            .await;
    let temp = tempfile::tempdir().expect("tempdir");
    let config_path = temp.path().join("fantasma.toml");
    let mut config = CliConfig::default();
    let prod_profile = config.ensure_instance_mut("prod", prod.base_url.clone());
    prod_profile.operator_token = Some(prod.operator_token.clone());
    prod_profile.active_project_id = Some(prod.project_id);
    prod_profile.store_read_key(
        prod.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: prod.read_key_id,
            name: "prod-read".to_owned(),
            secret: prod.read_key_secret.clone(),
            created_at: "2026-03-13T12:00:00Z".to_owned(),
        },
    );
    let staging_profile = config.ensure_instance_mut("staging", staging.base_url.clone());
    staging_profile.operator_token = Some(staging.operator_token.clone());
    staging_profile.active_project_id = Some(staging.project_id);
    staging_profile.store_read_key(
        staging.project_id,
        fantasma_cli::config::StoredReadKey {
            key_id: staging.read_key_id,
            name: "staging-read".to_owned(),
            secret: staging.read_key_secret.clone(),
            created_at: "2026-03-13T12:30:00Z".to_owned(),
        },
    );
    config.active_instance = Some("prod".to_owned());
    save_config(&config_path, &config).expect("save config");
    let app = App::new(config_path);

    app.run(Cli::parse_from(["fantasma", "keys", "list"]))
        .await
        .expect("prod keys list succeeds");
    app.run(Cli::parse_from([
        "fantasma",
        "metrics",
        "sessions",
        "--metric",
        "count",
        "--granularity",
        "day",
        "--start",
        "2026-03-01",
        "--end",
        "2026-03-02",
    ]))
    .await
    .expect("prod metrics succeeds");

    app.run(Cli::parse_from(["fantasma", "instances", "use", "staging"]))
        .await
        .expect("instance use succeeds");
    app.run(Cli::parse_from(["fantasma", "keys", "list"]))
        .await
        .expect("staging keys list succeeds");
    app.run(Cli::parse_from([
        "fantasma",
        "metrics",
        "sessions",
        "--metric",
        "count",
        "--granularity",
        "day",
        "--start",
        "2026-03-01",
        "--end",
        "2026-03-02",
    ]))
    .await
    .expect("staging metrics succeeds");

    assert!(prod.records().iter().any(|record| {
        record.path == format!("/v1/projects/{}/keys", prod.project_id)
            && record.authorization.as_deref() == Some("Bearer prod-token")
    }));
    assert!(prod.records().iter().any(|record| {
        record.path == "/v1/metrics/sessions"
            && record.fantasma_key.as_deref() == Some("prod-read-secret")
    }));
    assert!(staging.records().iter().any(|record| {
        record.path == format!("/fantasma/v1/projects/{}/keys", staging.project_id)
            && record.authorization.as_deref() == Some("Bearer staging-token")
    }));
    assert!(staging.records().iter().any(|record| {
        record.path == "/fantasma/v1/metrics/sessions"
            && record.fantasma_key.as_deref() == Some("staging-read-secret")
    }));
}

async fn list_projects(
    State(state): State<ServerState>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = auth_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "GET",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    (
        StatusCode::OK,
        Json(json!({
            "projects": [
                {
                    "id": state.project_id,
                    "name": "CLI Project",
                    "state": "active",
                    "created_at": "2026-03-13T12:00:00Z"
                }
            ]
        })),
    )
        .into_response()
}

async fn create_project(
    State(state): State<ServerState>,
    original_uri: OriginalUri,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    if let Some(status) = auth_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "POST",
        original_uri.path(),
        original_uri.query(),
        headers,
        Some(body),
    );
    (
        StatusCode::CREATED,
        Json(json!({
            "project": {
                "id": state.project_id,
                "name": "CLI Project",
                "state": "active",
                "created_at": "2026-03-13T12:00:00Z"
            },
            "ingest_key": {
                "id": Uuid::new_v4(),
                "name": "ios-sdk",
                "kind": "ingest",
                "prefix": "fg_ing_",
                "secret": "fg_ing_created",
                "created_at": "2026-03-13T12:00:00Z",
                "revoked_at": Value::Null
            }
        })),
    )
        .into_response()
}

async fn create_key(
    State(state): State<ServerState>,
    Path(_project_id): Path<Uuid>,
    original_uri: OriginalUri,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    if let Some(status) = auth_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "POST",
        original_uri.path(),
        original_uri.query(),
        headers,
        Some(body.clone()),
    );
    let kind = body
        .get("kind")
        .and_then(Value::as_str)
        .expect("key kind present");
    let (key_id, prefix, secret) = if kind == "ingest" {
        (Uuid::new_v4(), "fg_ing_", "fg_ing_created")
    } else {
        (state.read_key_id, "fg_rd_", state.read_key_secret.as_str())
    };
    (
        StatusCode::CREATED,
        Json(json!({
            "key": {
                "id": key_id,
                "name": body["name"],
                "kind": kind,
                "prefix": prefix,
                "secret": secret,
                "created_at": "2026-03-13T12:00:00Z",
                "revoked_at": Value::Null
            }
        })),
    )
        .into_response()
}

async fn list_keys(
    State(state): State<ServerState>,
    Path(_project_id): Path<Uuid>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = auth_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "GET",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    (
        StatusCode::OK,
        Json(json!({
            "keys": [
                {
                    "id": state.read_key_id,
                    "name": "cli-read",
                    "kind": "read",
                    "prefix": "fg_rd_",
                    "created_at": "2026-03-13T12:00:00Z",
                    "revoked_at": Value::Null
                }
            ]
        })),
    )
        .into_response()
}

async fn session_metrics(
    State(state): State<ServerState>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = project_key_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    let metric = original_uri
        .query()
        .and_then(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .find_map(|(key, value)| (key == "metric").then(|| value.into_owned()))
        })
        .unwrap_or_else(|| "count".to_owned());
    record_request(
        &state,
        "GET",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    if metric == "active_installs" {
        (
            StatusCode::OK,
            Json(json!({
                "metric": "active_installs",
                "start": "2026-03-01",
                "end": "2026-03-17",
                "interval": "week",
                "series": [
                    {
                        "dimensions": {},
                        "points": [
                            {
                                "start": "2026-03-01",
                                "end": "2026-03-17",
                                "value": 9
                            }
                        ]
                    }
                ]
            })),
        )
            .into_response()
    } else {
        (
            StatusCode::OK,
            Json(json!({
                "metric": metric,
                "granularity": "day",
                "series": [
                    {
                        "dimensions": {},
                        "points": [
                            {
                                "bucket": "2026-03-01",
                                "value": 9
                            }
                        ]
                    }
                ]
            })),
        )
            .into_response()
    }
}

async fn live_installs(
    State(state): State<ServerState>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = project_key_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "GET",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    (
        StatusCode::OK,
        Json(json!({
            "metric": "live_installs",
            "window_seconds": 120,
            "as_of": "2026-03-17T12:34:56Z",
            "value": 9
        })),
    )
        .into_response()
}

async fn event_metrics(
    State(state): State<ServerState>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = project_key_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "GET",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    (
        StatusCode::OK,
        Json(json!({
            "metric": "count",
            "granularity": "day",
            "series": [
                {
                    "dimensions": { "platform": "ios" },
                    "points": [
                        {
                            "bucket": "2026-03-01",
                            "value": 4
                        }
                    ]
                }
            ]
        })),
    )
        .into_response()
}

async fn top_events(
    State(state): State<ServerState>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = project_key_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "GET",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    (
        StatusCode::OK,
        Json(json!({
            "events": [
                {
                    "name": "app_open",
                    "count": 12
                },
                {
                    "name": "button_pressed",
                    "count": 4
                }
            ]
        })),
    )
        .into_response()
}

async fn event_catalog(
    State(state): State<ServerState>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = project_key_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "GET",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    (
        StatusCode::OK,
        Json(json!({
            "events": [
                {
                    "name": "app_open",
                    "last_seen_at": "2026-03-02T10:15:00Z"
                },
                {
                    "name": "button_pressed",
                    "last_seen_at": "2026-03-01T08:00:00Z"
                }
            ]
        })),
    )
        .into_response()
}

async fn usage_events(
    State(state): State<ServerState>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = auth_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "GET",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    (
        StatusCode::OK,
        Json(json!({
            "start": "2026-03-01",
            "end": "2026-03-31",
            "total_events_processed": 12,
            "projects": [UsageProjectEvents {
                project: ProjectSummary {
                    id: state.project_id,
                    name: "Usage Project".to_owned(),
                    created_at: chrono::DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
                        .expect("created_at")
                        .with_timezone(&chrono::Utc),
                },
                events_processed: 12,
            }],
            "api_revision": "2026-03-19",
        })),
    )
        .into_response()
}

async fn revoke_key(
    State(state): State<ServerState>,
    Path((_project_id, _key_id)): Path<(Uuid, Uuid)>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = auth_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "DELETE",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    StatusCode::NO_CONTENT.into_response()
}

async fn delete_project(
    State(state): State<ServerState>,
    Path(_project_id): Path<Uuid>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = auth_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "DELETE",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "deletion": deletion_payload(&state, "project_purge", Value::Null)
        })),
    )
        .into_response()
}

async fn list_project_deletions(
    State(state): State<ServerState>,
    Path(_project_id): Path<Uuid>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = auth_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "GET",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    (
        StatusCode::OK,
        Json(json!({
            "deletions": [deletion_payload(&state, "project_purge", Value::Null)]
        })),
    )
        .into_response()
}

async fn get_project_deletion(
    State(state): State<ServerState>,
    Path((_project_id, _deletion_id)): Path<(Uuid, Uuid)>,
    original_uri: OriginalUri,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(status) = auth_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "GET",
        original_uri.path(),
        original_uri.query(),
        headers,
        None,
    );
    (
        StatusCode::OK,
        Json(json!({
            "deletion": deletion_payload(&state, "project_purge", Value::Null)
        })),
    )
        .into_response()
}

async fn create_range_deletion(
    State(state): State<ServerState>,
    Path(_project_id): Path<Uuid>,
    original_uri: OriginalUri,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    if let Some(status) = auth_status(&state, &headers) {
        return (status, Json(json!({ "error": status_error(status) }))).into_response();
    }
    record_request(
        &state,
        "POST",
        original_uri.path(),
        original_uri.query(),
        headers,
        Some(body.clone()),
    );
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "deletion": deletion_payload(&state, "range_delete", body)
        })),
    )
        .into_response()
}

fn deletion_payload(state: &ServerState, kind: &str, scope: Value) -> Value {
    json!({
        "id": state.deletion_id,
        "project_id": state.project_id,
        "project_name": "CLI Project",
        "kind": kind,
        "status": "queued",
        "scope": scope,
        "created_at": "2026-03-13T12:00:00Z",
        "started_at": Value::Null,
        "finished_at": Value::Null,
        "error_code": Value::Null,
        "error_message": Value::Null,
        "deleted_raw_events": 0,
        "deleted_sessions": 0,
        "rebuilt_installs": 0,
        "rebuilt_days": 0,
        "rebuilt_hours": 0
    })
}

fn record_request(
    state: &ServerState,
    method: &str,
    path: &str,
    query: Option<&str>,
    headers: HeaderMap,
    body: Option<Value>,
) {
    state
        .records
        .lock()
        .expect("records lock")
        .push(RequestRecord {
            method: method.to_owned(),
            path: path.to_owned(),
            query: query.map(ToOwned::to_owned),
            authorization: headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned),
            fantasma_key: headers
                .get("x-fantasma-key")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned),
            body,
        });
}

fn auth_status(state: &ServerState, headers: &HeaderMap) -> Option<StatusCode> {
    match headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
    {
        Some("Bearer unprocessable-token") => Some(StatusCode::UNPROCESSABLE_ENTITY),
        Some("Bearer missing-route") => Some(StatusCode::NOT_FOUND),
        Some("Bearer server-error") => Some(StatusCode::INTERNAL_SERVER_ERROR),
        Some(value) if value == format!("Bearer {}", state.operator_token) => None,
        _ => Some(StatusCode::UNAUTHORIZED),
    }
}

fn project_key_status(state: &ServerState, headers: &HeaderMap) -> Option<StatusCode> {
    match headers
        .get("x-fantasma-key")
        .and_then(|value| value.to_str().ok())
    {
        Some("unprocessable-key") => Some(StatusCode::UNPROCESSABLE_ENTITY),
        Some("missing-route") => Some(StatusCode::NOT_FOUND),
        Some("server-error") => Some(StatusCode::INTERNAL_SERVER_ERROR),
        Some(value) if value == state.read_key_secret => None,
        _ => Some(StatusCode::UNAUTHORIZED),
    }
}

fn status_error(status: StatusCode) -> &'static str {
    match status {
        StatusCode::UNAUTHORIZED => "unauthorized",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::INTERNAL_SERVER_ERROR => "server_error",
        _ => "unexpected_error",
    }
}
