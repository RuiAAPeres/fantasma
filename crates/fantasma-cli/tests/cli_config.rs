use clap::Parser;
use fantasma_cli::{
    cli::{Cli, Command},
    config::{
        CliConfig, CliError, StoredReadKey, default_config_path_from_env, load_config,
        normalize_api_base_url, resolve_logout_instance, resolve_project_for_command, save_config,
    },
};
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;
use url::Url;
use uuid::Uuid;

#[test]
fn projects_use_requires_uuid_selector() {
    let parsed = Cli::try_parse_from(["fantasma", "projects", "use", "not-a-uuid"]);

    assert!(
        parsed.is_err(),
        "project selection must reject non-uuid values"
    );
}

#[test]
fn logout_without_instance_uses_active_instance() {
    let instance = "prod".to_owned();
    let config = CliConfig {
        active_instance: Some(instance.clone()),
        ..CliConfig::default()
    };

    let resolved = resolve_logout_instance(&config, None).expect("active instance resolves");

    assert_eq!(resolved, instance);
}

#[test]
fn logout_without_instance_errors_when_no_active_instance_exists() {
    let error = resolve_logout_instance(&CliConfig::default(), None).expect_err("missing instance");

    assert!(matches!(error, CliError::NoActiveInstance));
}

#[test]
fn omitted_project_uses_active_project() {
    let project_id = Uuid::new_v4();
    let mut config = CliConfig::default();
    let profile =
        config.ensure_instance_mut("prod", Url::parse("https://api.example.com").unwrap());
    profile.active_project_id = Some(project_id);
    config.active_instance = Some("prod".to_owned());

    let resolved =
        resolve_project_for_command(&config, "prod", None).expect("active project resolves");

    assert_eq!(resolved, project_id);
}

#[test]
fn omitted_project_errors_when_no_active_project_exists() {
    let mut config = CliConfig::default();
    config.ensure_instance_mut("prod", Url::parse("https://api.example.com").unwrap());

    let error =
        resolve_project_for_command(&config, "prod", None).expect_err("missing project errors");

    assert!(matches!(error, CliError::NoActiveProject { .. }));
}

#[test]
fn storing_read_key_replaces_existing_entry_for_project() {
    let project_id = Uuid::new_v4();
    let mut config = CliConfig::default();
    let profile =
        config.ensure_instance_mut("prod", Url::parse("https://api.example.com").unwrap());
    profile.store_read_key(
        project_id,
        StoredReadKey {
            key_id: Uuid::new_v4(),
            name: "first".to_owned(),
            secret: "fg_rd_first".to_owned(),
            created_at: "2026-03-13T10:00:00Z".to_owned(),
        },
    );
    profile.store_read_key(
        project_id,
        StoredReadKey {
            key_id: Uuid::new_v4(),
            name: "second".to_owned(),
            secret: "fg_rd_second".to_owned(),
            created_at: "2026-03-13T11:00:00Z".to_owned(),
        },
    );

    let stored = profile.read_keys.get(&project_id).expect("stored key");

    assert_eq!(profile.read_keys.len(), 1);
    assert_eq!(stored.name, "second");
    assert_eq!(stored.secret, "fg_rd_second");
}

#[test]
fn parser_defaults_logout_to_active_instance_by_omitting_flag() {
    let parsed = Cli::parse_from(["fantasma", "auth", "logout"]);

    match parsed.command {
        Command::Auth(auth) => assert!(auth.instance.is_none()),
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn config_path_prefers_xdg_config_home() {
    let path = default_config_path_from_env(
        Some(PathBuf::from("/tmp/xdg")),
        Some(PathBuf::from("/tmp/home")),
    )
    .expect("config path");

    assert_eq!(path, PathBuf::from("/tmp/xdg/fantasma/config.toml"));
}

#[test]
fn config_path_falls_back_to_home_dot_config() {
    let path =
        default_config_path_from_env(None, Some(PathBuf::from("/tmp/home"))).expect("config path");

    assert_eq!(
        path,
        PathBuf::from("/tmp/home/.config/fantasma/config.toml")
    );
}

#[cfg(unix)]
#[test]
fn save_config_uses_owner_only_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("fantasma").join("config.toml");

    save_config(&path, &CliConfig::default()).expect("save config");

    let metadata = std::fs::metadata(path).expect("metadata");
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
}

#[test]
fn load_config_rejects_unsupported_future_version() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("fantasma").join("config.toml");
    let parent = path.parent().expect("config parent");
    fs::create_dir_all(parent).expect("create config dir");
    fs::write(
        &path,
        r#"
version = 99
active_instance = "prod"

[instances.prod]
api_base_url = "https://api.example.com/"
"#,
    )
    .expect("write config");

    let error = load_config(&path).expect_err("future version should fail");

    assert!(error.to_string().contains("unsupported config version"));
}

#[test]
fn normalize_api_base_url_canonicalizes_root_and_prefixed_paths() {
    let root = normalize_api_base_url(Url::parse("https://host").expect("root url"));
    let root_with_slash =
        normalize_api_base_url(Url::parse("https://host/").expect("root slash url"));
    let prefixed =
        normalize_api_base_url(Url::parse("https://host/fantasma").expect("prefixed url"));

    assert_eq!(root.as_str(), "https://host/");
    assert_eq!(root_with_slash.as_str(), "https://host/");
    assert_eq!(prefixed.as_str(), "https://host/fantasma/");
}

#[test]
fn load_config_normalizes_stored_instance_urls() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("fantasma").join("config.toml");
    let parent = path.parent().expect("config parent");
    fs::create_dir_all(parent).expect("create config dir");
    fs::write(
        &path,
        r#"
version = 1
active_instance = "prod"

[instances.prod]
api_base_url = "https://host/fantasma"
"#,
    )
    .expect("write config");

    let config = load_config(&path).expect("load config");
    let profile = config.instances.get("prod").expect("prod profile");

    assert_eq!(profile.api_base_url.as_str(), "https://host/fantasma/");
}
