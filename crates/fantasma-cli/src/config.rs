use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct CliConfig {
    #[serde(default = "current_config_version")]
    pub version: u32,
    pub active_instance: Option<String>,
    #[serde(default)]
    pub instances: BTreeMap<String, InstanceProfile>,
}

impl CliConfig {
    pub fn ensure_instance_mut(&mut self, name: &str, api_base_url: Url) -> &mut InstanceProfile {
        self.instances
            .entry(name.to_owned())
            .or_insert_with(|| InstanceProfile::new(api_base_url))
    }
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            version: current_config_version(),
            active_instance: None,
            instances: BTreeMap::new(),
        }
    }
}

fn current_config_version() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct InstanceProfile {
    pub api_base_url: Url,
    pub operator_token: Option<String>,
    pub active_project_id: Option<Uuid>,
    #[serde(default)]
    pub read_keys: BTreeMap<Uuid, StoredReadKey>,
}

impl InstanceProfile {
    pub fn new(api_base_url: Url) -> Self {
        Self {
            api_base_url: normalize_api_base_url(api_base_url),
            operator_token: None,
            active_project_id: None,
            read_keys: BTreeMap::new(),
        }
    }

    pub fn store_read_key(&mut self, project_id: Uuid, key: StoredReadKey) {
        self.read_keys.insert(project_id, key);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct StoredReadKey {
    pub key_id: Uuid,
    pub name: String,
    pub secret: String,
    pub created_at: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CliError {
    #[error(
        "no active instance configured; run `fantasma instances add <name> --url <api-base-url>` and `fantasma instances use <name>`"
    )]
    NoActiveInstance,
    #[error(
        "no active project configured for instance '{instance}'; run `fantasma projects use <project-id>`"
    )]
    NoActiveProject { instance: String },
    #[error("unknown instance '{0}'; run `fantasma instances list`")]
    UnknownInstance(String),
}

pub fn resolve_logout_instance(
    config: &CliConfig,
    instance: Option<&str>,
) -> Result<String, CliError> {
    match instance {
        Some(instance) => Ok(instance.to_owned()),
        None => config
            .active_instance
            .clone()
            .ok_or(CliError::NoActiveInstance),
    }
}

pub fn resolve_project_for_command(
    config: &CliConfig,
    instance: &str,
    project: Option<Uuid>,
) -> Result<Uuid, CliError> {
    if let Some(project) = project {
        return Ok(project);
    }

    let profile = config
        .instances
        .get(instance)
        .ok_or_else(|| CliError::UnknownInstance(instance.to_owned()))?;

    profile
        .active_project_id
        .ok_or_else(|| CliError::NoActiveProject {
            instance: instance.to_owned(),
        })
}

pub fn load_config(path: &Path) -> anyhow::Result<CliConfig> {
    if !path.exists() {
        return Ok(CliConfig::default());
    }

    let contents =
        fs::read_to_string(path).with_context(|| format!("read config file {}", path.display()))?;
    let mut config: CliConfig = toml::from_str(&contents)
        .with_context(|| format!("parse config file {}", path.display()))?;
    validate_config_version(config.version)?;
    for profile in config.instances.values_mut() {
        profile.api_base_url = normalize_api_base_url(profile.api_base_url.clone());
    }
    Ok(config)
}

pub fn save_config(path: &Path, config: &CliConfig) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create config directory {}", parent.display()))?;
    }
    let contents = toml::to_string_pretty(config).context("serialize config")?;
    write_config_file(path, &contents)?;
    Ok(())
}

pub fn default_config_path() -> anyhow::Result<PathBuf> {
    default_config_path_from_env(
        std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    )
}

pub fn default_config_path_from_env(
    xdg_config_home: Option<PathBuf>,
    home: Option<PathBuf>,
) -> anyhow::Result<PathBuf> {
    let config_root = if let Some(xdg_config_home) = xdg_config_home {
        xdg_config_home
    } else {
        home.context("HOME is not set and XDG_CONFIG_HOME is missing")?
            .join(".config")
    };
    Ok(config_root.join("fantasma").join("config.toml"))
}

pub fn ensure_secure_persistence_supported() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        Ok(())
    }

    #[cfg(not(unix))]
    {
        anyhow::bail!(
            "secure config persistence is currently supported only on unix-like platforms"
        )
    }
}

#[cfg(unix)]
fn write_config_file(path: &Path, contents: &str) -> anyhow::Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    use std::time::{SystemTime, UNIX_EPOCH};

    let parent = path
        .parent()
        .with_context(|| format!("resolve config parent for {}", path.display()))?;
    let file_name = path
        .file_name()
        .with_context(|| format!("resolve config filename for {}", path.display()))?
        .to_string_lossy();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("read system clock for config temp file")?
        .as_nanos();
    let temp_path = parent.join(format!(".{file_name}.{nonce}.tmp"));

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&temp_path)
        .with_context(|| format!("write config file {}", temp_path.display()))?;
    file.write_all(contents.as_bytes())
        .with_context(|| format!("write config file {}", temp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync config file {}", temp_path.display()))?;
    if let Err(error) = maybe_fail_before_rename() {
        cleanup_temp_config_file(&temp_path);
        return Err(error);
    }
    fs::rename(&temp_path, path)
        .with_context(|| format!("replace config file {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("set config file permissions {}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn write_config_file(path: &Path, contents: &str) -> anyhow::Result<()> {
    let _ = (path, contents);
    anyhow::bail!("secure config persistence is currently supported only on unix-like platforms")
}

fn validate_config_version(version: u32) -> anyhow::Result<()> {
    if version == current_config_version() {
        Ok(())
    } else {
        anyhow::bail!(
            "unsupported config version {version}; expected {}",
            current_config_version()
        )
    }
}

pub fn normalize_api_base_url(mut api_base_url: Url) -> Url {
    let normalized_path = match api_base_url.path() {
        "" | "/" => "/".to_owned(),
        path if path.ends_with('/') => path.to_owned(),
        path => format!("{path}/"),
    };
    api_base_url.set_path(&normalized_path);
    api_base_url
}

#[cfg(unix)]
fn cleanup_temp_config_file(path: &Path) {
    let _ = fs::remove_file(path);
}

#[cfg(unix)]
fn maybe_fail_before_rename() -> anyhow::Result<()> {
    #[cfg(test)]
    if std::env::var_os("FANTASMA_CLI_TEST_FAIL_BEFORE_RENAME").is_some() {
        anyhow::bail!("config rename failed due to injected test failpoint");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{CliConfig, save_config};
    use std::{
        fs,
        path::Path,
        sync::{LazyLock, Mutex},
    };
    use tempfile::tempdir;

    static SAVE_CONFIG_ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[cfg(unix)]
    #[test]
    fn save_config_keeps_existing_file_when_atomic_replace_fails() {
        let _guard = SAVE_CONFIG_ENV_LOCK.lock().expect("env lock");
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("fantasma").join("config.toml");
        let parent = path.parent().expect("parent");
        fs::create_dir_all(parent).expect("create dir");
        fs::write(&path, "version = 1\nactive_instance = \"prod\"\n").expect("seed config");
        unsafe {
            std::env::set_var("FANTASMA_CLI_TEST_FAIL_BEFORE_RENAME", "1");
        }

        let result = save_config(&path, &CliConfig::default());

        unsafe {
            std::env::remove_var("FANTASMA_CLI_TEST_FAIL_BEFORE_RENAME");
        }

        assert!(
            result.is_err(),
            "save should fail when the rename failpoint is enabled"
        );
        let contents = fs::read_to_string(&path).expect("read original config");
        assert_eq!(contents, "version = 1\nactive_instance = \"prod\"\n");
        assert_eq!(count_temp_config_files(parent), 0);
    }

    #[cfg(unix)]
    #[test]
    fn save_config_does_not_leave_temp_files_after_successive_saves() {
        let _guard = SAVE_CONFIG_ENV_LOCK.lock().expect("env lock");
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("fantasma").join("config.toml");
        let parent = path.parent().expect("parent");
        fs::create_dir_all(parent).expect("create dir");

        save_config(&path, &CliConfig::default()).expect("first save");
        save_config(&path, &CliConfig::default()).expect("second save");

        assert_eq!(count_temp_config_files(parent), 0);
    }

    fn count_temp_config_files(dir: &Path) -> usize {
        fs::read_dir(dir)
            .expect("read dir")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .map(|name| name.to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".config.toml.") && name.ends_with(".tmp"))
            .count()
    }
}
