use std::path::PathBuf;

use anyhow::{Context, bail};
use chrono::{SecondsFormat, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{
    cli::{
        AuthSubcommand, Cli, Command, EventCatalogArgs, EventMetricArg, EventMetricsArgs,
        InstanceAddArgs, InstanceRemoveArgs, InstanceUseArgs, InstancesSubcommand, KeyCreateArgs,
        KeyKind, KeysSubcommand, LiveInstallsArgs, MetricsSubcommand, ProjectsSubcommand,
        ReadOutputArgs, SessionMetricArg, SessionMetricsArgs, TopEventsArgs,
    },
    config::{
        CliConfig, InstanceProfile, StoredReadKey, ensure_secure_persistence_supported,
        load_config, normalize_api_base_url, resolve_logout_instance, resolve_project_for_command,
        save_config,
    },
};

#[derive(Debug, Clone)]
pub struct App {
    config_path: PathBuf,
    client: Client,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandOutput {
    pub stdout: String,
}

impl App {
    pub fn new(config_path: PathBuf) -> Self {
        Self {
            config_path,
            client: Client::new(),
        }
    }

    pub async fn run(&self, cli: Cli) -> anyhow::Result<CommandOutput> {
        match cli.command {
            Command::Instances(instances) => match instances.command {
                InstancesSubcommand::Add(add) => self.add_instance(add),
                InstancesSubcommand::List(output) => self.list_instances(output),
                InstancesSubcommand::Use(use_instance) => self.use_instance(use_instance),
                InstancesSubcommand::Remove(remove) => self.remove_instance(remove),
            },
            Command::Auth(auth) => match auth.command {
                AuthSubcommand::Login(login) => {
                    let instance = auth
                        .instance
                        .as_deref()
                        .context("missing --instance for auth login")?;
                    self.login(instance, &login.token).await
                }
                AuthSubcommand::Logout => {
                    ensure_secure_persistence_supported()?;
                    let mut config = load_config(&self.config_path)?;
                    let instance = resolve_logout_instance(&config, auth.instance.as_deref())?;
                    let profile = config
                        .instances
                        .get_mut(&instance)
                        .with_context(|| format!("unknown instance '{instance}'"))?;
                    profile.operator_token = None;
                    save_config(&self.config_path, &config)?;
                    Ok(CommandOutput {
                        stdout: format!("logged out instance {instance}"),
                    })
                }
            },
            Command::Projects(projects) => match projects.command {
                ProjectsSubcommand::List(output) => self.list_projects(output).await,
                ProjectsSubcommand::Create(create) => {
                    self.create_project(&create.name, &create.ingest_key_name)
                        .await
                }
                ProjectsSubcommand::Use(selection) => {
                    ensure_secure_persistence_supported()?;
                    let mut config = load_config(&self.config_path)?;
                    let instance = active_instance_name(&config)?.to_owned();
                    let profile = active_instance_mut(&mut config)?;
                    profile.active_project_id = Some(selection.project_id);
                    save_config(&self.config_path, &config)?;
                    Ok(CommandOutput {
                        stdout: format!(
                            "active project for {instance} set to {}",
                            selection.project_id
                        ),
                    })
                }
            },
            Command::Keys(keys) => match keys.command {
                KeysSubcommand::Create(create) => self.create_key(create).await,
                KeysSubcommand::List(list) => self.list_keys(list.project, list.output).await,
                KeysSubcommand::Revoke(revoke) => {
                    self.revoke_key(revoke.project, revoke.key_id).await
                }
            },
            Command::Status(output) => self.status(output).await,
            Command::Metrics(metrics) => match metrics.command {
                MetricsSubcommand::Events(events) => self.metrics_events(events).await,
                MetricsSubcommand::EventsTop(top) => self.metrics_events_top(top).await,
                MetricsSubcommand::EventsCatalog(catalog) => {
                    self.metrics_events_catalog(catalog).await
                }
                MetricsSubcommand::LiveInstalls(live_installs) => {
                    self.metrics_live_installs(live_installs).await
                }
                MetricsSubcommand::Sessions(sessions) => self.metrics_sessions(sessions).await,
            },
        }
    }

    fn add_instance(&self, add: InstanceAddArgs) -> anyhow::Result<CommandOutput> {
        ensure_secure_persistence_supported()?;
        let mut config = load_config(&self.config_path)?;
        let url = normalize_api_base_url(
            Url::parse(&add.url).with_context(|| format!("invalid url '{}'", add.url))?,
        );
        let is_first_instance = config.instances.is_empty();
        match config.instances.get_mut(&add.name) {
            Some(profile) => {
                if profile.api_base_url != url {
                    bail!(
                        "instance '{}' already exists with a different url; remove it first to rebind credentials",
                        add.name
                    );
                }
            }
            None => {
                config
                    .instances
                    .insert(add.name.clone(), InstanceProfile::new(url));
            }
        }
        if is_first_instance || config.active_instance.is_none() {
            config.active_instance = Some(add.name.clone());
        }
        save_config(&self.config_path, &config)?;
        Ok(CommandOutput {
            stdout: format!("saved instance {}", add.name),
        })
    }

    fn list_instances(&self, output: ReadOutputArgs) -> anyhow::Result<CommandOutput> {
        let config = load_config(&self.config_path)?;
        if output.json {
            let instances = config
                .instances
                .iter()
                .map(|(name, profile)| {
                    (
                        name.clone(),
                        serde_json::json!({
                            "api_base_url": profile.api_base_url,
                            "is_active": config.active_instance.as_deref() == Some(name),
                            "has_operator_token": profile.operator_token.is_some(),
                            "active_project_id": profile.active_project_id,
                            "stored_read_key_projects": profile.read_keys.keys().collect::<Vec<_>>(),
                        }),
                    )
                })
                .collect::<serde_json::Map<String, serde_json::Value>>();
            let body = serde_json::to_string_pretty(&serde_json::json!({
                "active_instance": config.active_instance,
                "instances": instances,
            }))
            .context("serialize instances")?;
            return Ok(CommandOutput { stdout: body });
        }

        let mut lines = Vec::new();
        for (name, profile) in &config.instances {
            let active = if config.active_instance.as_deref() == Some(name) {
                " (active)"
            } else {
                ""
            };
            lines.push(format!("{name}\t{}{}", profile.api_base_url, active));
        }
        Ok(CommandOutput {
            stdout: lines.join("\n"),
        })
    }

    fn use_instance(&self, use_instance: InstanceUseArgs) -> anyhow::Result<CommandOutput> {
        ensure_secure_persistence_supported()?;
        let mut config = load_config(&self.config_path)?;
        if !config.instances.contains_key(&use_instance.name) {
            bail!("unknown instance '{}'", use_instance.name);
        }
        config.active_instance = Some(use_instance.name.clone());
        save_config(&self.config_path, &config)?;
        Ok(CommandOutput {
            stdout: format!("active instance set to {}", use_instance.name),
        })
    }

    fn remove_instance(&self, remove: InstanceRemoveArgs) -> anyhow::Result<CommandOutput> {
        ensure_secure_persistence_supported()?;
        let mut config = load_config(&self.config_path)?;
        if config.instances.remove(&remove.name).is_none() {
            bail!("unknown instance '{}'", remove.name);
        }
        if config.active_instance.as_deref() == Some(remove.name.as_str()) {
            config.active_instance = None;
        }
        save_config(&self.config_path, &config)?;
        Ok(CommandOutput {
            stdout: format!("removed instance {}", remove.name),
        })
    }

    async fn login(&self, instance: &str, token: &str) -> anyhow::Result<CommandOutput> {
        ensure_secure_persistence_supported()?;
        let mut config = load_config(&self.config_path)?;
        let profile = config
            .instances
            .get(instance)
            .with_context(|| format!("unknown instance '{instance}'"))?;

        let url = api_url(&profile.api_base_url, "v1/projects").context("build projects url")?;

        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .context("request operator validation")?;
        ensure_success(response.status(), "validate operator token")?;

        let profile = config
            .instances
            .get_mut(instance)
            .with_context(|| format!("unknown instance '{instance}'"))?;
        profile.operator_token = Some(token.to_owned());
        save_config(&self.config_path, &config)?;

        Ok(CommandOutput {
            stdout: format!("saved operator token for {instance}"),
        })
    }

    async fn list_projects(&self, output: ReadOutputArgs) -> anyhow::Result<CommandOutput> {
        let config = load_config(&self.config_path)?;
        let profile = active_instance(&config)?;
        let token = profile
            .operator_token
            .as_deref()
            .context(
                "operator token is not configured for the active instance; run `fantasma auth login --instance <name> --token <operator-token>`",
            )?;
        let url = api_url(&profile.api_base_url, "v1/projects").context("build projects url")?;
        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .context("list projects")?;
        ensure_success(response.status(), "list projects")?;
        let body = response.text().await.context("read project list body")?;
        if output.json {
            return Ok(CommandOutput { stdout: body });
        }
        let listed: ProjectListResponse =
            serde_json::from_str(&body).context("decode project list body")?;
        let lines = listed
            .projects
            .into_iter()
            .map(|project| format!("{}\t{}\t{}", project.id, project.name, project.created_at))
            .collect::<Vec<_>>();
        Ok(CommandOutput {
            stdout: lines.join("\n"),
        })
    }

    async fn create_project(
        &self,
        name: &str,
        ingest_key_name: &str,
    ) -> anyhow::Result<CommandOutput> {
        let config = load_config(&self.config_path)?;
        let profile = active_instance(&config)?;
        let token = profile
            .operator_token
            .as_deref()
            .context(
                "operator token is not configured for the active instance; run `fantasma auth login --instance <name> --token <operator-token>`",
            )?;
        let url = api_url(&profile.api_base_url, "v1/projects").context("build projects url")?;

        let response = self
            .client
            .post(url)
            .bearer_auth(token)
            .json(&CreateProjectRequest {
                name,
                ingest_key_name,
            })
            .send()
            .await
            .context("create project")?;
        ensure_success(response.status(), "create project")?;

        let created: CreateProjectResponse =
            response.json().await.context("decode project body")?;

        Ok(CommandOutput {
            stdout: format!(
                "project {} created\nproject id: {}\ningest key: {}",
                created.project.name, created.project.id, created.ingest_key.secret
            ),
        })
    }

    async fn list_keys(
        &self,
        project: Option<Uuid>,
        output: ReadOutputArgs,
    ) -> anyhow::Result<CommandOutput> {
        let config = load_config(&self.config_path)?;
        let instance = active_instance_name(&config)?.to_owned();
        let project_id = resolve_project_for_command(&config, &instance, project)?;
        let profile = active_instance(&config)?;
        let token = profile
            .operator_token
            .as_deref()
            .context(
                "operator token is not configured for the active instance; run `fantasma auth login --instance <name> --token <operator-token>`",
            )?;
        let url = api_url(
            &profile.api_base_url,
            &format!("v1/projects/{project_id}/keys"),
        )
        .context("build key list url")?;
        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .context("list project keys")?;
        ensure_success(response.status(), "list project keys")?;
        let body = response.text().await.context("read key list body")?;
        if output.json {
            return Ok(CommandOutput { stdout: body });
        }
        let listed: KeyListResponse =
            serde_json::from_str(&body).context("decode key list body")?;
        let lines = listed
            .keys
            .into_iter()
            .map(|key| format!("{}\t{}\t{}\t{}", key.id, key.kind, key.name, key.prefix))
            .collect::<Vec<_>>();
        Ok(CommandOutput {
            stdout: lines.join("\n"),
        })
    }

    async fn create_key(&self, create: KeyCreateArgs) -> anyhow::Result<CommandOutput> {
        if create.kind == KeyKind::Read {
            ensure_secure_persistence_supported()?;
        }
        let mut config = load_config(&self.config_path)?;
        let instance = active_instance_name(&config)?.to_owned();
        let project_id = resolve_project_for_command(&config, &instance, create.project)?;
        let profile = config
            .instances
            .get(&instance)
            .with_context(|| format!("unknown instance '{instance}'"))?;
        let token = profile
            .operator_token
            .as_deref()
            .context(
                "operator token is not configured for the active instance; run `fantasma auth login --instance <name> --token <operator-token>`",
            )?;
        let url = api_url(
            &profile.api_base_url,
            &format!("v1/projects/{project_id}/keys"),
        )
        .context("build project-key url")?;

        let response = self
            .client
            .post(url)
            .bearer_auth(token)
            .json(&CreateKeyRequest {
                name: &create.name,
                kind: create.kind,
            })
            .send()
            .await
            .context("create project key")?;
        ensure_success(response.status(), "create project key")?;

        let created: CreateKeyResponse = response.json().await.context("decode key body")?;
        if create.kind == KeyKind::Read {
            let profile = config
                .instances
                .get_mut(&instance)
                .with_context(|| format!("unknown instance '{instance}'"))?;
            profile.store_read_key(
                project_id,
                StoredReadKey {
                    key_id: created.key.id,
                    name: create.name.clone(),
                    secret: created.key.secret.clone(),
                    created_at: created.key.created_at.clone(),
                },
            );
            save_config(&self.config_path, &config)?;
        }

        Ok(CommandOutput {
            stdout: format!(
                "{} key {}\nsecret: {}",
                created.key.kind, created.key.id, created.key.secret
            ),
        })
    }

    async fn revoke_key(
        &self,
        project: Option<Uuid>,
        key_id: Uuid,
    ) -> anyhow::Result<CommandOutput> {
        let mut config = load_config(&self.config_path)?;
        let instance = active_instance_name(&config)?.to_owned();
        let project_id = resolve_project_for_command(&config, &instance, project)?;
        let should_remove_local_key = config
            .instances
            .get(&instance)
            .and_then(|profile| profile.read_keys.get(&project_id))
            .is_some_and(|stored| stored.key_id == key_id);
        if should_remove_local_key {
            ensure_secure_persistence_supported()?;
        }
        let profile = config
            .instances
            .get(&instance)
            .with_context(|| format!("unknown instance '{instance}'"))?;
        let token = profile
            .operator_token
            .as_deref()
            .context("operator token is not configured for the active instance")?;
        let url = api_url(
            &profile.api_base_url,
            &format!("v1/projects/{project_id}/keys/{key_id}"),
        )
        .context("build key revoke url")?;
        let response = self
            .client
            .delete(url)
            .bearer_auth(token)
            .send()
            .await
            .context("revoke project key")?;
        ensure_success(response.status(), "revoke project key")?;

        let mut removed_local_key = false;
        if let Some(profile) = config.instances.get_mut(&instance) {
            let should_remove = profile
                .read_keys
                .get(&project_id)
                .is_some_and(|stored| stored.key_id == key_id);
            if should_remove {
                profile.read_keys.remove(&project_id);
                removed_local_key = true;
            }
        }
        if removed_local_key {
            save_config(&self.config_path, &config)?;
        }

        Ok(CommandOutput {
            stdout: format!("revoked key {key_id}"),
        })
    }

    async fn status(&self, output: ReadOutputArgs) -> anyhow::Result<CommandOutput> {
        let config = load_config(&self.config_path)?;
        let mut lines = Vec::new();
        let mut api_base_url = None;
        let mut active_project_id = None;
        let mut operator_status = "missing".to_owned();
        let mut read_key_status = "not checked".to_owned();

        match config.active_instance.as_deref() {
            Some(instance_name) => {
                lines.push(format!("active instance: {instance_name}"));
                if let Some(profile) = config.instances.get(instance_name) {
                    api_base_url = Some(profile.api_base_url.clone());
                    active_project_id = profile.active_project_id;
                    lines.push(format!("api base url: {}", profile.api_base_url));
                    match profile.active_project_id {
                        Some(project_id) => lines.push(format!("active project: {project_id}")),
                        None => lines.push("active project: missing".to_owned()),
                    }

                    if let Some(token) = profile.operator_token.as_deref() {
                        let projects_url = api_url(&profile.api_base_url, "v1/projects")
                            .context("build projects url")?;
                        operator_status = match self
                            .client
                            .get(projects_url)
                            .bearer_auth(token)
                            .send()
                            .await
                        {
                            Ok(response) => diagnostic_status(response.status()).to_owned(),
                            Err(_) => "unreachable".to_owned(),
                        };
                    }

                    if let Some(project_id) = profile.active_project_id {
                        if let Some(read_key) = profile.read_keys.get(&project_id) {
                            let today = Utc::now().date_naive();
                            let mut url = api_url(&profile.api_base_url, "v1/metrics/sessions")
                                .context("build sessions metrics url")?;
                            {
                                let mut query = url.query_pairs_mut();
                                query.append_pair("metric", "count");
                                query.append_pair("granularity", "day");
                                query.append_pair("start", &today.to_string());
                                query.append_pair("end", &today.to_string());
                            }
                            read_key_status = match self
                                .client
                                .get(url)
                                .header("x-fantasma-key", &read_key.secret)
                                .send()
                                .await
                            {
                                Ok(response) => diagnostic_status(response.status()).to_owned(),
                                Err(_) => "unreachable".to_owned(),
                            };
                        } else {
                            read_key_status = "missing".to_owned();
                        }
                    }
                } else {
                    lines.push("api base url: missing".to_owned());
                    lines.push("active project: missing".to_owned());
                }
            }
            None => {
                lines.push("active instance: missing".to_owned());
                lines.push("api base url: missing".to_owned());
                lines.push("active project: missing".to_owned());
            }
        }
        lines.push(format!("operator token: {operator_status}"));
        lines.push(format!("read key: {read_key_status}"));

        if output.json {
            return Ok(CommandOutput {
                stdout: serde_json::to_string_pretty(&serde_json::json!({
                    "active_instance": config.active_instance,
                    "api_base_url": api_base_url,
                    "active_project_id": active_project_id,
                    "operator_token_status": operator_status,
                    "read_key_status": read_key_status,
                }))
                .context("serialize status output")?,
            });
        }

        Ok(CommandOutput {
            stdout: lines.join("\n"),
        })
    }

    async fn metrics_sessions(
        &self,
        sessions: SessionMetricsArgs,
    ) -> anyhow::Result<CommandOutput> {
        validate_session_metrics_args(&sessions)?;
        let body = self
            .metrics_request(MetricsRequest {
                path: "/v1/metrics/sessions",
                metric: sessions.metric.as_str(),
                granularity: sessions.granularity.as_str(),
                start: &sessions.start,
                end: &sessions.end,
                event: None,
                filters: &sessions.filters,
                group_by: &sessions.group_by,
            })
            .await?;
        render_metrics_output(body, sessions.output.json)
    }

    async fn metrics_events(&self, events: EventMetricsArgs) -> anyhow::Result<CommandOutput> {
        validate_event_metrics_args(&events)?;
        let body = self
            .metrics_request(MetricsRequest {
                path: "/v1/metrics/events",
                metric: events.metric.as_str(),
                granularity: events.granularity.as_str(),
                start: &events.start,
                end: &events.end,
                event: events.event,
                filters: &events.filters,
                group_by: &events.group_by,
            })
            .await?;
        render_metrics_output(body, events.output.json)
    }

    async fn metrics_events_top(&self, top: TopEventsArgs) -> anyhow::Result<CommandOutput> {
        let body = self
            .project_key_request(
                "/v1/metrics/events/top",
                &top.start,
                &top.end,
                &top.filters,
                &[("limit", top.limit.to_string())],
            )
            .await?;
        render_top_events_output(body, top.output.json)
    }

    async fn metrics_events_catalog(
        &self,
        catalog: EventCatalogArgs,
    ) -> anyhow::Result<CommandOutput> {
        let body = self
            .project_key_request(
                "/v1/metrics/events/catalog",
                &catalog.start,
                &catalog.end,
                &catalog.filters,
                &[],
            )
            .await?;
        render_event_catalog_output(body, catalog.output.json)
    }

    async fn metrics_live_installs(
        &self,
        live_installs: LiveInstallsArgs,
    ) -> anyhow::Result<CommandOutput> {
        let body = self.project_key_get("/v1/metrics/live_installs").await?;
        render_current_metric_output(body, live_installs.output.json)
    }

    async fn metrics_request(&self, request: MetricsRequest<'_>) -> anyhow::Result<String> {
        let config = load_config(&self.config_path)?;
        let profile = active_instance(&config)?;
        let project_id = profile
            .active_project_id
            .context(
                "active project is not configured for the active instance; run `fantasma projects use <project-id>`",
            )?;
        let read_key = profile
            .read_keys
            .get(&project_id)
            .context(
                "read key is not configured for the active project; run `fantasma keys create --kind read --name <key-name>`",
            )?;
        let parsed_filters = parse_filters(request.filters)?;
        let mut url = api_url(&profile.api_base_url, request.path).context("build metrics url")?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("metric", request.metric);
            query.append_pair("granularity", request.granularity);
            query.append_pair("start", request.start);
            query.append_pair("end", request.end);
            if let Some(event) = request.event.as_deref() {
                query.append_pair("event", event);
            }
            for (key, value) in &parsed_filters {
                query.append_pair(key, value);
            }
            for dimension in request.group_by {
                query.append_pair("group_by", dimension);
            }
        }
        let response = self
            .client
            .get(url)
            .header("x-fantasma-key", &read_key.secret)
            .send()
            .await
            .context("load metrics")?;
        ensure_success(response.status(), "load metrics")?;
        response.text().await.context("read metrics body")
    }

    async fn project_key_get(&self, path: &str) -> anyhow::Result<String> {
        let config = load_config(&self.config_path)?;
        let profile = active_instance(&config)?;
        let project_id = profile
            .active_project_id
            .context(
                "active project is not configured for the active instance; run `fantasma projects use <project-id>`",
            )?;
        let read_key = profile
            .read_keys
            .get(&project_id)
            .context(
                "read key is not configured for the active project; run `fantasma keys create --kind read --name <key-name>`",
            )?;
        let url = api_url(&profile.api_base_url, path).context("build current metric url")?;
        let response = self
            .client
            .get(url)
            .header("x-fantasma-key", &read_key.secret)
            .send()
            .await
            .context("load current metric")?;
        ensure_success(response.status(), "load current metric")?;
        response.text().await.context("read current metric body")
    }

    async fn project_key_request(
        &self,
        path: &str,
        start: &str,
        end: &str,
        filters: &[String],
        extra_query: &[(&str, String)],
    ) -> anyhow::Result<String> {
        let config = load_config(&self.config_path)?;
        let profile = active_instance(&config)?;
        let project_id = profile
            .active_project_id
            .context(
                "active project is not configured for the active instance; run `fantasma projects use <project-id>`",
            )?;
        let read_key = profile
            .read_keys
            .get(&project_id)
            .context(
                "read key is not configured for the active project; run `fantasma keys create --kind read --name <key-name>`",
            )?;
        let parsed_filters = parse_filters(filters)?;
        let mut url = api_url(&profile.api_base_url, path).context("build event metrics url")?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("start", start);
            query.append_pair("end", end);
            for (key, value) in extra_query {
                query.append_pair(key, value);
            }
            for (key, value) in &parsed_filters {
                query.append_pair(key, value);
            }
        }
        let response = self
            .client
            .get(url)
            .header("x-fantasma-key", &read_key.secret)
            .send()
            .await
            .context("load event discovery")?;
        ensure_success(response.status(), "load event discovery")?;
        response.text().await.context("read event discovery body")
    }
}

struct MetricsRequest<'a> {
    path: &'a str,
    metric: &'a str,
    granularity: &'a str,
    start: &'a str,
    end: &'a str,
    event: Option<String>,
    filters: &'a [String],
    group_by: &'a [String],
}

fn active_instance_name(config: &CliConfig) -> anyhow::Result<&str> {
    config
        .active_instance
        .as_deref()
        .context(
            "no active instance configured; run `fantasma instances add <name> --url <api-base-url>` and `fantasma instances use <name>`",
        )
}

fn active_instance(config: &CliConfig) -> anyhow::Result<&crate::config::InstanceProfile> {
    let instance = active_instance_name(config)?;
    config
        .instances
        .get(instance)
        .with_context(|| format!("unknown instance '{instance}'"))
}

fn active_instance_mut(
    config: &mut CliConfig,
) -> anyhow::Result<&mut crate::config::InstanceProfile> {
    let instance = config
        .active_instance
        .clone()
        .context(
            "no active instance configured; run `fantasma instances add <name> --url <api-base-url>` and `fantasma instances use <name>`",
        )?;
    config
        .instances
        .get_mut(&instance)
        .with_context(|| format!("unknown instance '{instance}'"))
}

fn api_url(base_url: &Url, path: &str) -> anyhow::Result<Url> {
    base_url
        .join(path.trim_start_matches('/'))
        .with_context(|| format!("join '{}' onto {}", path, base_url))
}

fn ensure_success(status: StatusCode, action: &str) -> anyhow::Result<()> {
    if status.is_success() {
        Ok(())
    } else {
        match status {
            StatusCode::UNAUTHORIZED => {
                bail!(
                    "{action} failed with 401 unauthorized; check the configured operator token or read key"
                )
            }
            StatusCode::NOT_FOUND => {
                bail!(
                    "{action} failed with 404 not found; verify the API base URL and target project/key identifiers"
                )
            }
            StatusCode::UNPROCESSABLE_ENTITY => {
                bail!("{action} failed with 422 invalid request; review the command arguments")
            }
            _ => bail!("{action} failed with status {status}"),
        }
    }
}

fn diagnostic_status(status: StatusCode) -> &'static str {
    if status.is_success() {
        "valid"
    } else {
        match status {
            StatusCode::UNAUTHORIZED => "invalid",
            StatusCode::NOT_FOUND => "missing_route",
            status if status.is_server_error() => "server_error",
            _ => "invalid_response",
        }
    }
}

#[derive(Debug, Serialize)]
struct CreateProjectRequest<'a> {
    name: &'a str,
    ingest_key_name: &'a str,
}

#[derive(Debug, Deserialize)]
struct CreateProjectResponse {
    project: ProjectPayload,
    ingest_key: KeyPayload,
}

#[derive(Debug, Deserialize)]
struct ProjectListResponse {
    projects: Vec<ProjectPayload>,
}

#[derive(Debug, Deserialize)]
struct CreateKeyResponse {
    key: KeyPayload,
}

#[derive(Debug, Deserialize)]
struct KeyListResponse {
    keys: Vec<KeyMetadata>,
}

#[derive(Debug, Deserialize)]
struct ProjectPayload {
    id: Uuid,
    name: String,
    created_at: String,
}

#[derive(Debug, Deserialize)]
struct KeyPayload {
    id: Uuid,
    kind: String,
    secret: String,
    created_at: String,
}

#[derive(Debug, Deserialize)]
struct KeyMetadata {
    id: Uuid,
    name: String,
    kind: String,
    prefix: String,
}

#[derive(Debug, Serialize)]
struct CreateKeyRequest<'a> {
    name: &'a str,
    kind: KeyKind,
}

impl Serialize for KeyKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value = match self {
            KeyKind::Ingest => "ingest",
            KeyKind::Read => "read",
        };
        serializer.serialize_str(value)
    }
}

impl EventMetricArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
        }
    }
}

impl SessionMetricArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::DurationTotal => "duration_total",
            Self::NewInstalls => "new_installs",
            Self::ActiveInstalls => "active_installs",
        }
    }
}

fn validate_session_metrics_args(args: &SessionMetricsArgs) -> anyhow::Result<()> {
    match args.metric {
        SessionMetricArg::ActiveInstalls => match args.granularity {
            crate::cli::MetricGranularityArg::Day
            | crate::cli::MetricGranularityArg::Week
            | crate::cli::MetricGranularityArg::Month
            | crate::cli::MetricGranularityArg::Year => Ok(()),
            crate::cli::MetricGranularityArg::Hour => {
                bail!("active_installs does not support hour granularity")
            }
        },
        SessionMetricArg::Count
        | SessionMetricArg::DurationTotal
        | SessionMetricArg::NewInstalls => match args.granularity {
            crate::cli::MetricGranularityArg::Hour | crate::cli::MetricGranularityArg::Day => {
                Ok(())
            }
            crate::cli::MetricGranularityArg::Week
            | crate::cli::MetricGranularityArg::Month
            | crate::cli::MetricGranularityArg::Year => {
                bail!("only active_installs supports week/month/year granularity")
            }
        },
    }
}

fn validate_event_metrics_args(args: &EventMetricsArgs) -> anyhow::Result<()> {
    if args.event.is_none() {
        bail!("count requires --event");
    }

    match args.granularity {
        crate::cli::MetricGranularityArg::Hour | crate::cli::MetricGranularityArg::Day => Ok(()),
        crate::cli::MetricGranularityArg::Week
        | crate::cli::MetricGranularityArg::Month
        | crate::cli::MetricGranularityArg::Year => {
            bail!("event metrics only support hour/day granularity")
        }
    }
}

impl crate::cli::MetricGranularityArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Year => "year",
        }
    }
}

fn render_metrics_output(body: String, json_output: bool) -> anyhow::Result<CommandOutput> {
    if json_output {
        return Ok(CommandOutput { stdout: body });
    }

    let response: fantasma_core::MetricsResponse =
        serde_json::from_str(&body).context("decode metrics body")?;
    let mut lines = Vec::new();
    for series in response.series {
        let dimensions = if series.dimensions.is_empty() {
            "{}".to_owned()
        } else {
            serde_json::to_string(&series.dimensions).context("serialize metric dimensions")?
        };
        for point in series.points {
            lines.push(format!("{dimensions}\t{}\t{}", point.bucket, point.value));
        }
    }
    Ok(CommandOutput {
        stdout: lines.join("\n"),
    })
}

fn render_current_metric_output(body: String, json_output: bool) -> anyhow::Result<CommandOutput> {
    if json_output {
        return Ok(CommandOutput { stdout: body });
    }

    let response: fantasma_core::CurrentMetricResponse =
        serde_json::from_str(&body).context("decode current metric body")?;
    Ok(CommandOutput {
        stdout: format!(
            "{}\t{}\t{}\t{}",
            response.metric,
            response.value,
            response.as_of.to_rfc3339_opts(SecondsFormat::Secs, true),
            response.window_seconds
        ),
    })
}

fn render_top_events_output(body: String, json_output: bool) -> anyhow::Result<CommandOutput> {
    if json_output {
        return Ok(CommandOutput { stdout: body });
    }

    let response: TopEventsResponse =
        serde_json::from_str(&body).context("decode top events body")?;
    let lines = response
        .events
        .into_iter()
        .map(|event| format!("{}\t{}", event.name, event.count))
        .collect::<Vec<_>>();
    Ok(CommandOutput {
        stdout: lines.join("\n"),
    })
}

fn render_event_catalog_output(body: String, json_output: bool) -> anyhow::Result<CommandOutput> {
    if json_output {
        return Ok(CommandOutput { stdout: body });
    }

    let response: EventCatalogResponse =
        serde_json::from_str(&body).context("decode event catalog body")?;
    let lines = response
        .events
        .into_iter()
        .map(|event| format!("{}\t{}", event.name, event.last_seen_at))
        .collect::<Vec<_>>();
    Ok(CommandOutput {
        stdout: lines.join("\n"),
    })
}

fn parse_filters(filters: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    filters
        .iter()
        .map(|filter| {
            let (key, value) = filter
                .split_once('=')
                .with_context(|| format!("invalid filter '{filter}', expected key=value"))?;
            Ok((key.to_owned(), value.to_owned()))
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct TopEventsResponse {
    events: Vec<TopEventRecord>,
}

#[derive(Debug, Deserialize)]
struct TopEventRecord {
    name: String,
    count: u64,
}

#[derive(Debug, Deserialize)]
struct EventCatalogResponse {
    events: Vec<EventCatalogItem>,
}

#[derive(Debug, Deserialize)]
struct EventCatalogItem {
    name: String,
    last_seen_at: String,
}
