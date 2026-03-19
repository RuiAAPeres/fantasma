use std::path::PathBuf;

use anyhow::{Context, bail};
use chrono::{SecondsFormat, Utc};
use fantasma_core::UsageEventsResponse;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{
    cli::{
        AuthSubcommand, Cli, Command, EventCatalogArgs, EventMetricArg, EventMetricsArgs,
        InstanceAddArgs, InstanceRemoveArgs, InstanceUseArgs, InstancesSubcommand, KeyCreateArgs,
        KeyKind, KeysSubcommand, LiveInstallsArgs, MetricsSubcommand, ProjectDeletionGetArgs,
        ProjectDeletionsSubcommand, ProjectRangeDeleteArgs, ProjectsSubcommand, ReadOutputArgs,
        SessionMetricArg, SessionMetricsArgs, TopEventsArgs, UsageEventsArgs, UsageSubcommand,
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
                ProjectsSubcommand::Delete(delete) => self.delete_project(delete.project).await,
                ProjectsSubcommand::Deletions(deletions) => match deletions.command {
                    ProjectDeletionsSubcommand::List(list) => {
                        self.list_project_deletions(list.project, list.output).await
                    }
                    ProjectDeletionsSubcommand::Get(get) => self.get_project_deletion(get).await,
                    ProjectDeletionsSubcommand::Range(range) => {
                        self.create_range_deletion(range).await
                    }
                },
            },
            Command::Usage(usage) => match usage.command {
                UsageSubcommand::Events(events) => self.usage_events(events).await,
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
            .map(|project| {
                format!(
                    "{}\t{}\t{}\t{}",
                    project.id, project.name, project.state, project.created_at
                )
            })
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

    async fn usage_events(&self, events: UsageEventsArgs) -> anyhow::Result<CommandOutput> {
        let config = load_config(&self.config_path)?;
        let profile = active_instance(&config)?;
        let token = profile
            .operator_token
            .as_deref()
            .context(
                "operator token is not configured for the active instance; run `fantasma auth login --instance <name> --token <operator-token>`",
            )?;
        let url =
            api_url(&profile.api_base_url, "v1/usage/events").context("build usage events url")?;

        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .query(&[
                ("start", events.start.as_str()),
                ("end", events.end.as_str()),
            ])
            .send()
            .await
            .context("load usage events")?;
        ensure_success(response.status(), "load usage events")?;
        let body = response.text().await.context("read usage events body")?;
        if events.output.json {
            return Ok(CommandOutput { stdout: body });
        }

        let response: UsageEventsResponse =
            serde_json::from_str(&body).context("decode usage events body")?;

        Ok(render_usage_events_output(response))
    }

    async fn delete_project(&self, project: Option<Uuid>) -> anyhow::Result<CommandOutput> {
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
        let url = api_url(&profile.api_base_url, &format!("v1/projects/{project_id}"))
            .context("build project delete url")?;
        let response = self
            .client
            .delete(url)
            .bearer_auth(token)
            .send()
            .await
            .context("delete project")?;
        ensure_success(response.status(), "delete project")?;
        let body = response
            .text()
            .await
            .context("read project deletion body")?;
        let deletion: ProjectDeletionEnvelope =
            serde_json::from_str(&body).context("decode project deletion body")?;
        Ok(render_deletion_output(
            deletion.deletion,
            ReadOutputArgs::default(),
            Some("project purge queued"),
            Some(body),
        ))
    }

    async fn list_project_deletions(
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
            &format!("v1/projects/{project_id}/deletions"),
        )
        .context("build project deletion list url")?;
        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .context("list project deletions")?;
        ensure_success(response.status(), "list project deletions")?;
        let body = response
            .text()
            .await
            .context("read project deletion list body")?;
        if output.json {
            return Ok(CommandOutput { stdout: body });
        }
        let listed: ProjectDeletionListResponse =
            serde_json::from_str(&body).context("decode project deletion list body")?;
        let lines = listed
            .deletions
            .into_iter()
            .map(format_deletion_text_line)
            .collect::<Vec<_>>();
        Ok(CommandOutput {
            stdout: lines.join("\n"),
        })
    }

    async fn get_project_deletion(
        &self,
        args: ProjectDeletionGetArgs,
    ) -> anyhow::Result<CommandOutput> {
        let config = load_config(&self.config_path)?;
        let instance = active_instance_name(&config)?.to_owned();
        let project_id = resolve_project_for_command(&config, &instance, args.project)?;
        let profile = active_instance(&config)?;
        let token = profile
            .operator_token
            .as_deref()
            .context(
                "operator token is not configured for the active instance; run `fantasma auth login --instance <name> --token <operator-token>`",
            )?;
        let url = api_url(
            &profile.api_base_url,
            &format!("v1/projects/{project_id}/deletions/{}", args.deletion_id),
        )
        .context("build project deletion url")?;
        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .context("get project deletion")?;
        ensure_success(response.status(), "get project deletion")?;
        let body = response
            .text()
            .await
            .context("read project deletion body")?;
        let deletion: ProjectDeletionEnvelope =
            serde_json::from_str(&body).context("decode project deletion body")?;
        Ok(render_deletion_output(
            deletion.deletion,
            args.output,
            None,
            Some(body),
        ))
    }

    async fn create_range_deletion(
        &self,
        args: ProjectRangeDeleteArgs,
    ) -> anyhow::Result<CommandOutput> {
        let config = load_config(&self.config_path)?;
        let instance = active_instance_name(&config)?.to_owned();
        let project_id = resolve_project_for_command(&config, &instance, args.project)?;
        let profile = active_instance(&config)?;
        let token = profile
            .operator_token
            .as_deref()
            .context(
                "operator token is not configured for the active instance; run `fantasma auth login --instance <name> --token <operator-token>`",
            )?;
        let url = api_url(
            &profile.api_base_url,
            &format!("v1/projects/{project_id}/deletions"),
        )
        .context("build range deletion url")?;
        let response = self
            .client
            .post(url)
            .bearer_auth(token)
            .json(&CreateRangeDeletionRequest {
                start_at: args.start_at,
                end_before: args.end_before,
                event: args.event,
                filters: parse_key_value_pairs(&args.filters, "filter")?,
                properties: parse_key_value_pairs(&args.properties, "property")?,
            })
            .send()
            .await
            .context("create range deletion")?;
        ensure_success(response.status(), "create range deletion")?;
        let body = response.text().await.context("read range deletion body")?;
        let deletion: ProjectDeletionEnvelope =
            serde_json::from_str(&body).context("decode range deletion body")?;
        Ok(render_deletion_output(
            deletion.deletion,
            args.output,
            Some("range deletion queued"),
            Some(body),
        ))
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
        let granularity = match sessions.metric {
            SessionMetricArg::ActiveInstalls => None,
            SessionMetricArg::Count
            | SessionMetricArg::DurationTotal
            | SessionMetricArg::NewInstalls => Some(
                sessions
                    .granularity
                    .expect("validated bucket granularity")
                    .as_str(),
            ),
        };
        let interval = match sessions.metric {
            SessionMetricArg::ActiveInstalls => sessions.interval.map(|value| value.as_str()),
            SessionMetricArg::Count
            | SessionMetricArg::DurationTotal
            | SessionMetricArg::NewInstalls => None,
        };
        let body = self
            .metrics_request(MetricsRequest {
                path: "/v1/metrics/sessions",
                metric: sessions.metric.as_str(),
                granularity,
                interval,
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
                granularity: Some(events.granularity.as_str()),
                interval: None,
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
            if let Some(granularity) = request.granularity {
                query.append_pair("granularity", granularity);
            }
            query.append_pair("start", request.start);
            query.append_pair("end", request.end);
            if let Some(interval) = request.interval {
                query.append_pair("interval", interval);
            }
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
    granularity: Option<&'a str>,
    interval: Option<&'a str>,
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
    state: String,
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
struct CreateRangeDeletionRequest {
    start_at: String,
    end_before: String,
    event: Option<String>,
    filters: std::collections::BTreeMap<String, String>,
    properties: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct ProjectDeletionEnvelope {
    deletion: ProjectDeletionPayload,
}

#[derive(Debug, Deserialize)]
struct ProjectDeletionListResponse {
    deletions: Vec<ProjectDeletionPayload>,
}

#[derive(Debug, Deserialize)]
struct ProjectDeletionPayload {
    id: Uuid,
    project_id: Uuid,
    project_name: String,
    kind: String,
    status: String,
    scope: Option<serde_json::Value>,
    created_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    deleted_raw_events: i64,
    deleted_sessions: i64,
    rebuilt_installs: i64,
    rebuilt_days: i64,
    rebuilt_hours: i64,
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
        SessionMetricArg::ActiveInstalls => {
            if args.granularity.is_some() {
                bail!("active_installs uses --interval instead of --granularity")
            }

            Ok(())
        }
        SessionMetricArg::Count
        | SessionMetricArg::DurationTotal
        | SessionMetricArg::NewInstalls => {
            if args.interval.is_some() {
                bail!("only active_installs supports --interval")
            }

            match args.granularity {
                Some(crate::cli::MetricGranularityArg::Hour)
                | Some(crate::cli::MetricGranularityArg::Day) => Ok(()),
                Some(
                    crate::cli::MetricGranularityArg::Week
                    | crate::cli::MetricGranularityArg::Month
                    | crate::cli::MetricGranularityArg::Year,
                ) => bail!("only active_installs supports week/month/year granularity"),
                None => bail!("session metrics require --granularity"),
            }
        }
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

impl crate::cli::MetricIntervalArg {
    fn as_str(self) -> &'static str {
        match self {
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

    let response: fantasma_core::SessionMetricsReadResponse =
        serde_json::from_str(&body).context("decode metrics body")?;
    let mut lines = Vec::new();
    match response {
        fantasma_core::SessionMetricsReadResponse::Bucketed(response) => {
            for series in response.series {
                let dimensions = if series.dimensions.is_empty() {
                    "{}".to_owned()
                } else {
                    serde_json::to_string(&series.dimensions)
                        .context("serialize metric dimensions")?
                };
                for point in series.points {
                    lines.push(format!("{dimensions}\t{}\t{}", point.bucket, point.value));
                }
            }
        }
        fantasma_core::SessionMetricsReadResponse::ActiveInstalls(response) => {
            lines.push("dimensions\tstart\tend\tvalue".to_owned());
            for series in response.series {
                let dimensions = if series.dimensions.is_empty() {
                    "{}".to_owned()
                } else {
                    serde_json::to_string(&series.dimensions)
                        .context("serialize metric dimensions")?
                };
                for point in series.points {
                    lines.push(format!(
                        "{dimensions}\t{}\t{}\t{}",
                        point.start, point.end, point.value
                    ));
                }
            }
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

fn render_usage_events_output(response: UsageEventsResponse) -> CommandOutput {
    let mut lines = vec![format!(
        "events processed\t{}",
        response.total_events_processed
    )];

    for project_events in response.projects {
        lines.push(format!(
            "{}\t{}\t{}\t{}",
            project_events.project.id,
            project_events.project.name,
            project_events
                .project
                .created_at
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
            project_events.events_processed
        ));
    }

    CommandOutput {
        stdout: lines.join("\n"),
    }
}

fn parse_filters(filters: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    Ok(parse_key_value_pairs(filters, "filter")?
        .into_iter()
        .collect())
}

fn parse_key_value_pairs(
    values: &[String],
    kind: &str,
) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
    values
        .iter()
        .map(|value| {
            let (key, parsed) = value
                .split_once('=')
                .with_context(|| format!("invalid {kind} '{value}', expected key=value"))?;
            Ok((key.to_owned(), parsed.to_owned()))
        })
        .collect()
}

fn format_deletion_text_line(deletion: ProjectDeletionPayload) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}",
        deletion.id, deletion.kind, deletion.status, deletion.project_name, deletion.created_at
    )
}

fn render_deletion_output(
    deletion: ProjectDeletionPayload,
    output: ReadOutputArgs,
    prefix: Option<&str>,
    json_body: Option<String>,
) -> CommandOutput {
    if output.json {
        return CommandOutput {
            stdout: json_body.unwrap_or_else(|| {
                serde_json::to_string(&serde_json::json!({
                    "deletion": {
                        "id": deletion.id,
                        "project_id": deletion.project_id,
                        "project_name": deletion.project_name,
                        "kind": deletion.kind,
                        "status": deletion.status,
                        "scope": deletion.scope,
                        "created_at": deletion.created_at,
                        "started_at": deletion.started_at,
                        "finished_at": deletion.finished_at,
                        "error_code": deletion.error_code,
                        "error_message": deletion.error_message,
                        "deleted_raw_events": deletion.deleted_raw_events,
                        "deleted_sessions": deletion.deleted_sessions,
                        "rebuilt_installs": deletion.rebuilt_installs,
                        "rebuilt_days": deletion.rebuilt_days,
                        "rebuilt_hours": deletion.rebuilt_hours
                    }
                }))
                .expect("serialize deletion output")
            }),
        };
    }

    let mut lines = Vec::new();
    if let Some(prefix) = prefix {
        lines.push(prefix.to_owned());
    }
    lines.push(format!("deletion id: {}", deletion.id));
    lines.push(format!("project id: {}", deletion.project_id));
    lines.push(format!("project name: {}", deletion.project_name));
    lines.push(format!("kind: {}", deletion.kind));
    lines.push(format!("status: {}", deletion.status));
    lines.push(format!("created_at: {}", deletion.created_at));
    if let Some(started_at) = deletion.started_at {
        lines.push(format!("started_at: {started_at}"));
    }
    if let Some(finished_at) = deletion.finished_at {
        lines.push(format!("finished_at: {finished_at}"));
    }
    if let Some(error_code) = deletion.error_code {
        lines.push(format!("error_code: {error_code}"));
    }
    if let Some(error_message) = deletion.error_message {
        lines.push(format!("error_message: {error_message}"));
    }
    lines.push(format!(
        "deleted_raw_events: {}",
        deletion.deleted_raw_events
    ));
    lines.push(format!("deleted_sessions: {}", deletion.deleted_sessions));
    lines.push(format!("rebuilt_installs: {}", deletion.rebuilt_installs));
    lines.push(format!("rebuilt_days: {}", deletion.rebuilt_days));
    lines.push(format!("rebuilt_hours: {}", deletion.rebuilt_hours));
    if let Some(scope) = deletion.scope {
        lines.push(format!("scope: {}", scope));
    }

    CommandOutput {
        stdout: lines.join("\n"),
    }
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
