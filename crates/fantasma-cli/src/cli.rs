use clap::{Args, Parser, Subcommand, ValueEnum};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "fantasma")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Instances(InstancesCommand),
    Auth(AuthCommand),
    Status(ReadOutputArgs),
    Projects(ProjectsCommand),
    Keys(KeysCommand),
    Metrics(MetricsCommand),
}

#[derive(Debug, Args)]
pub struct InstancesCommand {
    #[command(subcommand)]
    pub command: InstancesSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum InstancesSubcommand {
    Add(InstanceAddArgs),
    List(ReadOutputArgs),
    Use(InstanceUseArgs),
    Remove(InstanceRemoveArgs),
}

#[derive(Debug, Args)]
pub struct InstanceAddArgs {
    pub name: String,
    #[arg(long)]
    pub url: String,
}

#[derive(Debug, Args)]
pub struct InstanceUseArgs {
    pub name: String,
}

#[derive(Debug, Args)]
pub struct InstanceRemoveArgs {
    pub name: String,
}

#[derive(Debug, Args)]
pub struct AuthCommand {
    #[command(subcommand)]
    pub command: AuthSubcommand,
    #[arg(long, global = true)]
    pub instance: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum AuthSubcommand {
    Login(LoginArgs),
    Logout,
}

#[derive(Debug, Args)]
pub struct LoginArgs {
    #[arg(long)]
    pub token: String,
}

#[derive(Debug, Args)]
pub struct ProjectsCommand {
    #[command(subcommand)]
    pub command: ProjectsSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum ProjectsSubcommand {
    List(ReadOutputArgs),
    Create(ProjectCreateArgs),
    Use(ProjectUseArgs),
}

#[derive(Debug, Args)]
pub struct ProjectCreateArgs {
    #[arg(long)]
    pub name: String,
    #[arg(long = "ingest-key-name")]
    pub ingest_key_name: String,
}

#[derive(Debug, Args)]
pub struct ProjectUseArgs {
    pub project_id: Uuid,
}

#[derive(Debug, Args)]
pub struct KeysCommand {
    #[command(subcommand)]
    pub command: KeysSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum KeysSubcommand {
    List(ProjectScopedReadArgs),
    Create(KeyCreateArgs),
    Revoke(KeyRevokeArgs),
}

#[derive(Debug, Args)]
pub struct ProjectScopedArgs {
    #[arg(long)]
    pub project: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct ProjectScopedReadArgs {
    #[arg(long)]
    pub project: Option<Uuid>,
    #[command(flatten)]
    pub output: ReadOutputArgs,
}

#[derive(Debug, Args)]
pub struct KeyCreateArgs {
    #[arg(long)]
    pub kind: KeyKind,
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub project: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
#[value(rename_all = "snake_case")]
pub enum KeyKind {
    Ingest,
    Read,
}

#[derive(Debug, Args)]
pub struct KeyRevokeArgs {
    pub key_id: Uuid,
    #[arg(long)]
    pub project: Option<Uuid>,
}

#[derive(Debug, Args)]
pub struct MetricsCommand {
    #[command(subcommand)]
    pub command: MetricsSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum MetricsSubcommand {
    Events(EventMetricsArgs),
    #[command(name = "events-top")]
    EventsTop(TopEventsArgs),
    #[command(name = "events-catalog")]
    EventsCatalog(EventCatalogArgs),
    #[command(name = "live-installs")]
    LiveInstalls(LiveInstallsArgs),
    Sessions(SessionMetricsArgs),
}

#[derive(Debug, Args)]
#[command(
    after_help = "Examples:\n  fantasma metrics events --event app_open --metric count --granularity day --start 2026-03-01 --end 2026-03-02 --filter plan=pro --group-by provider"
)]
pub struct EventMetricsArgs {
    #[arg(long)]
    pub event: Option<String>,
    #[arg(long)]
    pub metric: EventMetricArg,
    #[arg(long)]
    pub granularity: MetricGranularityArg,
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(
        long = "filter",
        help = "Repeat key=value filters. Filters plus group-by may reference at most 2 distinct dimensions total."
    )]
    pub filters: Vec<String>,
    #[arg(
        long = "group-by",
        help = "Repeat up to twice. Combined with filters, the API accepts at most 2 distinct dimensions total."
    )]
    pub group_by: Vec<String>,
    #[command(flatten)]
    pub output: ReadOutputArgs,
}

#[derive(Debug, Args)]
pub struct TopEventsArgs {
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(long, default_value_t = 10)]
    pub limit: u32,
    #[arg(long = "filter")]
    pub filters: Vec<String>,
    #[command(flatten)]
    pub output: ReadOutputArgs,
}

#[derive(Debug, Args)]
pub struct EventCatalogArgs {
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(long = "filter")]
    pub filters: Vec<String>,
    #[command(flatten)]
    pub output: ReadOutputArgs,
}

#[derive(Debug, Args)]
pub struct LiveInstallsArgs {
    #[command(flatten)]
    pub output: ReadOutputArgs,
}

#[derive(Debug, Args)]
#[command(
    after_help = "Examples:\n  fantasma metrics sessions --metric count --granularity day --start 2026-03-01 --end 2026-03-02 --filter plan=pro --group-by provider\n  fantasma metrics sessions --metric active_installs --granularity week --start 2026-03-02 --end 2026-03-16 --filter plan=pro --group-by provider"
)]
pub struct SessionMetricsArgs {
    #[arg(long)]
    pub metric: SessionMetricArg,
    #[arg(long)]
    pub granularity: MetricGranularityArg,
    #[arg(long)]
    pub start: String,
    #[arg(long)]
    pub end: String,
    #[arg(
        long = "filter",
        help = "Repeat key=value filters. Filters plus group-by may reference at most 2 distinct dimensions total."
    )]
    pub filters: Vec<String>,
    #[arg(
        long = "group-by",
        help = "Repeat up to twice. Combined with filters, the API accepts at most 2 distinct dimensions total."
    )]
    pub group_by: Vec<String>,
    #[command(flatten)]
    pub output: ReadOutputArgs,
}

#[derive(Debug, Args, Default, Clone, Copy)]
pub struct ReadOutputArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
#[value(rename_all = "snake_case")]
pub enum EventMetricArg {
    Count,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
#[value(rename_all = "snake_case")]
pub enum SessionMetricArg {
    Count,
    DurationTotal,
    NewInstalls,
    ActiveInstalls,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
#[value(rename_all = "snake_case")]
pub enum MetricGranularityArg {
    Hour,
    Day,
    Week,
    Month,
    Year,
}
