use std::{env, time::Duration};

use fantasma_store::{DatabaseConfig, connect, run_migrations};
use tracing::info;

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            env::var("FANTASMA_LOG_LEVEL").unwrap_or_else(|_| "fantasma_worker=info".to_owned()),
        )
        .try_init();

    let database_url = env::var("FANTASMA_DATABASE_URL").expect("FANTASMA_DATABASE_URL");
    let config = fantasma_worker::WorkerConfig {
        idle_poll_interval: Duration::from_millis(parse_env_u64(
            "FANTASMA_WORKER_IDLE_POLL_INTERVAL_MS",
            250,
        )),
        session_batch_size: parse_env_i64("FANTASMA_WORKER_SESSION_BATCH_SIZE", 1_000),
        event_batch_size: parse_env_i64("FANTASMA_WORKER_EVENT_BATCH_SIZE", 5_000),
        session_incremental_concurrency: parse_env_usize(
            "FANTASMA_WORKER_SESSION_INCREMENTAL_CONCURRENCY",
            8,
        )
        .max(1),
        session_repair_concurrency: parse_env_usize(
            "FANTASMA_WORKER_SESSION_REPAIR_CONCURRENCY",
            2,
        )
        .max(1),
    };
    let mut database_config = DatabaseConfig::new(database_url);
    database_config.max_connections = database_config
        .max_connections
        .max(config.required_db_connections());
    let pool = connect(&database_config).await.expect("connect database");
    run_migrations(&pool).await.expect("migrate database");

    info!(
        idle_poll_interval_ms = config.idle_poll_interval.as_millis() as u64,
        session_batch_size = config.session_batch_size,
        event_batch_size = config.event_batch_size,
        session_incremental_concurrency = config.session_incremental_concurrency,
        session_repair_concurrency = config.session_repair_concurrency,
        max_db_connections = database_config.max_connections,
        "fantasma-worker started"
    );

    fantasma_worker::run_worker(pool, config)
        .await
        .expect("run worker");
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_env_i64(name: &str, default: i64) -> i64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
}

fn parse_env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}
