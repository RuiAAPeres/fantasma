use std::{env, time::Duration};

use fantasma_store::{
    BootstrapConfig, DatabaseConfig, connect, ensure_local_project, run_migrations,
};
use tracing::{error, info};
use uuid::Uuid;

const DEFAULT_BATCH_SIZE: i64 = 500;

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            env::var("FANTASMA_LOG_LEVEL").unwrap_or_else(|_| "fantasma_worker=info".to_owned()),
        )
        .try_init();

    let database_url = env::var("FANTASMA_DATABASE_URL").expect("FANTASMA_DATABASE_URL");
    let project_id = env::var("FANTASMA_PROJECT_ID")
        .ok()
        .and_then(|value| Uuid::parse_str(&value).ok())
        .unwrap_or_else(default_project_id);
    let project_name = env::var("FANTASMA_PROJECT_NAME")
        .unwrap_or_else(|_| "Local Development Project".to_owned());
    let ingest_key = env::var("FANTASMA_INGEST_KEY").ok();
    let poll_interval = env::var("FANTASMA_WORKER_POLL_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5_000);
    let batch_size = env::var("FANTASMA_WORKER_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(DEFAULT_BATCH_SIZE);
    let pool = connect(&DatabaseConfig::new(database_url))
        .await
        .expect("connect database");
    run_migrations(&pool).await.expect("migrate database");
    ensure_local_project(
        &pool,
        Some(&BootstrapConfig {
            project_id,
            project_name,
            ingest_key,
        }),
    )
    .await
    .expect("seed database");

    info!("fantasma-worker started with {poll_interval}ms poll interval");

    loop {
        match fantasma_worker::process_session_batch(&pool, batch_size).await {
            Ok(processed) if processed > 0 => {
                info!("processed {processed} raw events into sessions");
            }
            Ok(_) => {
                info!("worker tick: no new raw events");
            }
            Err(err) => {
                error!(?err, "failed to process session batch");
            }
        }
        tokio::time::sleep(Duration::from_millis(poll_interval)).await;
    }
}

fn default_project_id() -> Uuid {
    Uuid::from_u128(0x9bad8b88_5e7a_44ed_98ce_4cf9ddde713a)
}
