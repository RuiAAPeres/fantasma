use std::{env, time::Duration};

use tracing::info;

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            env::var("FANTASMA_LOG_LEVEL").unwrap_or_else(|_| "fantasma_worker=info".to_owned()),
        )
        .try_init();

    let poll_interval = env::var("FANTASMA_WORKER_POLL_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5_000);

    info!("fantasma-worker started with {poll_interval}ms poll interval");

    loop {
        info!("worker tick: awaiting aggregate implementation");
        tokio::time::sleep(Duration::from_millis(poll_interval)).await;
    }
}
