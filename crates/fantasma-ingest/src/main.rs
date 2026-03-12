use std::env;

use fantasma_store::{
    BootstrapConfig, DatabaseConfig, connect, ensure_local_project, run_migrations,
};
use tracing::info;
use uuid::Uuid;

#[tokio::main]
async fn main() {
    init_tracing();

    let bind_address =
        env::var("FANTASMA_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8081".to_owned());
    let database_url = env::var("FANTASMA_DATABASE_URL").expect("FANTASMA_DATABASE_URL");
    let project_id = env::var("FANTASMA_PROJECT_ID")
        .ok()
        .and_then(|value| Uuid::parse_str(&value).ok())
        .unwrap_or_else(default_project_id);
    let project_name = env::var("FANTASMA_PROJECT_NAME")
        .unwrap_or_else(|_| "Local Development Project".to_owned());
    let ingest_key = env::var("FANTASMA_INGEST_KEY").ok();
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
    let app = fantasma_ingest::app(pool);

    let listener = tokio::net::TcpListener::bind(&bind_address)
        .await
        .expect("bind ingest listener");
    let local_addr = listener.local_addr().expect("read local address");
    info!("fantasma-ingest listening on {local_addr}");

    axum::serve(listener, app)
        .await
        .expect("serve ingest application");
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            env::var("FANTASMA_LOG_LEVEL")
                .unwrap_or_else(|_| "fantasma_ingest=info,tower_http=info".to_owned()),
        )
        .try_init();
}

fn default_project_id() -> Uuid {
    Uuid::from_u128(0x9bad8b88_5e7a_44ed_98ce_4cf9ddde713a)
}
