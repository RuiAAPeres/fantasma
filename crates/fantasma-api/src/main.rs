use std::{env, sync::Arc};

use fantasma_auth::StaticAdminAuthorizer;
use fantasma_store::{DatabaseConfig, connect, run_migrations};
use tracing::info;

#[tokio::main]
async fn main() {
    init_tracing();

    let bind_address =
        env::var("FANTASMA_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8082".to_owned());
    let database_url = env::var("FANTASMA_DATABASE_URL").expect("FANTASMA_DATABASE_URL");
    let pool = connect(&DatabaseConfig::new(database_url))
        .await
        .expect("connect database");
    run_migrations(&pool).await.expect("migrate database");

    let app = fantasma_api::app(pool, Arc::new(load_authorizer()));

    let listener = tokio::net::TcpListener::bind(&bind_address)
        .await
        .expect("bind api listener");
    let local_addr = listener.local_addr().expect("read local address");
    info!("fantasma-api listening on {local_addr}");

    axum::serve(listener, app)
        .await
        .expect("serve api application");
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            env::var("FANTASMA_LOG_LEVEL")
                .unwrap_or_else(|_| "fantasma_api=info,tower_http=info".to_owned()),
        )
        .try_init();
}

fn load_authorizer() -> StaticAdminAuthorizer {
    let admin_token = env::var("FANTASMA_ADMIN_TOKEN").unwrap_or_else(|_| "fg_pat_dev".to_owned());

    StaticAdminAuthorizer::new(admin_token)
}
