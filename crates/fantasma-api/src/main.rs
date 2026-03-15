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

    let app = fantasma_api::app(
        pool,
        Arc::new(
            load_authorizer_from_env().expect("FANTASMA_ADMIN_TOKEN must be set for fantasma-api"),
        ),
    );

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

fn load_authorizer_from_env() -> Result<StaticAdminAuthorizer, env::VarError> {
    let admin_token = env::var("FANTASMA_ADMIN_TOKEN")?;
    Ok(StaticAdminAuthorizer::new(admin_token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn load_authorizer_from_env_requires_explicit_token() {
        let _guard = env_lock().lock().expect("lock env");
        unsafe {
            env::remove_var("FANTASMA_ADMIN_TOKEN");
        }

        let error = load_authorizer_from_env().expect_err("missing token must fail");

        assert_eq!(error, env::VarError::NotPresent);
    }

    #[test]
    fn load_authorizer_from_env_uses_configured_token() {
        let _guard = env_lock().lock().expect("lock env");
        unsafe {
            env::set_var("FANTASMA_ADMIN_TOKEN", "configured-secret");
        }

        let authorizer = load_authorizer_from_env().expect("load authorizer");
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_static("Bearer configured-secret"),
        );

        assert_eq!(authorizer.authorize(&headers), Ok(()));
    }
}
