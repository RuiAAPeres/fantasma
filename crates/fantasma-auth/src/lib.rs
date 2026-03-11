use http::HeaderMap;
use thiserror::Error;
use uuid::Uuid;

const INGEST_HEADER: &str = "x-fantasma-key";
const AUTHORIZATION_HEADER: &str = "authorization";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectScope {
    pub project_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct StaticKeyAuthorizer {
    project_scope: ProjectScope,
    ingest_key: String,
    admin_token: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("missing credentials")]
    MissingCredentials,
    #[error("invalid credentials")]
    InvalidCredentials,
}

impl StaticKeyAuthorizer {
    pub fn new(
        project_id: Uuid,
        ingest_key: impl Into<String>,
        admin_token: impl Into<String>,
    ) -> Self {
        Self {
            project_scope: ProjectScope { project_id },
            ingest_key: ingest_key.into(),
            admin_token: admin_token.into(),
        }
    }

    pub fn authorize_ingest(&self, headers: &HeaderMap) -> Result<ProjectScope, AuthError> {
        let key = headers
            .get(INGEST_HEADER)
            .ok_or(AuthError::MissingCredentials)?
            .to_str()
            .map_err(|_| AuthError::InvalidCredentials)?;

        if key == self.ingest_key {
            return Ok(self.project_scope.clone());
        }

        Err(AuthError::InvalidCredentials)
    }

    pub fn authorize_admin(&self, headers: &HeaderMap) -> Result<ProjectScope, AuthError> {
        let token = headers
            .get(AUTHORIZATION_HEADER)
            .ok_or(AuthError::MissingCredentials)?
            .to_str()
            .map_err(|_| AuthError::InvalidCredentials)?;

        let expected = format!("Bearer {}", self.admin_token);
        if token == expected {
            return Ok(self.project_scope.clone());
        }

        Err(AuthError::InvalidCredentials)
    }

    pub fn project_id(&self) -> Uuid {
        self.project_scope.project_id
    }
}

impl Default for StaticKeyAuthorizer {
    fn default() -> Self {
        Self::new(
            Uuid::from_u128(0x9bad8b88_5e7a_44ed_98ce_4cf9ddde713a),
            "fg_ing_dev",
            "fg_pat_dev",
        )
    }
}
