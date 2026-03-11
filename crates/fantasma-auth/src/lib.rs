use http::HeaderMap;
use sha2::{Digest, Sha256};
use thiserror::Error;

const AUTHORIZATION_HEADER: &str = "authorization";

#[derive(Debug, Clone)]
pub struct StaticAdminAuthorizer {
    admin_token: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("missing credentials")]
    MissingCredentials,
    #[error("invalid credentials")]
    InvalidCredentials,
}

impl StaticAdminAuthorizer {
    pub fn new(admin_token: impl Into<String>) -> Self {
        Self {
            admin_token: admin_token.into(),
        }
    }

    pub fn authorize(&self, headers: &HeaderMap) -> Result<(), AuthError> {
        let token = headers
            .get(AUTHORIZATION_HEADER)
            .ok_or(AuthError::MissingCredentials)?
            .to_str()
            .map_err(|_| AuthError::InvalidCredentials)?;

        let expected = format!("Bearer {}", self.admin_token);
        if token == expected {
            return Ok(());
        }

        Err(AuthError::InvalidCredentials)
    }
}

impl Default for StaticAdminAuthorizer {
    fn default() -> Self {
        Self::new("fg_pat_dev")
    }
}

pub fn hash_ingest_key(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn derive_key_prefix(key: &str) -> String {
    let mut underscore_positions = key.match_indices('_').map(|(index, _)| index);
    if let Some(second_underscore) = underscore_positions.nth(1) {
        return key[..=second_underscore].to_owned();
    }

    key.chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn authorize_accepts_matching_bearer_token() {
        let authorizer = StaticAdminAuthorizer::new("secret-token");
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION_HEADER,
            HeaderValue::from_static("Bearer secret-token"),
        );

        let result = authorizer.authorize(&headers);

        assert_eq!(result, Ok(()));
    }

    #[test]
    fn derive_key_prefix_returns_two_segment_prefix_when_available() {
        let prefix = derive_key_prefix("fg_ing_test");

        assert_eq!(prefix, "fg_ing_");
    }

    #[test]
    fn hash_ingest_key_returns_stable_sha256_hex() {
        let hash = hash_ingest_key("fg_ing_test");

        assert_eq!(
            hash,
            "5a7bb2912be58212418d3ab018f57f55c00a5386b6d3bbf01b20e90e3943c376"
        );
    }
}
