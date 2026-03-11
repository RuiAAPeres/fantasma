use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

const MAX_BATCH_SIZE: usize = 1_000;
const MAX_PROPERTIES_KEYS: usize = 32;
const MAX_PROPERTIES_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Platform {
    Ios,
    Android,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventPayload {
    pub event: String,
    pub timestamp: DateTime<Utc>,
    pub install_id: Uuid,
    pub user_id: Option<String>,
    pub session_id: Uuid,
    pub platform: Platform,
    pub app_version: String,
    #[serde(default)]
    pub properties: Map<String, Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventBatchRequest {
    pub events: Vec<EventPayload>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventValidationIssue {
    pub index: usize,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventBatchResponse {
    pub accepted: usize,
    pub rejected: usize,
    pub errors: Vec<EventValidationIssue>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EventValidationError {
    #[error("event name must not be empty")]
    EmptyEventName,
    #[error("app version must not be empty")]
    EmptyAppVersion,
    #[error("properties object may contain at most {MAX_PROPERTIES_KEYS} keys")]
    TooManyProperties,
    #[error("properties object may not exceed {MAX_PROPERTIES_BYTES} bytes")]
    PropertiesTooLarge,
}

impl EventPayload {
    pub fn validate(&self) -> Result<(), EventValidationError> {
        if self.event.trim().is_empty() {
            return Err(EventValidationError::EmptyEventName);
        }

        if self.app_version.trim().is_empty() {
            return Err(EventValidationError::EmptyAppVersion);
        }

        if self.properties.len() > MAX_PROPERTIES_KEYS {
            return Err(EventValidationError::TooManyProperties);
        }

        let size = serde_json::to_vec(&self.properties)
            .map(|payload| payload.len())
            .unwrap_or(MAX_PROPERTIES_BYTES + 1);
        if size > MAX_PROPERTIES_BYTES {
            return Err(EventValidationError::PropertiesTooLarge);
        }

        Ok(())
    }
}

impl EventBatchRequest {
    pub fn validate(&self) -> Vec<EventValidationIssue> {
        if self.events.len() > MAX_BATCH_SIZE {
            return vec![EventValidationIssue {
                index: 0,
                message: format!("batch may contain at most {MAX_BATCH_SIZE} events"),
            }];
        }

        self.events
            .iter()
            .enumerate()
            .filter_map(|(index, event)| {
                event.validate().err().map(|error| EventValidationIssue {
                    index,
                    message: error.to_string(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_event() -> EventPayload {
        EventPayload {
            event: "screen_view".to_owned(),
            timestamp: Utc::now(),
            install_id: Uuid::new_v4(),
            user_id: Some("user_123".to_owned()),
            session_id: Uuid::new_v4(),
            platform: Platform::Ios,
            app_version: "1.2.3".to_owned(),
            properties: Map::new(),
        }
    }

    mod event_payload_validate {
        use super::*;

        #[test]
        fn returns_error_when_event_name_is_empty() {
            let mut event = valid_event();
            event.event.clear();

            let result = event.validate().unwrap_err();

            assert_eq!(result, EventValidationError::EmptyEventName);
        }

        #[test]
        fn returns_error_when_app_version_is_empty() {
            let mut event = valid_event();
            event.app_version.clear();

            let result = event.validate().unwrap_err();

            assert_eq!(result, EventValidationError::EmptyAppVersion);
        }
    }

    mod event_batch_request_validate {
        use super::*;

        #[test]
        fn returns_indexed_errors_for_invalid_events() {
            let mut invalid = valid_event();
            invalid.event.clear();

            let request = EventBatchRequest {
                events: vec![valid_event(), invalid],
            };

            let result = request.validate();

            assert_eq!(
                result,
                vec![EventValidationIssue {
                    index: 1,
                    message: "event name must not be empty".to_owned(),
                }]
            );
        }
    }
}
