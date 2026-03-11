use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

const MAX_BATCH_SIZE: usize = 100;
const MAX_EVENT_BYTES: usize = 8 * 1024;
const MAX_PROPERTIES_KEYS: usize = 32;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Platform {
    Ios,
    Android,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventPayload {
    pub event: String,
    pub timestamp: DateTime<Utc>,
    pub install_id: String,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub platform: Platform,
    pub app_version: Option<String>,
    #[serde(default)]
    pub properties: Map<String, Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventBatchRequest {
    pub events: Vec<EventPayload>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventValidationIssue {
    pub index: usize,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventAcceptedResponse {
    pub accepted: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventValidationResponse {
    pub errors: Vec<EventValidationIssue>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EventValidationError {
    #[error("batch must contain at least one event")]
    EmptyBatch,
    #[error("event name must not be empty")]
    EmptyEventName,
    #[error("install_id must not be empty")]
    EmptyInstallId,
    #[error("session_id must not be empty when provided")]
    EmptySessionId,
    #[error("app version must not be empty when provided")]
    EmptyAppVersion,
    #[error("properties object may contain at most {MAX_PROPERTIES_KEYS} keys")]
    TooManyProperties,
    #[error("event payload may not exceed {MAX_EVENT_BYTES} bytes")]
    EventTooLarge,
}

impl EventPayload {
    pub fn validate(&self) -> Result<(), EventValidationError> {
        if self.event.trim().is_empty() {
            return Err(EventValidationError::EmptyEventName);
        }

        if self.install_id.trim().is_empty() {
            return Err(EventValidationError::EmptyInstallId);
        }

        if self
            .session_id
            .as_deref()
            .is_some_and(|session_id| session_id.trim().is_empty())
        {
            return Err(EventValidationError::EmptySessionId);
        }

        if self
            .app_version
            .as_deref()
            .is_some_and(|app_version| app_version.trim().is_empty())
        {
            return Err(EventValidationError::EmptyAppVersion);
        }

        if self.properties.len() > MAX_PROPERTIES_KEYS {
            return Err(EventValidationError::TooManyProperties);
        }

        let size = serde_json::to_vec(self)
            .map(|payload| payload.len())
            .unwrap_or(MAX_EVENT_BYTES + 1);
        if size > MAX_EVENT_BYTES {
            return Err(EventValidationError::EventTooLarge);
        }

        Ok(())
    }
}

impl EventBatchRequest {
    pub fn validate(&self) -> Vec<EventValidationIssue> {
        if self.events.is_empty() {
            return vec![EventValidationIssue {
                index: 0,
                message: EventValidationError::EmptyBatch.to_string(),
            }];
        }

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
            install_id: "install_123".to_owned(),
            user_id: Some("user_123".to_owned()),
            session_id: Some("session_123".to_owned()),
            platform: Platform::Ios,
            app_version: Some("1.2.3".to_owned()),
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
            event.app_version = Some(String::new());

            let result = event.validate().unwrap_err();

            assert_eq!(result, EventValidationError::EmptyAppVersion);
        }

        #[test]
        fn returns_error_when_install_id_is_empty() {
            let mut event = valid_event();
            event.install_id.clear();

            let result = event.validate().unwrap_err();

            assert_eq!(result, EventValidationError::EmptyInstallId);
        }

        #[test]
        fn allows_missing_session_id_and_app_version() {
            let mut event = valid_event();
            event.session_id = None;
            event.app_version = None;

            let result = event.validate();

            assert_eq!(result, Ok(()));
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

        #[test]
        fn returns_error_when_batch_is_empty() {
            let request = EventBatchRequest { events: Vec::new() };

            let result = request.validate();

            assert_eq!(
                result,
                vec![EventValidationIssue {
                    index: 0,
                    message: "batch must contain at least one event".to_owned(),
                }]
            );
        }
    }
}
