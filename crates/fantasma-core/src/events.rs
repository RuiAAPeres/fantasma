use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

const MAX_BATCH_SIZE: usize = 100;
const MAX_EVENT_BYTES: usize = 8 * 1024;
const MAX_PROPERTIES_KEYS: usize = 3;

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
    pub platform: Platform,
    pub app_version: Option<String>,
    pub os_version: Option<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawEventPayload {
    pub event: String,
    pub timestamp: DateTime<Utc>,
    pub install_id: String,
    pub platform: Platform,
    pub app_version: Option<String>,
    pub os_version: Option<String>,
    #[serde(default)]
    pub properties: Map<String, Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventBatchRequest {
    pub events: Vec<EventPayload>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawEventBatchRequest {
    pub events: Vec<RawEventPayload>,
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
    #[error("app version must not be empty when provided")]
    EmptyAppVersion,
    #[error("os_version must not be empty when provided")]
    EmptyOsVersion,
    #[error("properties object may contain at most {MAX_PROPERTIES_KEYS} keys")]
    TooManyProperties,
    #[error("properties values must be strings")]
    NonStringPropertyValue,
    #[error("properties key '{0}' is reserved")]
    ReservedPropertyKey(String),
    #[error("properties key '{0}' is invalid")]
    InvalidPropertyKey(String),
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
            .app_version
            .as_deref()
            .is_some_and(|app_version| app_version.trim().is_empty())
        {
            return Err(EventValidationError::EmptyAppVersion);
        }

        if self
            .os_version
            .as_deref()
            .is_some_and(|os_version| os_version.trim().is_empty())
        {
            return Err(EventValidationError::EmptyOsVersion);
        }

        if self.properties.len() > MAX_PROPERTIES_KEYS {
            return Err(EventValidationError::TooManyProperties);
        }

        for key in self.properties.keys() {
            if is_reserved_event_property_key(key) {
                return Err(EventValidationError::ReservedPropertyKey(key.clone()));
            }

            if !is_valid_event_property_key(key) {
                return Err(EventValidationError::InvalidPropertyKey(key.clone()));
            }
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

impl RawEventPayload {
    pub fn normalize(self) -> Result<EventPayload, EventValidationError> {
        if self.properties.len() > MAX_PROPERTIES_KEYS {
            return Err(EventValidationError::TooManyProperties);
        }

        let mut properties = BTreeMap::new();
        for (key, value) in self.properties {
            if is_reserved_event_property_key(&key) {
                return Err(EventValidationError::ReservedPropertyKey(key));
            }

            if !is_valid_event_property_key(&key) {
                return Err(EventValidationError::InvalidPropertyKey(key));
            }

            let Value::String(value) = value else {
                return Err(EventValidationError::NonStringPropertyValue);
            };
            properties.insert(key, value);
        }

        let event = EventPayload {
            event: self.event,
            timestamp: self.timestamp,
            install_id: self.install_id,
            platform: self.platform,
            app_version: self.app_version,
            os_version: self.os_version,
            properties,
        };
        event.validate()?;

        Ok(event)
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

impl RawEventBatchRequest {
    pub fn normalize(self) -> Result<EventBatchRequest, Vec<EventValidationIssue>> {
        if self.events.is_empty() {
            return Err(vec![EventValidationIssue {
                index: 0,
                message: EventValidationError::EmptyBatch.to_string(),
            }]);
        }

        if self.events.len() > MAX_BATCH_SIZE {
            return Err(vec![EventValidationIssue {
                index: 0,
                message: format!("batch may contain at most {MAX_BATCH_SIZE} events"),
            }]);
        }

        let mut normalized = Vec::with_capacity(self.events.len());
        let mut issues = Vec::new();

        for (index, event) in self.events.into_iter().enumerate() {
            match event.normalize() {
                Ok(event) => normalized.push(event),
                Err(error) => issues.push(EventValidationIssue {
                    index,
                    message: error.to_string(),
                }),
            }
        }

        if issues.is_empty() {
            Ok(EventBatchRequest { events: normalized })
        } else {
            Err(issues)
        }
    }
}

pub fn is_reserved_event_property_key(key: &str) -> bool {
    matches!(
        key,
        "project_id"
            | "event"
            | "timestamp"
            | "install_id"
            | "properties"
            | "start_date"
            | "end_date"
            | "group_by"
            | "platform"
            | "app_version"
            | "os_version"
    )
}

pub fn is_valid_event_property_key(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !first.is_ascii_lowercase() || key.len() > 63 {
        return false;
    }

    chars.all(|char| char.is_ascii_lowercase() || char.is_ascii_digit() || char == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_event() -> EventPayload {
        EventPayload {
            event: "screen_view".to_owned(),
            timestamp: Utc::now(),
            install_id: "install_123".to_owned(),
            platform: Platform::Ios,
            app_version: Some("1.2.3".to_owned()),
            os_version: Some("18.3".to_owned()),
            properties: BTreeMap::new(),
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
        fn allows_missing_app_version() {
            let mut event = valid_event();
            event.app_version = None;

            let result = event.validate();

            assert_eq!(result, Ok(()));
        }

        #[test]
        fn returns_error_when_os_version_is_empty() {
            let mut event = valid_event();
            event.os_version = Some(String::new());

            let result = event.validate().unwrap_err();

            assert_eq!(result, EventValidationError::EmptyOsVersion);
        }

        #[test]
        fn rejects_legacy_public_identity_fields() {
            let payload = serde_json::json!({
                "event": "screen_view",
                "timestamp": "2026-01-01T00:00:00Z",
                "install_id": "install_123",
                "user_id": "user_123",
                "session_id": "session_123",
                "platform": "ios"
            });

            let result = serde_json::from_value::<EventPayload>(payload);

            assert!(result.is_err());
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

    mod raw_event_batch_request_normalize {
        use super::*;

        #[test]
        fn returns_indexed_error_when_property_value_is_not_a_string() {
            let request: RawEventBatchRequest = serde_json::from_value(serde_json::json!({
                "events": [
                    {
                        "event": "app_open",
                        "timestamp": "2026-03-01T00:00:00Z",
                        "install_id": "install_123",
                        "platform": "ios",
                        "properties": {
                            "provider": 7
                        }
                    }
                ]
            }))
            .expect("raw request parses");

            let result = request.normalize().unwrap_err();

            assert_eq!(
                result,
                vec![EventValidationIssue {
                    index: 0,
                    message: "properties values must be strings".to_owned(),
                }]
            );
        }

        #[test]
        fn normalizes_missing_properties_to_an_empty_map() {
            let request: RawEventBatchRequest = serde_json::from_value(serde_json::json!({
                "events": [
                    {
                        "event": "app_open",
                        "timestamp": "2026-03-01T00:00:00Z",
                        "install_id": "install_123",
                        "platform": "ios"
                    }
                ]
            }))
            .expect("raw request parses");

            let normalized = request.normalize().expect("request normalizes");

            assert!(normalized.events[0].properties.is_empty());
        }

        #[test]
        fn returns_indexed_error_when_property_key_is_reserved() {
            let request: RawEventBatchRequest = serde_json::from_value(serde_json::json!({
                "events": [
                    {
                        "event": "app_open",
                        "timestamp": "2026-03-01T00:00:00Z",
                        "install_id": "install_123",
                        "platform": "ios",
                        "properties": {
                            "platform": "ios"
                        }
                    }
                ]
            }))
            .expect("raw request parses");

            let result = request.normalize().unwrap_err();

            assert_eq!(
                result,
                vec![EventValidationIssue {
                    index: 0,
                    message: "properties key 'platform' is reserved".to_owned(),
                }]
            );
        }
    }
}
