use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

const MAX_BATCH_SIZE: usize = 200;
const MAX_EVENT_BYTES: usize = 8 * 1024;
pub const MAX_EVENT_NAME_CHARS: usize = 64;
pub const MAX_INSTALL_ID_CHARS: usize = 128;
pub const MAX_APP_VERSION_CHARS: usize = 64;
pub const MAX_OS_VERSION_CHARS: usize = 64;
pub const MAX_LOCALE_CHARS: usize = 64;

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
    pub locale: Option<String>,
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
    pub locale: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_legacy_properties_field",
        skip_serializing
    )]
    pub properties: Option<Option<serde_json::Value>>,
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
    #[error("event must not have leading or trailing whitespace")]
    EventNameHasOuterWhitespace,
    #[error("event must not exceed {MAX_EVENT_NAME_CHARS} characters")]
    EventNameTooLong,
    #[error("install_id must not be empty")]
    EmptyInstallId,
    #[error("install_id must not have leading or trailing whitespace")]
    InstallIdHasOuterWhitespace,
    #[error("install_id must not exceed {MAX_INSTALL_ID_CHARS} characters")]
    InstallIdTooLong,
    #[error("app version must not be empty when provided")]
    EmptyAppVersion,
    #[error("app_version must not have leading or trailing whitespace when provided")]
    AppVersionHasOuterWhitespace,
    #[error("app_version must not exceed {MAX_APP_VERSION_CHARS} characters")]
    AppVersionTooLong,
    #[error("os_version must not be empty when provided")]
    EmptyOsVersion,
    #[error("os_version must not have leading or trailing whitespace when provided")]
    OsVersionHasOuterWhitespace,
    #[error("os_version must not exceed {MAX_OS_VERSION_CHARS} characters")]
    OsVersionTooLong,
    #[error("locale must not be empty when provided")]
    EmptyLocale,
    #[error("locale must not have leading or trailing whitespace when provided")]
    LocaleHasOuterWhitespace,
    #[error("locale must not exceed {MAX_LOCALE_CHARS} characters")]
    LocaleTooLong,
    #[error("properties are not supported")]
    LegacyPropertiesUnsupported,
    #[error("event payload may not exceed {MAX_EVENT_BYTES} bytes")]
    EventTooLarge,
}

fn has_outer_whitespace(value: &str) -> bool {
    value.trim() != value
}

impl EventPayload {
    pub fn validate(&self) -> Result<(), EventValidationError> {
        if self.event.trim().is_empty() {
            return Err(EventValidationError::EmptyEventName);
        }
        if has_outer_whitespace(&self.event) {
            return Err(EventValidationError::EventNameHasOuterWhitespace);
        }
        if self.event.chars().count() > MAX_EVENT_NAME_CHARS {
            return Err(EventValidationError::EventNameTooLong);
        }

        if self.install_id.trim().is_empty() {
            return Err(EventValidationError::EmptyInstallId);
        }
        if has_outer_whitespace(&self.install_id) {
            return Err(EventValidationError::InstallIdHasOuterWhitespace);
        }
        if self.install_id.chars().count() > MAX_INSTALL_ID_CHARS {
            return Err(EventValidationError::InstallIdTooLong);
        }

        if self
            .app_version
            .as_deref()
            .is_some_and(|app_version| app_version.trim().is_empty())
        {
            return Err(EventValidationError::EmptyAppVersion);
        }
        if self
            .app_version
            .as_deref()
            .is_some_and(has_outer_whitespace)
        {
            return Err(EventValidationError::AppVersionHasOuterWhitespace);
        }
        if self
            .app_version
            .as_deref()
            .is_some_and(|app_version| app_version.chars().count() > MAX_APP_VERSION_CHARS)
        {
            return Err(EventValidationError::AppVersionTooLong);
        }

        if self
            .os_version
            .as_deref()
            .is_some_and(|os_version| os_version.trim().is_empty())
        {
            return Err(EventValidationError::EmptyOsVersion);
        }
        if self.os_version.as_deref().is_some_and(has_outer_whitespace) {
            return Err(EventValidationError::OsVersionHasOuterWhitespace);
        }
        if self
            .os_version
            .as_deref()
            .is_some_and(|os_version| os_version.chars().count() > MAX_OS_VERSION_CHARS)
        {
            return Err(EventValidationError::OsVersionTooLong);
        }

        if self
            .locale
            .as_deref()
            .is_some_and(|locale| locale.trim().is_empty())
        {
            return Err(EventValidationError::EmptyLocale);
        }
        if self.locale.as_deref().is_some_and(has_outer_whitespace) {
            return Err(EventValidationError::LocaleHasOuterWhitespace);
        }
        if self
            .locale
            .as_deref()
            .is_some_and(|locale| locale.chars().count() > MAX_LOCALE_CHARS)
        {
            return Err(EventValidationError::LocaleTooLong);
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
        if self.properties.is_some() {
            return Err(EventValidationError::LegacyPropertiesUnsupported);
        }

        let event = EventPayload {
            event: self.event,
            timestamp: self.timestamp,
            install_id: self.install_id,
            platform: self.platform,
            app_version: self.app_version,
            os_version: self.os_version,
            locale: self.locale,
        };
        event.validate()?;

        Ok(event)
    }
}

fn deserialize_legacy_properties_field<'de, D>(
    deserializer: D,
) -> Result<Option<Option<serde_json::Value>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(Some(match value {
        serde_json::Value::Null => None,
        other => Some(other),
    }))
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
            locale: Some("en-GB".to_owned()),
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
        fn allows_missing_locale() {
            let mut event = valid_event();
            event.locale = None;

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
        fn returns_error_when_locale_is_empty() {
            let mut event = valid_event();
            event.locale = Some(String::new());

            let result = event.validate().unwrap_err();

            assert_eq!(result, EventValidationError::EmptyLocale);
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

        #[test]
        fn returns_error_when_event_name_has_leading_or_trailing_whitespace() {
            let mut event = valid_event();
            event.event = " screen_view ".to_owned();

            let result = event.validate().unwrap_err();

            assert_eq!(
                result.to_string(),
                "event must not have leading or trailing whitespace"
            );
        }

        #[test]
        fn returns_error_when_install_id_has_leading_or_trailing_whitespace() {
            let mut event = valid_event();
            event.install_id = " install_123 ".to_owned();

            let result = event.validate().unwrap_err();

            assert_eq!(
                result.to_string(),
                "install_id must not have leading or trailing whitespace"
            );
        }

        #[test]
        fn returns_error_when_event_name_exceeds_max_length() {
            let mut event = valid_event();
            event.event = "a".repeat(65);

            let result = event.validate().unwrap_err();

            assert_eq!(result.to_string(), "event must not exceed 64 characters");
        }

        #[test]
        fn returns_error_when_install_id_exceeds_max_length() {
            let mut event = valid_event();
            event.install_id = "a".repeat(129);

            let result = event.validate().unwrap_err();

            assert_eq!(
                result.to_string(),
                "install_id must not exceed 128 characters"
            );
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
        fn accepts_top_level_locale() {
            let request: RawEventBatchRequest = serde_json::from_value(serde_json::json!({
                "events": [
                    {
                        "event": "app_open",
                        "timestamp": "2026-03-01T00:00:00Z",
                        "install_id": "install_123",
                        "platform": "ios",
                        "locale": "pt-PT"
                    }
                ]
            }))
            .expect("raw request parses");

            let normalized = request.normalize().expect("request normalizes");

            assert_eq!(normalized.events[0].locale.as_deref(), Some("pt-PT"));
        }

        #[test]
        fn normalizes_missing_locale_to_none() {
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

            assert_eq!(normalized.events[0].locale, None);
        }

        #[test]
        fn rejects_legacy_properties_during_normalization() {
            let request: RawEventBatchRequest = serde_json::from_value(serde_json::json!({
                "events": [
                    {
                        "event": "app_open",
                        "timestamp": "2026-03-01T00:00:00Z",
                        "install_id": "install_123",
                        "platform": "ios",
                        "properties": {
                            "provider": "strava"
                        }
                    }
                ]
            }))
            .expect("raw request parses");

            let issues = request.normalize().unwrap_err();

            assert_eq!(
                issues,
                vec![EventValidationIssue {
                    index: 0,
                    message: "properties are not supported".to_owned(),
                }]
            );
        }

        #[test]
        fn rejects_legacy_properties_null_during_normalization() {
            let request: RawEventBatchRequest = serde_json::from_value(serde_json::json!({
                "events": [
                    {
                        "event": "app_open",
                        "timestamp": "2026-03-01T00:00:00Z",
                        "install_id": "install_123",
                        "platform": "ios",
                        "properties": null
                    }
                ]
            }))
            .expect("raw request parses");

            let issues = request.normalize().unwrap_err();

            assert_eq!(
                issues,
                vec![EventValidationIssue {
                    index: 0,
                    message: "properties are not supported".to_owned(),
                }]
            );
        }

        #[test]
        fn shared_event_batch_schema_matches_runtime_batch_limit() {
            let schema: serde_json::Value = serde_json::from_str(include_str!(
                "../../../schemas/events/event-batch.schema.json"
            ))
            .expect("schema parses");

            assert_eq!(
                schema["properties"]["events"]["maxItems"],
                serde_json::json!(200)
            );
        }

        #[test]
        fn shared_mobile_event_schema_documents_no_outer_whitespace_constraints() {
            let schema: serde_json::Value = serde_json::from_str(include_str!(
                "../../../schemas/events/mobile-event.schema.json"
            ))
            .expect("schema parses");
            let properties = &schema["properties"];

            assert_eq!(properties["event"]["pattern"], "^\\S(?:[\\s\\S]*\\S)?$");
            assert_eq!(
                properties["install_id"]["pattern"],
                "^\\S(?:[\\s\\S]*\\S)?$"
            );
            assert_eq!(
                properties["app_version"]["pattern"],
                "^\\S(?:[\\s\\S]*\\S)?$"
            );
            assert_eq!(
                properties["os_version"]["pattern"],
                "^\\S(?:[\\s\\S]*\\S)?$"
            );
            assert_eq!(properties["locale"]["pattern"], "^\\S(?:[\\s\\S]*\\S)?$");
        }
    }
}
