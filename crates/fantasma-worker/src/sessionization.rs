use chrono::{DateTime, Utc};
use fantasma_core::Platform;
use uuid::Uuid;

const SESSION_TIMEOUT_SECS: i64 = 30 * 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionEvent {
    pub(crate) project_id: Uuid,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) install_id: String,
    pub(crate) user_id: Option<String>,
    pub(crate) platform: Platform,
    pub(crate) app_version: Option<String>,
}

pub(crate) fn derive_sessions(events: &[SessionEvent]) -> Vec<fantasma_store::SessionRecord> {
    if events.is_empty() {
        return Vec::new();
    }

    let mut sessions = Vec::new();
    let mut current = session_from_event(&events[0]);

    for event in &events[1..] {
        if continues_session(&current, event.timestamp) {
            apply_event(&mut current, event);
            continue;
        }

        sessions.push(current);
        current = session_from_event(event);
    }

    sessions.push(current);
    sessions
}

fn continues_session(session: &fantasma_store::SessionRecord, timestamp: DateTime<Utc>) -> bool {
    timestamp
        .signed_duration_since(session.session_end)
        .num_seconds()
        <= SESSION_TIMEOUT_SECS
}

fn session_from_event(event: &SessionEvent) -> fantasma_store::SessionRecord {
    fantasma_store::SessionRecord {
        project_id: event.project_id,
        install_id: event.install_id.clone(),
        user_id: event.user_id.clone(),
        session_id: session_id(&event.install_id, event.timestamp),
        session_start: event.timestamp,
        session_end: event.timestamp,
        event_count: 1,
        duration_seconds: 0,
        platform: event.platform.clone(),
        app_version: event.app_version.clone(),
    }
}

fn apply_event(session: &mut fantasma_store::SessionRecord, event: &SessionEvent) {
    session.session_end = event.timestamp;
    session.event_count += 1;
    session.duration_seconds = session
        .session_end
        .signed_duration_since(session.session_start)
        .num_seconds()
        .max(0) as i32;

    if let Some(user_id) = &event.user_id {
        session.user_id = Some(user_id.clone());
    }

    if let Some(app_version) = &event.app_version {
        session.app_version = Some(app_version.clone());
    }
}

fn session_id(install_id: &str, session_start: DateTime<Utc>) -> String {
    format!("{install_id}:{}", session_start.to_rfc3339())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn project_id() -> Uuid {
        Uuid::from_u128(0x9bad8b88_5e7a_44ed_98ce_4cf9ddde713a)
    }

    fn timestamp(day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, day, hour, minute, 0)
            .single()
            .expect("valid timestamp")
    }

    fn event(
        install_id: &str,
        day: u32,
        hour: u32,
        minute: u32,
        user_id: Option<&str>,
        app_version: Option<&str>,
    ) -> SessionEvent {
        SessionEvent {
            project_id: project_id(),
            timestamp: timestamp(day, hour, minute),
            install_id: install_id.to_owned(),
            user_id: user_id.map(str::to_owned),
            platform: Platform::Ios,
            app_version: app_version.map(str::to_owned),
        }
    }

    #[test]
    fn keeps_events_within_thirty_minutes_in_one_session() {
        let sessions = derive_sessions(&[
            event("install-1", 1, 0, 0, None, Some("1.0.0")),
            event("install-1", 1, 0, 10, Some("user-1"), None),
            event("install-1", 1, 0, 29, None, Some("1.0.1")),
        ]);

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_start, timestamp(1, 0, 0));
        assert_eq!(sessions[0].session_end, timestamp(1, 0, 29));
        assert_eq!(sessions[0].event_count, 3);
        assert_eq!(sessions[0].duration_seconds, 29 * 60);
        assert_eq!(sessions[0].user_id.as_deref(), Some("user-1"));
        assert_eq!(sessions[0].app_version.as_deref(), Some("1.0.1"));
    }

    #[test]
    fn splits_sessions_when_gap_exceeds_thirty_minutes() {
        let sessions = derive_sessions(&[
            event("install-1", 1, 0, 0, None, Some("1.0.0")),
            event("install-1", 1, 0, 31, None, Some("1.0.0")),
        ]);

        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].session_start, timestamp(1, 0, 0));
        assert_eq!(sessions[1].session_start, timestamp(1, 0, 31));
    }

    #[test]
    fn crosses_midnight_without_changing_start_day() {
        let sessions = derive_sessions(&[
            event("install-1", 1, 23, 55, None, Some("1.0.0")),
            event("install-1", 2, 0, 20, Some("user-1"), Some("1.0.1")),
        ]);

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_start, timestamp(1, 23, 55));
        assert_eq!(sessions[0].session_end, timestamp(2, 0, 20));
        assert_eq!(sessions[0].duration_seconds, 25 * 60);
        assert_eq!(sessions[0].user_id.as_deref(), Some("user-1"));
        assert_eq!(sessions[0].app_version.as_deref(), Some("1.0.1"));
    }
}
