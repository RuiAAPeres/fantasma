use chrono::{DateTime, Utc};
use fantasma_core::Platform;
use uuid::Uuid;

const SESSION_TIMEOUT_SECS: i64 = 30 * 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionEvent {
    pub(crate) project_id: Uuid,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) install_id: String,
    pub(crate) platform: Platform,
    pub(crate) app_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct AppendSessions {
    pub(crate) updated_tail: Option<fantasma_store::SessionRecord>,
    pub(crate) new_sessions: Vec<fantasma_store::SessionRecord>,
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

pub(crate) fn derive_append_sessions(
    tail_session: Option<fantasma_store::SessionRecord>,
    events: &[SessionEvent],
) -> AppendSessions {
    if events.is_empty() {
        return AppendSessions {
            updated_tail: tail_session,
            new_sessions: Vec::new(),
        };
    }

    let mut updated_tail = tail_session;
    let mut new_sessions = Vec::new();

    for event in events {
        if let Some(current) = new_sessions.last_mut() {
            if continues_session(current, event.timestamp) {
                apply_event(current, event);
            } else {
                new_sessions.push(session_from_event(event));
            }
            continue;
        }

        if let Some(current_tail) = updated_tail.as_mut() {
            if continues_session(current_tail, event.timestamp) {
                apply_event(current_tail, event);
            } else {
                new_sessions.push(session_from_event(event));
            }
            continue;
        }

        new_sessions.push(session_from_event(event));
    }

    AppendSessions {
        updated_tail,
        new_sessions,
    }
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
        app_version: Option<&str>,
    ) -> SessionEvent {
        SessionEvent {
            project_id: project_id(),
            timestamp: timestamp(day, hour, minute),
            install_id: install_id.to_owned(),
            platform: Platform::Ios,
            app_version: app_version.map(str::to_owned),
        }
    }

    #[test]
    fn keeps_events_within_thirty_minutes_in_one_session() {
        let sessions = derive_sessions(&[
            event("install-1", 1, 0, 0, Some("1.0.0")),
            event("install-1", 1, 0, 10, None),
            event("install-1", 1, 0, 29, Some("1.0.1")),
        ]);

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_start, timestamp(1, 0, 0));
        assert_eq!(sessions[0].session_end, timestamp(1, 0, 29));
        assert_eq!(sessions[0].event_count, 3);
        assert_eq!(sessions[0].duration_seconds, 29 * 60);
        assert_eq!(sessions[0].app_version.as_deref(), Some("1.0.0"));
    }

    #[test]
    fn splits_sessions_when_gap_exceeds_thirty_minutes() {
        let sessions = derive_sessions(&[
            event("install-1", 1, 0, 0, Some("1.0.0")),
            event("install-1", 1, 0, 31, Some("1.0.0")),
        ]);

        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].session_start, timestamp(1, 0, 0));
        assert_eq!(sessions[1].session_start, timestamp(1, 0, 31));
    }

    #[test]
    fn crosses_midnight_without_changing_start_day() {
        let sessions = derive_sessions(&[
            event("install-1", 1, 23, 55, Some("1.0.0")),
            event("install-1", 2, 0, 20, Some("1.0.1")),
        ]);

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_start, timestamp(1, 23, 55));
        assert_eq!(sessions[0].session_end, timestamp(2, 0, 20));
        assert_eq!(sessions[0].duration_seconds, 25 * 60);
        assert_eq!(sessions[0].app_version.as_deref(), Some("1.0.0"));
    }

    #[test]
    fn append_sessions_extends_existing_tail_once_and_then_starts_new_sessions() {
        let tail = fantasma_store::SessionRecord {
            project_id: project_id(),
            install_id: "install-1".to_owned(),
            session_id: "install-1:2026-01-01T00:00:00+00:00".to_owned(),
            session_start: timestamp(1, 0, 0),
            session_end: timestamp(1, 0, 10),
            event_count: 2,
            duration_seconds: 10 * 60,
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
        };

        let appended = derive_append_sessions(
            Some(tail),
            &[
                event("install-1", 1, 0, 20, Some("1.0.1")),
                event("install-1", 1, 1, 0, Some("1.1.0")),
                event("install-1", 1, 1, 10, Some("1.1.1")),
            ],
        );

        let updated_tail = appended.updated_tail.expect("updated tail exists");
        assert_eq!(updated_tail.session_end, timestamp(1, 0, 20));
        assert_eq!(updated_tail.event_count, 3);
        assert_eq!(updated_tail.duration_seconds, 20 * 60);
        assert_eq!(updated_tail.app_version.as_deref(), Some("1.0.0"));
        assert_eq!(appended.new_sessions.len(), 1);
        assert_eq!(appended.new_sessions[0].session_start, timestamp(1, 1, 0));
        assert_eq!(appended.new_sessions[0].session_end, timestamp(1, 1, 10));
        assert_eq!(appended.new_sessions[0].duration_seconds, 10 * 60);
        assert_eq!(
            appended.new_sessions[0].app_version.as_deref(),
            Some("1.1.0")
        );
    }
}
