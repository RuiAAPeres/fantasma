use chrono::{DateTime, Utc};
use fantasma_core::Platform;
use uuid::Uuid;

const SESSION_TIMEOUT_SECS: i64 = 30 * 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionEvent {
    pub(crate) project_id: Uuid,
    pub(crate) event_id: i64,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) install_id: String,
    pub(crate) user_id: Option<String>,
    pub(crate) platform: Platform,
    pub(crate) app_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionRecord {
    pub(crate) project_id: Uuid,
    pub(crate) install_id: String,
    pub(crate) user_id: Option<String>,
    pub(crate) session_id: String,
    pub(crate) session_start: DateTime<Utc>,
    pub(crate) session_end: DateTime<Utc>,
    pub(crate) event_count: i32,
    pub(crate) duration_seconds: i32,
    pub(crate) platform: Platform,
    pub(crate) app_version: Option<String>,
}

pub(crate) fn derive_sessions(
    events: &[SessionEvent],
    tail: Option<&SessionRecord>,
) -> Vec<SessionRecord> {
    if events.is_empty() {
        return Vec::new();
    }

    let mut sessions = Vec::new();
    let mut current: Option<SessionRecord> = None;
    let mut tail_consumed = false;
    let mut current_from_tail = false;

    for event in events {
        if current
            .as_ref()
            .is_some_and(|session| install_changed(session, event))
        {
            sessions.push(current.take().expect("session exists"));
        }

        if current.is_none() {
            current = tail.cloned().filter(|session| {
                !tail_consumed
                    && session.project_id == event.project_id
                    && session.install_id == event.install_id
            });
            current_from_tail = current.is_some();
        }

        match current.as_mut() {
            Some(session) if continues_session(session, event.timestamp) => {
                apply_event(session, event);
                tail_consumed = true;
            }
            Some(_) => {
                if !current_from_tail {
                    sessions.push(current.take().expect("session exists"));
                }
                current = Some(session_from_event(event));
                current_from_tail = false;
            }
            None => {
                current = Some(session_from_event(event));
                current_from_tail = false;
            }
        }
    }

    if let Some(session) = current {
        sessions.push(session);
    }

    sessions
}

fn install_changed(session: &SessionRecord, event: &SessionEvent) -> bool {
    session.project_id != event.project_id || session.install_id != event.install_id
}

fn continues_session(session: &SessionRecord, timestamp: DateTime<Utc>) -> bool {
    timestamp
        .signed_duration_since(session.session_end)
        .num_seconds()
        <= SESSION_TIMEOUT_SECS
}

fn session_from_event(event: &SessionEvent) -> SessionRecord {
    SessionRecord {
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

fn apply_event(session: &mut SessionRecord, event: &SessionEvent) {
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
    use chrono::{Duration, TimeZone};

    fn project_id() -> Uuid {
        Uuid::from_u128(0x9bad8b88_5e7a_44ed_98ce_4cf9ddde713a)
    }

    fn timestamp(minutes: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("valid timestamp")
            + Duration::minutes(minutes)
    }

    fn event(
        event_id: i64,
        install_id: &str,
        minutes: i64,
        user_id: Option<&str>,
        app_version: Option<&str>,
    ) -> SessionEvent {
        SessionEvent {
            project_id: project_id(),
            event_id,
            timestamp: timestamp(minutes),
            install_id: install_id.to_owned(),
            user_id: user_id.map(str::to_owned),
            platform: Platform::Ios,
            app_version: app_version.map(str::to_owned),
        }
    }

    fn tail_session(session_start_minutes: i64, session_end_minutes: i64) -> SessionRecord {
        SessionRecord {
            project_id: project_id(),
            install_id: "install-1".to_owned(),
            user_id: Some("user-1".to_owned()),
            session_id: "install-1:2026-01-01T00:00:00+00:00".to_owned(),
            session_start: timestamp(session_start_minutes),
            session_end: timestamp(session_end_minutes),
            event_count: 2,
            duration_seconds: ((session_end_minutes - session_start_minutes) * 60) as i32,
            platform: Platform::Ios,
            app_version: Some("1.0.0".to_owned()),
        }
    }

    #[test]
    fn keeps_events_within_thirty_minutes_in_one_session() {
        let sessions = derive_sessions(
            &[
                event(1, "install-1", 0, None, Some("1.0.0")),
                event(2, "install-1", 10, Some("user-1"), None),
                event(3, "install-1", 29, None, Some("1.0.1")),
            ],
            None,
        );

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_start, timestamp(0));
        assert_eq!(sessions[0].session_end, timestamp(29));
        assert_eq!(sessions[0].event_count, 3);
        assert_eq!(sessions[0].duration_seconds, 29 * 60);
        assert_eq!(sessions[0].user_id.as_deref(), Some("user-1"));
        assert_eq!(sessions[0].app_version.as_deref(), Some("1.0.1"));
        assert_eq!(
            sessions[0].session_id,
            "install-1:2026-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn splits_sessions_when_gap_exceeds_thirty_minutes() {
        let sessions = derive_sessions(
            &[
                event(1, "install-1", 0, None, Some("1.0.0")),
                event(2, "install-1", 31, None, Some("1.0.0")),
            ],
            None,
        );

        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].session_start, timestamp(0));
        assert_eq!(sessions[1].session_start, timestamp(31));
    }

    #[test]
    fn keeps_different_installs_separate() {
        let sessions = derive_sessions(
            &[
                event(1, "install-1", 0, None, Some("1.0.0")),
                event(2, "install-2", 5, None, Some("1.0.0")),
            ],
            None,
        );

        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].install_id, "install-1");
        assert_eq!(sessions[1].install_id, "install-2");
    }

    #[test]
    fn extends_existing_tail_when_new_event_is_within_thirty_minutes() {
        let sessions = derive_sessions(
            &[event(3, "install-1", 25, None, Some("1.0.1"))],
            Some(&tail_session(0, 10)),
        );

        assert_eq!(sessions.len(), 1);
        assert_eq!(
            sessions[0].session_id,
            "install-1:2026-01-01T00:00:00+00:00"
        );
        assert_eq!(sessions[0].session_start, timestamp(0));
        assert_eq!(sessions[0].session_end, timestamp(25));
        assert_eq!(sessions[0].event_count, 3);
        assert_eq!(sessions[0].duration_seconds, 25 * 60);
        assert_eq!(sessions[0].app_version.as_deref(), Some("1.0.1"));
    }

    #[test]
    fn starts_new_session_after_tail_when_gap_exceeds_thirty_minutes() {
        let sessions = derive_sessions(
            &[event(3, "install-1", 45, Some("user-2"), Some("1.0.1"))],
            Some(&tail_session(0, 10)),
        );

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_start, timestamp(45));
        assert_eq!(sessions[0].session_end, timestamp(45));
        assert_eq!(sessions[0].event_count, 1);
        assert_eq!(sessions[0].duration_seconds, 0);
        assert_eq!(
            sessions[0].session_id,
            "install-1:2026-01-01T00:45:00+00:00"
        );
        assert_eq!(sessions[0].user_id.as_deref(), Some("user-2"));
    }
}
