//! in-memory event storage for the diagnostics
//!
//! This module owns the part of diagnostics that must preserve history across
//! asynchronous runtime parts: recent global events, recent per-channel
//! events, recent per-session events, etc...
//!
//! Runtime code records events here as they happen, and the query layer later
//! reads those bounded histories when building operator responses.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Mutex, PoisonError};

use serde_json::{Map, Value};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::runtime::ChannelInstanceId;

use super::types::DiagnosticsEvent;
use o_sfu_protocol::shared::SessionId;

const GLOBAL_RECENT_EVENT_LIMIT: usize = 64;
const SCOPE_RECENT_EVENT_LIMIT: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SessionScopeKey {
    channel_uuid: String,
    session_id: SessionId,
}

#[derive(Debug, Default)]
struct DiagnosticsStoreState {
    channel_uuid_by_instance_id: BTreeMap<ChannelInstanceId, String>,
    global_recent_events: VecDeque<DiagnosticsEvent>,
    channel_recent_events: BTreeMap<String, VecDeque<DiagnosticsEvent>>,
    session_recent_events: BTreeMap<SessionScopeKey, VecDeque<DiagnosticsEvent>>,
}

#[derive(Debug, Default)]
pub(crate) struct DiagnosticsStore {
    state: Mutex<DiagnosticsStoreState>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DiagnosticsEventData {
    fields: Map<String, Value>,
    pub(crate) channel_uuid: String,
    pub(crate) connection_id: Option<u64>,
    pub(crate) event: &'static str,
    pub(crate) media_worker_id: Option<usize>,
    pub(crate) session_id: Option<SessionId>,
    pub(crate) transport_media_id: Option<u64>,
}

impl DiagnosticsEventData {
    #[must_use]
    pub(crate) fn for_channel(channel_uuid: &str, event: &'static str) -> Self {
        Self {
            channel_uuid: channel_uuid.to_owned(),
            event,
            ..Self::default()
        }
    }

    #[must_use]
    pub(crate) fn for_session(
        channel_uuid: &str,
        session_id: &SessionId,
        event: &'static str,
    ) -> Self {
        Self {
            channel_uuid: channel_uuid.to_owned(),
            event,
            session_id: Some(session_id.clone()),
            ..Self::default()
        }
    }

    #[must_use]
    pub(crate) fn with_connection_id(mut self, connection_id: u64) -> Self {
        self.connection_id = Some(connection_id);
        self
    }

    #[must_use]
    pub(crate) fn with_media_worker_id(mut self, media_worker_id: usize) -> Self {
        self.media_worker_id = Some(media_worker_id);
        self
    }

    #[must_use]
    pub(crate) fn with_transport_media_id(mut self, transport_media_id: u64) -> Self {
        self.transport_media_id = Some(transport_media_id);
        self
    }

    pub(crate) fn insert_field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.fields.insert(key.to_owned(), value.into());
        self
    }

    pub(crate) fn insert_fields(mut self, fields: Map<String, Value>) -> Self {
        self.fields.extend(fields);
        self
    }
}

impl DiagnosticsStore {
    pub(crate) fn register_channel_instance(
        &self,
        channel_instance_id: ChannelInstanceId,
        channel_uuid: &str,
    ) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .channel_uuid_by_instance_id
            .insert(channel_instance_id, channel_uuid.to_owned());
    }

    pub(crate) fn record(&self, data: DiagnosticsEventData) {
        let event = DiagnosticsEvent {
            channel_uuid: data.channel_uuid.clone(),
            connection_id: data.connection_id,
            event: data.event.to_owned(),
            fields: data.fields,
            media_worker_id: data.media_worker_id,
            session_id: data.session_id.clone(),
            timestamp: diagnostics_timestamp_now(),
            transport_media_id: data.transport_media_id,
        };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        push_bounded_event(
            &mut state.global_recent_events,
            event.clone(),
            GLOBAL_RECENT_EVENT_LIMIT,
        );
        push_bounded_event(
            state
                .channel_recent_events
                .entry(data.channel_uuid)
                .or_default(),
            event.clone(),
            SCOPE_RECENT_EVENT_LIMIT,
        );
        if let Some(session_id) = data.session_id {
            push_bounded_event(
                state
                    .session_recent_events
                    .entry(SessionScopeKey {
                        channel_uuid: event.channel_uuid.clone(),
                        session_id,
                    })
                    .or_default(),
                event,
                SCOPE_RECENT_EVENT_LIMIT,
            );
        }
    }

    pub(crate) fn forget_channel(&self, channel_uuid: &str) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .channel_uuid_by_instance_id
            .retain(|_, known_channel_uuid| known_channel_uuid != channel_uuid);
        state.channel_recent_events.remove(channel_uuid);
        state
            .session_recent_events
            .retain(|scope, _| scope.channel_uuid != channel_uuid);
    }

    pub(crate) fn forget_session(&self, channel_uuid: &str, session_id: &SessionId) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.session_recent_events.remove(&SessionScopeKey {
            channel_uuid: channel_uuid.to_owned(),
            session_id: session_id.clone(),
        });
    }

    pub(crate) fn global_recent_events(&self) -> Vec<DiagnosticsEvent> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        reversed_events(&state.global_recent_events)
    }

    pub(crate) fn channel_recent_events(&self, channel_uuid: &str) -> Vec<DiagnosticsEvent> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .channel_recent_events
            .get(channel_uuid)
            .map_or_else(Vec::new, reversed_events)
    }

    pub(crate) fn session_recent_events(
        &self,
        channel_uuid: &str,
        session_id: &SessionId,
    ) -> Vec<DiagnosticsEvent> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .session_recent_events
            .get(&SessionScopeKey {
                channel_uuid: channel_uuid.to_owned(),
                session_id: session_id.clone(),
            })
            .map_or_else(Vec::new, reversed_events)
    }

    pub(crate) fn record_transport_session_event(
        &self,
        channel_instance_id: ChannelInstanceId,
        session_id: &SessionId,
        event: &'static str,
        media_worker_id: usize,
        fields: Map<String, Value>,
    ) {
        let channel_uuid = {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state
                .channel_uuid_by_instance_id
                .get(&channel_instance_id)
                .cloned()
        };
        let Some(channel_uuid) = channel_uuid else {
            return;
        };
        self.record(
            DiagnosticsEventData::for_session(&channel_uuid, session_id, event)
                .with_media_worker_id(media_worker_id)
                .insert_fields(fields),
        );
    }
}

fn diagnostics_timestamp_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| String::from("1970-01-01T00:00:00Z"))
}

fn push_bounded_event(
    events: &mut VecDeque<DiagnosticsEvent>,
    event: DiagnosticsEvent,
    limit: usize,
) {
    if events.len() >= limit {
        let _ = events.pop_front();
    }
    events.push_back(event);
}

fn reversed_events(events: &VecDeque<DiagnosticsEvent>) -> Vec<DiagnosticsEvent> {
    events.iter().rev().cloned().collect()
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::{DiagnosticsEventData, DiagnosticsStore, GLOBAL_RECENT_EVENT_LIMIT};
    use crate::runtime::ChannelInstanceId;
    use o_sfu_protocol::shared::SessionId;

    #[test]
    fn global_events_keep_the_newest_entries_in_reverse_chronological_order() {
        let store = DiagnosticsStore::default();

        for index in 0..(GLOBAL_RECENT_EVENT_LIMIT + 3) {
            store.record(
                DiagnosticsEventData::for_channel("channel-a", "event")
                    .insert_field("index", Value::from(index.to_string())),
            );
        }

        let events = store.global_recent_events();
        assert_eq!(events.len(), GLOBAL_RECENT_EVENT_LIMIT);
        assert_eq!(
            events.first().and_then(|event| event.fields.get("index")),
            Some(&Value::from("66"))
        );
        assert_eq!(
            events.last().and_then(|event| event.fields.get("index")),
            Some(&Value::from("3"))
        );
    }

    #[test]
    fn forgetting_a_channel_clears_channel_and_session_scoped_history() {
        let store = DiagnosticsStore::default();
        let session_id = SessionId::Integer(7);
        store.record(DiagnosticsEventData::for_channel(
            "channel-a",
            "channel.event",
        ));
        store.record(DiagnosticsEventData::for_session(
            "channel-a",
            &session_id,
            "session.event",
        ));
        store.record(DiagnosticsEventData::for_channel(
            "channel-b",
            "other.event",
        ));

        store.forget_channel("channel-a");

        assert!(store.channel_recent_events("channel-a").is_empty());
        assert!(
            store
                .session_recent_events("channel-a", &session_id)
                .is_empty()
        );
        assert_eq!(store.channel_recent_events("channel-b").len(), 1);
        assert_eq!(store.global_recent_events().len(), 3);
    }

    #[test]
    fn transport_session_events_use_registered_runtime_to_find_channel_scope() {
        let store = DiagnosticsStore::default();
        let session_id = SessionId::Integer(9);
        let mut fields = Map::new();
        fields.insert(String::from("state"), Value::from("connected"));

        store.record_transport_session_event(
            ChannelInstanceId::from_raw(12),
            &session_id,
            "transport.health_changed",
            2,
            fields,
        );
        assert!(store.global_recent_events().is_empty());

        store.register_channel_instance(ChannelInstanceId::from_raw(12), "channel-a");
        let mut fields = Map::new();
        fields.insert(String::from("state"), Value::from("connected"));
        store.record_transport_session_event(
            ChannelInstanceId::from_raw(12),
            &session_id,
            "transport.health_changed",
            2,
            fields,
        );

        let events = store.session_recent_events("channel-a", &session_id);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events.first().map(|event| event.channel_uuid.as_str()),
            Some("channel-a")
        );
        assert_eq!(
            events.first().and_then(|event| event.media_worker_id),
            Some(2)
        );
        assert_eq!(
            events.first().and_then(|event| event.fields.get("state")),
            Some(&Value::from("connected"))
        );
    }
}
