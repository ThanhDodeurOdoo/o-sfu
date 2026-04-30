//! in-memory event storage for the diagnostics
//!
//! This module owns the part of diagnostics that must preserve history across
//! asynchronous runtime parts: recent global events, recent per-room
//! events, recent per-user events, etc...
//!
//! Runtime code records events here as they happen, and the query layer later
//! reads those bounded histories when building operator responses.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Mutex, PoisonError},
};

use serde_json::{Map, Value};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::types::DiagnosticsEvent;
use crate::runtime::{RoomInstanceId, UserId};

const GLOBAL_RECENT_EVENT_LIMIT: usize = 64;
const SCOPE_RECENT_EVENT_LIMIT: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UserScopeKey {
    room_id: String,
    user_id: UserId,
}

#[derive(Debug, Default)]
struct DiagnosticsStoreState {
    room_uuid_by_instance_id: BTreeMap<RoomInstanceId, String>,
    global_recent_events: VecDeque<DiagnosticsEvent>,
    room_recent_events: BTreeMap<String, VecDeque<DiagnosticsEvent>>,
    user_recent_events: BTreeMap<UserScopeKey, VecDeque<DiagnosticsEvent>>,
    user_lookup: BTreeMap<String, BTreeMap<String, BTreeSet<UserId>>>,
}

#[derive(Debug, Default)]
pub struct DiagnosticsStore {
    state: Mutex<DiagnosticsStoreState>,
}

#[derive(Debug, Clone, Default)]
pub struct DiagnosticsEventData {
    fields: Map<String, Value>,
    pub room_id: String,
    pub connection_id: Option<u64>,
    pub event: &'static str,
    pub media_worker_id: Option<usize>,
    pub user_id: Option<UserId>,
    pub transport_media_id: Option<u64>,
}

impl DiagnosticsEventData {
    #[must_use]
    pub fn for_room(room_id: &str, event: &'static str) -> Self {
        Self {
            room_id: room_id.to_owned(),
            event,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn for_user(room_id: &str, user_id: &UserId, event: &'static str) -> Self {
        Self {
            room_id: room_id.to_owned(),
            event,
            user_id: Some(user_id.clone()),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn with_connection_id(mut self, connection_id: u64) -> Self {
        self.connection_id = Some(connection_id);
        self
    }

    #[must_use]
    pub fn with_media_worker_id(mut self, media_worker_id: usize) -> Self {
        self.media_worker_id = Some(media_worker_id);
        self
    }

    #[must_use]
    pub fn with_transport_media_id(mut self, transport_media_id: u64) -> Self {
        self.transport_media_id = Some(transport_media_id);
        self
    }

    pub fn insert_field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.fields.insert(key.to_owned(), value.into());
        self
    }

    pub fn insert_fields(mut self, fields: Map<String, Value>) -> Self {
        self.fields.extend(fields);
        self
    }
}

impl DiagnosticsStore {
    pub fn register_room_instance(&self, room_instance_id: RoomInstanceId, room_id: &str) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .room_uuid_by_instance_id
            .insert(room_instance_id, room_id.to_owned());
    }

    /// Adds a live user to the diagnostics-owned lookup index.
    ///
    /// The room lifecycle calls this after the join transition commits. The
    /// lookup key intentionally matches the raw diagnostics route segment:
    /// integer ids use decimal strings and compatibility string ids keep their
    /// original value.
    pub fn register_user(&self, room_id: &str, user_id: &UserId) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .user_lookup
            .entry(user_lookup_key(user_id))
            .or_default()
            .entry(room_id.to_owned())
            .or_default()
            .insert(user_id.clone());
    }

    /// Returns live room ids that may contain `requested_user_id`.
    ///
    /// The query layer still asks each returned room to build the final
    /// diagnostics view from current room and transport state. This method only
    /// narrows the candidate room set while preserving the route's existing
    /// missing, found, and conflict semantics.
    pub fn user_lookup_room_ids(&self, requested_user_id: &str) -> Vec<String> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .user_lookup
            .get(requested_user_id)
            .map_or_else(Vec::new, |rooms| rooms.keys().cloned().collect())
    }

    pub fn record(&self, data: DiagnosticsEventData) {
        let event = DiagnosticsEvent {
            room_id: data.room_id.clone(),
            connection_id: data.connection_id,
            event: data.event.to_owned(),
            fields: data.fields,
            media_worker_id: data.media_worker_id,
            user_id: data.user_id.clone(),
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
            state.room_recent_events.entry(data.room_id).or_default(),
            event.clone(),
            SCOPE_RECENT_EVENT_LIMIT,
        );
        if let Some(user_id) = data.user_id {
            push_bounded_event(
                state
                    .user_recent_events
                    .entry(UserScopeKey {
                        room_id: event.room_id.clone(),
                        user_id,
                    })
                    .or_default(),
                event,
                SCOPE_RECENT_EVENT_LIMIT,
            );
        }
    }

    pub fn forget_room(&self, room_id: &str) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .room_uuid_by_instance_id
            .retain(|_, known_room_id| known_room_id != room_id);
        state.room_recent_events.remove(room_id);
        state
            .user_recent_events
            .retain(|scope, _| scope.room_id != room_id);
        state.user_lookup.retain(|_, rooms| {
            rooms.remove(room_id);
            !rooms.is_empty()
        });
    }

    pub fn forget_user(&self, room_id: &str, user_id: &UserId) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.user_recent_events.remove(&UserScopeKey {
            room_id: room_id.to_owned(),
            user_id: user_id.clone(),
        });
        let lookup_key = user_lookup_key(user_id);
        let remove_lookup_key = state.user_lookup.get_mut(&lookup_key).is_some_and(|rooms| {
            if let Some(users) = rooms.get_mut(room_id) {
                users.remove(user_id);
                if users.is_empty() {
                    rooms.remove(room_id);
                }
            }
            rooms.is_empty()
        });
        if remove_lookup_key {
            state.user_lookup.remove(&lookup_key);
        }
    }

    pub fn global_recent_events(&self) -> Vec<DiagnosticsEvent> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        reversed_events(&state.global_recent_events)
    }

    pub fn room_recent_events(&self, room_id: &str) -> Vec<DiagnosticsEvent> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .room_recent_events
            .get(room_id)
            .map_or_else(Vec::new, reversed_events)
    }

    pub fn user_recent_events(&self, room_id: &str, user_id: &UserId) -> Vec<DiagnosticsEvent> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .user_recent_events
            .get(&UserScopeKey {
                room_id: room_id.to_owned(),
                user_id: user_id.clone(),
            })
            .map_or_else(Vec::new, reversed_events)
    }

    pub fn record_transport_user_event(
        &self,
        room_instance_id: RoomInstanceId,
        user_id: &UserId,
        event: &'static str,
        media_worker_id: usize,
        fields: Map<String, Value>,
    ) {
        let room_id = {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state
                .room_uuid_by_instance_id
                .get(&room_instance_id)
                .cloned()
        };
        let Some(room_id) = room_id else {
            return;
        };
        self.record(
            DiagnosticsEventData::for_user(&room_id, user_id, event)
                .with_media_worker_id(media_worker_id)
                .insert_fields(fields),
        );
    }
}

fn user_lookup_key(user_id: &UserId) -> String {
    match user_id {
        UserId::Integer(value) => value.to_string(),
        UserId::String(value) => value.clone(),
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
    use crate::runtime::{RoomInstanceId, UserId};

    #[test]
    fn global_events_keep_the_newest_entries_in_reverse_chronological_order() {
        let store = DiagnosticsStore::default();

        for index in 0..(GLOBAL_RECENT_EVENT_LIMIT + 3) {
            store.record(
                DiagnosticsEventData::for_room("room-a", "event")
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
    fn forgetting_a_room_clears_room_and_user_scoped_history() {
        let store = DiagnosticsStore::default();
        let user_id = UserId::Integer(7);
        store.register_user("room-a", &user_id);
        store.record(DiagnosticsEventData::for_room("room-a", "room.event"));
        store.record(DiagnosticsEventData::for_user(
            "room-a",
            &user_id,
            "user.event",
        ));
        store.record(DiagnosticsEventData::for_room("room-b", "other.event"));

        store.forget_room("room-a");

        assert!(store.room_recent_events("room-a").is_empty());
        assert!(store.user_recent_events("room-a", &user_id).is_empty());
        assert!(store.user_lookup_room_ids("7").is_empty());
        assert_eq!(store.room_recent_events("room-b").len(), 1);
        assert_eq!(store.global_recent_events().len(), 3);
    }

    #[test]
    fn user_lookup_tracks_integer_and_string_user_ids_by_room() {
        let store = DiagnosticsStore::default();
        let integer_user = UserId::Integer(7);
        let string_user = UserId::String(String::from("guest-7"));
        store.register_user("room-a", &integer_user);
        store.register_user("room-b", &integer_user);
        store.register_user("room-c", &string_user);

        assert_eq!(
            store.user_lookup_room_ids("7"),
            vec![String::from("room-a"), String::from("room-b")]
        );
        assert_eq!(
            store.user_lookup_room_ids("guest-7"),
            vec![String::from("room-c")]
        );

        store.forget_user("room-a", &integer_user);

        assert_eq!(
            store.user_lookup_room_ids("7"),
            vec![String::from("room-b")]
        );

        store.forget_room("room-c");

        assert!(store.user_lookup_room_ids("guest-7").is_empty());
    }

    #[test]
    fn transport_user_events_use_registered_runtime_to_find_room_scope() {
        let store = DiagnosticsStore::default();
        let user_id = UserId::Integer(9);
        let mut fields = Map::new();
        fields.insert(String::from("state"), Value::from("connected"));

        store.record_transport_user_event(
            RoomInstanceId::from_raw(12),
            &user_id,
            "transport.health_changed",
            2,
            fields,
        );
        assert!(store.global_recent_events().is_empty());

        store.register_room_instance(RoomInstanceId::from_raw(12), "room-a");
        let mut fields = Map::new();
        fields.insert(String::from("state"), Value::from("connected"));
        store.record_transport_user_event(
            RoomInstanceId::from_raw(12),
            &user_id,
            "transport.health_changed",
            2,
            fields,
        );

        let events = store.user_recent_events("room-a", &user_id);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events.first().map(|event| event.room_id.as_str()),
            Some("room-a")
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
