//! in-memory event storage for diagnostics
//!
//! This module contains the part of diagnostics that must preserve recent
//! global, per-room and per-user event history across asynchronous runtime
//! paths.
//!
//! Runtime code records events here as they happen. The query layer later
//! reads those bounded histories when building operator responses.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Mutex, PoisonError},
};

use o_sfu_model::UserId;
use serde_json::{Map, Value};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::types::DiagnosticsEvent;

const GLOBAL_RECENT_EVENT_LIMIT: usize = 64;
const SCOPE_RECENT_EVENT_LIMIT: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DiagnosticsRoomInstanceId(u64);

impl DiagnosticsRoomInstanceId {
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UserScopeKey {
    room_id: String,
    user_id: UserId,
}

#[derive(Debug, Default)]
struct DiagnosticsStoreState {
    room_uuid_by_instance_id: BTreeMap<DiagnosticsRoomInstanceId, String>,
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

    #[must_use]
    pub fn insert_field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.fields.insert(key.to_owned(), value.into());
        self
    }

    #[must_use]
    pub fn insert_fields(mut self, fields: Map<String, Value>) -> Self {
        self.fields.extend(fields);
        self
    }
}

impl DiagnosticsStore {
    pub fn register_room_instance(
        &self,
        room_instance_id: DiagnosticsRoomInstanceId,
        room_id: &str,
    ) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .room_uuid_by_instance_id
            .insert(room_instance_id, room_id.to_owned());
    }

    /// Adds a live user to the diagnostics lookup index.
    ///
    /// The room lifecycle calls this after the join transition commits. The
    /// lookup key matches the raw diagnostics route segment:
    /// integer ids use decimal strings and compatibility string ids keep their
    /// original value.
    pub fn register_user(&self, room_id: &str, user_id: &UserId) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .user_lookup
            .entry(user_id.path_segment().into_owned())
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
        let lookup_key = user_id.path_segment();
        let remove_lookup_key =
            state
                .user_lookup
                .get_mut(lookup_key.as_ref())
                .is_some_and(|rooms| {
                    if let Some(users) = rooms.get_mut(room_id) {
                        users.remove(user_id);
                        if users.is_empty() {
                            rooms.remove(room_id);
                        }
                    }
                    rooms.is_empty()
                });
        if remove_lookup_key {
            state.user_lookup.remove(lookup_key.as_ref());
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
        room_instance_id: DiagnosticsRoomInstanceId,
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
#[path = "TESTS/store.rs"]
mod tests;
