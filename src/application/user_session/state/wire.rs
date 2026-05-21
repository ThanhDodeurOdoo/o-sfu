//! compatibility track snapshot projection for one browser
//!
//! this module turns room-authored events and media bootstrap notifications into
//! Odoo-compatible server messages, then owns only the local snapshot needed by
//! this browser so room membership and media ownership stay with `Room` and
//! `MediaSession`

use std::collections::BTreeMap;

use o_sfu_protocol::wire::{
    PeerInfoPayload, PeerLeftPayload, ServerBroadcastPayload, ServerMessage, StreamType,
    TrackBinding, UserId, UserInfo,
};

use super::super::projection::source_descriptor_from_source;
use crate::{
    application::stream_catalog::stream_type_for_stream_id,
    core::server::source_model::PublishedSourceDescriptor,
    runtime::room::{RemoteTrackBootstrap, RoomEventMessage, TrackBindingUpdate},
};

/// envelope for ordered server messages produced by one room event
///
/// `needs_renegotiation` is a session-local signal that means this browser's
/// current track snapshot changed in a way that requires `User` to request a
/// new offer after the compatibility messages are emitted
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application::user_session) struct UserWireMessages {
    /// messages to be batched and sent through the websocket
    pub messages: Vec<ServerMessage>,
    /// whether the event invalidated the browser's current track snapshot
    pub needs_renegotiation: bool,
}

impl UserWireMessages {
    fn messages(messages: Vec<ServerMessage>) -> Self {
        Self {
            messages,
            needs_renegotiation: false,
        }
    }
}

/// per-connection state used to build compatibility server messages
///
/// this state owns the remote track snapshot for one browser because room
/// events may update presence, remove peers or change publication activity and
/// those compatibility updates must be reflected locally before the websocket edge
/// serializes the output
#[derive(Debug, Default)]
pub(in crate::application::user_session) struct UserWireState {
    bindings_by_mid: BTreeMap<String, TrackBinding>,
}

impl UserWireState {
    /// apply one room message to the local wire state
    ///
    /// this method updates the local track snapshot and returns the ordered
    /// signals that the browser needs to see the transition
    pub fn apply_room_event(&mut self, message: RoomEventMessage) -> UserWireMessages {
        match message {
            RoomEventMessage::Broadcast { sender_id, message } => {
                UserWireMessages::messages(vec![ServerMessage::Broadcast(ServerBroadcastPayload {
                    sender_id,
                    message: message.to_json(),
                })])
            }
            RoomEventMessage::UserJoined { user_id, info } => {
                UserWireMessages::messages(vec![ServerMessage::PeerJoined(PeerInfoPayload {
                    user_id,
                    info,
                })])
            }
            RoomEventMessage::UserDeparted { user_id } => {
                let initial_len = self.bindings_by_mid.len();
                self.bindings_by_mid
                    .retain(|_mid, binding| binding.user_id != user_id);
                UserWireMessages {
                    messages: vec![ServerMessage::PeerLeft(PeerLeftPayload { user_id })],
                    needs_renegotiation: self.bindings_by_mid.len() != initial_len,
                }
            }
            RoomEventMessage::UserInfoChanged(snapshot) => {
                let mut messages = Vec::with_capacity(snapshot.len().saturating_add(1));
                let mut track_snapshot_changed = false;
                for (user_id, info) in snapshot {
                    track_snapshot_changed |= self.apply_user_info_to_tracks(&user_id, &info);
                    messages.push(ServerMessage::PeerInfo(PeerInfoPayload { user_id, info }));
                }
                if track_snapshot_changed {
                    messages.push(ServerMessage::Tracks(self.snapshot()));
                }
                UserWireMessages {
                    messages,
                    needs_renegotiation: false,
                }
            }
            RoomEventMessage::RecordingStateChanged(state) => {
                UserWireMessages::messages(vec![ServerMessage::RecordingChange(state)])
            }
        }
    }

    /// bootstrap one newly visible remote track for the browser
    pub fn apply_remote_track_bootstrap(&mut self, track: &RemoteTrackBootstrap) {
        let Some(stream_type) = stream_type_for_stream_id(track.stream_id()) else {
            return;
        };
        self.apply_track_binding(
            track.mid().to_owned(),
            track.user_id().clone(),
            stream_type,
            track.active(),
            track.source_descriptor(),
        );
    }

    /// build a full snapshot of all current track bindings
    pub fn snapshot(&self) -> Vec<TrackBinding> {
        self.bindings_by_mid.values().cloned().collect()
    }

    pub fn apply_track_binding_update(&mut self, update: &TrackBindingUpdate) -> UserWireMessages {
        let Some(stream_type) = stream_type_for_stream_id(&update.stream_id) else {
            return UserWireMessages::messages(Vec::new());
        };
        let changed = match update.active {
            Some(active) => self.set_track_active(&update.user_id, stream_type, active),
            None => self.remove_track_binding(&update.user_id, stream_type),
        };
        if !changed {
            return UserWireMessages::messages(Vec::new());
        }
        UserWireMessages {
            messages: vec![ServerMessage::Tracks(self.snapshot())],
            needs_renegotiation: update.active.is_none(),
        }
    }

    fn apply_track_binding(
        &mut self,
        mid: String,
        user_id: UserId,
        stream_type: StreamType,
        active: bool,
        source: &PublishedSourceDescriptor,
    ) {
        self.bindings_by_mid.insert(
            mid.clone(),
            TrackBinding {
                mid,
                user_id: user_id.clone(),
                stream_type,
                active,
                source: Some(source_descriptor_from_source(
                    source,
                    user_id,
                    stream_type,
                    active,
                )),
            },
        );
    }

    fn apply_user_info_to_tracks(&mut self, user_id: &UserId, info: &UserInfo) -> bool {
        let mut changed = false;
        for binding in self.bindings_by_mid.values_mut() {
            if &binding.user_id != user_id {
                continue;
            }
            let next_active = match binding.stream_type {
                StreamType::Camera => info.is_camera_on,
                StreamType::Screen => info.is_screen_sharing_on,
                StreamType::Audio => None,
            };
            let Some(next_active) = next_active else {
                continue;
            };
            if binding.active != next_active {
                binding.active = next_active;
                if let Some(source) = binding.source.as_mut() {
                    source.active = next_active;
                }
                changed = true;
            }
        }
        changed
    }

    fn set_track_active(
        &mut self,
        user_id: &UserId,
        stream_type: StreamType,
        active: bool,
    ) -> bool {
        let mut changed = false;
        for binding in self.bindings_by_mid.values_mut() {
            if &binding.user_id != user_id || binding.stream_type != stream_type {
                continue;
            }
            if binding.active != active {
                binding.active = active;
                if let Some(source) = binding.source.as_mut() {
                    source.active = active;
                }
                changed = true;
            }
        }
        changed
    }

    fn remove_track_binding(&mut self, user_id: &UserId, stream_type: StreamType) -> bool {
        let binding_count = self.bindings_by_mid.len();
        self.bindings_by_mid.retain(|_mid, binding| {
            &binding.user_id != user_id || binding.stream_type != stream_type
        });
        self.bindings_by_mid.len() != binding_count
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "test assertions use expect for direct fixture failures"
    )]

    use o_sfu_protocol::wire::StreamType;
    use o_sfu_router::{Mid, Rid};

    use super::*;
    use crate::{
        application::stream_catalog::source_publish_intent_for_stream_type,
        core::{
            prelude::Bitrate,
            server::source_model::{
                PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
                SourceEncodingDescriptor, SourceEncodingDescriptorParts, SourceEncodingId,
            },
        },
    };

    #[test]
    fn source_descriptor_mid_uses_published_source_mid_not_consumer_binding_mid() {
        let source = published_source("published-cam-0");
        let mut wire_state = UserWireState::default();

        wire_state.apply_track_binding(
            "subscriber-down-0".to_owned(),
            UserId::Integer(7),
            StreamType::Camera,
            true,
            &source,
        );

        let snapshot = wire_state.snapshot();
        let binding = snapshot
            .first()
            .expect("wire state should contain the inserted track binding");
        assert_eq!(binding.mid, "subscriber-down-0");
        let source = binding
            .source
            .as_ref()
            .expect("track binding should carry a source descriptor");
        assert_eq!(source.mid.as_deref(), Some("published-cam-0"));
    }

    fn published_source(mid: &str) -> PublishedSourceDescriptor {
        let source_id = PublishedSourceId::from_raw(1);
        let encoding = SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id: SourceEncodingId::from_raw(2),
            source_id,
            rid: Some(Rid::new("hi")),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate: Some(Bitrate::from_kbps(900)),
            resolution_scale: None,
            max_framerate: None,
            policy_role: None,
            max_temporal_layer_id: None,
            negotiated_format: None,
        });
        let intent = source_publish_intent_for_stream_type(StreamType::Camera);
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner: PublishedSourceOwner::new(UserId::Integer(7)),
            stream_id: intent.stream_id().clone(),
            media_kind: intent.media_kind(),
            policy: intent.policy(),
            mid: Some(Mid::new(mid)),
            encodings: vec![encoding],
        })
        .expect("test source descriptor should satisfy source graph invariants")
    }
}
