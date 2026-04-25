use std::collections::BTreeMap;

use o_sfu_protocol::{
    shared::{SessionId, SessionInfo, StreamType},
    signaling::{
        PeerInfoPayload, PeerLeftPayload, ServerBroadcastPayload, ServerMessage, SourceDescriptor,
        SourceEncodingDescriptor, TrackBinding,
    },
};

use crate::runtime::{
    channel::{ChannelEventMessage, RemoteTrackBootstrap, TrackBindingUpdate},
    source_model::{PublishedSourceDescriptor, SourceTemporalLayerId},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TranslatedServerMessage {
    pub(super) messages: Vec<ServerMessage>,
    pub(super) needs_renegotiation: bool,
}

impl TranslatedServerMessage {
    pub(super) fn messages(messages: Vec<ServerMessage>) -> Self {
        Self {
            messages,
            needs_renegotiation: false,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct RemoteTrackProjection {
    bindings_by_mid: BTreeMap<String, TrackBinding>,
}

impl RemoteTrackProjection {
    pub(super) fn translate_server_message(
        &mut self,
        message: ChannelEventMessage,
    ) -> TranslatedServerMessage {
        match message {
            ChannelEventMessage::Broadcast { sender_id, message } => {
                TranslatedServerMessage::messages(vec![ServerMessage::Broadcast(
                    ServerBroadcastPayload { sender_id, message },
                )])
            }
            ChannelEventMessage::SessionJoined { session_id, info } => {
                TranslatedServerMessage::messages(vec![ServerMessage::PeerJoined(
                    PeerInfoPayload { session_id, info },
                )])
            }
            ChannelEventMessage::SessionDeparted { session_id } => {
                let removed_tracks = self
                    .bindings_by_mid
                    .values()
                    .any(|binding| binding.session_id == session_id);
                self.bindings_by_mid
                    .retain(|_mid, binding| binding.session_id != session_id);
                TranslatedServerMessage {
                    messages: vec![ServerMessage::PeerLeft(PeerLeftPayload { session_id })],
                    needs_renegotiation: removed_tracks,
                }
            }
            ChannelEventMessage::SessionInfoChanged(snapshot) => {
                self.translate_session_info_snapshot(snapshot)
            }
            ChannelEventMessage::RecordingStateChanged(state) => {
                TranslatedServerMessage::messages(vec![ServerMessage::RecordingChange(state)])
            }
        }
    }

    pub(super) fn apply_remote_track_bootstrap(&mut self, payload: &RemoteTrackBootstrap) {
        let mid = payload.mid().to_owned();
        self.bindings_by_mid.insert(
            mid.clone(),
            TrackBinding {
                mid,
                session_id: payload.session_id().clone(),
                stream_type: payload.stream_type(),
                active: payload.active(),
                source: Some(source_descriptor_from_bootstrap(payload)),
            },
        );
    }

    pub(super) fn snapshot(&self) -> Vec<TrackBinding> {
        self.bindings_by_mid.values().cloned().collect()
    }

    pub(super) fn translate_track_binding_update(
        &mut self,
        update: &TrackBindingUpdate,
    ) -> TranslatedServerMessage {
        let changed = match update.active {
            Some(active) => self.set_track_active(&update.session_id, update.stream_type, active),
            None => self.remove_track_binding(&update.session_id, update.stream_type),
        };
        if !changed {
            return TranslatedServerMessage::messages(Vec::new());
        }
        TranslatedServerMessage {
            messages: vec![ServerMessage::Tracks(self.snapshot())],
            needs_renegotiation: update.active.is_none(),
        }
    }

    fn translate_session_info_snapshot(
        &mut self,
        snapshot: BTreeMap<SessionId, SessionInfo>,
    ) -> TranslatedServerMessage {
        let mut messages = Vec::with_capacity(snapshot.len().saturating_add(1));
        let mut track_snapshot_changed = false;
        for (session_id, info) in snapshot {
            track_snapshot_changed |= self.apply_session_info_to_tracks(&session_id, &info);
            messages.push(ServerMessage::PeerInfo(PeerInfoPayload {
                session_id,
                info,
            }));
        }
        if track_snapshot_changed {
            messages.push(ServerMessage::Tracks(self.snapshot()));
        }
        TranslatedServerMessage {
            messages,
            needs_renegotiation: false,
        }
    }

    fn apply_session_info_to_tracks(&mut self, session_id: &SessionId, info: &SessionInfo) -> bool {
        let mut changed = false;
        for binding in self.bindings_by_mid.values_mut() {
            if &binding.session_id != session_id {
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
        session_id: &SessionId,
        stream_type: StreamType,
        active: bool,
    ) -> bool {
        let mut changed = false;
        for binding in self.bindings_by_mid.values_mut() {
            if &binding.session_id != session_id || binding.stream_type != stream_type {
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

    fn remove_track_binding(&mut self, session_id: &SessionId, stream_type: StreamType) -> bool {
        let binding_count = self.bindings_by_mid.len();
        self.bindings_by_mid.retain(|_mid, binding| {
            &binding.session_id != session_id || binding.stream_type != stream_type
        });
        self.bindings_by_mid.len() != binding_count
    }
}

fn source_descriptor_from_bootstrap(payload: &RemoteTrackBootstrap) -> SourceDescriptor {
    let source = payload.source_descriptor();
    SourceDescriptor {
        source_id: source.source_id().to_string(),
        session_id: payload.session_id().clone(),
        stream_type: payload.stream_type(),
        active: payload.active(),
        mid: Some(payload.mid().to_owned()),
        encodings: source_encodings(source),
    }
}

fn source_encodings(source: &PublishedSourceDescriptor) -> Vec<SourceEncodingDescriptor> {
    source
        .encodings()
        .map(|encoding| SourceEncodingDescriptor {
            encoding_id: encoding.encoding_id().to_string(),
            rid: encoding.rid().map(|rid| rid.as_str().to_owned()),
            max_bitrate: encoding.max_bitrate(),
            max_temporal_layer_id: encoding
                .max_temporal_layer_id()
                .map(SourceTemporalLayerId::as_u8),
        })
        .collect()
}
