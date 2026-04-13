use std::collections::BTreeMap;

use crate::signaling::{
    current_protocol::{
        CurrentRemoteTrackBootstrapPayload, CurrentServerMessage, CurrentSessionInfoSnapshotById,
    },
    protocol::{
        PeerInfoPayload, PeerLeftPayload, ServerBroadcastPayload, ServerMessage, TrackBinding,
        WebSocketCloseCode,
    },
    shared::{SessionId, SessionInfo, StreamType},
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
        message: CurrentServerMessage,
    ) -> TranslatedServerMessage {
        match message {
            CurrentServerMessage::Broadcast(payload) => {
                TranslatedServerMessage::messages(vec![ServerMessage::Broadcast(
                    ServerBroadcastPayload {
                        sender_id: payload.sender_id,
                        message: payload.message,
                    },
                )])
            }
            CurrentServerMessage::SessionDeparted(payload) => {
                let removed_tracks = self
                    .bindings_by_mid
                    .values()
                    .any(|binding| binding.session_id == payload.session_id);
                self.bindings_by_mid
                    .retain(|_mid, binding| binding.session_id != payload.session_id);
                TranslatedServerMessage {
                    messages: vec![ServerMessage::PeerLeft(PeerLeftPayload {
                        session_id: payload.session_id,
                    })],
                    needs_renegotiation: removed_tracks,
                }
            }
            CurrentServerMessage::SessionInfoChanged(snapshot) => {
                self.translate_session_info_snapshot(snapshot)
            }
            CurrentServerMessage::ChannelStateChanged(state) => {
                TranslatedServerMessage::messages(vec![ServerMessage::RecordingChange(state)])
            }
        }
    }

    pub(super) fn apply_remote_track_bootstrap(
        &mut self,
        payload: CurrentRemoteTrackBootstrapPayload,
    ) -> Result<(), WebSocketCloseCode> {
        let Some(mid) = payload
            .rtp_parameters
            .0
            .get("mid")
            .and_then(serde_json::Value::as_str)
        else {
            return Err(WebSocketCloseCode::Error);
        };
        self.bindings_by_mid.insert(
            mid.to_owned(),
            TrackBinding {
                mid: mid.to_owned(),
                session_id: payload.session_id,
                stream_type: payload.stream_type,
                active: payload.active,
            },
        );
        Ok(())
    }

    pub(super) fn snapshot(&self) -> Vec<TrackBinding> {
        self.bindings_by_mid.values().cloned().collect()
    }

    fn translate_session_info_snapshot(
        &mut self,
        snapshot: CurrentSessionInfoSnapshotById,
    ) -> TranslatedServerMessage {
        let mut messages = Vec::with_capacity(snapshot.len().saturating_add(1));
        let mut track_snapshot_changed = false;
        for (bundle_key, info) in snapshot {
            let session_id = parse_bundle_session_info_key(&bundle_key);
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
                changed = true;
            }
        }
        changed
    }
}

fn parse_bundle_session_info_key(key: &str) -> SessionId {
    match key.parse::<i64>() {
        Ok(value) => SessionId::Integer(value),
        Err(_error) => SessionId::String(key.to_owned()),
    }
}
