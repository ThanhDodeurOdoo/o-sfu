use std::collections::BTreeMap;

use crate::{
    shared::SessionId,
    signaling::{PeerInfoPayload, ServerBroadcastPayload, ServerMessage, TrackBinding},
};

use super::{Command, Commands, ProtocolCore, ProtocolEvent};

pub(super) fn handle_server_message(core: &mut ProtocolCore, message: ServerMessage) -> Commands {
    match message {
        ServerMessage::Welcome(payload) => core.on_welcome(payload),
        ServerMessage::Tracks(bindings) => {
            replace_track_bindings(core, bindings);
            Vec::new()
        }
        ServerMessage::PeerInfo(payload) | ServerMessage::PeerJoined(payload) => {
            peer_info_commands(payload)
        }
        ServerMessage::PeerLeft(payload) => {
            remove_track_bindings_for_session(core, &payload.session_id);
            vec![Command::EmitEvent {
                event: ProtocolEvent::PeerLeft {
                    session_id: payload.session_id,
                },
            }]
        }
        ServerMessage::Broadcast(payload) => broadcast_commands(payload),
        ServerMessage::RecordingChange(payload) => {
            core.recording_state = payload.state.clone();
            vec![Command::EmitEvent {
                event: ProtocolEvent::RecordingStateChanged { state: payload },
            }]
        }
    }
}

fn replace_track_bindings(core: &mut ProtocolCore, bindings: Vec<TrackBinding>) {
    core.track_bindings = bindings
        .into_iter()
        .map(|binding| (binding.mid.clone(), binding))
        .collect::<BTreeMap<_, _>>();
}

fn remove_track_bindings_for_session(core: &mut ProtocolCore, session_id: &SessionId) {
    core.track_bindings
        .retain(|_, binding| &binding.session_id != session_id);
}

fn peer_info_commands(payload: PeerInfoPayload) -> Commands {
    vec![Command::EmitEvent {
        event: ProtocolEvent::PeerInfo {
            session_id: payload.session_id,
            info: payload.info,
        },
    }]
}

fn broadcast_commands(payload: ServerBroadcastPayload) -> Commands {
    vec![Command::EmitEvent {
        event: ProtocolEvent::Broadcast {
            sender_id: payload.sender_id,
            message: payload.message,
        },
    }]
}
