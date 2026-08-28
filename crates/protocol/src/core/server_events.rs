use super::{Command, Commands, ProtocolCore, ProtocolEvent};
use crate::signaling::{ServerMessage, TrackBinding};

pub(super) fn handle_server_message(core: &mut ProtocolCore, message: ServerMessage) -> Commands {
    match message {
        ServerMessage::Welcome(payload) => core.accept_welcome(payload),
        ServerMessage::Tracks(bindings) => replace_track_snapshot(core, bindings),
        ServerMessage::Sources(_) => Vec::new(),
        ServerMessage::PeerInfo(payload) | ServerMessage::PeerJoined(payload) => {
            vec![Command::EmitEvent {
                event: ProtocolEvent::PeerInfo {
                    user_id: payload.user_id,
                    info: payload.info,
                },
            }]
        }
        ServerMessage::PeerLeft(payload) => {
            core.track_bindings
                .retain(|_, binding| binding.user_id != payload.user_id);
            vec![Command::EmitEvent {
                event: ProtocolEvent::PeerLeft {
                    user_id: payload.user_id,
                },
            }]
        }
        ServerMessage::Broadcast(payload) => vec![Command::EmitEvent {
            event: ProtocolEvent::Broadcast {
                sender_id: payload.sender_id,
                message: payload.message,
            },
        }],
        ServerMessage::RecordingChange(payload) => vec![
            Command::SetRecordingState {
                state: payload.state.clone(),
            },
            Command::EmitEvent {
                event: ProtocolEvent::RecordingStateChanged { state: payload },
            },
        ],
    }
}

fn replace_track_snapshot(core: &mut ProtocolCore, bindings: Vec<TrackBinding>) -> Commands {
    core.track_bindings = bindings
        .iter()
        .cloned()
        .map(|binding| (binding.mid.clone(), binding))
        .collect();
    vec![Command::EmitEvent {
        event: ProtocolEvent::TrackSnapshot { bindings },
    }]
}
