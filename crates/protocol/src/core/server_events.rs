use std::collections::BTreeMap;

use super::{Command, Commands, ProtocolCore, ProtocolEvent};
use crate::{
    shared::UserId,
    signaling::{
        PeerInfoPayload, ServerBroadcastPayload, ServerMessage, SourceDescriptor, TrackBinding,
    },
};

pub(super) fn handle_server_message(core: &mut ProtocolCore, message: ServerMessage) -> Commands {
    match message {
        ServerMessage::Welcome(payload) => core.accept_welcome(payload),
        ServerMessage::Tracks(bindings) => replace_track_snapshot(core, bindings),
        ServerMessage::PeerInfo(payload) | ServerMessage::PeerJoined(payload) => {
            peer_info_commands(payload)
        }
        ServerMessage::PeerLeft(payload) => {
            let source_snapshot_changed = remove_peer_bindings(core, &payload.user_id);
            let mut commands = if source_snapshot_changed {
                vec![Command::EmitEvent {
                    event: ProtocolEvent::SourceSnapshot {
                        sources: source_descriptors_from_track_bindings(core),
                    },
                }]
            } else {
                Vec::new()
            };
            commands.push(Command::EmitEvent {
                event: ProtocolEvent::PeerLeft {
                    user_id: payload.user_id,
                },
            });
            commands
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

fn replace_track_snapshot(core: &mut ProtocolCore, bindings: Vec<TrackBinding>) -> Commands {
    let had_source_descriptors = core
        .track_bindings
        .values()
        .any(|binding| binding.source.is_some());
    let next_sources = source_descriptors_from_bindings(&bindings);
    core.track_bindings = bindings
        .iter()
        .cloned()
        .map(|binding| (binding.mid.clone(), binding))
        .collect::<BTreeMap<_, _>>();
    let mut commands = vec![Command::EmitEvent {
        event: ProtocolEvent::TrackSnapshot { bindings },
    }];
    if had_source_descriptors || !next_sources.is_empty() {
        let sources = next_sources.values().cloned().collect();
        commands.push(Command::EmitEvent {
            event: ProtocolEvent::SourceSnapshot { sources },
        });
    }
    commands
}

fn remove_peer_bindings(core: &mut ProtocolCore, user_id: &UserId) -> bool {
    let mut removed_source = false;
    core.track_bindings.retain(|_, binding| {
        let remove = &binding.user_id == user_id;
        removed_source |= remove && binding.source.is_some();
        !remove
    });
    removed_source
}

fn source_descriptors_from_bindings(
    bindings: &[TrackBinding],
) -> BTreeMap<String, SourceDescriptor> {
    bindings
        .iter()
        .filter_map(|binding| {
            let source = binding.source.clone()?;
            Some((source.source_id.clone(), source))
        })
        .collect()
}

fn source_descriptors_from_track_bindings(core: &ProtocolCore) -> Vec<SourceDescriptor> {
    let mut sources = BTreeMap::new();
    for binding in core.track_bindings.values() {
        if let Some(source) = binding.source.clone() {
            sources.insert(source.source_id.clone(), source);
        }
    }
    sources.into_values().collect()
}

fn peer_info_commands(payload: PeerInfoPayload) -> Commands {
    vec![Command::EmitEvent {
        event: ProtocolEvent::PeerInfo {
            user_id: payload.user_id,
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
