use std::collections::BTreeMap;

use super::{Command, Commands, ProtocolCore, ProtocolEvent};
use crate::signaling::{
    PeerInfoPayload, ServerBroadcastPayload, ServerMessage, SourceDescriptor, TrackBinding,
};

pub(super) fn handle_server_message(core: &mut ProtocolCore, message: ServerMessage) -> Commands {
    match message {
        ServerMessage::Welcome(payload) => core.accept_welcome(payload),
        ServerMessage::Tracks(bindings) => replace_track_snapshot(core, bindings),
        ServerMessage::Sources(sources) => replace_source_snapshot(core, sources),
        ServerMessage::PeerInfo(payload) | ServerMessage::PeerJoined(payload) => {
            peer_info_commands(payload)
        }
        ServerMessage::PeerLeft(payload) => {
            let source_count = core.source_descriptors.len();
            core.track_bindings
                .retain(|_, binding| binding.user_id != payload.user_id);
            core.source_descriptors
                .retain(|_, source| source.user_id != payload.user_id);
            let mut commands = if core.source_descriptors.len() == source_count {
                Vec::new()
            } else {
                vec![Command::EmitEvent {
                    event: ProtocolEvent::SourceSnapshot {
                        sources: core.source_descriptors.values().cloned().collect(),
                    },
                }]
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
    let legacy_sources = source_descriptors_from_bindings(&bindings);
    core.track_bindings = bindings
        .iter()
        .cloned()
        .map(|binding| (binding.mid.clone(), binding))
        .collect();
    let mut commands = vec![Command::EmitEvent {
        event: ProtocolEvent::TrackSnapshot { bindings },
    }];
    if !legacy_sources.is_empty() || core.source_descriptors_from_legacy_tracks {
        core.source_descriptors = legacy_sources;
        core.source_descriptors_from_legacy_tracks = !core.source_descriptors.is_empty();
        commands.push(Command::EmitEvent {
            event: ProtocolEvent::SourceSnapshot {
                sources: core.source_descriptors.values().cloned().collect(),
            },
        });
    }
    commands
}

fn replace_source_snapshot(core: &mut ProtocolCore, sources: Vec<SourceDescriptor>) -> Commands {
    core.source_descriptors_from_legacy_tracks = false;
    core.source_descriptors = sources
        .iter()
        .cloned()
        .map(|source| (source.source_id.clone(), source))
        .collect();
    vec![Command::EmitEvent {
        event: ProtocolEvent::SourceSnapshot { sources },
    }]
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
