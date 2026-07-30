use o_sfu_protocol::wire::{
    PeerInfoPayload, PeerLeftPayload, ServerBroadcastPayload, ServerEnvelope, ServerMessage,
    TrackBinding,
};

use super::{User, UserError, UserOutput};
use crate::{
    application::stream_catalog::stream_type_for_stream_id,
    runtime::room::{RoomEventMessage, UserOutbound},
};

impl User {
    pub(crate) async fn apply_room_outbound(
        &mut self,
        outbound: UserOutbound,
    ) -> Result<UserOutput, UserError> {
        let mut output = UserOutput::new();
        match outbound {
            UserOutbound::Close(_) => {}
            UserOutbound::Message(message) => match message {
                RoomEventMessage::Broadcast { sender_id, message } => {
                    output.push(ServerEnvelope::Message(ServerMessage::Broadcast(
                        ServerBroadcastPayload {
                            sender_id,
                            message: message.to_json(),
                        },
                    )));
                }
                RoomEventMessage::UserJoined { user_id, info } => {
                    output.push(ServerEnvelope::Message(ServerMessage::PeerJoined(
                        PeerInfoPayload { user_id, info },
                    )));
                }
                RoomEventMessage::UserDeparted { user_id } => {
                    output.push(ServerEnvelope::Message(ServerMessage::PeerLeft(
                        PeerLeftPayload { user_id },
                    )));
                }
                RoomEventMessage::UserInfoChanged(snapshot) => {
                    output.extend(snapshot.into_iter().map(|(user_id, info)| {
                        ServerEnvelope::Message(ServerMessage::PeerInfo(PeerInfoPayload {
                            user_id,
                            info,
                        }))
                    }));
                }
                RoomEventMessage::RecordingStateChanged(state) => {
                    output.push(ServerEnvelope::Message(ServerMessage::RecordingChange(
                        state,
                    )));
                }
            },
            UserOutbound::RemoteTracks(snapshot) => {
                output.push(ServerEnvelope::Message(ServerMessage::Tracks(
                    snapshot
                        .tracks
                        .into_iter()
                        .filter_map(|track| {
                            Some(TrackBinding {
                                mid: track.consumer_mid,
                                user_id: track.user_id,
                                stream_type: stream_type_for_stream_id(&track.stream_id)?,
                                active: track.producer_active,
                            })
                        })
                        .collect(),
                )));
                if snapshot.requires_negotiation {
                    output.extend(self.renegotiate().await?);
                }
            }
        }
        Ok(output)
    }
}
