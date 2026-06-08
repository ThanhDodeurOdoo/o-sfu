use o_sfu_protocol::wire::{
    PeerInfoPayload, PeerLeftPayload, ServerBroadcastPayload, ServerEnvelope, ServerMessage,
};

use super::{User, UserError, UserOutput};
use crate::runtime::room::{RoomEventMessage, UserOutbound};

impl User {
    pub(crate) async fn apply_room_outbound(
        &mut self,
        outbound: UserOutbound,
    ) -> Result<UserOutput, UserError> {
        let mut output = UserOutput::new();
        let mut needs_renegotiation = false;
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
                    needs_renegotiation = self.tracks.remove_user(&user_id);
                    output.push(ServerEnvelope::Message(ServerMessage::PeerLeft(
                        PeerLeftPayload { user_id },
                    )));
                }
                RoomEventMessage::UserInfoChanged(snapshot) => {
                    let tracks_changed = self.tracks.apply_infos(&snapshot);
                    output.extend(snapshot.into_iter().map(|(user_id, info)| {
                        ServerEnvelope::Message(ServerMessage::PeerInfo(PeerInfoPayload {
                            user_id,
                            info,
                        }))
                    }));
                    if tracks_changed {
                        output.push(ServerEnvelope::Message(self.tracks.message()));
                    }
                }
                RoomEventMessage::RecordingStateChanged(state) => {
                    output.push(ServerEnvelope::Message(ServerMessage::RecordingChange(
                        state,
                    )));
                }
            },
            UserOutbound::SetupRemoteTrack(track) => {
                self.tracks.add_remote(&track);
                output.push(ServerEnvelope::Message(self.tracks.message()));
                needs_renegotiation = true;
            }
            UserOutbound::TrackBindingUpdate(update) => {
                if self.tracks.apply_update(&update) {
                    output.push(ServerEnvelope::Message(self.tracks.message()));
                    needs_renegotiation |= update.active.is_none();
                }
            }
        }
        if needs_renegotiation {
            output.extend(self.renegotiate().await?);
        }
        Ok(output)
    }
}
