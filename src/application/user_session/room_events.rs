use o_sfu_protocol::wire::ServerMessage;

use super::{User, UserError, UserOutput, state};
use crate::runtime::room::{RemoteTrackBootstrap, RoomEventMessage, TrackBindingUpdate};

impl User {
    /// Bootstrap one newly visible remote track for this websocket user.
    ///
    /// The room has already decided that the receiver should see the source.
    /// This method updates only the user-local compatibility track snapshot and
    /// then requests renegotiation so the browser can receive the media.
    pub async fn add_remote_track(
        &mut self,
        track: RemoteTrackBootstrap,
    ) -> Result<UserOutput, UserError> {
        self.state.wire_state.apply_remote_track_bootstrap(&track);
        let mut output = UserOutput::new()
            .with_signal(ServerMessage::Tracks(self.state.wire_state.snapshot()).into());
        output.extend(self.renegotiate().await?);
        Ok(output)
    }

    /// Apply a room-authored remote track binding delta for this websocket.
    ///
    /// Activity updates only refresh the local track snapshot. Removal also
    /// requests renegotiation because the browser must stop receiving that
    /// remote media section.
    pub async fn update_remote_track(
        &mut self,
        update: TrackBindingUpdate,
    ) -> Result<UserOutput, UserError> {
        let wire_messages = self.state.wire_state.apply_track_binding_update(&update);
        self.finalize_wire_messages(wire_messages).await
    }

    /// Convert a room-authored notification into this user's websocket output.
    ///
    /// Room state has already authorized and applied the transition. This method
    /// only updates the connection-local wire snapshot before the websocket edge
    /// serializes the resulting signals.
    pub(crate) async fn apply_room_message(
        &mut self,
        message: RoomEventMessage,
    ) -> Result<UserOutput, UserError> {
        let wire_messages = self.state.wire_state.apply_room_event(message);
        self.finalize_wire_messages(wire_messages).await
    }

    async fn finalize_wire_messages(
        &mut self,
        wire_messages: state::UserWireMessages,
    ) -> Result<UserOutput, UserError> {
        let mut output = UserOutput::from_messages(wire_messages.messages);
        if wire_messages.needs_renegotiation {
            output.extend(self.renegotiate().await?);
        }
        Ok(output)
    }
}
