use o_sfu_protocol::signaling::WelcomePayload;

use crate::{core::runtime::UserId, runtime::room::Room};

pub(crate) async fn welcome_payload(room: &Room, current_user_id: &UserId) -> WelcomePayload {
    WelcomePayload {
        features: room.available_features(),
        recording: room.recording_state().await,
        peers: room.user_snapshots_except(current_user_id).await,
    }
}
