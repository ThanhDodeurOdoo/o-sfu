use super::*;
use crate::{
    Bitrate,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, UserId,
        media_transport::{ReceiverBweTargetUpdate, TransportSessionKey},
    },
};

#[test]
fn receiver_without_video_routes_gets_zero_bwe_target() {
    let plan = receiver_video_selection_plan(
        &[],
        [(
            UserId::Integer(42),
            ReceiverBweTargetUpdate::new(
                TransportSessionKey::new(
                    RoomInstanceId::from_raw(0),
                    MediaWorkerId::from_raw(0),
                    ConnectionId::from_raw(10),
                    UserId::Integer(42),
                ),
                Bitrate::zero(),
            ),
        )]
        .into(),
        usize::MAX,
    );

    assert_eq!(plan.receiver_bwe_targets.len(), 1);
    assert_eq!(
        plan.receiver_bwe_targets
            .first()
            .map(ReceiverBweTargetUpdate::target),
        Some(Bitrate::zero())
    );
}
