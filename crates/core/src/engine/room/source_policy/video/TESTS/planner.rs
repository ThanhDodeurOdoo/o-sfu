use o_sfu_router::RouterId;

use super::*;
use crate::{
    Bitrate, MediaCodecFlags,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, UserId,
        media_transport::{ReceiverBweTargetUpdate, TransportSessionKey},
        room::{
            RoomRuntimeContext, RouterPlacement, media_graph::RoomTopology,
            rtp_capabilities::router_rtp_capabilities,
        },
    },
};

#[test]
fn receiver_without_video_routes_gets_zero_bwe_target() {
    let plan = receiver_video_selection_plan(
        &test_topology(),
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

fn test_topology() -> RoomTopology {
    let context = RoomRuntimeContext::new(
        RoomInstanceId::from_raw(0),
        RouterPlacement {
            router: RouterId(1),
            media_worker: MediaWorkerId::from_raw(0),
        },
        Vec::new(),
    );
    RoomTopology::new(
        &context,
        router_rtp_capabilities(MediaCodecFlags::default()),
    )
}
