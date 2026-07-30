use super::fixtures::*;
use crate::engine::{
    metrics::RuntimeMetrics,
    room::{
        RemoteTrackProjection, RemoteTrackSnapshot, UserOutboundOverflowKind,
        UserOutboundQueueLimits, UserOutboundSendError,
    },
    source_model::UserStreamId,
};

#[test]
fn remote_track_snapshot_accounts_for_string_bytes() {
    let (sender, _receiver) = UserOutboundSender::channel_with_limits(
        UserOutboundQueueLimits::new(8, 1_536),
        Arc::new(RuntimeMetrics::default()),
    );
    let snapshot = RemoteTrackSnapshot {
        tracks: vec![RemoteTrackProjection {
            consumer_mid: "0".to_owned(),
            user_id: UserId::String("x".repeat(256)),
            stream_id: UserStreamId::new("camera"),
            producer_active: true,
        }],
        requires_negotiation: true,
    };

    let UserOutboundSendError::Full(overflow) = sender
        .send(UserOutbound::RemoteTracks(snapshot))
        .expect_err("large track snapshots should exhaust byte capacity")
    else {
        panic!("large track snapshots should fail with a queue overflow");
    };

    assert_eq!(overflow.kind(), UserOutboundOverflowKind::QueuedBytes);
    assert!(overflow.message_bytes() > overflow.byte_capacity());
}
