use o_sfu_router::MediaKind;

use super::fixtures::*;
use crate::engine::{
    UserInfo,
    metrics::RuntimeMetrics,
    room::{
        RemoteSourceProjection, RemoteSourceSnapshot, UserOutboundOverflowKind,
        UserOutboundQueueLimits, UserOutboundSendError,
    },
    source_model::{
        PublishedSourceDescriptor, PublishedSourceDescriptorParts, PublishedSourceId,
        PublishedSourceOwner, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
        SourceEncodingId, SourcePolicy, UserStreamId,
    },
};

#[test]
fn remote_source_snapshots_count_projected_sources_toward_byte_capacity() {
    let (sender, _receiver) = UserOutboundSender::channel_with_limits(
        UserOutboundQueueLimits::new(8, 1_300),
        Arc::new(RuntimeMetrics::default()),
    );
    let snapshot = RemoteSourceSnapshot {
        sources: vec![remote_source_projection()],
        requires_negotiation: true,
    };

    let UserOutboundSendError::Full(overflow) = sender
        .send(UserOutbound::RemoteSources(snapshot))
        .expect_err("large source snapshots should exhaust byte capacity")
    else {
        panic!("large source snapshots should fail with a queue overflow");
    };

    assert_eq!(overflow.kind(), UserOutboundOverflowKind::QueuedBytes);
    assert!(overflow.message_bytes() > overflow.byte_capacity());
}

fn remote_source_projection() -> RemoteSourceProjection {
    let source_id = PublishedSourceId::from_raw(100);
    let source = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(UserId::Integer(1)),
        stream_id: UserStreamId::new("camera"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::hidden(),
        mid: None,
        encodings: vec![SourceEncodingDescriptor::new(
            SourceEncodingDescriptorParts {
                encoding_id: SourceEncodingId::from_raw(1_000),
                source_id,
                rid: None,
                primary_ssrc: None,
                repair_ssrc: None,
                max_bitrate: None,
                resolution_scale: None,
                max_framerate: None,
                policy_role: None,
                max_temporal_layer_id: None,
                negotiated_format: None,
            },
        )],
    })
    .expect("test source descriptor should be valid");

    RemoteSourceProjection {
        consumer_mid: "subscriber-consumer-mid".to_owned(),
        source,
        owner_info: UserInfo::default(),
        producer_active: true,
    }
}
