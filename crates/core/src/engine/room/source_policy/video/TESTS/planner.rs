#![allow(
    clippy::expect_used,
    reason = "test fixtures should fail loudly when they build invalid source graphs"
)]

use o_sfu_router::{MediaKind, RouterId, rtp::Rid};

use super::*;
use crate::{
    Bitrate, MediaCodecFlags,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, UserId,
        media_transport::{
            ReceiverBweTargetUpdate, SourcePacketGate, TransportMediaId, TransportSessionKey,
        },
        room::{
            RoomRuntimeContext, RouterPlacement,
            media_graph::{ConsumerRouteTransportRef, RoomTopology},
            rtp_capabilities::router_rtp_capabilities,
            source_policy::video::{
                input::ReceiverVideoRouteInput, layout::ReceiverVideoLayoutIntent,
            },
        },
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceDescriptorParts,
            PublishedSourceId, PublishedSourceOwner, SourceAdaptationPolicy,
            SourceEncodingDescriptor, SourceEncodingDescriptorParts, SourceEncodingId,
            SourceLayoutPolicy, SourcePolicy, SourceRoomPolicySelector, SourceSelector,
            UserStreamId,
        },
    },
};

#[test]
fn single_visible_thumbnail_can_use_full_receiver_budget() {
    let source_id = PublishedSourceId::from_raw(7);
    let low_encoding_id = SourceEncodingId::from_raw(1);
    let high_encoding_id = SourceEncodingId::from_raw(2);
    let source = scalable_video_source(
        source_id,
        UserId::Integer(1),
        vec![
            encoding(source_id, low_encoding_id, "lo", Bitrate::from_kbps(300)),
            encoding(source_id, high_encoding_id, "hi", Bitrate::from_kbps(1_000)),
        ],
    );
    let mut current_selection = ConsumerSourceSelection::open(true);
    current_selection.set_selector(SourceSelector::Encoding(low_encoding_id));
    current_selection.set_adaptation_observations(0, 2);
    let route = receiver_video_route_input(
        &source,
        UserId::Integer(2),
        current_selection,
        1,
        Bitrate::from_kbps(1_200),
    );

    let plan =
        receiver_video_selection_plan(&test_topology(), &[route], BTreeMap::new(), usize::MAX);

    let update = plan
        .transport_packet_updates
        .first()
        .map(|packet| &packet.update)
        .expect("single route should emit a quality update");
    assert_eq!(
        update.selector,
        SourceSelector::Encoding(high_encoding_id),
        "one visible thumbnail should not halve the receiver budget"
    );
    assert_eq!(
        update.packet_gate,
        Some(SourcePacketGate::Rid(String::from("hi")))
    );
    assert!(!update.outcomes.is_degraded());
    assert!(!update.outcomes.is_protected_over_budget());
}

#[test]
fn multiple_visible_thumbnails_keep_safety_budget() {
    let source_id = PublishedSourceId::from_raw(7);
    let low_encoding_id = SourceEncodingId::from_raw(1);
    let high_encoding_id = SourceEncodingId::from_raw(2);
    let source = scalable_video_source(
        source_id,
        UserId::Integer(1),
        vec![
            encoding(source_id, low_encoding_id, "lo", Bitrate::from_kbps(300)),
            encoding(source_id, high_encoding_id, "hi", Bitrate::from_kbps(1_000)),
        ],
    );
    let mut current_selection = ConsumerSourceSelection::open(true);
    current_selection.set_selector(SourceSelector::Encoding(high_encoding_id));
    current_selection.set_adaptation_observations(1, 0);
    let route = receiver_video_route_input(
        &source,
        UserId::Integer(2),
        current_selection,
        2,
        Bitrate::from_kbps(1_200),
    );

    let plan =
        receiver_video_selection_plan(&test_topology(), &[route], BTreeMap::new(), usize::MAX);

    let update = plan
        .transport_packet_updates
        .first()
        .map(|packet| &packet.update)
        .expect("multi-thumbnail route should emit a quality update");
    assert_eq!(update.selector, SourceSelector::Encoding(low_encoding_id));
    assert!(update.outcomes.is_degraded());
}

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
    let context =
        RoomRuntimeContext::new(RoomInstanceId::from_raw(0), test_placement(), Vec::new());
    let mut topology = RoomTopology::new(
        &context,
        router_rtp_capabilities(MediaCodecFlags::default()),
    );
    assert!(
        topology
            .commit_session_placement(
                &UserId::Integer(1),
                ConnectionId::from_raw(11),
                None,
                test_placement(),
            )
            .is_ok()
    );
    assert!(
        topology
            .commit_session_placement(
                &UserId::Integer(2),
                ConnectionId::from_raw(22),
                None,
                test_placement(),
            )
            .is_ok()
    );
    topology
}

fn test_placement() -> RouterPlacement {
    RouterPlacement {
        router: RouterId(1),
        media_worker: MediaWorkerId::from_raw(0),
    }
}

fn scalable_video_source(
    source_id: PublishedSourceId,
    owner: UserId,
    encodings: Vec<SourceEncodingDescriptor>,
) -> PublishedSourceDescriptor {
    PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(owner),
        stream_id: UserStreamId::new("main-video"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::new(
            Some(SourceLayoutPolicy::new(
                SourceRoomPolicySelector::VisibleThumbnail,
                None,
            )),
            SourceAdaptationPolicy::ScalableVideo,
            None,
        ),
        mid: None,
        encodings,
    })
    .expect("test source graph should be valid")
}

fn encoding(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: &str,
    max_bitrate: Bitrate,
) -> SourceEncodingDescriptor {
    SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
        encoding_id,
        source_id,
        rid: Some(Rid::new(rid)),
        primary_ssrc: None,
        repair_ssrc: None,
        max_bitrate: Some(max_bitrate),
        resolution_scale: None,
        max_framerate: None,
        policy_role: None,
        max_temporal_layer_id: None,
        negotiated_format: None,
    })
}

fn receiver_video_route_input(
    source: &PublishedSourceDescriptor,
    consumer: UserId,
    current_selection: ConsumerSourceSelection,
    visible_scalable_route_count: usize,
    receiver_bandwidth: Bitrate,
) -> ReceiverVideoRouteInput<'_> {
    let source_owner = source.owner().user_id().clone();
    let source_connection_id = ConnectionId::from_raw(11);
    let consumer_connection_id = ConnectionId::from_raw(22);
    let source_media = TransportMediaId::new(101);
    let consumer_media = TransportMediaId::new(202);

    ReceiverVideoRouteInput {
        user_count: 3,
        source,
        transport_ref: ConsumerRouteTransportRef::from_parts(
            consumer,
            consumer_connection_id,
            consumer_media,
            source_owner,
            source_connection_id,
            source_media,
        ),
        current_selection,
        layout_intent: ReceiverVideoLayoutIntent::new(SourceRoomPolicySelector::VisibleThumbnail),
        visible_scalable_route_count,
        active_speaker_rank: None,
        receiver_bandwidth: Some(receiver_bandwidth),
    }
}
