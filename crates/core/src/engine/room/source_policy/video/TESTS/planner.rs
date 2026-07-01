#![allow(
    clippy::expect_used,
    reason = "test fixtures should fail loudly when they build invalid source graphs"
)]

use std::sync::Arc;

use o_sfu_router::{MediaKind, RouterId, rtp::Rid};

use super::*;
use crate::{
    Bitrate, MediaCodecFlags, RoomMediaLimits,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, UserId, UserPermissions,
        media_transport::{SourcePacketGate, TransportMediaId},
        metrics::RuntimeMetrics,
        room::{
            RoomAdmissionPolicy, RoomRuntimeContext, RouterPlacement, UserOutboundSender,
            media_graph::ConsumerRouteTransportRef,
            rtp_capabilities::router_rtp_capabilities,
            source_policy::video::{
                input::ReceiverVideoRouteInput, layout::ReceiverVideoLayoutIntent,
            },
            state::RoomState,
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
    let (state, route) = receiver_video_route_input(
        &source,
        &UserId::Integer(2),
        current_selection,
        1,
        Bitrate::from_kbps(1_200),
    );

    let mut tx = SourcePolicyTransaction::default();
    append_receiver_video_selection(&mut tx, &state, &[route], BTreeMap::new(), usize::MAX);

    let mut updates = tx.route_updates_for_test();
    let update = updates
        .next()
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
    let (state, route) = receiver_video_route_input(
        &source,
        &UserId::Integer(2),
        current_selection,
        2,
        Bitrate::from_kbps(1_200),
    );

    let mut tx = SourcePolicyTransaction::default();
    append_receiver_video_selection(&mut tx, &state, &[route], BTreeMap::new(), usize::MAX);

    let mut updates = tx.route_updates_for_test();
    let update = updates
        .next()
        .expect("multi-thumbnail route should emit a quality update");
    assert_eq!(update.selector, SourceSelector::Encoding(low_encoding_id));
    assert!(update.outcomes.is_degraded());
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

fn receiver_video_route_input<'a>(
    source: &'a PublishedSourceDescriptor,
    consumer: &UserId,
    current_selection: ConsumerSourceSelection,
    visible_scalable_route_count: usize,
    receiver_bandwidth: Bitrate,
) -> (RoomState, ReceiverVideoRouteInput<'a>) {
    let mut state = test_state();
    let source_owner = source.owner().user_id().clone();
    let source_connection_id = join_test_user(&mut state, &source_owner);
    let consumer_connection_id = join_test_user(&mut state, consumer);
    let source_media = TransportMediaId::new(101);
    let consumer_media = TransportMediaId::new(202);
    let transport_ref = ConsumerRouteTransportRef::from_parts(
        consumer.clone(),
        consumer_connection_id,
        consumer_media,
        source_owner.clone(),
        source_connection_id,
        source_media,
    );

    (
        state,
        ReceiverVideoRouteInput {
            user_count: 3,
            source,
            transport_ref,
            current_selection,
            layout_intent: ReceiverVideoLayoutIntent::new(
                SourceRoomPolicySelector::VisibleThumbnail,
            ),
            visible_scalable_route_count,
            active_speaker_rank: None,
            receiver_bandwidth: Some(receiver_bandwidth),
        },
    )
}

fn test_state() -> RoomState {
    let runtime_context = RoomRuntimeContext::new(
        RoomInstanceId::from_raw(0),
        RouterPlacement {
            router: RouterId(1),
            media_worker: MediaWorkerId::from_raw(0),
        },
        Vec::new(),
    );
    RoomState::new(
        &runtime_context,
        RoomAdmissionPolicy::new(4),
        RoomMediaLimits::default(),
        router_rtp_capabilities(MediaCodecFlags::default()),
    )
}

fn test_sender() -> UserOutboundSender {
    UserOutboundSender::channel(128, Arc::new(RuntimeMetrics::default())).0
}

fn join_test_user(state: &mut RoomState, user_id: &UserId) -> ConnectionId {
    state
        .apply_join(user_id, UserPermissions::default(), test_sender())
        .expect("test user should join")
        .receipt
        .connection_id
}
