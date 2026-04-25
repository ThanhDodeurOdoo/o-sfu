#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::sync::Arc;

use o_sfu_protocol::shared::{DownloadStates, SessionId, SessionPermissions, StreamType};
use o_sfu_router::{
    ConsumerCapability, MediaKind as RouterMediaKind, ProducerId, RouterId,
    StreamType as RouterStreamType, derive_consumable_rtp_parameters,
};
use tokio::sync::mpsc;

use super::super::{
    ids::ProducerRuntimeId,
    shared::{
        ChannelState, ConsumerKey, ConsumerState, PublishedProducer, SourceKey,
        SourceTransportMediaIndexEntry,
    },
};
use crate::{
    config::MediaCodecFlags,
    runtime::{
        ChannelInstanceId, ConnectionId,
        channel::{
            ChannelAdmissionPolicy, rtp_capabilities::router_rtp_capabilities,
            session_negotiation::SessionTransportReady, topology::RoutedProducerId,
        },
        metrics::RuntimeMetrics,
        recording::{MediaSource, MediaTap, RecordingService},
        source_model::{
            PublishedSourceDescriptor, PublishedSourceDescriptorParts, PublishedSourceId,
            PublishedSourceOwner, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
            SourceEncodingId, SourceSelector,
        },
        test_rtp_samples::{
            sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
            sample_video_rtp_parameters,
        },
        transport_adapter::TransportMediaId,
    },
};

fn test_state() -> ChannelState {
    let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
    ChannelState::new(
        RouterId(1),
        ChannelAdmissionPolicy::new(4),
        router_rtp_capabilities(MediaCodecFlags::default()),
        Arc::new(RecordingService::new(
            ChannelInstanceId::from_raw(0),
            media_source,
            Arc::new(RuntimeMetrics::default()),
        )),
    )
}

fn join_test_session(state: &mut ChannelState, session_id: &SessionId) {
    let (sender, _rx) = mpsc::unbounded_channel();
    assert!(
        state
            .apply_join(
                session_id,
                None,
                SessionPermissions::default(),
                sender,
                false,
            )
            .is_ok()
    );
}

fn install_test_consumer_route(
    state: &mut ChannelState,
    producer_session_id: &SessionId,
    consumer_session_id: &SessionId,
) -> (ConsumerKey, ConnectionId) {
    let producer_connection_id = state
        .session_connection_id(producer_session_id)
        .expect("producer session should have a connection id");
    let consumer_connection_id = state
        .session_connection_id(consumer_session_id)
        .expect("consumer session should have a connection id");
    let routed_producer_id = state
        .topology
        .add_producer(
            producer_session_id,
            RouterMediaKind::Video,
            RouterStreamType::Camera,
        )
        .unwrap_or_else(|error| panic!("failed to create test producer route: {error:?}"));
    let routed_consumer_id = state
        .topology
        .add_consumer(
            consumer_session_id,
            routed_producer_id,
            RouterMediaKind::Video,
            RouterStreamType::Camera,
            ConsumerCapability::Compatible,
        )
        .unwrap_or_else(|error| panic!("failed to create test consumer route: {error:?}"));
    let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
    let source_id = install_test_source_graph(
        state,
        producer_session_id,
        producer_connection_id,
        StreamType::Camera,
        producer_id,
        TransportMediaId::new(1),
    );
    state.producers.insert(
        producer_id,
        PublishedProducer {
            source_id,
            owner_session_id: producer_session_id.clone(),
            owner_connection_id: producer_connection_id,
            stream_type: StreamType::Camera,
            media_kind: RouterMediaKind::Video,
            consumable_rtp_parameters: sample_video_rtp_parameters(None, 77_777),
            routed_producer_id,
            transport_media_id: Some(TransportMediaId::new(1)),
            active: true,
        },
    );
    let route_key = ConsumerKey::new(consumer_session_id, source_id);
    let consumer_state = ConsumerState {
        routed_consumer_id,
        consumer_connection_id,
        source_connection_id: producer_connection_id,
        source_media: TransportMediaId::new(1),
        consumer_media: TransportMediaId::new(2),
    };
    state
        .consumer_index
        .insert(route_key.clone(), consumer_state);
    (route_key, consumer_connection_id)
}

fn install_test_source_graph(
    state: &mut ChannelState,
    session_id: &SessionId,
    connection_id: ConnectionId,
    stream_type: StreamType,
    producer_id: ProducerRuntimeId,
    transport_media_id: TransportMediaId,
) -> PublishedSourceId {
    let source_id = PublishedSourceId::allocate(&mut state.next_source_id);
    let encoding_id = SourceEncodingId::allocate(&mut state.next_source_encoding_id);
    let source = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(session_id.clone()),
        stream_type,
        media_kind: RouterMediaKind::Video,
        mid: None,
        encodings: vec![SourceEncodingDescriptor::new(
            SourceEncodingDescriptorParts {
                encoding_id,
                source_id,
                rid: None,
                primary_ssrc: None,
                repair_ssrc: None,
                max_bitrate: None,
                max_temporal_layer_id: None,
                negotiated_format: None,
            },
        )],
    })
    .expect("test source graph should be valid");
    state.sources.insert(source_id, source);
    state
        .source_ids_by_owner_stream
        .insert(SourceKey::new(session_id, stream_type), source_id);
    state
        .producer_id_by_source_id
        .insert(source_id, producer_id);
    state.source_transport_media_index.insert(
        transport_media_id,
        SourceTransportMediaIndexEntry::new(
            source_id,
            vec![encoding_id],
            session_id.clone(),
            connection_id,
            stream_type,
        ),
    );
    source_id
}

#[test]
fn producer_activity_does_not_flip_channel_state_when_router_update_fails() {
    let mut state = test_state();
    let session_id = SessionId::Integer(1);
    let (sender, _rx) = mpsc::unbounded_channel();

    let join = state.apply_join(
        &session_id,
        None,
        SessionPermissions::default(),
        sender,
        false,
    );
    assert!(join.is_ok());
    let connection_id = state
        .session_connection_id(&session_id)
        .unwrap_or(ConnectionId::from_raw(u64::MAX));

    let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
    let routed_producer_id = RoutedProducerId::new(RouterId(1), ProducerId(777));
    let transport_media_id = TransportMediaId::default();
    let source_id = install_test_source_graph(
        &mut state,
        &session_id,
        connection_id,
        StreamType::Camera,
        producer_id,
        transport_media_id,
    );
    state.producers.insert(
        producer_id,
        PublishedProducer {
            source_id,
            owner_session_id: session_id.clone(),
            owner_connection_id: connection_id,
            stream_type: StreamType::Camera,
            media_kind: RouterMediaKind::Video,
            consumable_rtp_parameters: sample_video_rtp_parameters(None, 77_777),
            routed_producer_id,
            transport_media_id: Some(transport_media_id),
            active: true,
        },
    );

    let producer_target = state
        .producer_route_target(&session_id, connection_id, StreamType::Camera)
        .expect("inserted producer should resolve back to a route target");
    let outcome =
        state.apply_producer_activity(&session_id, &producer_target, StreamType::Camera, false);
    assert!(outcome.is_none());
    assert!(
        state
            .producers
            .get(&producer_id)
            .is_some_and(|producer| producer.active),
        "channel state must keep the previous activity flag when router pause propagation fails"
    );
}

#[test]
fn stale_replaced_connection_cannot_update_download_state() {
    let mut state = test_state();
    let producer_session_id = SessionId::Integer(1);
    let consumer_session_id = SessionId::Integer(2);
    let (replacement_sender, _replacement_rx) = mpsc::unbounded_channel();

    join_test_session(&mut state, &producer_session_id);
    join_test_session(&mut state, &consumer_session_id);
    let (route_key, stale_connection_id) =
        install_test_consumer_route(&mut state, &producer_session_id, &consumer_session_id);

    assert!(
        state
            .apply_join(
                &consumer_session_id,
                Some(String::from("replacement")),
                SessionPermissions::default(),
                replacement_sender,
                false,
            )
            .is_ok()
    );

    let (committed_updates, planned_bootstraps) = state
        .plan_subscription_change(
            &consumer_session_id,
            stale_connection_id,
            &producer_session_id,
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
            },
        )
        .into_parts();

    assert!(committed_updates.is_empty());
    assert!(planned_bootstraps.is_empty());
    assert!(
        state.desired_download_active(
            &consumer_session_id,
            &producer_session_id,
            StreamType::Camera,
        ),
        "stale subscription updates must not overwrite the replacement session's stored preferences"
    );
    assert_eq!(
        state.consumer_index.get(&route_key),
        None,
        "replacement join should clear stale consumer routes before the new connection reboots them"
    );
}

#[test]
fn subscription_change_reserves_missing_bootstrap_for_existing_publisher() {
    let mut state = test_state();
    let publisher_session_id = SessionId::Integer(1);
    let subscriber_session_id = SessionId::Integer(2);

    join_test_session(&mut state, &publisher_session_id);
    join_test_session(&mut state, &subscriber_session_id);

    let publisher_connection_id = state
        .session_connection_id(&publisher_session_id)
        .expect("publisher should have a connection id");
    let subscriber_connection_id = state
        .session_connection_id(&subscriber_session_id)
        .expect("subscriber should have a connection id");

    assert!(
        state
            .set_client_rtp_capabilities_for_test(
                &subscriber_session_id,
                subscriber_connection_id,
                &sample_client_rtp_capabilities(),
            )
            .session_present
    );
    assert!(
        state
            .set_transport_ready_for_test(
                &subscriber_session_id,
                subscriber_connection_id,
                SessionTransportReady::Consume,
            )
            .session_present
    );

    let routed_producer_id = state
        .topology
        .add_producer(
            &publisher_session_id,
            RouterMediaKind::Video,
            RouterStreamType::Camera,
        )
        .expect("publisher route should be added");
    let producer_id = ProducerRuntimeId::allocate(&mut state.next_producer_id);
    let producer_rtp_parameters = sample_video_rtp_parameters(None, 22_222);
    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &producer_rtp_parameters,
        &state.router_rtp_capabilities(),
    )
    .expect("publisher RTP parameters should derive consumable router parameters");
    let source_id = install_test_source_graph(
        &mut state,
        &publisher_session_id,
        publisher_connection_id,
        StreamType::Camera,
        producer_id,
        TransportMediaId::new(10),
    );
    state.producers.insert(
        producer_id,
        PublishedProducer {
            source_id,
            owner_session_id: publisher_session_id.clone(),
            owner_connection_id: publisher_connection_id,
            stream_type: StreamType::Camera,
            media_kind: RouterMediaKind::Video,
            consumable_rtp_parameters,
            routed_producer_id,
            transport_media_id: Some(TransportMediaId::new(10)),
            active: true,
        },
    );

    let (route_updates, planned_bootstraps) = state
        .plan_subscription_change(
            &subscriber_session_id,
            subscriber_connection_id,
            &publisher_session_id,
            &DownloadStates {
                camera: Some(false),
                audio: None,
                screen: None,
            },
        )
        .into_parts();

    assert!(route_updates.is_empty());
    assert_eq!(planned_bootstraps.len(), 1);
    let selection = state
        .consumer_source_selections
        .get(&ConsumerKey::new(&subscriber_session_id, source_id))
        .expect("compat subscription should create a source-level selection");
    assert!(!selection.active());
    assert_eq!(
        selection.selector(),
        SourceSelector::Open,
        "compat downloads default to an unconstrained source selector"
    );
    assert!(
        state.subscription_count() >= 1,
        "planning the bootstrap must reserve the pending consumer slot immediately"
    );
}

#[test]
fn commit_published_track_populates_transport_media_owner_index() {
    let mut state = test_state();
    let session_id = SessionId::Integer(1);

    join_test_session(&mut state, &session_id);
    let connection_id = state
        .session_connection_id(&session_id)
        .expect("publisher should have a connection id");
    assert!(
        state
            .set_client_rtp_capabilities_for_test(
                &session_id,
                connection_id,
                &sample_client_rtp_capabilities(),
            )
            .session_present
    );
    assert!(
        state
            .set_transport_ready_for_test(
                &session_id,
                connection_id,
                SessionTransportReady::Publish,
            )
            .session_present
    );

    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_video_rtp_parameters(None, 42_000),
        &state.router_rtp_capabilities(),
    )
    .expect("publisher RTP parameters should derive consumable router parameters");
    let prepared_track = state
        .validate_publish_descriptor(
            &session_id,
            connection_id,
            StreamType::Camera,
            RouterMediaKind::Video,
        )
        .expect("publish descriptor should validate once the session is publish-ready")
        .into_prepared_track(consumable_rtp_parameters);
    let transport_media_id = TransportMediaId::new(99);

    assert!(
        state
            .commit_published_track(prepared_track, transport_media_id)
            .is_some()
    );
    assert_eq!(
        state.producer_stream_type_for_transport_media_id(transport_media_id),
        Some(StreamType::Camera)
    );
    assert_eq!(
        state.inspect_producer_owner_session_id_for_transport_media_id(transport_media_id),
        Some(session_id)
    );
    assert_eq!(
        state.inspect_producer_owner_connection_id_for_transport_media_id(transport_media_id),
        Some(connection_id)
    );
    assert_eq!(state.sources.len(), 1);
    let source_id = state
        .inspect_source_id_for_transport_media_id(transport_media_id)
        .expect("transport media should resolve to a source id");
    assert!(
        state.sources.contains_key(&source_id),
        "transport media source id should point into the source registry"
    );
    assert_eq!(
        state
            .inspect_source_encoding_ids_for_transport_media_id(transport_media_id)
            .expect("transport media should resolve to source encodings")
            .len(),
        1
    );
}

#[test]
fn commit_published_track_registers_all_source_encodings() {
    let mut state = test_state();
    let session_id = SessionId::Integer(1);

    join_test_session(&mut state, &session_id);
    let connection_id = state
        .session_connection_id(&session_id)
        .expect("publisher should have a connection id");
    assert!(
        state
            .set_client_rtp_capabilities_for_test(
                &session_id,
                connection_id,
                &sample_client_rtp_capabilities(),
            )
            .session_present
    );
    assert!(
        state
            .set_transport_ready_for_test(
                &session_id,
                connection_id,
                SessionTransportReady::Publish,
            )
            .session_present
    );

    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_simulcast_video_rtp_parameters(Some("camera-0")),
        &state.router_rtp_capabilities(),
    )
    .expect("simulcast RTP parameters should derive consumable router parameters");
    let prepared_track = state
        .validate_publish_descriptor(
            &session_id,
            connection_id,
            StreamType::Camera,
            RouterMediaKind::Video,
        )
        .expect("publish descriptor should validate once the session is publish-ready")
        .into_prepared_track(consumable_rtp_parameters);
    let transport_media_id = TransportMediaId::new(101);

    assert!(
        state
            .commit_published_track(prepared_track, transport_media_id)
            .is_some()
    );

    let source_id = state
        .inspect_source_id_for_transport_media_id(transport_media_id)
        .expect("transport media should resolve to the committed source");
    let source = state
        .sources
        .get(&source_id)
        .expect("source registry should own the committed source");
    assert_eq!(source.owner().session_id(), &session_id);
    assert_eq!(source.stream_type(), StreamType::Camera);
    assert_eq!(
        source.mid().map(o_sfu_router::Mid::as_str),
        Some("camera-0")
    );
    let encodings = source.encodings().collect::<Vec<_>>();
    assert_eq!(encodings.len(), 2);
    assert_eq!(
        encodings[0].rid().map(o_sfu_router::Rid::as_str),
        Some("lo")
    );
    assert_eq!(
        encodings[1].rid().map(o_sfu_router::Rid::as_str),
        Some("hi")
    );
    assert_eq!(
        encodings[0].primary_ssrc(),
        Some(o_sfu_router::Ssrc::new(31_001))
    );
    assert_eq!(
        encodings[1].primary_ssrc(),
        Some(o_sfu_router::Ssrc::new(31_002))
    );
    assert_eq!(encodings[0].max_bitrate(), Some(150_000));
    assert_eq!(encodings[1].max_bitrate(), Some(900_000));
    assert_eq!(
        state
            .inspect_source_encoding_ids_for_transport_media_id(transport_media_id)
            .expect("transport media should resolve to source encoding ids"),
        encodings
            .iter()
            .map(|encoding| encoding.encoding_id())
            .collect::<Vec<_>>()
    );
}

#[test]
fn unpublish_track_clears_transport_media_owner_index() {
    let mut state = test_state();
    let session_id = SessionId::Integer(1);

    join_test_session(&mut state, &session_id);
    let connection_id = state
        .session_connection_id(&session_id)
        .expect("publisher should have a connection id");
    assert!(
        state
            .set_client_rtp_capabilities_for_test(
                &session_id,
                connection_id,
                &sample_client_rtp_capabilities(),
            )
            .session_present
    );
    assert!(
        state
            .set_transport_ready_for_test(
                &session_id,
                connection_id,
                SessionTransportReady::Publish,
            )
            .session_present
    );

    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_video_rtp_parameters(None, 43_000),
        &state.router_rtp_capabilities(),
    )
    .expect("publisher RTP parameters should derive consumable router parameters");
    let prepared_track = state
        .validate_publish_descriptor(
            &session_id,
            connection_id,
            StreamType::Camera,
            RouterMediaKind::Video,
        )
        .expect("publish descriptor should validate once the session is publish-ready")
        .into_prepared_track(consumable_rtp_parameters);
    let transport_media_id = TransportMediaId::new(100);

    assert!(
        state
            .commit_published_track(prepared_track, transport_media_id)
            .is_some()
    );
    assert!(
        state
            .unpublish_track(&session_id, connection_id, StreamType::Camera)
            .is_some()
    );
    assert_eq!(
        state.producer_stream_type_for_transport_media_id(transport_media_id),
        None
    );
    assert_eq!(
        state.inspect_producer_owner_session_id_for_transport_media_id(transport_media_id),
        None
    );
    assert_eq!(
        state.inspect_producer_owner_connection_id_for_transport_media_id(transport_media_id),
        None
    );
    assert!(state.sources.is_empty());
    assert!(state.source_ids_by_owner_stream.is_empty());
    assert!(state.producer_id_by_source_id.is_empty());
}
