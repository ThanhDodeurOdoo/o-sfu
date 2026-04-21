#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::sync::Arc;

use o_sfu_router::{
    ConsumerCapability, MediaKind as RouterMediaKind, MediaStream, ProducerId, RouterId,
    StreamType as RouterStreamType, derive_consumable_rtp_parameters,
};
use tokio::sync::mpsc;

use super::super::{
    ids::ProducerRuntimeId,
    shared::{ChannelState, ConsumerKey, ConsumerState, ProducerKey, PublishedProducer},
};
use crate::config::MediaCodecFlags;
use crate::runtime::channel::{
    ChannelAdmissionPolicy, rtp_capabilities::router_rtp_capabilities,
    session_negotiation::SessionTransportReady, topology::RoutedProducerId,
};
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::{MediaSource, MediaTap, RecordingService};
use crate::runtime::test_rtp_samples::{
    sample_client_rtp_capabilities, sample_video_rtp_parameters,
};
use crate::runtime::transport_adapter::TransportMediaId;
use crate::runtime::{ChannelRuntimeId, ConnectionId};
use o_sfu_protocol::shared::{DownloadStates, SessionId, SessionPermissions, StreamType};

fn test_state() -> ChannelState {
    let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
    ChannelState::new(
        RouterId(1),
        ChannelAdmissionPolicy::new(4),
        router_rtp_capabilities(MediaCodecFlags::default()),
        Arc::new(RecordingService::new(
            ChannelRuntimeId::from_raw(0),
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
    let route_key = ConsumerKey {
        consumer_session_id: consumer_session_id.clone(),
        producer_session_id: producer_session_id.clone(),
        stream_type: StreamType::Camera,
    };
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
    state.producer_ids_by_owner_stream.insert(
        ProducerKey::new(&session_id, StreamType::Camera),
        producer_id,
    );
    state.producers.insert(
        producer_id,
        PublishedProducer {
            owner_session_id: session_id.clone(),
            owner_connection_id: connection_id,
            stream_type: StreamType::Camera,
            media_kind: RouterMediaKind::Video,
            consumable_rtp_parameters: MediaStream::new(vec![], vec![], vec![]),
            routed_producer_id,
            transport_media_id: Some(transport_media_id),
            source_packet_selection: None,
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
    state.producer_ids_by_owner_stream.insert(
        ProducerKey::new(&publisher_session_id, StreamType::Camera),
        producer_id,
    );
    state.producers.insert(
        producer_id,
        PublishedProducer {
            owner_session_id: publisher_session_id.clone(),
            owner_connection_id: publisher_connection_id,
            stream_type: StreamType::Camera,
            media_kind: RouterMediaKind::Video,
            consumable_rtp_parameters,
            routed_producer_id,
            transport_media_id: Some(TransportMediaId::new(10)),
            source_packet_selection: None,
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
    assert!(
        state.subscription_count() >= 1,
        "planning the bootstrap must reserve the pending consumer slot immediately"
    );
}
