#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::sync::Arc;

use o_sfu_router::{
    ConsumerCapability, MediaKind as RouterMediaKind, ProducerId, RouterId, RtpParameters,
    StreamType as RouterStreamType,
};
use tokio::sync::mpsc;

use super::super::{
    ids::ProducerRuntimeId,
    shared::{ChannelState, ConsumerKey, ConsumerState, ProducerKey, PublishedProducer},
};
use crate::config::MediaCodecFlags;
use crate::runtime::channel::{
    ChannelAdmissionPolicy, rtp_capabilities::router_rtp_capabilities, topology::RoutedProducerId,
};
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::{MediaSource, MediaTap, RecordingService};
use crate::runtime::transport_adapter::TransportMediaId;
use crate::signaling::shared::{DownloadStates, SessionId, SessionPermissions, StreamType};

fn test_state() -> ChannelState {
    let media_source: Arc<dyn MediaSource> = Arc::new(MediaTap::default());
    ChannelState::new(
        RouterId(1),
        ChannelAdmissionPolicy::new(4),
        router_rtp_capabilities(MediaCodecFlags::default()),
        Arc::new(RecordingService::new(
            0,
            media_source,
            Arc::new(RuntimeMetrics::default()),
        )),
    )
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
    let connection_id = state.session_connection_id(&session_id).unwrap_or(u64::MAX);

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
            consumable_rtp_parameters: RtpParameters::new(vec![], vec![], vec![]),
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
#[allow(
    clippy::panic,
    reason = "test fixture wiring intentionally aborts on impossible setup failures"
)]
fn stale_download_route_updates_are_ignored_before_transport_commit() {
    let mut state = test_state();
    let producer_session_id = SessionId::Integer(1);
    let consumer_session_id = SessionId::Integer(2);
    let (producer_sender, _producer_rx) = mpsc::unbounded_channel();
    let (consumer_sender, _consumer_rx) = mpsc::unbounded_channel();

    assert!(
        state
            .apply_join(
                &producer_session_id,
                None,
                SessionPermissions::default(),
                producer_sender,
                false,
            )
            .is_ok()
    );
    assert!(
        state
            .apply_join(
                &consumer_session_id,
                None,
                SessionPermissions::default(),
                consumer_sender,
                false,
            )
            .is_ok()
    );

    let Some(producer_connection_id) = state.session_connection_id(&producer_session_id) else {
        panic!("producer session should have a connection id");
    };
    let Some(consumer_connection_id) = state.session_connection_id(&consumer_session_id) else {
        panic!("consumer session should have a connection id");
    };
    let routed_producer_id = match state.topology.add_producer(
        &producer_session_id,
        RouterMediaKind::Video,
        RouterStreamType::Camera,
    ) {
        Ok(routed_producer_id) => routed_producer_id,
        Err(error) => panic!("failed to create test producer route: {error:?}"),
    };
    let routed_consumer_id = match state.topology.add_consumer(
        &consumer_session_id,
        routed_producer_id,
        RouterMediaKind::Video,
        RouterStreamType::Camera,
        ConsumerCapability::Compatible,
    ) {
        Ok(routed_consumer_id) => routed_consumer_id,
        Err(error) => panic!("failed to create test consumer route: {error:?}"),
    };

    let consumer_media = TransportMediaId::new(2);
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
        consumer_media,
    };
    state
        .consumer_index
        .insert(route_key.clone(), consumer_state);

    let route_updates = state.download_route_updates(
        &consumer_session_id,
        &producer_session_id,
        &DownloadStates {
            camera: Some(false),
            audio: None,
            screen: None,
        },
    );
    assert_eq!(route_updates.len(), 1);

    state.consumer_index.insert(
        route_key,
        ConsumerState {
            consumer_connection_id: consumer_connection_id.saturating_add(1),
            ..consumer_state
        },
    );

    let committed_updates = state.commit_download_route_updates(
        &consumer_session_id,
        &producer_session_id,
        route_updates,
    );

    assert!(committed_updates.is_empty());
}
