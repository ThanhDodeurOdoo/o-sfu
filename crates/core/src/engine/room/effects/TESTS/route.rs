use std::{error::Error, io, sync::Arc};

use o_sfu_router::{
    MediaKind,
    rtp::{MediaStream, StreamBinding},
};

use super::*;
use crate::{
    Bitrate, MediaWorkerId,
    engine::{
        ConnectionId, RoomInstanceId, UserId,
        media_transport::{
            ConsumerActivity, SourcePacketGate, SourcePacketOperatingPoint, TransportSessionKey,
            test_support::{
                DebugPacketGate, DebugRouteEntry, test_media_transport_config,
                test_media_transport_deps, test_rtc_port_range,
            },
        },
        metrics::{RuntimeMetrics, test_support::RuntimeMetricsSnapshotTestExt},
        room::media_graph::ConsumerRouteTransportRef,
        source_model::{
            ConsumerSourceSelection, PolicyPauseReason, PublishedSourceId, UserStreamId,
        },
    },
};

#[tokio::test]
async fn room_route_effects_apply_transport_route_work() -> Result<(), Box<dyn Error>> {
    let fixture = RouteFixture::new().await?;
    fixture.request_standalone_keyframe().await;
    fixture.pause_route_activity().await?;
    fixture.reactivate_source().await;
    fixture.apply_source_selection().await?;
    Ok(())
}

struct RouteFixture {
    media_transport: MediaTransport,
    metrics: Arc<RuntimeMetrics>,
    route: TransportConsumerRoute,
    route_ref: ConsumerRouteTransportRef,
    target: ConsumerRouteTarget,
}

impl RouteFixture {
    async fn new() -> Result<Self, Box<dyn Error>> {
        let port_range =
            test_rtc_port_range(1).ok_or_else(|| io::Error::other("rtc test ports unavailable"))?;
        let metrics = Arc::new(RuntimeMetrics::default());
        let mut deps = test_media_transport_deps();
        deps.metrics = Arc::clone(&metrics);
        let media_transport = MediaTransport::builder()
            .transport_config(test_media_transport_config(port_range))
            .deps(deps)
            .build()?;
        let source_session = session_key(1, UserId::Integer(1));
        let consumer_session = session_key(2, UserId::Integer(2));
        media_transport
            .create_initial_session_offer(&source_session)
            .await?;
        media_transport
            .create_initial_session_offer(&consumer_session)
            .await?;

        let source_media = media_transport
            .publish_media(
                &source_session,
                MediaKind::Video,
                &sample_rtp_parameters("cam-up", 71_000),
            )
            .await?;
        let consumer_media = media_transport
            .consume_media(
                &consumer_session,
                MediaKind::Video,
                &source_session,
                source_media,
                &sample_rtp_parameters("cam-down", 72_000),
                ConsumerActivity::Active,
            )
            .await?;
        let source = TransportSourceKey::new(source_session.clone(), source_media);
        let route = TransportConsumerRoute::new(consumer_session.clone(), consumer_media, source);
        let transport_ref = ConsumerRouteTransportRef::from_parts(
            UserId::Integer(2),
            consumer_session.connection_id(),
            consumer_media,
            UserId::Integer(1),
            source_session.connection_id(),
            source_media,
        );
        let target = ConsumerRouteTarget::for_test(
            transport_ref.clone(),
            route.clone(),
            UserStreamId::from("camera"),
            MediaKind::Video,
        );
        Ok(Self {
            media_transport,
            metrics,
            route,
            route_ref: transport_ref,
            target,
        })
    }

    async fn request_standalone_keyframe(&self) {
        let mut effects = RoomRouteEffects::default();
        effects.push_keyframe(self.target.clone());
        effects.execute(&self.media_transport).await;
        assert_eq!(self.metrics.snapshot().rtc_keyframe_requests_forwarded(), 1);
    }

    async fn pause_route_activity(&self) -> Result<(), io::Error> {
        let mut effects = RoomRouteEffects::default();
        effects.push_producer(
            self.route.source().clone(),
            false,
            diagnostics(self.route.source_session_key(), "producer.activity"),
        );
        effects.push_activity(
            ReceiverRouteActivity::new(self.target.clone(), false),
            diagnostics(self.route.consumer_session_key(), "consumer.activity"),
        );

        let outcome = effects.execute(&self.media_transport).await;

        assert_eq!(outcome.diagnostics.len(), 2);
        assert!(
            outcome
                .diagnostics
                .iter()
                .any(|event| event.event == "producer.activity")
        );
        assert!(
            outcome
                .diagnostics
                .iter()
                .any(|event| event.event == "consumer.activity")
        );
        let route = self.route_entry().await?;
        assert!(!route.source_active);
        assert_eq!(route.active_destination_count, 0);
        assert!(route.destinations.iter().any(|destination| {
            destination.dest_transport_media_id == self.route.consumer_transport_media_id()
                && !destination.active
        }));
        Ok(())
    }

    async fn reactivate_source(&self) {
        let mut effects = RoomRouteEffects::default();
        effects.push_producer(
            self.route.source().clone(),
            true,
            diagnostics(self.route.source_session_key(), "producer.activity"),
        );
        effects.execute(&self.media_transport).await;
    }

    async fn apply_source_selection(&self) -> Result<(), io::Error> {
        let mut current_selection = ConsumerSourceSelection::open(true);
        current_selection.set_policy_pause_reason(Some(PolicyPauseReason::BudgetPressure));
        let mut update = ConsumerPacketSelectionUpdate::route_activity(
            self.route_ref.clone(),
            PublishedSourceId::from_raw(42),
            current_selection,
            None,
        )
        .ok_or_else(|| io::Error::other("route activity update should be created"))?;
        update.packet_gate = Some(SourcePacketGate::OperatingPoint(
            SourcePacketOperatingPoint::new(None, 0),
        ));
        update.request_keyframe = true;
        let mut effects = RoomRouteEffects::default();
        effects.push_source_selection(TransportPacketSelectionUpdate {
            update: update.clone(),
            target: self.target.clone(),
        });
        effects.set_receiver_bwe_targets(vec![ReceiverBweTargetUpdate::new(
            self.route.consumer_session_key().clone(),
            Bitrate::from_kbps(600),
        )]);

        let outcome = effects.execute(&self.media_transport).await;

        assert_eq!(outcome.packet_updates, vec![update]);
        let snapshot = self.metrics.snapshot();
        assert_eq!(
            snapshot.rtc_keyframe_requests_forwarded() + snapshot.rtc_keyframe_requests_absorbed(),
            2
        );
        assert_eq!(
            self.media_transport
                .test_api()
                .session_receiver_bwe_target(self.route.consumer_session_key())
                .await,
            Some(Bitrate::from_kbps(600))
        );
        let route = self.route_entry().await?;
        assert!(route.source_active);
        assert_eq!(route.active_destination_count, 1);
        assert_eq!(
            route.effective_packet_gate,
            DebugPacketGate::OperatingPoint {
                rid: None,
                max_temporal_layer_id: 0,
            }
        );
        assert!(route.destinations.iter().any(|destination| {
            destination.dest_transport_media_id == self.route.consumer_transport_media_id()
                && destination.active
        }));
        Ok(())
    }

    async fn route_entry(&self) -> Result<DebugRouteEntry, io::Error> {
        self.media_transport
            .test_api()
            .route_entry_by_media_id(self.route.source_transport_media_id())
            .await
            .ok_or_else(|| io::Error::other("video route should exist"))
    }
}

fn session_key(connection_id: u64, user_id: UserId) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(70),
        MediaWorkerId::from_raw(0),
        ConnectionId::from_raw(connection_id),
        user_id,
    )
}

fn sample_rtp_parameters(mid: &str, ssrc: u32) -> MediaStream {
    MediaStream::new(vec![], vec![], vec![StreamBinding::new().with_ssrc(ssrc)])
        .with_mid(String::from(mid))
}

fn diagnostics(session: &TransportSessionKey, event: &'static str) -> DiagnosticsEventData {
    DiagnosticsEventData::for_user("room-route-effects", session.user_id(), event)
}
