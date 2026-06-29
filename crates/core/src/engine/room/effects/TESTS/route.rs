use std::{error::Error, io, sync::Arc};

use o_sfu_router::{
    MediaKind,
    rtp::{MediaStream, StreamBinding},
};

use super::*;
use crate::{
    MediaWorkerId,
    engine::{
        ConnectionId, RoomInstanceId, UserId,
        media_transport::{
            ConsumerActivity, ProducerActivity, RouteControlPlan, SourcePacketGate,
            SourcePacketOperatingPoint, TransportSessionKey,
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
async fn route_control_executor_applies_room_finish_work() -> Result<(), Box<dyn Error>> {
    let fixture = RouteFixture::new().await?;
    fixture.request_standalone_keyframe().await;
    fixture.apply_source_selection().await?;
    fixture.apply_setup_activity_correction().await?;
    fixture.pause_route_activity().await?;
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
        let mut route_control = RouteControlPlan::new();
        route_control.push_consumer(keyframe_control(&self.target));
        execute_route_control(
            route_control,
            Vec::new(),
            vec![ConsumerRouteFinish::Keyframe(self.target.clone())],
            &self.media_transport,
        )
        .await;
        assert_eq!(self.metrics.snapshot().rtc_keyframe_requests_forwarded(), 1);
    }

    async fn pause_route_activity(&self) -> Result<(), io::Error> {
        let activity = ReceiverRouteActivity::new(self.target.clone(), false);
        let mut route_control = RouteControlPlan::new();
        route_control.push_producer(self.route.source().clone(), ProducerActivity::Inactive);
        route_control.push_consumer(activity_control(&activity));

        let outcome = execute_route_control(
            route_control,
            vec![self.producer_finish(ProducerActivity::Inactive)],
            vec![ConsumerRouteFinish::Activity(
                activity,
                diagnostics(self.route.consumer_session_key(), "consumer.activity"),
            )],
            &self.media_transport,
        )
        .await;

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
        self.assert_route_state(false, 0, false).await?;
        Ok(())
    }

    async fn apply_source_selection(&self) -> Result<(), io::Error> {
        let keyframes_before = self.keyframe_requests();
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
        let selection = TransportPacketSelectionUpdate {
            update: update.clone(),
            target: self.target.clone(),
        };
        let mut route_control = RouteControlPlan::new();
        route_control.push_consumer(source_selection_control(&selection));

        let outcome = execute_route_control(
            route_control,
            Vec::new(),
            vec![ConsumerRouteFinish::SourceSelection(selection)],
            &self.media_transport,
        )
        .await;

        assert_eq!(outcome.packet_updates, vec![update]);
        assert_eq!(self.keyframe_requests(), keyframes_before + 1);
        let route = self.assert_route_state(true, 1, true).await?;
        assert_eq!(
            route.effective_packet_gate,
            DebugPacketGate::OperatingPoint {
                rid: None,
                max_temporal_layer_id: 0,
            }
        );
        Ok(())
    }

    async fn apply_setup_activity_correction(&self) -> Result<(), io::Error> {
        let keyframes_before = self.keyframe_requests();
        execute_setup_activity_correction(
            self.route.clone(),
            MediaKind::Video,
            false,
            &self.media_transport,
        )
        .await;

        assert_eq!(self.keyframe_requests(), keyframes_before);
        self.assert_route_state(true, 0, false).await?;

        execute_setup_activity_correction(
            self.route.clone(),
            MediaKind::Video,
            true,
            &self.media_transport,
        )
        .await;

        assert_eq!(self.keyframe_requests(), keyframes_before + 1);
        self.assert_route_state(true, 1, true).await?;
        Ok(())
    }

    async fn assert_route_state(
        &self,
        source_active: bool,
        active_destination_count: usize,
        destination_active: bool,
    ) -> Result<DebugRouteEntry, io::Error> {
        let route = self.route_entry().await?;
        assert_eq!(route.source_active, source_active);
        assert_eq!(route.active_destination_count, active_destination_count);
        assert!(route.destinations.iter().any(|destination| {
            destination.dest_transport_media_id == self.route.consumer_transport_media_id()
                && destination.active == destination_active
        }));
        Ok(route)
    }

    async fn route_entry(&self) -> Result<DebugRouteEntry, io::Error> {
        self.media_transport
            .test_api()
            .route_entry_by_media_id(self.route.source_transport_media_id())
            .await
            .ok_or_else(|| io::Error::other("video route should exist"))
    }

    fn keyframe_requests(&self) -> u64 {
        let snapshot = self.metrics.snapshot();
        snapshot.rtc_keyframe_requests_forwarded() + snapshot.rtc_keyframe_requests_absorbed()
    }

    fn producer_finish(&self, activity: ProducerActivity) -> ProducerRouteFinish {
        ProducerRouteFinish {
            source: self.route.source().clone(),
            activity,
            diagnostics: diagnostics(self.route.source_session_key(), "producer.activity"),
        }
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
