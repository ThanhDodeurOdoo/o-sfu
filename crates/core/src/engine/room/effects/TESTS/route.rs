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
            ConsumerActivity, RelayRouteActivity, TransportMediaId, TransportRelayRouteAction,
            TransportSessionKey, TransportTeardown,
            test_support::{
                DebugRouteEntry, test_media_transport_config, test_media_transport_deps,
                test_rtc_port_range,
            },
        },
        metrics::{RuntimeMetrics, test_support::RuntimeMetricsSnapshotTestExt},
        source_model::UserStreamId,
    },
};

#[test]
fn room_transport_plan_moves_relay_release_to_teardown() {
    let source_session = session_key(3, UserId::Integer(3));
    let source = TransportSourceKey::new(source_session, TransportMediaId::new(31));
    let target_media_worker_id = MediaWorkerId::from_raw(2);
    let activity = TransportRelayRouteAction::SetActivity(RelayRouteActivity::Inactive);
    let plan = RoomTransportPlan::from_relays_and_teardown(
        vec![
            TransportRelayRouteEffect {
                source: source.clone(),
                target_media_worker_id,
                action: TransportRelayRouteAction::Release,
            },
            TransportRelayRouteEffect {
                source: source.clone(),
                target_media_worker_id,
                action: activity,
            },
        ],
        [],
    );
    let (relays, teardown) = plan.relays_and_teardown();

    assert_eq!(
        relays,
        [TransportRelayRouteEffect {
            source: source.clone(),
            target_media_worker_id,
            action: activity,
        }]
    );
    assert!(matches!(
        teardown,
        [TransportTeardown::ReleaseRelayRoute {
            source: teardown_source,
            target_media_worker_id: teardown_target,
        }] if teardown_source == &source && *teardown_target == target_media_worker_id
    ));
}

#[tokio::test]
async fn room_route_effects_execute_finish_work() -> Result<(), Box<dyn Error>> {
    let fixture = RouteFixture::new().await?;
    fixture.request_standalone_keyframe().await;
    fixture.pause_route_activity().await?;
    fixture.apply_setup_activity_correction(false).await?;
    fixture.apply_setup_activity_correction(true).await?;
    Ok(())
}

struct RouteFixture {
    media_transport: MediaTransport,
    metrics: Arc<RuntimeMetrics>,
    route: TransportConsumerRoute,
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
        let target = ConsumerRouteTarget::for_test(
            route.clone(),
            UserStreamId::from("camera"),
            MediaKind::Video,
        );
        Ok(Self {
            media_transport,
            metrics,
            route,
            target,
        })
    }

    async fn request_standalone_keyframe(&self) {
        let mut routes = RoomRouteEffects::default();
        routes.keyframe(self.target.clone());
        routes.execute(&self.media_transport).await;
        assert_eq!(self.metrics.snapshot().rtc_keyframe_requests_forwarded(), 1);
    }

    async fn pause_route_activity(&self) -> Result<(), io::Error> {
        let mut routes = RoomRouteEffects::default();
        routes.producer_activity(
            self.route.source().clone(),
            false,
            diagnostics(self.route.source_session_key(), "producer.activity"),
        );
        routes.receiver_activity(
            ReceiverRouteActivity::new(self.target.clone(), false),
            diagnostics(self.route.consumer_session_key(), "consumer.activity"),
        );

        let outcome = routes.execute(&self.media_transport).await;

        assert_eq!(outcome.diagnostics.len(), 2);
        let has_event = |name| outcome.diagnostics.iter().any(|event| event.event == name);
        assert!(has_event("producer.activity"));
        assert!(has_event("consumer.activity"));
        self.assert_route_state(0, false).await?;
        Ok(())
    }

    async fn apply_setup_activity_correction(&self, active: bool) -> Result<(), io::Error> {
        let keyframes = self.keyframe_request_count();
        let mut routes = RoomRouteEffects::default();
        routes.setup_activity(self.route.clone(), MediaKind::Video, active);
        routes.execute(&self.media_transport).await;

        let expected_keyframes = if active { keyframes + 1 } else { keyframes };
        assert_eq!(self.keyframe_request_count(), expected_keyframes);
        self.assert_route_state(usize::from(active), active).await
    }

    async fn assert_route_state(
        &self,
        active_destination_count: usize,
        destination_active: bool,
    ) -> Result<(), io::Error> {
        let route = self.route_entry().await?;
        assert!(!route.source_active);
        assert_eq!(route.active_destination_count, active_destination_count);
        assert!(route.destinations.iter().any(|destination| {
            destination.dest_transport_media_id == self.route.consumer_transport_media_id()
                && destination.active == destination_active
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

    fn keyframe_request_count(&self) -> u64 {
        let snapshot = self.metrics.snapshot();
        snapshot.rtc_keyframe_requests_forwarded() + snapshot.rtc_keyframe_requests_absorbed()
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
