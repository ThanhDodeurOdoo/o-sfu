use std::slice;

use o_sfu_router::{MediaKind, MediaStream as RouterRtpParameters};
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::{
    batch::RoomGaugeDelta, policy::RoomPolicyPlan, transport::execute_relay_route_effects,
};
use crate::engine::{
    diagnostics::DiagnosticsEventData,
    media_transport::{
        ConsumerActivity, ConsumerRouteControl, ConsumerRouteControlOutcome, MediaTransport,
        RouteControlPlan, TransportConsumerRoute,
    },
    room::{
        Room, UserOutbound,
        cleanup::TransportCleanupOperation,
        media_graph::{
            ConsumerRouteTarget, ConsumerSetupOrigin, ConsumerSetupOutcome, ConsumerSetupTarget,
            PendingConsumerSetup, ReceiverRouteActivity, ReceiverRouteWork,
            ResolvedRelayRouteEffect,
        },
    },
    source_model::UserStreamId,
};

#[derive(Debug, Default)]
pub(super) struct ReceiverRoutePlan {
    relays: Vec<ResolvedRelayRouteEffect>,
    activities: Vec<ReceiverRouteActivityEffect>,
    setups: Vec<ReceiverRouteSetup>,
    keyframes: Vec<ConsumerRouteTarget>,
}

impl ReceiverRoutePlan {
    pub(super) fn push_work(
        &mut self,
        work: ReceiverRouteWork,
        origin: ConsumerSetupOrigin,
        mut diagnostics: impl FnMut(&ReceiverRouteActivity) -> DiagnosticsEventData,
    ) {
        let (activities, setups, relays) = work.into_parts();
        self.relays.extend(relays);
        self.activities
            .extend(activities.into_iter().map(|activity| {
                let event = diagnostics(&activity);
                ReceiverRouteActivityEffect {
                    activity,
                    diagnostics: event,
                }
            }));
        self.push_setups(setups, origin);
    }

    pub(super) fn push_keyframes(&mut self, targets: Vec<ConsumerRouteTarget>) {
        self.keyframes.extend(targets);
    }

    pub(super) fn push_setups(
        &mut self,
        setups: Vec<PendingConsumerSetup>,
        origin: ConsumerSetupOrigin,
    ) {
        self.setups.extend(
            setups
                .into_iter()
                .map(|setup| ReceiverRouteSetup { setup, origin }),
        );
    }

    pub(super) async fn execute(
        self,
        room: &Room,
        media_transport: Option<&MediaTransport>,
    ) -> ReceiverRouteOutcome {
        let Some(media_transport) = media_transport else {
            return ReceiverRouteOutcome::default();
        };
        let mut outcome = ReceiverRouteOutcome::default();
        execute_relay_route_effects(room, media_transport, &self.relays).await;
        outcome
            .diagnostics
            .extend(execute_route_controls(media_transport, self.activities, self.keyframes).await);
        for setup in self.setups {
            let setup = setup.execute(room, media_transport).await;
            outcome.gauges.push(setup.gauge);
            if let Some(diagnostics) = setup.diagnostics {
                outcome.diagnostics.push(diagnostics);
            }
            outcome.policy.extend(setup.policy);
        }
        outcome
    }
}

#[derive(Debug, Default)]
pub(super) struct ReceiverRouteOutcome {
    pub(super) gauges: Vec<RoomGaugeDelta>,
    pub(super) diagnostics: Vec<DiagnosticsEventData>,
    pub(super) policy: RoomPolicyPlan,
}

#[derive(Debug)]
struct ReceiverRouteActivityEffect {
    activity: ReceiverRouteActivity,
    diagnostics: DiagnosticsEventData,
}

impl ReceiverRouteActivityEffect {
    fn control(&self) -> ConsumerRouteControl {
        let target = self.activity.target();
        let active = self.activity.active();
        ConsumerRouteControl::new(target.transport_route().clone())
            .activity(ConsumerActivity::from_active(active))
            .keyframe(target.request_keyframe_after_activity(active))
    }

    fn finish(self, outcome: ConsumerRouteControlOutcome) -> DiagnosticsEventData {
        let target = self.activity.target();
        let active = self.activity.active();
        if outcome.activity_failed() {
            warn!(
                route = ?target.transport_route(),
                stream_id = %target.stream_id(),
                active,
                "media transport failed to update consumer route activity"
            );
        } else if outcome.keyframe_failed() {
            warn!(
                route = ?target.transport_route(),
                stream_id = %target.stream_id(),
                "media transport failed to request a consumer keyframe refresh"
            );
        }
        self.diagnostics
    }
}

async fn execute_route_controls(
    media_transport: &MediaTransport,
    activities: Vec<ReceiverRouteActivityEffect>,
    keyframes: Vec<ConsumerRouteTarget>,
) -> Vec<DiagnosticsEventData> {
    if activities.is_empty() && keyframes.is_empty() {
        return Vec::new();
    }
    let mut plan = RouteControlPlan::new();
    for activity in &activities {
        plan.push_consumer(activity.control());
    }
    for target in &keyframes {
        plan.push_consumer(
            ConsumerRouteControl::new(target.transport_route().clone()).keyframe(true),
        );
    }
    let mut outcomes = media_transport
        .apply_route_control(plan.ready())
        .await
        .consumers
        .into_iter();
    let expected_outcomes = activities.len() + keyframes.len();
    debug_assert_eq!(outcomes.len(), expected_outcomes);
    let mut diagnostics = Vec::with_capacity(activities.len());
    for (activity, outcome) in activities.into_iter().zip(&mut outcomes) {
        diagnostics.push(activity.finish(outcome));
    }
    for (target, outcome) in keyframes.into_iter().zip(outcomes) {
        if outcome.keyframe_failed() {
            warn!(
                consumer_user_id = ?target.transport_route().consumer_session_key().user_id(),
                consumer_transport_media_id = ?target.transport_route().consumer_transport_media_id(),
                producer_user_id = ?target.producer_user_id(),
                source_transport_media_id = ?target.source_media_id(),
                "media transport failed to request a refreshed consumer keyframe"
            );
        }
    }
    diagnostics
}

#[derive(Debug)]
struct ReceiverRouteSetup {
    setup: PendingConsumerSetup,
    origin: ConsumerSetupOrigin,
}

impl ReceiverRouteSetup {
    async fn execute(
        self,
        room: &Room,
        media_transport: &MediaTransport,
    ) -> ReceiverRouteSetupOutcome {
        let relays = &self.setup.relays;
        if !execute_relay_route_effects(room, media_transport, relays).await {
            return release_failed_setup(room, self.setup, media_transport).await;
        }
        let target = self.setup.target.clone();
        let activity =
            ConsumerActivity::from_active(self.setup.reservation.selection().delivery_active());
        let Some((route, mid)) = declare_consumer(
            &target,
            &self.setup.track.rtp,
            activity,
            self.origin,
            media_transport,
        )
        .await
        else {
            return release_failed_setup(room, self.setup, media_transport).await;
        };
        let (before, after, outcome) = {
            let mut state = room.state.write().await;
            let commit = state.commit_pending_consumer_setup(
                self.setup,
                route.consumer_transport_media_id(),
                mid,
            );
            drop(state);
            commit
        };
        finish_setup(
            room,
            media_transport,
            target,
            self.origin,
            route,
            RoomGaugeDelta::media(before, after),
            outcome,
        )
        .await
    }
}

#[derive(Debug)]
struct ReceiverRouteSetupOutcome {
    gauge: RoomGaugeDelta,
    diagnostics: Option<DiagnosticsEventData>,
    policy: RoomPolicyPlan,
}

async fn declare_consumer(
    target: &ConsumerSetupTarget,
    rtp: &RouterRtpParameters,
    activity: ConsumerActivity,
    origin: ConsumerSetupOrigin,
    media_transport: &MediaTransport,
) -> Option<(TransportConsumerRoute, Option<String>)> {
    match media_transport
        .consume_media(
            &target.user_session,
            target.kind,
            &target.producer_session,
            target.media,
            rtp,
            activity,
        )
        .await
    {
        Ok(consumer_media) => {
            let mid = media_transport
                .transport_media_mid(&target.user_session, consumer_media)
                .await;
            Some((target.transport_consumer_route(consumer_media), mid))
        }
        Err(error) => {
            warn!(
                consumer_user_id = ?target.user,
                consumer_connection_id = ?target.connection,
                producer_user_id = ?target.producer_user,
                producer_connection_id = ?target.producer_connection,
                source_transport_media_id = ?target.media,
                error = ?error,
                consumer_mid = rtp.mid(),
                ?origin,
                "media transport rejected consume media declaration"
            );
            None
        }
    }
}

async fn finish_setup(
    room: &Room,
    media_transport: &MediaTransport,
    target: ConsumerSetupTarget,
    origin: ConsumerSetupOrigin,
    route: TransportConsumerRoute,
    gauge: RoomGaugeDelta,
    outcome: ConsumerSetupOutcome,
) -> ReceiverRouteSetupOutcome {
    match outcome {
        ConsumerSetupOutcome::Committed {
            sender,
            track,
            transport_activity_update,
        } => {
            if let Some(active) = transport_activity_update {
                sync_activity(media_transport, &route, &target.stream, target.kind, active).await;
            }
            let diagnostics = setup_diagnostics(room.uuid(), &target, origin, &route);
            let _ = sender.send(UserOutbound::SetupRemoteTrack(Box::new(track)));
            ReceiverRouteSetupOutcome {
                gauge,
                diagnostics: Some(diagnostics),
                policy: RoomPolicyPlan::default(),
            }
        }
        ConsumerSetupOutcome::Released(relays) => {
            execute_relay_route_effects(room, media_transport, &relays).await;
            let cleanup = TransportCleanupOperation::RemoveMedia {
                session_key: route.consumer_session_key().clone(),
                transport_media_id: route.consumer_transport_media_id(),
            };
            room.execute_transport_cleanup_operations(media_transport, slice::from_ref(&cleanup))
                .await;
            ReceiverRouteSetupOutcome {
                gauge,
                diagnostics: None,
                policy: fanout_pressure_plan(),
            }
        }
    }
}

fn setup_diagnostics(
    room_id: &str,
    target: &ConsumerSetupTarget,
    origin: ConsumerSetupOrigin,
    route: &TransportConsumerRoute,
) -> DiagnosticsEventData {
    DiagnosticsEventData::for_user(room_id, &target.user, telemetry_event::SUBSCRIBE_SUCCEEDED)
        .with_connection_id(target.connection.as_u64())
        .with_media_worker_id(route.consumer_session_key().media_worker_id().as_usize())
        .with_transport_media_id(route.consumer_transport_media_id().as_u64())
        .insert_field(
            "producer_user_id",
            serde_json::to_value(&target.producer_user).unwrap_or(serde_json::Value::Null),
        )
        .insert_field("source_transport_media_id", target.media.as_u64())
        .insert_field("stream_id", target.stream.to_string())
        .insert_field("origin", origin.as_diagnostic_str())
}

async fn sync_activity(
    media_transport: &MediaTransport,
    route: &TransportConsumerRoute,
    stream: &UserStreamId,
    kind: MediaKind,
    active: bool,
) {
    let mut plan = RouteControlPlan::new();
    plan.push_consumer(
        ConsumerRouteControl::new(route.clone())
            .activity(ConsumerActivity::from_active(active))
            .keyframe(active && kind == MediaKind::Video),
    );
    let outcome = media_transport.apply_route_control(plan.ready()).await;
    let Some(outcome) = outcome.consumers.into_iter().next() else {
        return;
    };
    if outcome.activity_failed() {
        warn!(
            ?route,
            stream_id = %stream,
            active,
            "media transport failed to correct in-flight consumer setup activity"
        );
        return;
    }
    if outcome.keyframe_failed() {
        warn!(
            ?route,
            stream_id = %stream,
            "media transport failed to request keyframe after consumer setup activity correction"
        );
    }
}

async fn release_failed_setup(
    room: &Room,
    setup: PendingConsumerSetup,
    media_transport: &MediaTransport,
) -> ReceiverRouteSetupOutcome {
    let (before, after, relays) = {
        let mut state = room.state.write().await;
        let (before, after, relays) = state.release_pending_consumer_setup(setup);
        drop(state);
        (before, after, relays)
    };
    execute_relay_route_effects(room, media_transport, &relays).await;
    ReceiverRouteSetupOutcome {
        gauge: RoomGaugeDelta::media(before, after),
        diagnostics: None,
        policy: fanout_pressure_plan(),
    }
}

fn fanout_pressure_plan() -> RoomPolicyPlan {
    let mut policy = RoomPolicyPlan::default();
    policy.fanout_pressure_changed();
    policy
}
