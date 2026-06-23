use std::slice;

use o_sfu_router::{MediaKind, rtp::MediaStream as RouterRtpParameters};
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::warn;

use super::batch::RoomGaugeDelta;
use crate::{
    TransportEffectOutcome,
    engine::{
        diagnostics::DiagnosticsEventData,
        media_transport::{
            ConsumerActivity, ConsumerRouteControl, ConsumerRouteControlOutcome, MediaTransport,
            ProducerActivity, ReceiverBweTargetUpdate, RouteControlPlan, TransportConsumerRoute,
            TransportRelayRouteAction, TransportRelayRouteEffect, TransportSourceKey,
        },
        room::{
            Room, UserOutbound,
            cleanup::TransportCleanupOperation,
            media_graph::{
                ConsumerRouteTarget, ConsumerSetupOrigin, ConsumerSetupOutcome,
                ConsumerSetupTarget, MediaTopologyEffects, PendingConsumerSetup,
                ReceiverRouteActivity, ReceiverRouteWork, ResolvedRelayRouteEffect,
            },
            source_policy::{
                ConsumerPacketSelectionUpdate, SourcePolicyWakeups, TransportPacketSelectionUpdate,
            },
        },
    },
};

#[derive(Debug, Default)]
pub(super) struct RoomTransportPlan {
    topology: MediaTopologyEffects,
    receiver_route_relays: Vec<ResolvedRelayRouteEffect>,
    routes: RoomRouteBatch,
    setups: Vec<ReceiverRouteSetup>,
}

impl RoomTransportPlan {
    pub(super) fn extend_topology(&mut self, effects: MediaTopologyEffects) {
        self.topology.extend(effects);
    }

    pub(super) fn extend_cleanup(&mut self, cleanup: Vec<TransportCleanupOperation>) {
        self.topology.extend_cleanup(cleanup);
    }

    pub(super) fn push_cleanup(&mut self, operation: TransportCleanupOperation) {
        self.topology.push_cleanup(operation);
    }

    pub(super) fn push_producer(
        &mut self,
        source: TransportSourceKey,
        active: bool,
        diagnostics: DiagnosticsEventData,
    ) {
        self.routes.push_producer(source, active, diagnostics);
    }

    pub(super) fn push_receiver_work(
        &mut self,
        work: ReceiverRouteWork,
        origin: ConsumerSetupOrigin,
        mut diagnostics: impl FnMut(&ReceiverRouteActivity) -> DiagnosticsEventData,
    ) {
        let (activities, setups, relays) = work.into_parts();
        self.receiver_route_relays.extend(relays);
        for activity in activities {
            let event = diagnostics(&activity);
            self.routes.push_activity(activity, event);
        }
        self.push_pending_consumer_setups(setups, origin);
    }

    pub(super) fn push_keyframes(&mut self, targets: Vec<ConsumerRouteTarget>) {
        for target in targets {
            self.routes.push_keyframe(target);
        }
    }

    pub(super) fn push_pending_consumer_setups(
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
    ) -> RoomTransportOutcome {
        let Some(media_transport) = media_transport else {
            return RoomTransportOutcome::default();
        };
        let mut outcome = RoomTransportOutcome::default();
        let (topology_relays, cleanup) = self.topology.into_parts();
        execute_relay_route_effects(room, media_transport, &topology_relays).await;
        execute_relay_route_effects(room, media_transport, &self.receiver_route_relays).await;
        let route_outcome = self.routes.execute(media_transport).await;
        outcome.diagnostics.extend(route_outcome.diagnostics);
        if !cleanup.is_empty() {
            room.execute_transport_cleanup_operations(media_transport, &cleanup)
                .await;
        }
        for setup in self.setups {
            let setup_outcome = setup.execute(room, media_transport).await;
            outcome.gauges.push(setup_outcome.gauge);
            if let Some(diagnostics) = setup_outcome.diagnostics {
                outcome.diagnostics.push(diagnostics);
            }
            outcome.source_policy.extend(setup_outcome.source_policy);
        }
        outcome
    }
}

#[derive(Debug, Default)]
pub(super) struct RoomTransportOutcome {
    pub(super) gauges: Vec<RoomGaugeDelta>,
    pub(super) diagnostics: Vec<DiagnosticsEventData>,
    pub(super) source_policy: SourcePolicyWakeups,
}

#[derive(Debug, Default)]
#[must_use = "room route batches must be executed after being populated"]
pub struct RoomRouteBatch {
    producers: Vec<ProducerEffect>,
    consumers: Vec<ConsumerEffect>,
    receiver_bwe_targets: Vec<ReceiverBweTargetUpdate>,
}

impl RoomRouteBatch {
    pub fn push_producer(
        &mut self,
        source: TransportSourceKey,
        active: bool,
        diagnostics: DiagnosticsEventData,
    ) {
        self.producers.push(ProducerEffect {
            source,
            active,
            diagnostics,
        });
    }

    pub fn push_activity(
        &mut self,
        activity: ReceiverRouteActivity,
        diagnostics: DiagnosticsEventData,
    ) {
        self.consumers
            .push(ConsumerEffect::Activity(activity, diagnostics));
    }

    pub fn push_keyframe(&mut self, target: ConsumerRouteTarget) {
        self.consumers.push(ConsumerEffect::Keyframe(target));
    }

    pub fn push_setup_activity(
        &mut self,
        route: TransportConsumerRoute,
        kind: MediaKind,
        active: bool,
    ) {
        self.consumers.push(ConsumerEffect::SetupActivity {
            route,
            active,
            keyframe: active && kind == MediaKind::Video,
        });
    }

    pub fn push_source_selection(&mut self, update: TransportPacketSelectionUpdate) {
        self.consumers.push(ConsumerEffect::SourceSelection(update));
    }

    pub fn set_receiver_bwe_targets(&mut self, updates: Vec<ReceiverBweTargetUpdate>) {
        self.receiver_bwe_targets = updates;
    }

    pub async fn execute(self, media_transport: &MediaTransport) -> RoomRouteOutcome {
        let Self {
            producers,
            consumers,
            receiver_bwe_targets,
        } = self;
        let mut plan = RouteControlPlan::new();
        for producer in &producers {
            plan.push_producer(
                producer.source.clone(),
                ProducerActivity::from_active(producer.active),
            );
        }
        for consumer in &consumers {
            let control = match consumer {
                ConsumerEffect::Activity(activity, _) => activity_control(activity),
                ConsumerEffect::Keyframe(target) => keyframe_control(target),
                ConsumerEffect::SetupActivity {
                    route,
                    active,
                    keyframe,
                } => ConsumerRouteControl::new(route.clone())
                    .activity(ConsumerActivity::from_active(*active))
                    .request_keyframe(*keyframe),
                ConsumerEffect::SourceSelection(selection) => source_selection_control(selection),
            };
            plan.push_consumer(control);
        }
        plan.set_receiver_bwe_targets(receiver_bwe_targets);
        let route_outcome = media_transport.apply_route_control(plan).await;
        let producer_results = route_outcome.producers;
        let consumer_results = route_outcome.consumers;
        debug_assert_eq!(producers.len(), producer_results.len());
        debug_assert_eq!(consumers.len(), consumer_results.len());

        let mut outcome = RoomRouteOutcome::default();
        for (producer, result) in producers.into_iter().zip(producer_results) {
            producer.finish(result.is_err(), &mut outcome);
        }
        for (consumer, result) in consumers.into_iter().zip(consumer_results) {
            consumer.finish(result, &mut outcome);
        }
        outcome
    }
}

#[derive(Debug, Default)]
pub struct RoomRouteOutcome {
    pub diagnostics: Vec<DiagnosticsEventData>,
    pub packet_updates: Vec<ConsumerPacketSelectionUpdate>,
}

#[derive(Debug)]
struct ProducerEffect {
    source: TransportSourceKey,
    active: bool,
    diagnostics: DiagnosticsEventData,
}

impl ProducerEffect {
    fn finish(self, failed: bool, outcome: &mut RoomRouteOutcome) {
        if failed {
            warn!(
                source = ?self.source,
                active = self.active,
                "media transport failed to update producer route activity"
            );
        }
        outcome.diagnostics.push(self.diagnostics);
    }
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
        if !execute_relay_route_effects(room, media_transport, &self.setup.relays).await {
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
    source_policy: SourcePolicyWakeups,
}

#[derive(Debug)]
enum ConsumerEffect {
    Activity(ReceiverRouteActivity, DiagnosticsEventData),
    Keyframe(ConsumerRouteTarget),
    SetupActivity {
        route: TransportConsumerRoute,
        active: bool,
        keyframe: bool,
    },
    SourceSelection(TransportPacketSelectionUpdate),
}

impl ConsumerEffect {
    fn finish(self, result: ConsumerRouteControlOutcome, outcome: &mut RoomRouteOutcome) {
        match self {
            Self::Activity(activity, diagnostics) => {
                finish_activity(&activity, diagnostics, result, outcome);
            }
            Self::Keyframe(target) => finish_keyframe(&target, result),
            Self::SetupActivity { route, active, .. } => {
                finish_setup_activity(&route, active, result);
            }
            Self::SourceSelection(selection) => {
                let TransportPacketSelectionUpdate { update, target } = selection;
                finish_source_selection(update, &target, result, outcome);
            }
        }
    }
}

fn activity_control(activity: &ReceiverRouteActivity) -> ConsumerRouteControl {
    let target = activity.target();
    let active = activity.active();
    ConsumerRouteControl::new(target.transport_route().clone())
        .activity(ConsumerActivity::from_active(active))
        .request_keyframe(target.request_keyframe_after_activity(active))
}

fn keyframe_control(target: &ConsumerRouteTarget) -> ConsumerRouteControl {
    ConsumerRouteControl::new(target.transport_route().clone()).request_keyframe(true)
}

fn source_selection_control(selection: &TransportPacketSelectionUpdate) -> ConsumerRouteControl {
    let update = &selection.update;
    let mut control = ConsumerRouteControl::new(selection.target.transport_route().clone())
        .request_keyframe(update.request_keyframe);
    if update.route_activity_changed {
        let active = update.policy_pause_reason.is_none();
        control = control.activity(ConsumerActivity::from_active(active));
    }
    if let Some(packet_gate) = &update.packet_gate {
        control = control.packet_gate(packet_gate.clone());
    }
    control
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
            let diagnostics = setup_diagnostics(room.uuid(), &target, origin, &route);
            if let Some(active) = transport_activity_update {
                let mut routes = RoomRouteBatch::default();
                routes.push_setup_activity(route, target.kind, active);
                routes.execute(media_transport).await;
            }
            let _ = sender.send(UserOutbound::SetupRemoteTrack(Box::new(track)));
            ReceiverRouteSetupOutcome {
                gauge,
                diagnostics: Some(diagnostics),
                source_policy: SourcePolicyWakeups::default(),
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
                source_policy: fanout_pressure_wakeups(),
            }
        }
    }
}

fn finish_activity(
    activity: &ReceiverRouteActivity,
    diagnostics: DiagnosticsEventData,
    result: ConsumerRouteControlOutcome,
    outcome: &mut RoomRouteOutcome,
) {
    let target = activity.target();
    if result.activity_failed() {
        warn!(
            route = ?target.transport_route(),
            stream_id = %target.stream_id(),
            active = activity.active(),
            "media transport failed to update consumer route activity"
        );
    } else if result.keyframe_failed() {
        warn!(
            route = ?target.transport_route(),
            stream_id = %target.stream_id(),
            "media transport failed to request a consumer keyframe refresh"
        );
    }
    outcome.diagnostics.push(diagnostics);
}

fn finish_keyframe(target: &ConsumerRouteTarget, result: ConsumerRouteControlOutcome) {
    if result.keyframe_failed() {
        warn!(
            route = ?target.transport_route(),
            "media transport failed to request a refreshed consumer keyframe"
        );
    }
}

fn finish_setup_activity(
    route: &TransportConsumerRoute,
    active: bool,
    result: ConsumerRouteControlOutcome,
) {
    if result.activity_failed() {
        warn!(
            ?route,
            active, "media transport failed to correct in-flight consumer setup activity"
        );
        return;
    }
    if result.keyframe_failed() {
        warn!(
            ?route,
            "media transport failed to request keyframe after consumer setup activity correction"
        );
    }
}

fn finish_source_selection(
    update: ConsumerPacketSelectionUpdate,
    target: &ConsumerRouteTarget,
    result: ConsumerRouteControlOutcome,
    outcome: &mut RoomRouteOutcome,
) {
    if result.packet_gate_failed() || result.activity_failed() {
        warn!(
            route = ?target.transport_route(),
            route_active = update.policy_pause_reason.is_none(),
            "media transport rejected the receiver-driven packet selection update"
        );
        return;
    }
    if result.keyframe_failed() {
        warn!(
            route = ?target.transport_route(),
            "media transport failed to request an adaptation keyframe refresh"
        );
    }
    outcome.packet_updates.push(update);
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
        source_policy: fanout_pressure_wakeups(),
    }
}

pub(super) async fn execute_relay_route_effects(
    room: &Room,
    media_transport: &MediaTransport,
    effects: &[ResolvedRelayRouteEffect],
) -> bool {
    let mut applied = true;
    for effect in effects {
        if effect.action == TransportRelayRouteAction::Release {
            let operation = [TransportCleanupOperation::ReleaseRelayRoute {
                source_session_key: effect.source_session_key.clone(),
                route: effect.route.clone(),
            }];
            if room
                .execute_transport_cleanup_operations(media_transport, &operation)
                .await
                == TransportEffectOutcome::Failed
            {
                applied = false;
            }
            continue;
        }
        let transport_effect = TransportRelayRouteEffect {
            source: TransportSourceKey::new(
                effect.source_session_key.clone(),
                effect.route.source_media,
            ),
            target_media_worker_id: effect.route.target_worker,
            action: effect.action,
        };
        if let Err(error) = media_transport
            .apply_relay_route_effect(&transport_effect)
            .await
        {
            applied = false;
            warn!(
                ?effect,
                ?error,
                "media transport failed to apply relay route effect"
            );
        }
    }
    applied
}

fn fanout_pressure_wakeups() -> SourcePolicyWakeups {
    let mut policy = SourcePolicyWakeups::default();
    policy.fanout_pressure_changed();
    policy
}

#[cfg(test)]
#[path = "TESTS/route.rs"]
mod route_tests;
