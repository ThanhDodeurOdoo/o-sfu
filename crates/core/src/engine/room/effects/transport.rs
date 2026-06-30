use std::mem;

use o_sfu_router::MediaKind;
use tracing::warn;

use super::{RoomGaugeDelta, receiver_route::execute_receiver_route_setup};
use crate::{
    TransportEffectOutcome,
    engine::{
        diagnostics::DiagnosticsEventData,
        media_transport::{
            ConsumerActivity, ConsumerRouteControl, ConsumerRouteControlOutcome, MediaTransport,
            ProducerActivity, RouteControlPlan, TransportConsumerRoute, TransportRelayRouteAction,
            TransportRelayRouteEffect, TransportSourceKey,
        },
        room::{
            Room,
            cleanup::TransportCleanupOperation,
            media_graph::{
                ConsumerRouteTarget, ConsumerSetupOrigin, MediaTopologyEffects,
                PendingConsumerSetup, ReceiverRouteActivity, ReceiverRouteWork,
                ResolvedRelayRouteEffect,
            },
            source_policy::{
                ConsumerPacketSelectionUpdate, SourcePolicyCommit, SourcePolicyWakeups,
                TransportPacketSelectionUpdate,
            },
        },
    },
};

#[derive(Debug, Default)]
pub(super) struct RoomTransportPlan {
    topology: MediaTopologyEffects,
    receiver_route_relays: Vec<ResolvedRelayRouteEffect>,
    route_control: RoomRouteControlPlan,
    setups: Vec<(PendingConsumerSetup, ConsumerSetupOrigin)>,
}

impl RoomTransportPlan {
    pub(super) fn extend_topology(&mut self, effects: MediaTopologyEffects) {
        self.topology.extend(effects);
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
        let activity = ProducerActivity::from_active(active);
        self.route_control.push_producer(
            source.clone(),
            activity,
            ProducerRouteFinish {
                source,
                activity,
                diagnostics,
            },
        );
    }

    pub(super) fn push_receiver_work(
        &mut self,
        work: ReceiverRouteWork,
        origin: ConsumerSetupOrigin,
        mut diagnostics: impl FnMut(&ReceiverRouteActivity) -> DiagnosticsEventData,
    ) {
        self.receiver_route_relays.extend(work.relays);
        for activity in work.activities {
            let event = diagnostics(&activity);
            self.route_control.push_consumer(
                activity_control(&activity),
                ConsumerRouteFinish::Activity(activity, event),
            );
        }
        self.setups
            .extend(work.setups.into_iter().map(|setup| (setup, origin)));
    }

    pub(super) fn push_keyframes(&mut self, targets: Vec<ConsumerRouteTarget>) {
        for target in targets {
            self.route_control.push_consumer(
                keyframe_control(&target),
                ConsumerRouteFinish::Keyframe(target),
            );
        }
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
        let route_outcome = execute_route_control(self.route_control, media_transport).await;
        outcome.diagnostics.extend(route_outcome.diagnostics);
        if !cleanup.is_empty() {
            room.execute_transport_cleanup_operations(media_transport, &cleanup)
                .await;
        }
        for (setup, origin) in self.setups {
            execute_receiver_route_setup(setup, origin, room, media_transport, &mut outcome).await;
        }
        outcome
    }

    pub(super) async fn execute_source_policy_route_control(
        commit: SourcePolicyCommit,
        media_transport: &MediaTransport,
    ) -> SourcePolicyCommit {
        let SourcePolicyCommit(mut plan) = commit;
        let mut route_control = RouteControlPlan::new();
        route_control.set_receiver_bwe_targets(mem::take(&mut plan.receiver_bwe_targets));
        for update in mem::take(&mut plan.route_packet_updates) {
            route_control.push_consumer(
                source_selection_control(&update),
                ConsumerRouteFinish::SourceSelection(update),
            );
        }
        let route_outcome = execute_route_control(route_control, media_transport).await;
        plan.state_packet_updates
            .extend(route_outcome.packet_updates);
        SourcePolicyCommit(plan)
    }
}

#[derive(Debug, Default)]
pub(super) struct RoomTransportOutcome {
    pub(super) gauges: Vec<RoomGaugeDelta>,
    pub(super) diagnostics: Vec<DiagnosticsEventData>,
    pub(super) source_policy: SourcePolicyWakeups,
}

#[derive(Debug, Default)]
pub(super) struct RoomRouteOutcome {
    diagnostics: Vec<DiagnosticsEventData>,
    packet_updates: Vec<ConsumerPacketSelectionUpdate>,
}

#[derive(Debug)]
struct ProducerRouteFinish {
    source: TransportSourceKey,
    activity: ProducerActivity,
    diagnostics: DiagnosticsEventData,
}

type RoomRouteControlPlan = RouteControlPlan<ProducerRouteFinish, ConsumerRouteFinish>;

#[derive(Debug)]
enum ConsumerRouteFinish {
    Activity(ReceiverRouteActivity, DiagnosticsEventData),
    Keyframe(ConsumerRouteTarget),
    SetupActivity {
        route: TransportConsumerRoute,
        active: bool,
    },
    SourceSelection(TransportPacketSelectionUpdate),
}

impl ConsumerRouteFinish {
    fn finish(self, result: ConsumerRouteControlOutcome, outcome: &mut RoomRouteOutcome) {
        match self {
            Self::Activity(activity, diagnostics) => {
                finish_activity(&activity, diagnostics, result, outcome);
            }
            Self::Keyframe(target) => finish_keyframe(&target, result),
            Self::SetupActivity { route, active } => {
                finish_setup_activity(&route, active, result);
            }
            Self::SourceSelection(TransportPacketSelectionUpdate { update, target }) => {
                finish_source_selection(update, &target, result, outcome);
            }
        }
    }
}

pub(super) async fn execute_setup_activity_correction(
    route: TransportConsumerRoute,
    kind: MediaKind,
    active: bool,
    media_transport: &MediaTransport,
) {
    let mut route_control = RouteControlPlan::new();
    route_control.push_consumer(
        ConsumerRouteControl::new(route.clone())
            .activity(ConsumerActivity::from_active(active))
            .request_keyframe(active && kind == MediaKind::Video),
        ConsumerRouteFinish::SetupActivity { route, active },
    );
    execute_route_control(route_control, media_transport).await;
}

async fn execute_route_control(
    route_control: RoomRouteControlPlan,
    media_transport: &MediaTransport,
) -> RoomRouteOutcome {
    let transport_outcome = media_transport.apply_route_control(route_control).await;

    let mut outcome = RoomRouteOutcome::default();
    for (completion, result) in transport_outcome.producers {
        if result.is_err() {
            warn!(
                source = ?completion.source,
                active = completion.activity.is_active(),
                "media transport failed to update producer route activity"
            );
        }
        outcome.diagnostics.push(completion.diagnostics);
    }
    for (completion, result) in transport_outcome.consumers {
        completion.finish(result, &mut outcome);
    }
    outcome
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
    } else if result.keyframe_failed() {
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

#[cfg(test)]
#[path = "TESTS/route.rs"]
mod route_tests;
