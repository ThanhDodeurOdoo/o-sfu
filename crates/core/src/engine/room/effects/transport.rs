use o_sfu_router::MediaKind;
use tracing::warn;

use super::{RoomGaugeDelta, receiver_route::ReceiverSetupTurn};
use crate::engine::{
    ConnectionId, UserId,
    diagnostics::DiagnosticsEventData,
    media_transport::{
        ConsumerActivity, ConsumerRouteControl, ConsumerRouteControlOutcome, MediaTransport,
        ProducerActivity, RouteControlPlan, TransportConsumerRoute, TransportRelayRouteAction,
        TransportRelayRouteEffect, TransportSourceKey,
    },
    room::{
        Room,
        cleanup::{TransportCleanupOperation, TransportEffectOutcome},
        media_graph::{
            ConsumerRouteTarget, ConsumerSetupOrigin, MediaTopologyEffects, ReceiverRouteActivity,
            ReceiverRouteWork, ResolvedRelayRouteEffect,
        },
        source_policy::{ConsumerPacketSelectionUpdate, SourcePolicyWakeups},
    },
};

#[derive(Debug, Default)]
pub(super) struct RoomTransportPlan {
    topology: MediaTopologyEffects,
    receiver_route_relays: Vec<ResolvedRelayRouteEffect>,
    route_control: RoomRouteEffects,
    setup_turns: Vec<ReceiverSetupTurn>,
    readiness_keyframe_refresh: Option<(UserId, ConnectionId)>,
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
        for setup in work.setups {
            self.setup_turns.push(ReceiverSetupTurn::new(setup, origin));
        }
    }

    pub(super) fn defer_readiness_keyframe_refresh(
        &mut self,
        user_id: UserId,
        connection_id: ConnectionId,
    ) {
        self.readiness_keyframe_refresh = Some((user_id, connection_id));
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
        for turn in self.setup_turns {
            turn.execute(room, media_transport, &mut outcome).await;
        }
        if let Some((user_id, connection_id)) = self.readiness_keyframe_refresh {
            let targets = {
                let state = room.state.read().await;
                state.active_video_keyframe_targets(&user_id, connection_id)
            };
            if let Some(targets) = targets.filter(|targets| !targets.is_empty()) {
                let mut route_control = RoomRouteEffects::new();
                for target in targets {
                    route_control.push_consumer(
                        keyframe_control(&target),
                        ConsumerRouteFinish::Keyframe(target),
                    );
                }
                let route_outcome = execute_route_control(route_control, media_transport).await;
                outcome.diagnostics.extend(route_outcome.diagnostics);
            }
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
pub(in crate::engine::room) struct RoomRouteOutcome {
    pub(super) diagnostics: Vec<DiagnosticsEventData>,
    pub(in crate::engine::room) accepted_policy_updates: Vec<ConsumerPacketSelectionUpdate>,
}

#[derive(Debug)]
pub(in crate::engine::room) struct ProducerRouteFinish {
    source: TransportSourceKey,
    activity: ProducerActivity,
    diagnostics: DiagnosticsEventData,
}

pub(in crate::engine::room) type RoomRouteEffects =
    RouteControlPlan<ProducerRouteFinish, ConsumerRouteFinish>;

#[derive(Debug)]
pub(in crate::engine::room) enum ConsumerRouteFinish {
    Activity(ReceiverRouteActivity, DiagnosticsEventData),
    Keyframe(ConsumerRouteTarget),
    SetupActivity {
        route: TransportConsumerRoute,
        active: bool,
    },
    SourcePolicy {
        update: ConsumerPacketSelectionUpdate,
        route: TransportConsumerRoute,
    },
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
            Self::SourcePolicy { update, route } => {
                if result.packet_gate_failed() || result.activity_failed() {
                    warn!(
                        ?route,
                        route_active = update.route_active(),
                        "media transport rejected the receiver-driven packet selection update"
                    );
                    return;
                }
                if result.keyframe_failed() {
                    warn!(
                        ?route,
                        "media transport failed to request an adaptation keyframe refresh"
                    );
                }
                outcome.accepted_policy_updates.push(update);
            }
        }
    }
}

pub(in crate::engine::room) fn push_source_policy_route(
    route_control: &mut RoomRouteEffects,
    update: ConsumerPacketSelectionUpdate,
    target: &ConsumerRouteTarget,
) {
    let route = target.transport_route().clone();
    let control = update.route_control(route.clone());
    route_control.push_consumer(control, ConsumerRouteFinish::SourcePolicy { update, route });
}

pub(super) async fn execute_setup_activity_correction(
    route: TransportConsumerRoute,
    kind: MediaKind,
    active: bool,
    media_transport: &MediaTransport,
) {
    let mut route_control = RoomRouteEffects::new();
    route_control.push_consumer(
        ConsumerRouteControl::new(route.clone())
            .activity(ConsumerActivity::from_active(active))
            .request_keyframe(active && kind == MediaKind::Video),
        ConsumerRouteFinish::SetupActivity { route, active },
    );
    execute_route_control(route_control, media_transport).await;
}

pub(in crate::engine::room) async fn execute_route_control(
    route_control: RoomRouteEffects,
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
