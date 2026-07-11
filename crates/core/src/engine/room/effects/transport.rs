use o_sfu_router::MediaKind;
use tracing::warn;

use super::{RoomGaugeDelta, receiver_route::ReceiverSetupTurn};
use crate::engine::{
    diagnostics::DiagnosticsEventData,
    media_transport::{
        ConsumerActivity, ConsumerRouteControl, ConsumerRouteControlOutcome, MediaTransport,
        ProducerActivity, ReceiverBweTargetUpdate, RouteControlPlan, TransportConsumerRoute,
        TransportRelayRouteAction, TransportRelayRouteEffect, TransportSourceKey,
        TransportTeardown,
    },
    room::{
        Room,
        media_graph::{
            ConsumerRouteTarget, ConsumerSetupOrigin, ReceiverRouteActivity, ReceiverRouteWork,
        },
        source_policy::{ConsumerPacketSelectionUpdate, SourcePolicyTurn},
    },
};

#[derive(Debug, Default)]
pub(in crate::engine::room) struct RoomTransportPlan {
    relays: Vec<TransportRelayRouteEffect>,
    teardown: Vec<TransportTeardown>,
    route_control: RoomRouteEffects,
    setup_turns: Vec<ReceiverSetupTurn>,
}

impl RoomTransportPlan {
    pub(in crate::engine::room) fn from_relays_and_teardown(
        mut relays: Vec<TransportRelayRouteEffect>,
        additional_teardown: impl IntoIterator<Item = TransportTeardown>,
    ) -> Self {
        let mut teardown = Vec::new();
        extract_relay_teardown(&mut relays, &mut teardown);
        teardown.extend(additional_teardown);
        Self {
            relays,
            teardown,
            ..Self::default()
        }
    }

    pub(in crate::engine::room) fn extend(&mut self, other: Self) {
        self.relays.extend(other.relays);
        self.teardown.extend(other.teardown);
        self.route_control.append(other.route_control);
        self.setup_turns.extend(other.setup_turns);
    }

    pub(in crate::engine::room) fn extend_teardown(
        &mut self,
        teardown: impl IntoIterator<Item = TransportTeardown>,
    ) {
        self.teardown.extend(teardown);
    }

    pub(super) fn push_producer(
        &mut self,
        source: TransportSourceKey,
        active: bool,
        diagnostics: DiagnosticsEventData,
    ) {
        self.route_control
            .producer_activity(source, active, diagnostics);
    }

    pub(super) fn push_receiver_work(
        &mut self,
        mut work: ReceiverRouteWork,
        origin: ConsumerSetupOrigin,
        mut diagnostics: impl FnMut(&ReceiverRouteActivity) -> DiagnosticsEventData,
    ) {
        extract_relay_teardown(&mut work.relays, &mut self.teardown);
        self.relays.extend(work.relays);
        for activity in work.activities {
            let event = diagnostics(&activity);
            self.route_control.receiver_activity(activity, event);
        }
        for setup in work.setups {
            self.setup_turns.push(ReceiverSetupTurn::new(setup, origin));
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
        execute_relay_route_effects(media_transport, self.relays).await;
        let route_outcome = self.route_control.execute(media_transport).await;
        outcome.diagnostics.extend(route_outcome.diagnostics);
        media_transport.teardown(self.teardown).await;
        for turn in self.setup_turns {
            turn.execute(room, media_transport, &mut outcome).await;
        }
        outcome
    }

    #[cfg(test)]
    pub(in crate::engine::room) fn relays_and_teardown(
        &self,
    ) -> (&[TransportRelayRouteEffect], &[TransportTeardown]) {
        (&self.relays, &self.teardown)
    }
}

#[derive(Debug, Default)]
pub(super) struct RoomTransportOutcome {
    pub(super) gauges: Vec<RoomGaugeDelta>,
    pub(super) diagnostics: Vec<DiagnosticsEventData>,
    pub(super) source_policy: SourcePolicyTurn,
}

#[derive(Debug, Default)]
pub(in crate::engine::room) struct RoomRouteOutcome {
    pub(super) diagnostics: Vec<DiagnosticsEventData>,
    pub(in crate::engine::room) accepted_policy_updates: Vec<ConsumerPacketSelectionUpdate>,
}

#[derive(Debug, Default)]
#[must_use = "room route effects must be executed or intentionally dropped"]
pub(in crate::engine::room) struct RoomRouteEffects(
    RouteControlPlan<ProducerRouteFinish, ConsumerRouteFinish>,
);

impl RoomRouteEffects {
    fn producer_activity(
        &mut self,
        source: TransportSourceKey,
        active: bool,
        diagnostics: DiagnosticsEventData,
    ) {
        let activity = ProducerActivity::from_active(active);
        self.0.push_producer(
            source.clone(),
            activity,
            ProducerRouteFinish {
                source,
                activity,
                diagnostics,
            },
        );
    }

    fn receiver_activity(
        &mut self,
        activity: ReceiverRouteActivity,
        diagnostics: DiagnosticsEventData,
    ) {
        let target = activity.target();
        let active = activity.active();
        self.0.push_consumer(
            ConsumerRouteControl::new(target.transport_route().clone())
                .activity(ConsumerActivity::from_active(active))
                .request_keyframe(target.request_keyframe_after_activity(active)),
            ConsumerRouteFinish::Activity(activity, diagnostics),
        );
    }

    pub(super) fn setup_activity(
        &mut self,
        route: TransportConsumerRoute,
        kind: MediaKind,
        active: bool,
    ) {
        self.0.push_consumer(
            ConsumerRouteControl::new(route.clone())
                .activity(ConsumerActivity::from_active(active))
                .request_keyframe(active && kind == MediaKind::Video),
            ConsumerRouteFinish::SetupActivity(route, active),
        );
    }

    pub(super) fn keyframe(&mut self, target: ConsumerRouteTarget) {
        self.0.push_consumer(
            ConsumerRouteControl::new(target.transport_route().clone()).request_keyframe(true),
            ConsumerRouteFinish::Keyframe(target),
        );
    }

    pub(in crate::engine::room) fn source_policy_update(
        &mut self,
        update: ConsumerPacketSelectionUpdate,
        target: &ConsumerRouteTarget,
    ) {
        let route = target.transport_route().clone();
        self.0.push_consumer(
            update.route_control(route.clone()),
            ConsumerRouteFinish::SourcePolicy(update, route),
        );
    }

    pub(in crate::engine::room) fn set_receiver_bwe_targets(
        &mut self,
        targets: Vec<ReceiverBweTargetUpdate>,
    ) {
        self.0.set_receiver_bwe_targets(targets);
    }

    fn append(&mut self, other: Self) {
        self.0.append(other.0);
    }

    pub(in crate::engine::room) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(in crate::engine::room) async fn execute(
        self,
        media_transport: &MediaTransport,
    ) -> RoomRouteOutcome {
        let transport_outcome = media_transport.apply_route_control(self.0).await;

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
}

#[derive(Debug)]
struct ProducerRouteFinish {
    source: TransportSourceKey,
    activity: ProducerActivity,
    diagnostics: DiagnosticsEventData,
}

#[derive(Debug)]
enum ConsumerRouteFinish {
    Activity(ReceiverRouteActivity, DiagnosticsEventData),
    Keyframe(ConsumerRouteTarget),
    SetupActivity(TransportConsumerRoute, bool),
    SourcePolicy(ConsumerPacketSelectionUpdate, TransportConsumerRoute),
}

impl ConsumerRouteFinish {
    #[allow(
        clippy::cognitive_complexity,
        reason = "closed route completion policy is clearer than one use finish helpers"
    )]
    fn finish(self, result: ConsumerRouteControlOutcome, outcome: &mut RoomRouteOutcome) {
        match self {
            Self::Activity(activity, diagnostics) => {
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
            Self::Keyframe(target) => {
                if result.keyframe_failed() {
                    warn!(
                        route = ?target.transport_route(),
                        "media transport failed to request a refreshed consumer keyframe"
                    );
                }
            }
            Self::SetupActivity(route, active) => {
                if result.activity_failed() {
                    warn!(
                        ?route,
                        active,
                        "media transport failed to correct in-flight consumer setup activity"
                    );
                } else if result.keyframe_failed() {
                    warn!(
                        ?route,
                        "media transport failed to request keyframe after consumer setup activity correction"
                    );
                }
            }
            Self::SourcePolicy(update, route) => {
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

pub(super) async fn execute_relays_and_teardown(
    media_transport: &MediaTransport,
    mut relays: Vec<TransportRelayRouteEffect>,
    additional_teardown: impl IntoIterator<Item = TransportTeardown>,
) -> bool {
    let mut teardown = Vec::new();
    extract_relay_teardown(&mut relays, &mut teardown);
    teardown.extend(additional_teardown);
    let relays_applied = execute_relay_route_effects(media_transport, relays).await;
    media_transport.teardown(teardown).await;
    relays_applied
}

fn extract_relay_teardown(
    relays: &mut Vec<TransportRelayRouteEffect>,
    teardown: &mut Vec<TransportTeardown>,
) {
    teardown.extend(
        relays
            .extract_if(.., |effect| {
                effect.action == TransportRelayRouteAction::Release
            })
            .map(|effect| TransportTeardown::ReleaseRelayRoute {
                source: effect.source,
                target_media_worker_id: effect.target_media_worker_id,
            }),
    );
}

async fn execute_relay_route_effects(
    media_transport: &MediaTransport,
    effects: impl IntoIterator<Item = TransportRelayRouteEffect>,
) -> bool {
    let mut applied = true;
    for effect in effects {
        if let Err(error) = media_transport.apply_relay_route_effect(&effect).await {
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
