use o_sfu_router::MediaKind;
use tracing::warn;

use crate::engine::{
    diagnostics::DiagnosticsEventData,
    media_transport::{
        ConsumerActivity, ConsumerRouteControl, ConsumerRouteControlOutcome, MediaTransport,
        ProducerActivity, ReceiverBweTargetUpdate, RouteControlPlan, TransportConsumerRoute,
        TransportSourceKey,
    },
    room::{
        media_graph::{ConsumerRouteTarget, ReceiverRouteActivity},
        source_policy::ConsumerPacketSelectionUpdate,
    },
};

#[derive(Debug, Default)]
#[must_use = "room route effects must be executed after being populated"]
pub struct RoomRouteEffects {
    producers: Vec<ProducerEffect>,
    consumers: Vec<ConsumerEffect>,
    receiver_bwe_targets: Vec<ReceiverBweTargetUpdate>,
}

impl RoomRouteEffects {
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
        self.consumers.push(ConsumerEffect::Setup {
            route,
            active,
            keyframe: active && kind == MediaKind::Video,
        });
    }

    pub fn push_source_selection(
        &mut self,
        update: ConsumerPacketSelectionUpdate,
        route: TransportConsumerRoute,
    ) {
        self.consumers
            .push(ConsumerEffect::SourceSelection(update, route));
    }

    pub fn set_receiver_bwe_targets(&mut self, updates: Vec<ReceiverBweTargetUpdate>) {
        self.receiver_bwe_targets = updates;
    }

    pub async fn execute(self, media_transport: &MediaTransport) -> RoomRouteEffectOutcome {
        if self.producers.is_empty()
            && self.consumers.is_empty()
            && self.receiver_bwe_targets.is_empty()
        {
            return RoomRouteEffectOutcome::default();
        }
        let mut plan = RouteControlPlan::new();
        plan.set_receiver_bwe_targets(self.receiver_bwe_targets);
        for producer in &self.producers {
            plan.push_producer(
                producer.source.clone(),
                ProducerActivity::from_active(producer.active),
            );
        }
        for consumer in &self.consumers {
            plan.push_consumer(consumer.control());
        }

        let route_outcome = media_transport.apply_route_control(plan.ready()).await;
        debug_assert_eq!(self.producers.len(), route_outcome.producers.len());
        debug_assert_eq!(self.consumers.len(), route_outcome.consumers.len());

        let mut outcome = RoomRouteEffectOutcome::default();
        let _ = route_outcome.receiver_bwe_targets;
        for (producer, result) in self.producers.into_iter().zip(route_outcome.producers) {
            producer.finish(result.is_err(), &mut outcome);
        }
        for (consumer, result) in self.consumers.into_iter().zip(route_outcome.consumers) {
            consumer.finish(result, &mut outcome);
        }
        outcome
    }
}

#[derive(Debug, Default)]
pub struct RoomRouteEffectOutcome {
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
    fn finish(self, failed: bool, outcome: &mut RoomRouteEffectOutcome) {
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
enum ConsumerEffect {
    Activity(ReceiverRouteActivity, DiagnosticsEventData),
    Keyframe(ConsumerRouteTarget),
    Setup {
        route: TransportConsumerRoute,
        active: bool,
        keyframe: bool,
    },
    SourceSelection(ConsumerPacketSelectionUpdate, TransportConsumerRoute),
}

impl ConsumerEffect {
    fn control(&self) -> ConsumerRouteControl {
        match self {
            Self::Activity(activity, _) => {
                let target = activity.target();
                let active = activity.active();
                ConsumerRouteControl::new(target.transport_route().clone())
                    .activity(ConsumerActivity::from_active(active))
                    .keyframe(target.request_keyframe_after_activity(active))
            }
            Self::Keyframe(target) => {
                ConsumerRouteControl::new(target.transport_route().clone()).keyframe(true)
            }
            Self::Setup {
                route,
                active,
                keyframe,
            } => ConsumerRouteControl::new(route.clone())
                .activity(ConsumerActivity::from_active(*active))
                .keyframe(*keyframe),
            Self::SourceSelection(update, route) => {
                let mut control =
                    ConsumerRouteControl::new(route.clone()).keyframe(update.request_keyframe);
                if update.route_activity_update {
                    control =
                        control.activity(ConsumerActivity::from_active(update.route_active()));
                }
                if let Some(packet_gate) = &update.packet_gate {
                    control = control.packet_gate(packet_gate.clone());
                }
                control
            }
        }
    }

    fn finish(self, result: ConsumerRouteControlOutcome, outcome: &mut RoomRouteEffectOutcome) {
        match self {
            Self::Activity(activity, diagnostics) => {
                finish_activity(&activity, diagnostics, result, outcome);
            }
            Self::Keyframe(target) => finish_keyframe(&target, result),
            Self::Setup { route, active, .. } => finish_setup(&route, active, result),
            Self::SourceSelection(update, _) => finish_source_selection(update, result, outcome),
        }
    }
}

fn finish_activity(
    activity: &ReceiverRouteActivity,
    diagnostics: DiagnosticsEventData,
    result: ConsumerRouteControlOutcome,
    outcome: &mut RoomRouteEffectOutcome,
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

fn finish_setup(route: &TransportConsumerRoute, active: bool, result: ConsumerRouteControlOutcome) {
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
    result: ConsumerRouteControlOutcome,
    outcome: &mut RoomRouteEffectOutcome,
) {
    if result.packet_gate_failed() || result.activity_failed() {
        warn!(
            route = ?update.route,
            route_active = update.route_active(),
            "media transport rejected the receiver-driven packet selection update"
        );
        return;
    }
    if result.keyframe_failed() {
        warn!(
            route = ?update.route,
            "media transport failed to request an adaptation keyframe refresh"
        );
    }
    outcome.packet_updates.push(update);
}

#[cfg(test)]
#[path = "TESTS/route.rs"]
mod tests;
