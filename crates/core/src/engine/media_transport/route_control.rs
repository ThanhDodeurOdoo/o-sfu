use std::marker::PhantomData;

use super::{
    ConsumerActivity, ConsumerPacketGateUpdate, MediaTransport, ProducerActivity,
    ReceiverBweTargetUpdate, SourcePacketGate, TransportAdapterError, TransportConsumerRoute,
    TransportResult, TransportSourceKey,
};

#[derive(Debug)]
pub(crate) enum Draft {}

#[derive(Debug)]
pub(crate) enum Ready {}

#[derive(Debug)]
#[must_use = "route-control plans must be executed or intentionally dropped"]
pub(crate) struct RouteControlPlan<State = Draft> {
    producers: Vec<ProducerRouteControl>,
    consumers: Vec<ConsumerRouteControl>,
    receiver_bwe_targets: Vec<ReceiverBweTargetUpdate>,
    _state: PhantomData<fn() -> State>,
}

impl RouteControlPlan<Draft> {
    pub(crate) const fn new() -> Self {
        Self {
            producers: Vec::new(),
            consumers: Vec::new(),
            receiver_bwe_targets: Vec::new(),
            _state: PhantomData,
        }
    }

    pub(crate) fn push_producer(&mut self, source: TransportSourceKey, activity: ProducerActivity) {
        self.producers
            .push(ProducerRouteControl { source, activity });
    }

    pub(crate) fn push_consumer(&mut self, control: ConsumerRouteControl) {
        self.consumers.push(control);
    }

    pub(crate) fn set_receiver_bwe_targets(&mut self, updates: Vec<ReceiverBweTargetUpdate>) {
        self.receiver_bwe_targets = updates;
    }

    pub(crate) fn ready(self) -> RouteControlPlan<Ready> {
        RouteControlPlan {
            producers: self.producers,
            consumers: self.consumers,
            receiver_bwe_targets: self.receiver_bwe_targets,
            _state: PhantomData,
        }
    }
}

#[derive(Debug, Clone)]
struct ProducerRouteControl {
    source: TransportSourceKey,
    activity: ProducerActivity,
}

#[derive(Debug, Clone)]
pub(crate) struct ConsumerRouteControl {
    route: TransportConsumerRoute,
    packet_gate: Option<SourcePacketGate>,
    activity: Option<ConsumerActivity>,
    keyframe: bool,
}

impl ConsumerRouteControl {
    pub(crate) const fn new(route: TransportConsumerRoute) -> Self {
        Self {
            route,
            packet_gate: None,
            activity: None,
            keyframe: false,
        }
    }

    pub(crate) fn packet_gate(mut self, packet_gate: SourcePacketGate) -> Self {
        self.packet_gate = Some(packet_gate);
        self
    }

    pub(crate) const fn activity(mut self, activity: ConsumerActivity) -> Self {
        self.activity = Some(activity);
        self
    }

    pub(crate) const fn keyframe(mut self, keyframe: bool) -> Self {
        self.keyframe = keyframe;
        self
    }
}

#[derive(Debug, Default)]
pub(crate) struct RouteControlOutcome {
    pub(crate) producers: Vec<TransportResult<()>>,
    pub(crate) consumers: Vec<ConsumerRouteControlOutcome>,
    pub(crate) receiver_bwe_targets: Vec<TransportResult<()>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ConsumerRouteControlOutcome {
    packet_gate_failed: bool,
    activity_failed: bool,
    keyframe_failed: bool,
}

impl ConsumerRouteControlOutcome {
    pub(crate) const fn packet_gate_failed(self) -> bool {
        self.packet_gate_failed
    }

    pub(crate) const fn activity_failed(self) -> bool {
        self.activity_failed
    }

    pub(crate) const fn keyframe_failed(self) -> bool {
        self.keyframe_failed
    }
}

impl MediaTransport {
    pub(crate) async fn apply_route_control(
        &self,
        plan: RouteControlPlan<Ready>,
    ) -> RouteControlOutcome {
        let RouteControlPlan {
            producers,
            consumers,
            receiver_bwe_targets,
            _state,
        } = plan;
        let receiver_bwe_targets = if receiver_bwe_targets.is_empty() {
            Vec::new()
        } else {
            self.set_receiver_bwe_targets(&receiver_bwe_targets).await
        };
        let mut outcome = RouteControlOutcome {
            producers: Vec::with_capacity(producers.len()),
            consumers: Vec::with_capacity(consumers.len()),
            receiver_bwe_targets,
        };
        for control in producers {
            outcome.producers.push(
                self.set_producer_active(&control.source, control.activity)
                    .await,
            );
        }
        let packet_gates = consumer_packet_gates(&consumers);
        let mut packet_gate_results = if packet_gates.is_empty() {
            Vec::new()
        } else {
            self.set_consumer_packet_gates(&packet_gates).await
        }
        .into_iter();
        for control in consumers {
            outcome.consumers.push(
                self.apply_consumer_route_control(control, &mut packet_gate_results)
                    .await,
            );
        }
        outcome
    }

    async fn apply_consumer_route_control(
        &self,
        control: ConsumerRouteControl,
        packet_gate_results: &mut impl Iterator<Item = TransportResult<()>>,
    ) -> ConsumerRouteControlOutcome {
        let packet_gate_failed = control.packet_gate.is_some()
            && packet_gate_results
                .next()
                .unwrap_or(Err(TransportAdapterError::TransportUnavailable))
                .is_err();
        if packet_gate_failed {
            return ConsumerRouteControlOutcome {
                packet_gate_failed,
                ..Default::default()
            };
        }
        let activity_failed = match control.activity {
            Some(activity) => self
                .set_consumer_active(&control.route, activity)
                .await
                .is_err(),
            None => false,
        };
        if activity_failed {
            return ConsumerRouteControlOutcome {
                packet_gate_failed,
                activity_failed,
                ..Default::default()
            };
        }
        let keyframe_failed = control.keyframe
            && self
                .request_consumer_keyframe(&control.route)
                .await
                .is_err();
        ConsumerRouteControlOutcome {
            packet_gate_failed,
            activity_failed,
            keyframe_failed,
        }
    }
}

fn consumer_packet_gates(controls: &[ConsumerRouteControl]) -> Vec<ConsumerPacketGateUpdate> {
    controls
        .iter()
        .filter_map(|control| {
            control.packet_gate.clone().map(|packet_gate| {
                ConsumerPacketGateUpdate::new(control.route.clone(), packet_gate)
            })
        })
        .collect()
}
