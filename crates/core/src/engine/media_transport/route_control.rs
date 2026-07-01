use tracing::{debug, warn};

use super::{
    ConsumerActivity, ConsumerPacketGateUpdate, MediaTransport, ProducerActivity,
    ReceiverBweTargetUpdate, SourcePacketGate, TransportAdapterError, TransportCommandOp,
    TransportConsumerRoute, TransportResult, TransportSourceKey,
    rtc::{RouteControlRequest, RtcWorkerCommand},
};

#[derive(Debug)]
#[must_use = "route-control plans must be executed or intentionally dropped"]
pub(crate) struct RouteControlPlan<P = (), C = ()> {
    producers: Vec<(ProducerRouteControl, P)>,
    consumers: Vec<(ConsumerRouteControl, C)>,
    receiver_bwe_targets: Vec<ReceiverBweTargetUpdate>,
}

impl<P, C> RouteControlPlan<P, C> {
    pub(crate) const fn new() -> Self {
        Self {
            producers: Vec::new(),
            consumers: Vec::new(),
            receiver_bwe_targets: Vec::new(),
        }
    }

    pub(crate) fn push_producer(
        &mut self,
        source: TransportSourceKey,
        activity: ProducerActivity,
        finish: P,
    ) {
        self.producers
            .push((ProducerRouteControl { source, activity }, finish));
    }

    pub(crate) fn push_consumer(&mut self, control: ConsumerRouteControl, finish: C) {
        self.consumers.push((control, finish));
    }

    pub(crate) fn set_receiver_bwe_targets(&mut self, updates: Vec<ReceiverBweTargetUpdate>) {
        self.receiver_bwe_targets = updates;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.producers.is_empty()
            && self.consumers.is_empty()
            && self.receiver_bwe_targets.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn consumer_finishes_for_test(&self) -> impl Iterator<Item = &C> {
        self.consumers.iter().map(|(_control, finish)| finish)
    }
}

impl<P, C> Default for RouteControlPlan<P, C> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct ProducerRouteControl {
    source: TransportSourceKey,
    activity: ProducerActivity,
}

#[derive(Debug)]
pub(crate) struct ConsumerRouteControl {
    route: TransportConsumerRoute,
    packet_gate: Option<SourcePacketGate>,
    activity: Option<ConsumerActivity>,
    request_keyframe: bool,
}

impl ConsumerRouteControl {
    pub(crate) const fn new(route: TransportConsumerRoute) -> Self {
        Self {
            route,
            packet_gate: None,
            activity: None,
            request_keyframe: false,
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

    pub(crate) const fn request_keyframe(mut self, request: bool) -> Self {
        self.request_keyframe = request;
        self
    }
}

#[derive(Debug)]
pub(crate) struct RouteControlOutcome<P, C> {
    pub(crate) producers: Vec<(P, TransportResult<()>)>,
    pub(crate) consumers: Vec<(C, ConsumerRouteControlOutcome)>,
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
    async fn set_producer_active(
        &self,
        source: &TransportSourceKey,
        activity: ProducerActivity,
    ) -> Result<(), TransportAdapterError> {
        self.request_session_command(
            source.session_key(),
            |response| {
                RtcWorkerCommand::media_control(
                    RouteControlRequest::SetProducerActive {
                        source: source.clone(),
                        active: activity.is_active(),
                    },
                    response,
                )
            },
            |error| {
                warn!(
                    ?source,
                    op = ?TransportCommandOp::SetProducerActive,
                    active = activity.is_active(),
                    ?error,
                    "media transport worker command failed"
                );
            },
        )
        .await
    }

    async fn set_consumer_active(
        &self,
        route: &TransportConsumerRoute,
        activity: ConsumerActivity,
    ) -> Result<(), TransportAdapterError> {
        self.request_consumer_route_command(
            route,
            |response| {
                RtcWorkerCommand::media_control(
                    RouteControlRequest::SetConsumerActive {
                        route: route.clone(),
                        active: activity.is_active(),
                    },
                    response,
                )
            },
            |error| {
                warn!(
                    ?route,
                    op = ?TransportCommandOp::SetConsumerActive,
                    active = activity.is_active(),
                    ?error,
                    "media transport worker command failed"
                );
            },
        )
        .await
    }

    async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let results = self.apply_consumer_pkt_gate_batch(updates).await;
        for (update, result) in updates.iter().zip(results.iter()) {
            if let Err(error) = result {
                warn!(
                    ?error,
                    route = ?update.route(),
                    packet_gate = ?update.packet_gate(),
                    "media transport failed to update a batched consumer packet gate"
                );
            }
        }
        results
    }

    async fn set_receiver_bwe_targets(
        &self,
        updates: &[ReceiverBweTargetUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let results = self.execute_receiver_bwe_target_batch(updates).await;
        for (update, result) in updates.iter().zip(results.iter()) {
            match result {
                Ok(()) => {}
                Err(TransportAdapterError::InvalidInput) => {
                    debug!(
                        session_key = ?update.session_key(),
                        target = update.target().as_bps(),
                        "media transport skipped a stale receiver BWE target"
                    );
                }
                Err(error) => {
                    warn!(
                        ?error,
                        session_key = ?update.session_key(),
                        target = update.target().as_bps(),
                        "media transport failed to update a receiver BWE target"
                    );
                }
            }
        }
        results
    }

    async fn request_consumer_keyframe(
        &self,
        route: &TransportConsumerRoute,
    ) -> Result<(), TransportAdapterError> {
        self.request_consumer_route_command(
            route,
            |response| {
                RtcWorkerCommand::media_control(
                    RouteControlRequest::RequestConsumerKeyframe {
                        route: route.clone(),
                    },
                    response,
                )
            },
            |error| {
                warn!(
                    ?route,
                    op = ?TransportCommandOp::RequestConsumerKeyframe,
                    ?error,
                    "media transport worker command failed"
                );
            },
        )
        .await
    }

    /// applies receiver BWE, producer state and consumer route controls as one batch
    ///
    /// consumer packet gates run before activity and keyframes for the same route
    /// a packet-gate failure suppresses later route work so transport state does not
    /// advertise activity for a packet policy that was not installed
    pub(crate) async fn apply_route_control<P, C>(
        &self,
        plan: RouteControlPlan<P, C>,
    ) -> RouteControlOutcome<P, C> {
        let RouteControlPlan {
            producers,
            consumers,
            receiver_bwe_targets: receiver_bwe_updates,
        } = plan;
        if !receiver_bwe_updates.is_empty() {
            self.set_receiver_bwe_targets(&receiver_bwe_updates).await;
        }
        let mut outcome = RouteControlOutcome {
            producers: Vec::with_capacity(producers.len()),
            consumers: Vec::with_capacity(consumers.len()),
        };
        for (control, finish) in producers {
            let result = self
                .set_producer_active(&control.source, control.activity)
                .await;
            outcome.producers.push((finish, result));
        }
        let packet_gate_updates: Vec<_> = consumers
            .iter()
            .filter_map(|(control, _)| {
                control.packet_gate.as_ref().map(|packet_gate| {
                    ConsumerPacketGateUpdate::new(control.route.clone(), packet_gate.clone())
                })
            })
            .collect();
        let mut packet_gate_results = if packet_gate_updates.is_empty() {
            Vec::new()
        } else {
            self.set_consumer_packet_gates(&packet_gate_updates).await
        }
        .into_iter();
        for (control, finish) in consumers {
            let packet_gate_failed = control.packet_gate.is_some()
                && !matches!(packet_gate_results.next(), Some(Ok(())));
            let result = self
                .apply_consumer_route_control(control, packet_gate_failed)
                .await;
            outcome.consumers.push((finish, result));
        }
        outcome
    }

    async fn apply_consumer_route_control(
        &self,
        control: ConsumerRouteControl,
        packet_gate_failed: bool,
    ) -> ConsumerRouteControlOutcome {
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
        let keyframe_failed = control.request_keyframe
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
