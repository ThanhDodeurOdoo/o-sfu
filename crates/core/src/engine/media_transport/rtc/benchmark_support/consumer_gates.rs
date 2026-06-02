use std::time::Instant;

use str0m::media::{Mid, Rid};

use super::super::{
    commands::ConsumerPacketGateCommand,
    route_control::PacketLayerGate,
    test_support::{MediaWorkerScenario, test_transport_session_key},
    worker::worker_set_consumer_packet_gates_for_benchmark,
};
use crate::engine::{
    UserId,
    media_transport::{
        TransportMediaId, TransportSessionKey, TransportSourceKey, rtc::state::PacketLoopState,
    },
};

/// fixed consumer packet-gate batch fixture for route-control benchmarks
///
/// setup installs one source with a caller-selected number of consumer
/// destinations. the measured path applies one source-scoped selected-RID gate
/// batch through the production worker route-control helper
pub struct ConsumerGateBatchBenchFixture {
    state: PacketLoopState,
    source: TransportSourceKey,
    updates: Vec<ConsumerPacketGateCommand>,
    now: Instant,
}

impl ConsumerGateBatchBenchFixture {
    #[must_use]
    pub fn consumers_64() -> Self {
        Self::with_consumers(64)
    }

    #[must_use]
    pub fn consumers_256() -> Self {
        Self::with_consumers(256)
    }

    fn with_consumers(destination_count: usize) -> Self {
        let source_session = test_transport_session_key(121, 0, 122, UserId::Integer(123));
        let consumer_session = test_transport_session_key(121, 0, 124, UserId::Integer(125));
        let mut state = PacketLoopState::default();
        let mut scenario = MediaWorkerScenario::new(&mut state);
        let source_transport_media_id =
            scenario.source(source_session.clone(), Mid::from("cam-up"));
        let source = TransportSourceKey::new(source_session, source_transport_media_id);
        let updates = route_gate_updates(
            &mut scenario,
            source_transport_media_id,
            &consumer_session,
            destination_count,
        );

        Self {
            state,
            source,
            updates,
            now: Instant::now(),
        }
    }

    #[must_use]
    pub fn apply_updates(self) -> usize {
        let Self {
            mut state,
            source,
            updates,
            now,
        } = self;
        let results =
            worker_set_consumer_packet_gates_for_benchmark(&mut state, &source, updates, now);
        let gate_state_evidence = gate_state_evidence(&state, source.transport_media_id());
        results.len() + gate_state_evidence
    }
}

fn gate_state_evidence(
    state: &PacketLoopState,
    source_transport_media_id: TransportMediaId,
) -> usize {
    let pending_gate = state
        .routes
        .local_route(source_transport_media_id)
        .and_then(|entry| entry.destinations.first())
        .is_some_and(|destination| destination.pending_packet_gate.is_some());
    let effective_gate = state
        .routes
        .effective_packet_gate(source_transport_media_id)
        .is_some();
    usize::from(pending_gate) + usize::from(effective_gate)
}

fn route_gate_updates(
    scenario: &mut MediaWorkerScenario<'_>,
    source_transport_media_id: TransportMediaId,
    consumer_session: &TransportSessionKey,
    destination_count: usize,
) -> Vec<ConsumerPacketGateCommand> {
    let mut updates = Vec::with_capacity(destination_count);
    let rid = Rid::from("hi");
    for destination_idx in 0..destination_count {
        let mid = Mid::from(format!("cam-down-{destination_idx}").as_str());
        let consumer_transport_media_id =
            scenario.destination(source_transport_media_id, consumer_session.clone(), mid);
        updates.push(ConsumerPacketGateCommand::new(
            consumer_session.clone(),
            consumer_transport_media_id,
            PacketLayerGate::Rid(rid),
        ));
    }
    updates
}
