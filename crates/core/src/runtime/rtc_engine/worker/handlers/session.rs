//! worker teardown at user level and auxiliary user bookkeeping.
//!
//! Closing a user is more than removing `RtcSessionState`: the worker also
//! has to clear demux indexes, media registries, route ownership, snapshot
//! state, bitrate tracking, and lifetime metrics without leaving
//! packet-loop-visible stuff behind

use std::{collections::BTreeSet, time::Instant};

use tokio::sync::oneshot;

use super::{
    super::super::{
        commands::{CloseSessionOutcome, CloseSessionState},
        observation::PacketLoopObservations,
        state::PacketLoopState,
    },
    WorkerCommandContext,
    media::{refresh_source_packet_gate, remove_source_route},
};
use crate::runtime::{
    media_transport::{TransportAdapterError, TransportSessionKey},
    metrics::{self, RuntimeMetrics},
};

pub(super) fn respond_close_session(
    state: &mut PacketLoopState,
    context: &mut WorkerCommandContext<'_>,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<Result<CloseSessionOutcome, TransportAdapterError>>,
) {
    let close_outcome =
        worker_close_session(state, context.observations, session_key, context.metrics);
    context.publish_observations();
    let _ = response.send(Ok(close_outcome));
}

fn worker_close_session(
    state: &mut PacketLoopState,
    observations: &mut PacketLoopObservations,
    session_key: &TransportSessionKey,
    metrics: &RuntimeMetrics,
) -> CloseSessionOutcome {
    state.clear_session_schedule(session_key);
    let removed_session = state.users.remove(session_key);
    state.remove_egress_bitrate_counter(session_key);
    state
        .remote_addr_demux
        .forget_user_remote_addrs(session_key);
    state
        .remote_addr_demux
        .forget_user_local_ice_ufrag(session_key);
    state
        .remote_addr_demux
        .forget_user_remote_candidate_addrs(session_key);
    let removed_media_handles = state.remove_session_media_handles(session_key);
    let mut affected_route_sources = BTreeSet::new();
    for (source_transport_media_id, _handle) in &removed_media_handles {
        remove_source_route(state, *source_transport_media_id);
    }
    state
        .media_route_index
        .retain(|source_transport_media_id, entry| {
            let destination_count = entry.destinations.len();
            entry
                .destinations
                .retain(|destination| destination.dest_session != *session_key);
            if entry.destinations.len() != destination_count {
                entry.active_destination_count = entry
                    .destinations
                    .iter()
                    .filter(|destination| destination.active)
                    .count();
                affected_route_sources.insert(*source_transport_media_id);
            }
            !entry.destinations.is_empty()
        });
    for source_transport_media_id in affected_route_sources {
        refresh_source_packet_gate(state, source_transport_media_id);
    }
    state.prune_unrouted_remote_sources();
    if state.users.is_empty() {
        state.shared_socket = None;
    }
    let previous = observations.remove_session(session_key);
    metrics.record_transport_health_transition(previous.map(metrics::transport_health_state), None);
    if let Some(removed_session) = removed_session {
        metrics.record_transport_user_lifetime(
            Instant::now().saturating_duration_since(removed_session.started_at),
        );
        metrics.add_active_transport_users(-1);
    }
    if state.users.is_empty() {
        CloseSessionOutcome::new(CloseSessionState::WorkerDrained)
    } else {
        CloseSessionOutcome::new(CloseSessionState::SessionClosed)
    }
}
