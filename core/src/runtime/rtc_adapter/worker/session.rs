//! worker teardown at user level and auxiliary user bookkeeping.
//!
//! Closing a user is more than removing `RtcSessionState`: the worker also
//! has to clear demux indexes, media registries, route ownership, relay cleanup
//! hints, snapshot state, bitrate tracking, and lifetime metrics without
//! leaving packet-loop-visible stuff behind

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::sync::oneshot;

use super::{
    super::{
        bitrate::RtcBitrateState,
        commands::{CloseSessionOutcome, CloseSessionState, RelayCleanup},
        media_registry::RegisteredMediaHandle,
        state::{RtcBootstrapState, RtcSnapshotState},
    },
    media::refresh_source_packet_gate,
};
use crate::runtime::{
    metrics::{self, RuntimeMetrics},
    transport_adapter::{TransportAdapterError, TransportMediaId, TransportSessionKey},
};

pub(super) fn respond_close_session(
    state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    session_key: &TransportSessionKey,
    metrics: &RuntimeMetrics,
    response: oneshot::Sender<Result<CloseSessionOutcome, TransportAdapterError>>,
) {
    let close_outcome =
        worker_close_session(state, bitrate_state, snapshot_state, session_key, metrics);
    let _ = response.send(Ok(close_outcome));
}

fn worker_close_session(
    state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    session_key: &TransportSessionKey,
    metrics: &RuntimeMetrics,
) -> CloseSessionOutcome {
    let removed_session = state.users.remove(session_key);
    state.clear_session_schedule(session_key);
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
    let relay_cleanup = relay_cleanup_for_removed_media(state, &removed_media_handles);
    let removed_media_ids = removed_media_handles
        .iter()
        .map(|(transport_media_id, _handle)| *transport_media_id)
        .collect::<Vec<_>>();
    let mut affected_route_sources = BTreeSet::new();
    state
        .media_route_index
        .retain(|source_transport_media_id, _| {
            !removed_media_ids.contains(source_transport_media_id)
        });
    state
        .media_route_index
        .retain(|source_transport_media_id, entry| {
            let destination_count = entry.destinations.len();
            entry
                .destinations
                .retain(|destination| destination.dest_session != *session_key);
            if entry.destinations.len() != destination_count {
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
    if let Ok(mut snapshot) = snapshot_state.lock() {
        let previous = snapshot.remove_session(session_key);
        metrics.record_transport_health_transition(
            previous.map(metrics::transport_health_state),
            None,
        );
    }
    if let Ok(mut bitrate) = bitrate_state.lock() {
        bitrate.remove_session(session_key);
    }
    if let Some(removed_session) = removed_session {
        metrics.record_transport_user_lifetime(
            Instant::now().saturating_duration_since(removed_session.started_at),
        );
        metrics.add_active_transport_users(-1);
    }
    if state.users.is_empty() {
        CloseSessionOutcome::new(CloseSessionState::WorkerDrained, relay_cleanup)
    } else {
        CloseSessionOutcome::new(CloseSessionState::SessionClosed, relay_cleanup)
    }
}

fn relay_cleanup_for_removed_media(
    state: &RtcBootstrapState,
    removed_media_handles: &[(TransportMediaId, RegisteredMediaHandle)],
) -> Vec<RelayCleanup> {
    removed_media_handles
        .iter()
        .filter_map(|(_transport_media_id, handle)| match handle {
            RegisteredMediaHandle::Producer { .. } => None,
            RegisteredMediaHandle::Consumer {
                source_transport_media_id,
                ..
            } => state
                .remote_source_registration(*source_transport_media_id)
                .map(|registration| {
                    RelayCleanup::new(
                        registration.source_session_key().clone(),
                        *source_transport_media_id,
                    )
                }),
        })
        .collect()
}
