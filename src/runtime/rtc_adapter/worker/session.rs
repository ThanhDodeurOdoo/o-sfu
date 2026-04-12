use std::sync::{Arc, Mutex};

#[cfg(feature = "internal-benchmarks")]
use std::net::SocketAddr;

use tokio::sync::oneshot;

use crate::runtime::transport_adapter::{TransportAdapterError, TransportSessionKey};

use super::super::{
    commands::CloseSessionOutcome,
    state::{RtcBootstrapState, RtcSnapshotState},
};

pub(super) fn respond_close_session(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<Result<CloseSessionOutcome, TransportAdapterError>>,
) {
    let close_outcome = worker_close_session(state, snapshot_state, session_key);
    let _ = response.send(Ok(close_outcome));
}

#[cfg(feature = "internal-benchmarks")]
pub(super) fn respond_remember_remote_addr(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let result = if state.sessions.contains_key(session_key) {
        state
            .remote_addr_demux
            .remember_remote_addr(source_addr, session_key);
        if let Ok(mut snapshot) = snapshot_state.lock() {
            snapshot
                .remote_addr_demux
                .remember_remote_addr(source_addr, session_key);
        }
        Ok(())
    } else {
        Err(TransportAdapterError::TransportUnavailable)
    };
    let _ = response.send(result);
}

fn worker_close_session(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    session_key: &TransportSessionKey,
) -> CloseSessionOutcome {
    state.sessions.remove(session_key);
    state.clear_session_schedule(session_key);
    state
        .remote_addr_demux
        .forget_session_remote_addrs(session_key);
    state
        .mid_registry
        .retain(|_id, handle| handle.session_key() != session_key);
    state
        .recv_media_ids
        .retain(|(source_session, _), _| source_session != session_key);
    state
        .media_route_index
        .retain(|(source_session, _), _| source_session != session_key);
    state.media_route_index.retain(|_source, entry| {
        entry
            .destinations
            .retain(|destination| destination.dest_session != *session_key);
        !entry.destinations.is_empty()
    });
    if state.sessions.is_empty() {
        state.shared_socket = None;
    }
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot.remove_session(session_key);
    }
    if state.sessions.is_empty() {
        CloseSessionOutcome::WorkerDrained
    } else {
        CloseSessionOutcome::SessionClosed
    }
}
