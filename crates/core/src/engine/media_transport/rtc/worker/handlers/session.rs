//! worker teardown at user level and auxiliary user bookkeeping.
//!
//! Closing a user is more than removing `RtcSessionState`: the worker also
//! has to clear demux indexes, media registries, route ownership, snapshot
//! state, bitrate tracking, and lifetime metrics. A drained worker keeps the
//! shared UDP socket idle so a new session can reuse the packet loop without
//! racing socket teardown.

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use super::{
    super::super::{
        bitrate::BitrateRegistry,
        state::{PacketLoopState, RtcSnapshotState},
    },
    media::remove_source_route,
};
use crate::engine::{
    media_transport::TransportSessionKey,
    metrics::{self, RuntimeMetrics},
};

pub(super) fn worker_close_session(
    state: &mut PacketLoopState,
    bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    session_key: &TransportSessionKey,
    metrics: &RuntimeMetrics,
) {
    state.clear_session_schedule(session_key);
    let removed_session = state.users.remove(session_key);
    state
        .remote_addr_demux
        .forget_user_remote_addrs(session_key);
    state
        .remote_addr_demux
        .forget_user_local_ice_ufrag(session_key);
    state
        .remote_addr_demux
        .forget_user_remote_candidates(session_key);
    let removed_media_handles = state.remove_session_media_handles(session_key);
    for (src_media, _handle) in &removed_media_handles {
        remove_source_route(state, *src_media);
    }
    let moved_routes = state.routes.remove_dsts_for_session(session_key);
    for (src_media, moved) in moved_routes {
        state.set_consumer_dst_idx(
            &moved.session_key,
            moved.mid,
            moved.media_id,
            src_media,
            Some(moved.dst_idx),
        );
    }
    let mid_registry = &state.mid_registry;
    state
        .routes
        .prune_unrouted_remote_srcs(|src_media| mid_registry.contains_key(src_media));
    if let Ok(mut snapshot) = snapshot_state.lock() {
        let previous = snapshot.remove_session(session_key);
        metrics.record_transport_health_transition(
            previous.map(metrics::transport_health_state),
            None,
        );
    }
    if let Ok(mut bitrate) = bitrate_registry.lock() {
        bitrate.remove_session(session_key);
    }
    if let Some(removed_session) = removed_session {
        metrics.record_transport_user_lifetime(
            Instant::now().saturating_duration_since(removed_session.started_at),
        );
        metrics.add_active_transport_users(-1);
    }
}
