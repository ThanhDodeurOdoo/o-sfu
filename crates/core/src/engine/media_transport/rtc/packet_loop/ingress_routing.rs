//! UDP ingress routing for RTC sessions
//!
//! the shared UDP socket receives datagrams for every session on a worker
//! this module decides which `str0m::Rtc` should see one datagram
//! indexes and caches are performance hints only
//! `Rtc::accepts()` remains the authoritative ownership check
//!
//! # routing strategy
//!
//! 1. use the pinned source-address index when a tuple is already known
//! 2. drop repeated identical misses through the recent-miss cache
//! 3. rate-limit sustained unknown-source probes
//! 4. probe STUN, DTLS or RTP shape to choose a narrow candidate set
//! 5. call `Rtc::accepts()` before calling `Rtc::handle_input()`
//! 6. pin successful source tuples so later packets take the indexed path
//!
//! recovery must stay a subset of `str0m` demux behavior so this module never
//! recovers traffic that `str0m` would reject downstream

use std::{
    fmt,
    net::SocketAddr,
    slice::Iter,
    sync::{Arc, Mutex},
    time::Instant,
};

use str0m::{
    Input,
    ice::StunMessage,
    net::{Protocol, Receive},
};
use tracing::{debug, trace, warn};

use super::super::{
    routing_miss::{DemuxRecoveryState, PacketLoopRoutingMissKey},
    state::{PacketLoopState, RtcSnapshotState},
};
use crate::engine::{
    hot_path::unlikely,
    media_transport::TransportSessionKey,
    metrics::{RtcDatagramDropReason, RtcDatagramRoutePath, RtcMetricsRecorder},
};

enum CachedRouteOutcome {
    Routed,
    NotMatched,
    Malformed,
}

/// outcome of probing the demux indexes for an unknown source tuple
///
/// `Matched` means one candidate passed `Rtc::accepts()`
/// `NoMatch` is a bounded miss that may be cached
/// `Malformed` means the packet cannot be represented as `str0m` receive input
enum IndexedSessionRecoveryOutcome {
    Matched {
        session_key: TransportSessionKey,
        examined_sessions: usize,
    },
    NoMatch {
        examined_sessions: usize,
    },
    Malformed,
}

/// recovery probe used to choose a bounded candidate session set
///
/// classification must stay aligned with `str0m` UDP multiplexing so the
/// packet loop never recovers traffic that `Rtc::accepts()` rejects
enum PacketIndexProbe<'a> {
    LocalIceUfrag(&'a str),
    RemoteCandidateAddr(SocketAddr),
}

impl fmt::Display for PacketIndexProbe<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalIceUfrag(local_ice_ufrag) => {
                write!(formatter, "local-ice-ufrag:{local_ice_ufrag}")
            }
            Self::RemoteCandidateAddr(remote_candidate_addr) => {
                write!(formatter, "remote-candidate-addr:{remote_candidate_addr}")
            }
        }
    }
}

/// fixture-owned datagram input for deterministic ingress benchmarks
#[derive(Clone, Copy)]
pub struct PacketRouteDatagram<'a> {
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &'a [u8],
    now: Instant,
}

impl<'a> PacketRouteDatagram<'a> {
    pub const fn new(
        source_addr: SocketAddr,
        candidate_addr: SocketAddr,
        packet: &'a [u8],
        now: Instant,
    ) -> Self {
        Self {
            source_addr,
            candidate_addr,
            packet,
            now,
        }
    }
}

enum CandidateSessionKeys<'a> {
    Single(Option<&'a TransportSessionKey>),
    Slice(Iter<'a, TransportSessionKey>),
}

impl<'a> Iterator for CandidateSessionKeys<'a> {
    type Item = &'a TransportSessionKey;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(session_key) => session_key.take(),
            Self::Slice(iter) => iter.next(),
        }
    }
}

/// route an incoming UDP datagram to its owning RTC session
///
/// # error handling
///
/// routing failure is not a transport error
/// malformed packets, unknown sessions, repeated misses and rate-limited sources
/// are dropped with metrics
/// `Rtc::handle_input()` errors are logged but keep the route learned because
/// ownership and packet validity are separate concerns
#[cfg(test)]
pub(super) fn route_pkt_to_session(
    state: &mut PacketLoopState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    demux: &mut DemuxRecoveryState,
    metrics: &RtcMetricsRecorder,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) {
    route_pkt_to_session_at(
        state,
        snapshot_state,
        demux,
        metrics,
        PacketRouteDatagram::new(source_addr, candidate_addr, packet, Instant::now()),
    );
}

pub fn route_pkt_to_session_at(
    state: &mut PacketLoopState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    demux: &mut DemuxRecoveryState,
    metrics: &RtcMetricsRecorder,
    datagram: PacketRouteDatagram<'_>,
) {
    match route_cached_pkt(
        state,
        snapshot_state,
        datagram.source_addr,
        datagram.candidate_addr,
        datagram.packet,
        datagram.now,
    ) {
        CachedRouteOutcome::Routed => {
            metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
            return;
        }
        CachedRouteOutcome::Malformed => {
            metrics.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
            return;
        }
        CachedRouteOutcome::NotMatched => {}
    }
    if demux.is_source_blocked(datagram.source_addr, datagram.now) {
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::SourceRateLimited);
        return;
    }
    let miss_key = PacketLoopRoutingMissKey::new(
        datagram.source_addr,
        datagram.candidate_addr,
        datagram.packet,
    );
    // recent misses are valid only until topology or ICE indexes change
    if demux.should_skip_scan(miss_key, datagram.packet) {
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::RecentMissCache);
        trace!(
            source = %datagram.source_addr,
            "dropping UDP datagram because a recent cache miss already proved no rtc user accepted it"
        );
        return;
    }
    // unknown sources should have been learned through STUN before media reaches this path
    if demux.should_rate_limit_source(datagram.source_addr, datagram.now) {
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::SourceRateLimited);
        return;
    }
    let route = PacketRouteContext {
        snapshot_state,
        metrics,
        source_addr: datagram.source_addr,
        candidate_addr: datagram.candidate_addr,
        packet: datagram.packet,
        now: datagram.now,
    };
    // one live session needs no recovery index before the `accepts()` check
    if state.users.len() == 1 {
        route_pkt_by_session(state, demux, miss_key, &route);
        return;
    }
    route_pkt_by_recovery(state, demux, miss_key, &route);
}

fn route_cached_pkt(
    state: &mut PacketLoopState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
    now: Instant,
) -> CachedRouteOutcome {
    let Some(session_key) = state
        .remote_addr_demux
        .session_key_for_remote_addr(source_addr)
    else {
        return CachedRouteOutcome::NotMatched;
    };
    let Some(session_state) = state.users.get_mut(session_key) else {
        state.remote_addr_demux.forget_remote_addr(source_addr);
        if let Ok(mut snapshot) = snapshot_state.lock() {
            // shared demux snapshots must not keep pins the worker already rejected
            snapshot.remote_addr_demux.forget_remote_addr(source_addr);
        }
        return CachedRouteOutcome::NotMatched;
    };
    let Ok(receive) = Receive::new(Protocol::Udp, source_addr, candidate_addr, packet) else {
        log_malformed_datagram(source_addr);
        return CachedRouteOutcome::Malformed;
    };
    let input = Input::Receive(now, receive);
    // cached source-address pins are hints because ICE state can change
    let accepts_input = session_state.rtc.accepts(&input);
    if !accepts_input {
        let _ = session_state;
        debug!(
            source_addr = %source_addr,
            candidate_addr = %candidate_addr,
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id().as_usize(),
            "indexed rtc source address no longer matched the cached user; clearing source-address pin"
        );
        state.remote_addr_demux.forget_remote_addr(source_addr);
        if let Ok(mut snapshot) = snapshot_state.lock() {
            // shared demux snapshots must not keep pins the worker already rejected
            snapshot.remote_addr_demux.forget_remote_addr(source_addr);
        }
        return CachedRouteOutcome::NotMatched;
    }
    let handle_result = session_state.rtc.handle_input(input);
    if unlikely(handle_result.is_err()) {
        // routing answers ownership, not packet validity
        warn!(
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id().as_usize(),
            "failed to feed indexed UDP datagram into rtc user state"
        );
        let _ = session_state;
    } else {
        let dirty_session_key = if session_state.packet_loop_dirty {
            None
        } else {
            Some(session_key.clone())
        };
        let _ = session_state;
        if let Some(dirty_session_key) = dirty_session_key {
            state.mark_session_dirty(&dirty_session_key);
        }
    }
    CachedRouteOutcome::Routed
}

fn indexed_session_for_pkt(
    state: &mut PacketLoopState,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
    input: &Input<'_>,
) -> IndexedSessionRecoveryOutcome {
    let packet_index_probe = match packet_index_probe(source_addr, packet) {
        Ok(packet_index_probe) => packet_index_probe,
        Err(
            IndexedSessionRecoveryOutcome::Malformed
            | IndexedSessionRecoveryOutcome::Matched { .. }
            | IndexedSessionRecoveryOutcome::NoMatch { .. },
        ) => {
            return IndexedSessionRecoveryOutcome::Malformed;
        }
    };
    // the probe only narrows candidates before `Rtc::accepts()` decides ownership
    let candidate_session_keys = match &packet_index_probe {
        PacketIndexProbe::LocalIceUfrag(local_ice_ufrag) => CandidateSessionKeys::Single(
            state
                .remote_addr_demux
                .session_for_local_ufrag(local_ice_ufrag),
        ),
        PacketIndexProbe::RemoteCandidateAddr(remote_candidate_addr) => state
            .remote_addr_demux
            .candidates_for_src_addr(*remote_candidate_addr)
            .map_or(
                CandidateSessionKeys::Single(None),
                |candidate_session_keys| CandidateSessionKeys::Slice(candidate_session_keys.iter()),
            ),
    };
    let mut examined_sessions: usize = 0;
    let mut stale_session_keys = Vec::new();
    let matched_session_key = {
        let mut matched_session_key = None;
        for session_key in candidate_session_keys {
            let Some(session_state) = state.users.get(session_key) else {
                // collect stale demux entries so cleanup runs after the scan
                stale_session_keys.push(session_key.clone());
                continue;
            };
            examined_sessions = examined_sessions.saturating_add(1);
            // `Rtc::accepts()` is the authoritative demux decision
            if session_state.rtc.accepts(input) {
                matched_session_key = Some(session_key.clone());
                break;
            }
        }
        matched_session_key
    };
    if let Some(matched_session_key) = matched_session_key {
        // stale-index cleanup must not depend on whether recovery succeeds
        for stale_session_key in stale_session_keys {
            state
                .remote_addr_demux
                .forget_user_remote_candidates(&stale_session_key);
            state
                .remote_addr_demux
                .forget_user_local_ice_ufrag(&stale_session_key);
        }
        debug!(
            source_addr = %source_addr,
            candidate_addr = %candidate_addr,
            probe = %packet_index_probe,
            user_id = ?matched_session_key.user_id(),
            media_worker_id = matched_session_key.media_worker_id().as_usize(),
            examined_sessions,
            "recovered rtc user routing from packet probe"
        );
        return IndexedSessionRecoveryOutcome::Matched {
            session_key: matched_session_key,
            examined_sessions,
        };
    }
    for stale_session_key in stale_session_keys {
        state
            .remote_addr_demux
            .forget_user_remote_candidates(&stale_session_key);
        state
            .remote_addr_demux
            .forget_user_local_ice_ufrag(&stale_session_key);
    }
    debug!(
        source_addr = %source_addr,
        candidate_addr = %candidate_addr,
        probe = %packet_index_probe,
        examined_sessions,
        "packet probe did not match any rtc user"
    );
    IndexedSessionRecoveryOutcome::NoMatch { examined_sessions }
}

/// probe an unknown-source datagram before consulting recovery indexes
///
/// STUN username recovery is the strongest signal because it names a local ICE
/// fragment. DTLS and RTP can only fall back to source-address recovery
fn packet_index_probe(
    source_addr: SocketAddr,
    packet: &[u8],
) -> Result<PacketIndexProbe<'_>, IndexedSessionRecoveryOutcome> {
    let Some(byte0) = packet.first().copied() else {
        return Err(IndexedSessionRecoveryOutcome::Malformed);
    };
    let packet_len = packet.len();
    // stay within str0m's RFC 5764 style STUN rule, not the wider RFC 7983 range
    if byte0 < 2 && packet_len >= 20 {
        let message = StunMessage::parse(packet)
            .map_err(|_error| IndexedSessionRecoveryOutcome::Malformed)?;
        if let Some(local_ice_ufrag) = message
            .username()
            .and_then(|username| username.split_once(':'))
            .map(|(local_ice_ufrag, _remote_ice_ufrag)| local_ice_ufrag)
        {
            // the demux index is keyed by the local USERNAME fragment
            return Ok(PacketIndexProbe::LocalIceUfrag(local_ice_ufrag));
        }
        // STUN responses may omit USERNAME, so source address is the only hint
        return Ok(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    if (20..64).contains(&byte0) {
        return Ok(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    if (128..192).contains(&byte0) && packet_len > 2 {
        // RTP and RTCP packets depend on a source tuple learned by ICE
        return Ok(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    Err(IndexedSessionRecoveryOutcome::Malformed)
}

fn receive_input(
    now: Instant,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) -> Option<Input<'_>> {
    let receive = Receive::new(Protocol::Udp, source_addr, candidate_addr, packet).ok()?;
    Some(Input::Receive(now, receive))
}

fn log_malformed_datagram(source_addr: SocketAddr) {
    trace!(
        source = %source_addr,
        "ignoring malformed UDP datagram in rtc packet loop"
    );
}

fn record_unknown_src_miss(
    demux: &mut DemuxRecoveryState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    if demux.record_miss(miss_key, route.packet, route.source_addr, route.now) {
        trace!(
            source = %route.source_addr,
            "entering rtc unknown-source recovery cooldown after sustained routing misses"
        );
    }
}

struct PacketRouteContext<'a> {
    snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    metrics: &'a RtcMetricsRecorder,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &'a [u8],
    now: Instant,
}

/// feed a datagram into a matched session and refresh the source-address pin
///
/// ownership must already be proven by `Rtc::accepts()`
/// a feed failure does not disprove ownership of the tuple
fn route_packet_to_session(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
    route: &PacketRouteContext<'_>,
    input: Input<'_>,
    route_resolution: &'static str,
) -> bool {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return false;
    };
    let handle_result = session_state.rtc.handle_input(input);
    let _ = session_state;
    if handle_result.is_err() {
        warn!(
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id().as_usize(),
            "failed to feed incoming UDP datagram into rtc user state"
        );
    } else {
        state.mark_session_dirty(session_key);
    }
    // the cached path revalidates the tuple before every use
    let previous_session_key = state
        .remote_addr_demux
        .session_key_for_remote_addr(route.source_addr)
        .cloned();
    if state
        .remote_addr_demux
        .remember_remote_addr(route.source_addr, session_key)
        && let Ok(mut snapshot) = route.snapshot_state.lock()
    {
        // mirror accepted pins so control-plane snapshots track worker state
        let _ = snapshot
            .remote_addr_demux
            .remember_remote_addr(route.source_addr, session_key);
        match previous_session_key {
            Some(previous_session_key) => {
                debug!(
                    source_addr = %route.source_addr,
                    candidate_addr = %route.candidate_addr,
                    route_resolution,
                    previous_session_id = ?previous_session_key.user_id(),
                    previous_media_worker_id = previous_session_key.media_worker_id().as_usize(),
                    user_id = ?session_key.user_id(),
                    media_worker_id = session_key.media_worker_id().as_usize(),
                    "remapped rtc source address to a different user"
                );
            }
            None => {
                debug!(
                    source_addr = %route.source_addr,
                    candidate_addr = %route.candidate_addr,
                    route_resolution,
                    user_id = ?session_key.user_id(),
                    media_worker_id = session_key.media_worker_id().as_usize(),
                    "pinned rtc source address to user"
                );
            }
        }
    }
    route
        .metrics
        .record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
    true
}

/// route an unknown-source datagram when the worker has only one live session
///
/// the single-session case still calls `Rtc::accepts()` and records misses
/// it avoids only the recovery-index probe
fn route_pkt_by_session(
    state: &mut PacketLoopState,
    demux: &mut DemuxRecoveryState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    #[cfg(test)]
    demux.record_fallback_attempt();
    let Some(session_key) = state.users.keys().next().cloned() else {
        return;
    };
    let Some(input) = receive_input(
        route.now,
        route.source_addr,
        route.candidate_addr,
        route.packet,
    ) else {
        drop_malformed_fallback(route);
        return;
    };
    let accepts_input = state
        .users
        .get(&session_key)
        .is_some_and(|session_state| session_state.rtc.accepts(&input));
    route.metrics.record_rtc_datagram_fallback_scan(1);
    if !accepts_input {
        record_no_user_miss(demux, miss_key, route);
        return;
    }
    if route_packet_to_session(state, &session_key, route, input, "single-user-scan") {
        record_route_success(demux, miss_key, route);
    }
}

/// route an unknown-source datagram through the recovery indexes
///
/// multi-session recovery verifies only indexed candidate sessions with
/// `Rtc::accepts()`
fn route_pkt_by_recovery(
    state: &mut PacketLoopState,
    demux: &mut DemuxRecoveryState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    #[cfg(test)]
    demux.record_fallback_attempt();
    let Some(input) = receive_input(
        route.now,
        route.source_addr,
        route.candidate_addr,
        route.packet,
    ) else {
        drop_malformed_fallback(route);
        return;
    };
    let session_key = match indexed_session_for_pkt(
        state,
        route.source_addr,
        route.candidate_addr,
        route.packet,
        &input,
    ) {
        IndexedSessionRecoveryOutcome::Matched {
            session_key,
            examined_sessions,
        } => {
            route
                .metrics
                .record_rtc_datagram_fallback_scan(examined_sessions);
            session_key
        }
        IndexedSessionRecoveryOutcome::NoMatch { examined_sessions } => {
            route
                .metrics
                .record_rtc_datagram_fallback_scan(examined_sessions);
            record_no_user_miss(demux, miss_key, route);
            return;
        }
        IndexedSessionRecoveryOutcome::Malformed => {
            drop_malformed_fallback(route);
            return;
        }
    };
    if route_packet_to_session(state, &session_key, route, input, "recovery-index") {
        record_route_success(demux, miss_key, route);
    }
}

fn drop_malformed_fallback(route: &PacketRouteContext<'_>) {
    route
        .metrics
        .record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
    log_malformed_datagram(route.source_addr);
}

fn record_no_user_miss(
    demux: &mut DemuxRecoveryState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    route
        .metrics
        .record_rtc_datagram_drop(RtcDatagramDropReason::NoUser);
    record_unknown_src_miss(demux, miss_key, route);
    trace!(
        source = %route.source_addr,
        "dropping UDP datagram because no rtc user accepted it"
    );
}

fn record_route_success(
    demux: &mut DemuxRecoveryState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    demux.record_fallback_route_success(miss_key, route.packet, route.source_addr);
}

#[cfg(test)]
#[path = "TESTS/ingress_routing.rs"]
mod tests;
