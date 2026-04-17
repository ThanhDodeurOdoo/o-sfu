use std::{
    net::SocketAddr,
    slice::Iter,
    sync::{Arc, Mutex},
    time::Instant,
};

use str0m::Input;
use str0m::ice::StunMessage;
use str0m::net::{Protocol, Receive};
use tracing::{debug, trace, warn};

use super::super::{
    routing_miss::{PacketLoopRoutingMissKey, PacketLoopRoutingState},
    state::{RtcBootstrapState, RtcSnapshotState},
};
use crate::runtime::metrics::{RtcDatagramDropReason, RtcDatagramRoutePath, RuntimeMetrics};
use crate::runtime::transport_adapter::TransportSessionKey;

enum CachedRouteOutcome {
    Routed,
    NotMatched,
    Malformed,
}

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

enum PacketIndexProbe {
    LocalIceUfrag(String),
    RemoteCandidateAddr(SocketAddr),
}

impl PacketIndexProbe {
    fn describe(&self) -> String {
        match self {
            Self::LocalIceUfrag(local_ice_ufrag) => {
                format!("local-ice-ufrag:{local_ice_ufrag}")
            }
            Self::RemoteCandidateAddr(remote_candidate_addr) => {
                format!("remote-candidate-addr:{remote_candidate_addr}")
            }
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

// TODO: needs documentation:
pub(super) fn route_packet_to_matching_session(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    routing_state: &mut PacketLoopRoutingState,
    metrics: &RuntimeMetrics,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) {
    let miss_key = PacketLoopRoutingMissKey::new(source_addr, candidate_addr, packet);
    match route_packet_with_cached_session(
        state,
        snapshot_state,
        source_addr,
        candidate_addr,
        packet,
    ) {
        CachedRouteOutcome::Routed => {
            metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
            routing_state.record_route_success(miss_key, packet, source_addr);
            return;
        }
        CachedRouteOutcome::Malformed => {
            metrics.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
            return;
        }
        CachedRouteOutcome::NotMatched => {}
    }
    if routing_state.should_skip_scan(miss_key, packet) {
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::RecentMissCache);
        trace!(
            source = %source_addr,
            "dropping UDP datagram because a recent cache miss already proved no rtc session accepted it"
        );
        return;
    }
    let now = Instant::now();
    if routing_state.should_rate_limit_source(source_addr, now) {
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::SourceRateLimited);
        trace!(
            source = %source_addr,
            "dropping UDP datagram because sustained unknown-source misses exhausted the rtc recovery budget for this source"
        );
        return;
    }
    let route = PacketRouteContext {
        snapshot_state,
        metrics,
        source_addr,
        candidate_addr,
        packet,
        now,
    };
    if state.sessions.len() == 1 {
        route_packet_by_single_session(state, routing_state, miss_key, &route);
        return;
    }
    route_packet_by_recovery_index(state, routing_state, miss_key, &route);
}

fn route_packet_with_cached_session(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) -> CachedRouteOutcome {
    let Some(session_key) = state
        .remote_addr_demux
        .session_key_for_remote_addr(source_addr)
        .cloned()
    else {
        return CachedRouteOutcome::NotMatched;
    };
    let Some(session_state) = state.sessions.get_mut(&session_key) else {
        state.remote_addr_demux.forget_remote_addr(source_addr);
        if let Ok(mut snapshot) = snapshot_state.lock() {
            snapshot.remote_addr_demux.forget_remote_addr(source_addr);
        }
        return CachedRouteOutcome::NotMatched;
    };
    let Ok(receive) = Receive::new(Protocol::Udp, source_addr, candidate_addr, packet) else {
        log_malformed_datagram(source_addr);
        return CachedRouteOutcome::Malformed;
    };
    let now = Instant::now();
    let input = Input::Receive(now, receive);
    let accepts_input = session_state.rtc.accepts(&input);
    if !accepts_input {
        let _ = session_state;
        debug!(
            source_addr = %source_addr,
            candidate_addr = %candidate_addr,
            session_id = ?session_key.session_id(),
            media_worker_id = session_key.media_worker_id(),
            "indexed rtc source address no longer matched the cached session; clearing source-address pin"
        );
        state.remote_addr_demux.forget_remote_addr(source_addr);
        if let Ok(mut snapshot) = snapshot_state.lock() {
            snapshot.remote_addr_demux.forget_remote_addr(source_addr);
        }
        return CachedRouteOutcome::NotMatched;
    }
    let handle_result = session_state.rtc.handle_input(input);
    let _ = session_state;
    if handle_result.is_err() {
        warn!(
            session_id = ?session_key.session_id(),
            media_worker_id = session_key.media_worker_id(),
            "failed to feed indexed UDP datagram into rtc session state"
        );
    } else {
        state.mark_session_dirty(&session_key);
    }
    if state
        .remote_addr_demux
        .remember_remote_addr(source_addr, &session_key)
        && let Ok(mut snapshot) = snapshot_state.lock()
    {
        snapshot
            .remote_addr_demux
            .remember_remote_addr(source_addr, &session_key);
    }
    CachedRouteOutcome::Routed
}

fn matching_indexed_session_key_for_packet(
    state: &mut RtcBootstrapState,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
    now: Instant,
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
    let packet_index_probe_description = packet_index_probe.describe();
    let candidate_session_keys = match packet_index_probe {
        PacketIndexProbe::LocalIceUfrag(local_ice_ufrag) => CandidateSessionKeys::Single(
            state
                .remote_addr_demux
                .session_key_for_local_ice_ufrag(&local_ice_ufrag),
        ),
        PacketIndexProbe::RemoteCandidateAddr(remote_candidate_addr) => state
            .remote_addr_demux
            .candidate_sessions_for_source_addr(remote_candidate_addr)
            .map_or(
                CandidateSessionKeys::Single(None),
                |candidate_session_keys| CandidateSessionKeys::Slice(candidate_session_keys.iter()),
            ),
    };
    let Some(input) = receive_input(now, source_addr, candidate_addr, packet) else {
        return IndexedSessionRecoveryOutcome::Malformed;
    };
    let mut examined_sessions: usize = 0;
    let mut stale_session_keys = Vec::new();
    let matched_session_key = {
        let mut matched_session_key = None;
        for session_key in candidate_session_keys {
            let Some(session_state) = state.sessions.get(session_key) else {
                stale_session_keys.push(session_key.clone());
                continue;
            };
            examined_sessions = examined_sessions.saturating_add(1);
            if session_state.rtc.accepts(&input) {
                matched_session_key = Some(session_key.clone());
                break;
            }
        }
        matched_session_key
    };
    if let Some(matched_session_key) = matched_session_key {
        for stale_session_key in stale_session_keys {
            state
                .remote_addr_demux
                .forget_session_remote_candidate_addrs(&stale_session_key);
            state
                .remote_addr_demux
                .forget_session_local_ice_ufrag(&stale_session_key);
        }
        debug!(
            source_addr = %source_addr,
            candidate_addr = %candidate_addr,
            probe = %packet_index_probe_description,
            session_id = ?matched_session_key.session_id(),
            media_worker_id = matched_session_key.media_worker_id(),
            examined_sessions,
            "recovered rtc session routing from packet probe"
        );
        return IndexedSessionRecoveryOutcome::Matched {
            session_key: matched_session_key,
            examined_sessions,
        };
    }
    for stale_session_key in stale_session_keys {
        state
            .remote_addr_demux
            .forget_session_remote_candidate_addrs(&stale_session_key);
        state
            .remote_addr_demux
            .forget_session_local_ice_ufrag(&stale_session_key);
    }
    debug!(
        source_addr = %source_addr,
        candidate_addr = %candidate_addr,
        probe = %packet_index_probe_description,
        examined_sessions,
        "packet probe did not match any rtc session"
    );
    IndexedSessionRecoveryOutcome::NoMatch { examined_sessions }
}

fn packet_index_probe(
    source_addr: SocketAddr,
    packet: &[u8],
) -> Result<PacketIndexProbe, IndexedSessionRecoveryOutcome> {
    let Some(byte0) = packet.first().copied() else {
        return Err(IndexedSessionRecoveryOutcome::Malformed);
    };
    let packet_len = packet.len();
    if byte0 < 2 && packet_len >= 20 {
        let message = StunMessage::parse(packet)
            .map_err(|_error| IndexedSessionRecoveryOutcome::Malformed)?;
        if let Some((local_ice_ufrag, _remote_ice_ufrag)) = message.split_username() {
            return Ok(PacketIndexProbe::LocalIceUfrag(local_ice_ufrag.to_owned()));
        }
        return Ok(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    if (20..64).contains(&byte0) {
        return Ok(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    if (128..192).contains(&byte0) && packet_len > 2 {
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

struct PacketRouteContext<'a> {
    snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    metrics: &'a RuntimeMetrics,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &'a [u8],
    now: Instant,
}

fn route_packet_to_session(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    route: &PacketRouteContext<'_>,
    route_resolution: &'static str,
) -> bool {
    let Some(session_state) = state.sessions.get_mut(session_key) else {
        return false;
    };
    let Some(input) = receive_input(
        route.now,
        route.source_addr,
        route.candidate_addr,
        route.packet,
    ) else {
        log_malformed_datagram(route.source_addr);
        return false;
    };
    let handle_result = session_state.rtc.handle_input(input);
    let _ = session_state;
    if handle_result.is_err() {
        warn!(
            session_id = ?session_key.session_id(),
            media_worker_id = session_key.media_worker_id(),
            "failed to feed incoming UDP datagram into rtc session state"
        );
    } else {
        state.mark_session_dirty(session_key);
    }
    let previous_session_key = state
        .remote_addr_demux
        .session_key_for_remote_addr(route.source_addr)
        .cloned();
    if state
        .remote_addr_demux
        .remember_remote_addr(route.source_addr, session_key)
        && let Ok(mut snapshot) = route.snapshot_state.lock()
    {
        snapshot
            .remote_addr_demux
            .remember_remote_addr(route.source_addr, session_key);
        match previous_session_key {
            Some(previous_session_key) => {
                debug!(
                    source_addr = %route.source_addr,
                    candidate_addr = %route.candidate_addr,
                    route_resolution,
                    previous_session_id = ?previous_session_key.session_id(),
                    previous_media_worker_id = previous_session_key.media_worker_id(),
                    session_id = ?session_key.session_id(),
                    media_worker_id = session_key.media_worker_id(),
                    "remapped rtc source address to a different session"
                );
            }
            None => {
                debug!(
                    source_addr = %route.source_addr,
                    candidate_addr = %route.candidate_addr,
                    route_resolution,
                    session_id = ?session_key.session_id(),
                    media_worker_id = session_key.media_worker_id(),
                    "pinned rtc source address to session"
                );
            }
        }
    }
    route
        .metrics
        .record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
    true
}

fn route_packet_by_single_session(
    state: &mut RtcBootstrapState,
    routing_state: &mut PacketLoopRoutingState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    #[cfg(test)]
    routing_state.record_fallback_attempt();
    let Some(session_key) = state.sessions.keys().next().cloned() else {
        return;
    };
    let Some(input) = receive_input(
        route.now,
        route.source_addr,
        route.candidate_addr,
        route.packet,
    ) else {
        route
            .metrics
            .record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
        log_malformed_datagram(route.source_addr);
        return;
    };
    let accepts_input = state
        .sessions
        .get(&session_key)
        .is_some_and(|session_state| session_state.rtc.accepts(&input));
    route.metrics.record_rtc_datagram_fallback_scan(1);
    if !accepts_input {
        route
            .metrics
            .record_rtc_datagram_drop(RtcDatagramDropReason::NoSession);
        routing_state.record_miss(miss_key, route.packet, route.source_addr, route.now);
        trace!(
            source = %route.source_addr,
            "dropping UDP datagram because no rtc session accepted it"
        );
        return;
    }
    if route_packet_to_session(state, &session_key, route, "single-session-scan") {
        routing_state.record_route_success(miss_key, route.packet, route.source_addr);
    }
}

fn route_packet_by_recovery_index(
    state: &mut RtcBootstrapState,
    routing_state: &mut PacketLoopRoutingState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    #[cfg(test)]
    routing_state.record_fallback_attempt();
    let session_key = match matching_indexed_session_key_for_packet(
        state,
        route.source_addr,
        route.candidate_addr,
        route.packet,
        route.now,
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
            route
                .metrics
                .record_rtc_datagram_drop(RtcDatagramDropReason::NoSession);
            routing_state.record_miss(miss_key, route.packet, route.source_addr, route.now);
            trace!(
                source = %route.source_addr,
                "dropping UDP datagram because no rtc session accepted it"
            );
            return;
        }
        IndexedSessionRecoveryOutcome::Malformed => {
            route
                .metrics
                .record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
            log_malformed_datagram(route.source_addr);
            return;
        }
    };
    if route_packet_to_session(state, &session_key, route, "recovery-index") {
        routing_state.record_route_success(miss_key, route.packet, route.source_addr);
    }
}
