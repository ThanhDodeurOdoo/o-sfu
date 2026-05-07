//! UDP ingress routing for RTC sessions.
//!
//! The shared UDP socket receives datagrams for every session on a shard. This
//! module decides which `str0m::Rtc` should see one datagram. Its indexes and
//! caches are performance hints only. `Rtc::accepts()` remains the authoritative
//! ownership check before any packet is trusted.
//!
//! # Routing strategy
//!
//! 1. Use the pinned source-address index when a tuple is already known.
//! 2. Drop repeated identical misses through the recent-miss cache.
//! 3. Rate-limit sustained unknown-source probes.
//! 4. Probe STUN, DTLS or RTP shape to choose a narrow candidate set.
//! 5. Call `Rtc::accepts()` before calling `Rtc::handle_input()`.
//! 6. Pin successful source tuples so later packets take the indexed path.
//!
//! The recovery path must stay a subset of `str0m` demux behavior. A packet may
//! fail to recover and be dropped, but this module must not recover traffic that
//! `str0m` would reject downstream.

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
    routing_miss::{PacketLoopRoutingMissKey, PacketLoopRoutingState},
    state::{RtcBootstrapState, RtcSnapshotState},
};
use crate::runtime::{
    media_transport::TransportSessionKey,
    metrics::{RtcDatagramDropReason, RtcDatagramRoutePath, RuntimeMetrics},
};

enum CachedRouteOutcome {
    Routed,
    NotMatched,
    Malformed,
}

/// Outcome of probing the demux indexes for an unknown source tuple.
///
/// `Matched` means one candidate passed `Rtc::accepts()`. `NoMatch` is a
/// bounded miss that may be cached. `Malformed` means the packet could not be
/// represented as `str0m` receive input or was outside the supported demux
/// ranges.
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

/// Recovery probe used to choose a bounded candidate session set.
///
/// This classification must stay aligned with `str0m`'s own UDP multiplexing so
/// the packet loop never "recovers" traffic that the authoritative
/// `Rtc::accepts()` / `Rtc::handle_input()` path would later reject.
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

/// Route an incoming UDP datagram to its owning RTC session.
///
/// # Error handling
///
/// Routing failure is not a transport error. Malformed packets, unknown
/// sessions, repeated misses and rate-limited sources are dropped with metrics. A
/// `Rtc::handle_input()` error after a successful ownership decision is logged,
/// but the route can still be considered learned because ownership and packet
/// validity are separate concerns.
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
    // The recent-miss cache proves that no session accepted an identical packet
    // from this source recently. It must be cleared on topology or ICE changes
    // or a stale negative result could hide a newly valid route.
    if routing_state.should_skip_scan(miss_key, packet) {
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::RecentMissCache);
        trace!(
            source = %source_addr,
            "dropping UDP datagram because a recent cache miss already proved no rtc user accepted it"
        );
        return;
    }
    let now = Instant::now();
    // This is purely a defensive mechanism against unknown-source traffic.
    // It is not part of ICE or RTP correctness.
    // Legitimate traffic should have been learned via STUN before hitting this path.
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
    // With one live session, routing degenerates to one `accepts()` check.
    // No recovery index is needed to narrow the candidate set.
    if state.users.len() == 1 {
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
    let Some(session_state) = state.users.get_mut(&session_key) else {
        state.remote_addr_demux.forget_remote_addr(source_addr);
        if let Ok(mut snapshot) = snapshot_state.lock() {
            // Stale pins must be cleared in the shared snapshot so that any
            // control-plane observation stays consistent with the worker's
            // routing discovery.
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
    // Cached source-address pins are not authoritative. ICE nomination,
    // credentials or remote candidates may have changed, so every packet is
    // revalidated against `Rtc::accepts()` before trusting the pin.
    let accepts_input = session_state.rtc.accepts(&input);
    if !accepts_input {
        let _ = session_state;
        debug!(
            source_addr = %source_addr,
            candidate_addr = %candidate_addr,
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id(),
            "indexed rtc source address no longer matched the cached user; clearing source-address pin"
        );
        state.remote_addr_demux.forget_remote_addr(source_addr);
        if let Ok(mut snapshot) = snapshot_state.lock() {
            // Stale pins must be cleared in the shared snapshot so that any
            // control-plane observation stays consistent with the worker's
            // routing discovery.
            snapshot.remote_addr_demux.forget_remote_addr(source_addr);
        }
        return CachedRouteOutcome::NotMatched;
    }
    let handle_result = session_state.rtc.handle_input(input);
    let _ = session_state;
    if handle_result.is_err() {
        // NOTE: We still consider the packet "routed" even if `handle_input` fails.
        // Routing answers "which user owns this packet", not "was the packet valid".
        //
        // This ensures we still update source-address pinning and avoid re-scanning.
        warn!(
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id(),
            "failed to feed indexed UDP datagram into rtc user state"
        );
    } else {
        state.mark_session_dirty(&session_key);
    }
    if state
        .remote_addr_demux
        .remember_remote_addr(source_addr, &session_key)
        && let Ok(mut snapshot) = snapshot_state.lock()
    {
        let _ = snapshot
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
    // The probe only narrows the candidate session set.
    // It is not authoritative: final ownership is decided by `Rtc::accepts()`.
    let candidate_session_keys = match &packet_index_probe {
        PacketIndexProbe::LocalIceUfrag(local_ice_ufrag) => CandidateSessionKeys::Single(
            state
                .remote_addr_demux
                .session_key_for_local_ice_ufrag(local_ice_ufrag),
        ),
        PacketIndexProbe::RemoteCandidateAddr(remote_candidate_addr) => state
            .remote_addr_demux
            .candidate_sessions_for_source_addr(*remote_candidate_addr)
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
            let Some(session_state) = state.users.get(session_key) else {
                // The demux index may contain stale entries after user teardown.
                // We track and clean them here to keep the index consistent.
                stale_session_keys.push(session_key.clone());
                continue;
            };
            examined_sessions = examined_sessions.saturating_add(1);
            // `Rtc::accepts()` is the authoritative demux decision.
            // It accounts for ICE nomination, credentials, and candidate sets.
            // The probe/index only reduces the number of sessions we test here.
            if session_state.rtc.accepts(&input) {
                matched_session_key = Some(session_key.clone());
                break;
            }
        }
        matched_session_key
    };
    if let Some(matched_session_key) = matched_session_key {
        // Cleanup must happen even on failed probes to prevent index drift.
        for stale_session_key in stale_session_keys {
            state
                .remote_addr_demux
                .forget_user_remote_candidate_addrs(&stale_session_key);
            state
                .remote_addr_demux
                .forget_user_local_ice_ufrag(&stale_session_key);
        }
        debug!(
            source_addr = %source_addr,
            candidate_addr = %candidate_addr,
            probe = %packet_index_probe,
            user_id = ?matched_session_key.user_id(),
            media_worker_id = matched_session_key.media_worker_id(),
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
            .forget_user_remote_candidate_addrs(&stale_session_key);
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

/// Probe an unknown-source datagram before consulting recovery indexes.
///
/// The result only chooses which index to try. STUN username recovery is the
/// strongest signal because it names a local ICE fragment. DTLS and RTP lack
/// such identity here, so they can only fall back to the candidate source
/// address index.
fn packet_index_probe(
    source_addr: SocketAddr,
    packet: &[u8],
) -> Result<PacketIndexProbe<'_>, IndexedSessionRecoveryOutcome> {
    let Some(byte0) = packet.first().copied() else {
        return Err(IndexedSessionRecoveryOutcome::Malformed);
    };
    let packet_len = packet.len();
    // This intentionally matches str0m's internal demux behavior, not the full
    // RFC 7983 range. str0m still uses the older RFC 5764 byte0 < 2 STUN rule,
    // so this recovery probe must remain a subset of that behavior.
    if byte0 < 2 && packet_len >= 20 {
        let message = StunMessage::parse(packet)
            .map_err(|_error| IndexedSessionRecoveryOutcome::Malformed)?;
        if let Some(local_ice_ufrag) = message
            .username()
            .and_then(|username| username.split_once(':'))
            .map(|(local_ice_ufrag, _remote_ice_ufrag)| local_ice_ufrag)
        {
            // The demux index is keyed by the first USERNAME fragment, matching
            // the engine's existing ICE ufrag registration contract.
            return Ok(PacketIndexProbe::LocalIceUfrag(local_ice_ufrag));
        }
        // STUN responses may not carry USERNAME, so we fall back to source-address recovery
        return Ok(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    if (20..64).contains(&byte0) {
        // DTLS packets are identified by first-byte range
        // We cannot extract routing information, so we fall back to address-based recovery.
        return Ok(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    if (128..192).contains(&byte0) && packet_len > 2 {
        // RTP/RTCP packets also lack routing identifiers here.
        // ICE must have already established the correct source tuple.
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

/// Feed a datagram into a matched session and refresh the source-address pin.
///
/// This function assumes ownership was already proven by `Rtc::accepts()`.
/// Feeding can still fail if the packet is invalid for the current transport
/// state, but a failure does not by itself disprove ownership of the tuple.
fn route_packet_to_session(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    route: &PacketRouteContext<'_>,
    route_resolution: &'static str,
) -> bool {
    let Some(session_state) = state.users.get_mut(session_key) else {
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
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id(),
            "failed to feed incoming UDP datagram into rtc user state"
        );
    } else {
        state.mark_session_dirty(session_key);
    }
    // ICE normally keeps subsequent media on the same tuple. If the tuple later
    // stops matching this session, the cached path revalidates and drops the pin.
    let previous_session_key = state
        .remote_addr_demux
        .session_key_for_remote_addr(route.source_addr)
        .cloned();
    if state
        .remote_addr_demux
        .remember_remote_addr(route.source_addr, session_key)
        && let Ok(mut snapshot) = route.snapshot_state.lock()
    {
        // The worker is the source of truth for address pins. New pins must
        // be mirrored in the snapshot state so they are visible to the rest
        // of the application.
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
                    previous_media_worker_id = previous_session_key.media_worker_id(),
                    user_id = ?session_key.user_id(),
                    media_worker_id = session_key.media_worker_id(),
                    "remapped rtc source address to a different user"
                );
            }
            None => {
                debug!(
                    source_addr = %route.source_addr,
                    candidate_addr = %route.candidate_addr,
                    route_resolution,
                    user_id = ?session_key.user_id(),
                    media_worker_id = session_key.media_worker_id(),
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

/// Route an unknown-source datagram when the shard has only one live session.
///
/// The single-session case still calls `Rtc::accepts()` and records misses.
/// It only avoids the recovery-index probe because there is no candidate set to
/// narrow.
fn route_packet_by_single_session(
    state: &mut RtcBootstrapState,
    routing_state: &mut PacketLoopRoutingState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    #[cfg(test)]
    routing_state.record_fallback_attempt();
    let Some(session_key) = state.users.keys().next().cloned() else {
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
        .users
        .get(&session_key)
        .is_some_and(|session_state| session_state.rtc.accepts(&input));
    route.metrics.record_rtc_datagram_fallback_scan(1);
    if !accepts_input {
        route
            .metrics
            .record_rtc_datagram_drop(RtcDatagramDropReason::NoUser);
        routing_state.record_miss(miss_key, route.packet, route.source_addr, route.now);
        trace!(
            source = %route.source_addr,
            "dropping UDP datagram because no rtc user accepted it"
        );
        return;
    }
    if route_packet_to_session(state, &session_key, route, "single-user-scan") {
        routing_state.record_route_success(miss_key, route.packet, route.source_addr);
    }
}

/// Route an unknown-source datagram through the recovery indexes.
///
/// Multi-session recovery never scans every session. It probes packet shape,
/// consults the corresponding demux index and verifies only the resulting
/// candidate sessions with `Rtc::accepts()`.
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
                .record_rtc_datagram_drop(RtcDatagramDropReason::NoUser);
            routing_state.record_miss(miss_key, route.packet, route.source_addr, route.now);
            trace!(
                source = %route.source_addr,
                "dropping UDP datagram because no rtc user accepted it"
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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use str0m::ice::{StunMessage, TransId};

    use super::{PacketIndexProbe, packet_index_probe};

    const STUN_TEST_PASSWORD: &[u8] = b"probe-password";

    fn serialize_stun_message(
        message: &StunMessage<'_>,
        password: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
        let mut buffer = [0_u8; 1024];
        let len = message
            .to_bytes(password, &mut buffer, |_key, _payloads| [0_u8; 20])
            .ok()?;
        buffer.get(..len).map(<[u8]>::to_vec)
    }

    #[test]
    fn packet_index_probe_extracts_the_local_ice_ufrag_from_binding_requests() {
        let packet = serialize_stun_message(
            &StunMessage::binding_request(
                "local-ufrag:remote-ufrag",
                TransId::new(),
                true,
                1,
                1,
                false,
            ),
            Some(STUN_TEST_PASSWORD),
        );

        assert!(matches!(
            packet
                .as_deref()
                .map(|packet| packet_index_probe(test_source_addr(), packet)),
            Some(Ok(PacketIndexProbe::LocalIceUfrag(local_ice_ufrag)))
                if local_ice_ufrag == "local-ufrag"
        ));
    }

    #[test]
    fn packet_index_probe_uses_the_source_addr_when_stun_has_no_username() {
        let source_addr = test_source_addr();
        let packet = serialize_stun_message(
            &StunMessage::binding_reply(TransId::new(), source_addr),
            Some(STUN_TEST_PASSWORD),
        );

        assert!(matches!(
            packet
                .as_deref()
                .map(|packet| packet_index_probe(source_addr, packet)),
            Some(Ok(PacketIndexProbe::RemoteCandidateAddr(probed_source_addr)))
                if probed_source_addr == source_addr
        ));
    }

    fn test_source_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_321)
    }
}
