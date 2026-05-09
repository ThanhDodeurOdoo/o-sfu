//! UDP ingress routing for RTC sessions.
//!
//! The shared UDP socket receives datagrams for every session on a shard. This
//! module decides which host session should see one datagram. Its indexes and
//! caches are performance hints only. The host session adapter remains the
//! authoritative `str0m` ownership check before any packet is trusted.
//!
//! # Routing strategy
//!
//! 1. Use the pinned source-address index when a tuple is already known.
//! 2. Drop repeated identical misses through the recent-miss cache.
//! 3. Rate-limit sustained unknown-source probes.
//! 4. Probe STUN, DTLS or RTP shape to choose a narrow candidate set.
//! 5. Ask the host session adapter to validate ownership before input handling.
//! 6. Pin successful source tuples so later packets take the indexed path.
//!
//! The recovery path must stay a subset of `str0m` demux behavior. A packet may
//! fail to recover and be dropped, but this module must not recover traffic that
//! `str0m` would reject downstream.

use std::{collections::BTreeMap, net::SocketAddr, slice::Iter, time::Instant};

use str0m::ice::StunMessage;
use tracing::{debug, trace, warn};

use super::{
    super::{
        routing_miss::{PacketLoopRoutingMissKey, PacketLoopRoutingState},
        session_adapter::{HostDatagramAccept, HostDatagramHandle, HostDatagramInput},
        state::{RtcBootstrapState, RtcSessionState},
    },
    machine::{
        effect::{PacketLoopEffect, PacketLoopEffects, PacketLoopMetricEffect},
        state::PacketLoopState,
    },
    time::PacketLoopTime,
};
use crate::runtime::{
    media_transport::TransportSessionKey,
    metrics::{RtcDatagramDropReason, RtcDatagramRoutePath},
};

enum DatagramDemuxPlan {
    Cached {
        session_key: TransportSessionKey,
    },
    Fallback {
        miss_key: PacketLoopRoutingMissKey,
        candidates: CandidateSource,
        route_resolution: &'static str,
    },
    Drop(RtcDatagramDropReason),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CachedRouteResult {
    Finished,
    RetryFallback,
}

struct DatagramDemuxSessionSnapshot {
    single_session_key: Option<TransportSessionKey>,
}

struct CandidateValidation {
    result: CandidateValidationResult,
    examined_sessions: usize,
}

enum CandidateValidationResult {
    Matched(TransportSessionKey),
    NoMatch,
    Malformed,
}

/// Recovery probe used to choose a bounded candidate session set.
///
/// This classification must stay aligned with `str0m`'s own UDP multiplexing so
/// the packet loop never "recovers" traffic that the authoritative
/// `str0m` validation and input handling would later reject.
enum PacketIndexProbe<'a> {
    LocalIceUfrag(&'a str),
    RemoteCandidateAddr(SocketAddr),
}

enum CandidateSource {
    Single(Option<TransportSessionKey>),
    RemoteCandidateAddr(SocketAddr),
}

enum CandidateSessionKeys<'a> {
    Single(Option<TransportSessionKey>),
    Slice(Iter<'a, TransportSessionKey>),
}

impl Iterator for CandidateSessionKeys<'_> {
    type Item = TransportSessionKey;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(session_key) => session_key.take(),
            Self::Slice(iter) => iter.next().cloned(),
        }
    }
}

impl DatagramDemuxSessionSnapshot {
    fn from_host_state(state: &RtcBootstrapState) -> Self {
        let single_session_key = if state.users.len() == 1 {
            state.users.keys().next().cloned()
        } else {
            None
        };
        Self { single_session_key }
    }
}

#[derive(Clone, Copy)]
pub(super) struct DatagramRouteInput<'a> {
    pub(super) source_addr: SocketAddr,
    pub(super) candidate_addr: SocketAddr,
    pub(super) packet: &'a [u8],
    pub(super) received_at: Instant,
    pub(super) packet_time: PacketLoopTime,
}

/// Route an incoming UDP datagram to its owning RTC session.
///
/// # Error handling
///
/// Routing failure is not a transport error. Malformed packets, unknown
/// sessions, repeated misses and rate-limited sources are dropped with metrics. A
/// A host input error after a successful ownership decision is logged,
/// but the route can still be considered learned because ownership and packet
/// validity are separate concerns.
pub(super) fn route_packet_to_matching_session(
    state: &mut RtcBootstrapState,
    routing_state: &mut PacketLoopRoutingState,
    effects: &mut PacketLoopEffects,
    input: DatagramRouteInput<'_>,
) {
    let DatagramRouteInput {
        source_addr,
        candidate_addr,
        packet,
        received_at,
        packet_time,
    } = input;
    let session_snapshot = DatagramDemuxSessionSnapshot::from_host_state(state);
    let plan = plan_datagram_demux(
        &state.packet_loop,
        routing_state,
        &session_snapshot,
        source_addr,
        candidate_addr,
        packet,
        packet_time,
    );
    let mut route = PacketRouteContext {
        effects,
        source_addr,
        candidate_addr,
        packet,
        received_at,
        packet_time,
    };
    match plan {
        DatagramDemuxPlan::Cached { session_key } => {
            if route_packet_with_cached_session(state, &session_key, &mut route)
                == CachedRouteResult::RetryFallback
            {
                route_packet_by_current_fallback_plan(
                    state,
                    routing_state,
                    &session_snapshot,
                    &mut route,
                );
            }
        }
        DatagramDemuxPlan::Fallback {
            miss_key,
            candidates,
            route_resolution,
        } => route_packet_by_fallback_plan(
            state,
            routing_state,
            miss_key,
            candidates,
            &mut route,
            route_resolution,
        ),
        DatagramDemuxPlan::Drop(drop) => record_planned_demux_drop(&mut route, drop),
    }
}

fn plan_datagram_demux(
    state: &PacketLoopState,
    routing_state: &mut PacketLoopRoutingState,
    session_snapshot: &DatagramDemuxSessionSnapshot,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
    packet_time: PacketLoopTime,
) -> DatagramDemuxPlan {
    if let Some(session_key) = state
        .remote_addr_demux
        .session_key_for_remote_addr(source_addr)
        .cloned()
    {
        return DatagramDemuxPlan::Cached { session_key };
    }
    let miss_key = PacketLoopRoutingMissKey::new(source_addr, candidate_addr, packet);
    if routing_state.should_skip_scan(miss_key, packet) {
        return DatagramDemuxPlan::Drop(RtcDatagramDropReason::RecentMissCache);
    }
    if routing_state.should_rate_limit_source(source_addr, packet_time) {
        return DatagramDemuxPlan::Drop(RtcDatagramDropReason::SourceRateLimited);
    }
    if session_snapshot.single_session_key.is_some() {
        return DatagramDemuxPlan::Fallback {
            miss_key,
            candidates: CandidateSource::Single(session_snapshot.single_session_key.clone()),
            route_resolution: "single-user-scan",
        };
    }
    let Some(packet_index_probe) = packet_index_probe(source_addr, packet) else {
        return DatagramDemuxPlan::Drop(RtcDatagramDropReason::Malformed);
    };
    let candidates = match &packet_index_probe {
        PacketIndexProbe::LocalIceUfrag(local_ice_ufrag) => CandidateSource::Single(
            state
                .remote_addr_demux
                .session_key_for_local_ice_ufrag(local_ice_ufrag)
                .cloned(),
        ),
        PacketIndexProbe::RemoteCandidateAddr(remote_candidate_addr) => {
            CandidateSource::RemoteCandidateAddr(*remote_candidate_addr)
        }
    };
    DatagramDemuxPlan::Fallback {
        miss_key,
        candidates,
        route_resolution: "recovery-index",
    }
}

fn route_packet_with_cached_session(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    route: &mut PacketRouteContext<'_>,
) -> CachedRouteResult {
    let Some(session_state) = state.users.get_mut(session_key) else {
        forget_cached_source_pin(&mut state.packet_loop, route.effects, route.source_addr);
        return CachedRouteResult::RetryFallback;
    };
    match session_state
        .host_session
        .accepts_datagram(route.host_datagram_input())
    {
        HostDatagramAccept::Accepted => {}
        HostDatagramAccept::Rejected => {
            let _ = session_state;
            debug!(
                source_addr = %route.source_addr,
                candidate_addr = %route.candidate_addr,
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id(),
                "indexed rtc source address no longer matched the cached user; clearing source-address pin"
            );
            forget_cached_source_pin(&mut state.packet_loop, route.effects, route.source_addr);
            return CachedRouteResult::RetryFallback;
        }
        HostDatagramAccept::Malformed => {
            record_malformed_drop(route);
            return CachedRouteResult::Finished;
        }
    }
    let _ = session_state;
    if feed_datagram_to_session(state, session_key, route) == HostFeedOutcome::Malformed {
        record_malformed_drop(route);
        return CachedRouteResult::Finished;
    }
    remember_routed_source_pin(&mut state.packet_loop, route, session_key, None);
    route
        .effects
        .record_metric(PacketLoopMetricEffect::RtcDatagramRoute(
            RtcDatagramRoutePath::Indexed,
        ));
    CachedRouteResult::Finished
}

fn prepare_fallback_candidates<'a>(
    state: &'a mut PacketLoopState,
    users: &BTreeMap<TransportSessionKey, RtcSessionState>,
    candidates: CandidateSource,
) -> CandidateSessionKeys<'a> {
    match candidates {
        CandidateSource::Single(Some(session_key)) if users.contains_key(&session_key) => {
            CandidateSessionKeys::Single(Some(session_key))
        }
        CandidateSource::Single(Some(stale_session_key)) => {
            forget_stale_candidate_indexes(state, &stale_session_key);
            CandidateSessionKeys::Single(None)
        }
        CandidateSource::Single(None) => CandidateSessionKeys::Single(None),
        CandidateSource::RemoteCandidateAddr(source_addr) => {
            let demux = &mut state.remote_addr_demux;
            demux.retain_candidate_sessions_for_source_addr(source_addr, |session_key| {
                users.contains_key(session_key)
            });
            demux
                .candidate_sessions_for_source_addr(source_addr)
                .map_or(CandidateSessionKeys::Single(None), |session_keys| {
                    CandidateSessionKeys::Slice(session_keys.iter())
                })
        }
    }
}

fn validate_fallback_candidates(
    users: &BTreeMap<TransportSessionKey, RtcSessionState>,
    route: &PacketRouteContext<'_>,
    candidates: CandidateSessionKeys<'_>,
) -> CandidateValidation {
    let mut examined_sessions: usize = 0;
    for session_key in candidates {
        let Some(session_state) = users.get(&session_key) else {
            continue;
        };
        examined_sessions = examined_sessions.saturating_add(1);
        match session_state
            .host_session
            .accepts_datagram(route.host_datagram_input())
        {
            HostDatagramAccept::Accepted => {
                return CandidateValidation {
                    result: CandidateValidationResult::Matched(session_key),
                    examined_sessions,
                };
            }
            HostDatagramAccept::Rejected => {}
            HostDatagramAccept::Malformed => {
                return CandidateValidation {
                    result: CandidateValidationResult::Malformed,
                    examined_sessions,
                };
            }
        }
    }
    CandidateValidation {
        result: CandidateValidationResult::NoMatch,
        examined_sessions,
    }
}

fn route_packet_by_current_fallback_plan(
    state: &mut RtcBootstrapState,
    routing_state: &mut PacketLoopRoutingState,
    session_snapshot: &DatagramDemuxSessionSnapshot,
    route: &mut PacketRouteContext<'_>,
) {
    let fallback_plan = plan_datagram_demux(
        &state.packet_loop,
        routing_state,
        session_snapshot,
        route.source_addr,
        route.candidate_addr,
        route.packet,
        route.packet_time,
    );
    match fallback_plan {
        DatagramDemuxPlan::Fallback {
            miss_key,
            candidates,
            route_resolution,
        } => route_packet_by_fallback_plan(
            state,
            routing_state,
            miss_key,
            candidates,
            route,
            route_resolution,
        ),
        DatagramDemuxPlan::Drop(drop) => record_planned_demux_drop(route, drop),
        DatagramDemuxPlan::Cached { .. } => {}
    }
}

/// Probe an unknown-source datagram before consulting recovery indexes.
///
/// The result only chooses which index to try. STUN username recovery is the
/// strongest signal because it names a local ICE fragment. DTLS and RTP lack
/// such identity here, so they can only fall back to the candidate source
/// address index.
fn packet_index_probe(source_addr: SocketAddr, packet: &[u8]) -> Option<PacketIndexProbe<'_>> {
    let byte0 = packet.first().copied()?;
    let packet_len = packet.len();
    // This intentionally matches str0m's internal demux behavior, not the full
    // RFC 7983 range. str0m still uses the older RFC 5764 byte0 < 2 STUN rule,
    // so this recovery probe must remain a subset of that behavior.
    if byte0 < 2 && packet_len >= 20 {
        let message = StunMessage::parse(packet).ok()?;
        if let Some(local_ice_ufrag) = message
            .username()
            .and_then(|username| username.split_once(':'))
            .map(|(local_ice_ufrag, _remote_ice_ufrag)| local_ice_ufrag)
        {
            // The demux index is keyed by the first USERNAME fragment, matching
            // the engine's existing ICE ufrag registration contract.
            return Some(PacketIndexProbe::LocalIceUfrag(local_ice_ufrag));
        }
        // STUN responses may not carry USERNAME, so we fall back to source-address recovery
        return Some(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    if (20..64).contains(&byte0) {
        // DTLS packets are identified by first-byte range
        // We cannot extract routing information, so we fall back to address-based recovery.
        return Some(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    if (128..192).contains(&byte0) && packet_len > 2 {
        // RTP/RTCP packets also lack routing identifiers here.
        // ICE must have already established the correct source tuple.
        return Some(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    None
}

fn log_malformed_datagram(source_addr: SocketAddr) {
    trace!(
        source = %source_addr,
        "ignoring malformed UDP datagram in rtc packet loop"
    );
}

struct PacketRouteContext<'a> {
    effects: &'a mut PacketLoopEffects,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &'a [u8],
    received_at: Instant,
    packet_time: PacketLoopTime,
}

impl PacketRouteContext<'_> {
    fn host_datagram_input(&self) -> HostDatagramInput<'_> {
        HostDatagramInput {
            source_addr: self.source_addr,
            candidate_addr: self.candidate_addr,
            packet: self.packet,
            received_at: self.received_at,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HostFeedOutcome {
    Routed,
    Malformed,
}

fn record_planned_demux_drop(route: &mut PacketRouteContext<'_>, reason: RtcDatagramDropReason) {
    route
        .effects
        .record_metric(PacketLoopMetricEffect::RtcDatagramDrop(reason));
    match reason {
        RtcDatagramDropReason::RecentMissCache => {
            trace!(
                source = %route.source_addr,
                "dropping UDP datagram because a recent cache miss already proved no rtc user accepted it"
            );
        }
        RtcDatagramDropReason::SourceRateLimited => {
            trace!(
                source = %route.source_addr,
                "dropping UDP datagram because sustained unknown-source misses exhausted the rtc recovery budget for this source"
            );
        }
        RtcDatagramDropReason::Malformed => log_malformed_datagram(route.source_addr),
        RtcDatagramDropReason::NoUser => {
            trace!(
                source = %route.source_addr,
                "dropping UDP datagram because no rtc user accepted it"
            );
        }
    }
}

fn record_malformed_drop(route: &mut PacketRouteContext<'_>) {
    record_planned_demux_drop(route, RtcDatagramDropReason::Malformed);
}

fn route_packet_by_fallback_candidates(
    state: &mut RtcBootstrapState,
    routing_state: &mut PacketLoopRoutingState,
    miss_key: PacketLoopRoutingMissKey,
    validation: CandidateValidation,
    route: &mut PacketRouteContext<'_>,
    route_resolution: &'static str,
) {
    route
        .effects
        .record_metric(PacketLoopMetricEffect::RtcDatagramFallbackScan(
            validation.examined_sessions,
        ));
    match validation.result {
        CandidateValidationResult::Matched(session_key) => {
            debug!(
                source_addr = %route.source_addr,
                candidate_addr = %route.candidate_addr,
                route_resolution,
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id(),
                examined_sessions = validation.examined_sessions,
                "recovered rtc user routing from packet-loop demux plan"
            );
            if feed_datagram_to_session(state, &session_key, route) == HostFeedOutcome::Malformed {
                record_malformed_drop(route);
                return;
            }
            remember_routed_source_pin(
                &mut state.packet_loop,
                route,
                &session_key,
                Some(route_resolution),
            );
            route
                .effects
                .record_metric(PacketLoopMetricEffect::RtcDatagramRoute(
                    RtcDatagramRoutePath::Scan,
                ));
            routing_state.record_fallback_route_success(miss_key, route.packet, route.source_addr);
        }
        CandidateValidationResult::NoMatch => {
            record_no_user_miss(routing_state, miss_key, route);
        }
        CandidateValidationResult::Malformed => {
            record_malformed_drop(route);
        }
    }
}

fn route_packet_by_fallback_plan(
    state: &mut RtcBootstrapState,
    routing_state: &mut PacketLoopRoutingState,
    miss_key: PacketLoopRoutingMissKey,
    candidates: CandidateSource,
    route: &mut PacketRouteContext<'_>,
    route_resolution: &'static str,
) {
    let validation = {
        let candidates =
            prepare_fallback_candidates(&mut state.packet_loop, &state.users, candidates);
        validate_fallback_candidates(&state.users, route, candidates)
    };
    route_packet_by_fallback_candidates(
        state,
        routing_state,
        miss_key,
        validation,
        route,
        route_resolution,
    );
}

fn forget_stale_candidate_indexes(
    state: &mut PacketLoopState,
    stale_session_key: &TransportSessionKey,
) {
    state
        .remote_addr_demux
        .forget_user_remote_candidate_addrs(stale_session_key);
    state
        .remote_addr_demux
        .forget_user_local_ice_ufrag(stale_session_key);
}

fn record_no_user_miss(
    routing_state: &mut PacketLoopRoutingState,
    miss_key: PacketLoopRoutingMissKey,
    route: &mut PacketRouteContext<'_>,
) {
    routing_state.record_miss(miss_key, route.packet, route.source_addr, route.packet_time);
    record_planned_demux_drop(route, RtcDatagramDropReason::NoUser);
}

fn feed_datagram_to_session(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    route: &PacketRouteContext<'_>,
) -> HostFeedOutcome {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return HostFeedOutcome::Malformed;
    };
    let handle_result = session_state
        .host_session
        .handle_datagram(route.host_datagram_input());
    let _ = session_state;
    match handle_result {
        HostDatagramHandle::Handled => {
            state.packet_loop.mark_session_dirty(session_key);
        }
        HostDatagramHandle::Failed => {
            warn!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id(),
                "failed to feed incoming UDP datagram into rtc user state"
            );
        }
        HostDatagramHandle::Malformed => {
            return HostFeedOutcome::Malformed;
        }
    }
    HostFeedOutcome::Routed
}

fn forget_cached_source_pin(
    state: &mut PacketLoopState,
    effects: &mut PacketLoopEffects,
    source_addr: SocketAddr,
) {
    state.remote_addr_demux.forget_remote_addr(source_addr);
    effects.push(PacketLoopEffect::ForgetSnapshotRemoteAddr(source_addr));
}

fn remember_routed_source_pin(
    state: &mut PacketLoopState,
    route: &mut PacketRouteContext<'_>,
    session_key: &TransportSessionKey,
    route_resolution: Option<&'static str>,
) {
    let route_resolution = route_resolution.unwrap_or("indexed");
    let previous_session_key = state
        .remote_addr_demux
        .session_key_for_remote_addr(route.source_addr)
        .cloned();
    if state
        .remote_addr_demux
        .remember_remote_addr(route.source_addr, session_key)
    {
        route
            .effects
            .push(PacketLoopEffect::RememberSnapshotRemoteAddr {
                source_addr: route.source_addr,
                session_key: session_key.clone(),
            });
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
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Instant,
    };

    use str0m::ice::{StunMessage, TransId};

    use super::{
        CandidateSource, DatagramDemuxPlan, DatagramDemuxSessionSnapshot, PacketIndexProbe,
        PacketLoopEffect, PacketLoopEffects, PacketLoopRoutingMissKey, PacketLoopRoutingState,
        PacketLoopState, PacketLoopTime, packet_index_probe, plan_datagram_demux,
        remember_routed_source_pin,
    };
    use crate::runtime::{
        ConnectionId, RoomInstanceId, UserId, media_transport::TransportSessionKey,
        metrics::RtcDatagramDropReason,
    };

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
            Some(Some(PacketIndexProbe::LocalIceUfrag(local_ice_ufrag)))
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
            Some(Some(PacketIndexProbe::RemoteCandidateAddr(probed_source_addr)))
                if probed_source_addr == source_addr
        ));
    }

    #[test]
    fn pure_demux_plan_returns_cached_remote_addr_without_host_session() {
        let source_addr = test_source_addr();
        let candidate_addr = test_candidate_addr();
        let session_key = test_session_key(1);
        let mut state = PacketLoopState::default();
        let mut routing_state = PacketLoopRoutingState::new();
        let session_snapshot = no_live_sessions();
        let _ = state
            .remote_addr_demux
            .remember_remote_addr(source_addr, &session_key);

        let plan = plan_datagram_demux(
            &state,
            &mut routing_state,
            &session_snapshot,
            source_addr,
            candidate_addr,
            valid_rtp_packet(),
            PacketLoopTime::ZERO,
        );

        assert!(matches!(
            plan,
            DatagramDemuxPlan::Cached { session_key: cached_session_key }
                if cached_session_key == session_key
        ));
    }

    #[test]
    fn pure_demux_plan_drops_exact_recent_misses_without_host_session() {
        let source_addr = test_source_addr();
        let candidate_addr = test_candidate_addr();
        let state = PacketLoopState::default();
        let mut routing_state = PacketLoopRoutingState::new();
        let session_snapshot = no_live_sessions();
        let packet = valid_rtp_packet();
        let miss_key = PacketLoopRoutingMissKey::new(source_addr, candidate_addr, packet);
        routing_state.record_miss(miss_key, packet, source_addr, PacketLoopTime::ZERO);

        let plan = plan_datagram_demux(
            &state,
            &mut routing_state,
            &session_snapshot,
            source_addr,
            candidate_addr,
            packet,
            PacketLoopTime::ZERO,
        );

        assert!(matches!(
            plan,
            DatagramDemuxPlan::Drop(RtcDatagramDropReason::RecentMissCache)
        ));
    }

    #[test]
    fn pure_demux_plan_rate_limits_varied_unknown_sources_without_host_session() {
        let source_addr = test_source_addr();
        let candidate_addr = test_candidate_addr();
        let state = PacketLoopState::default();
        let mut routing_state = PacketLoopRoutingState::new();
        let session_snapshot = no_live_sessions();

        for offset in 0..4 {
            let packet = varied_rtp_packet(offset);
            let miss_key = PacketLoopRoutingMissKey::new(source_addr, candidate_addr, &packet);
            routing_state.record_miss(
                miss_key,
                &packet,
                source_addr,
                PacketLoopTime::from_millis(offset),
            );
        }

        let packet = varied_rtp_packet(9);
        let plan = plan_datagram_demux(
            &state,
            &mut routing_state,
            &session_snapshot,
            source_addr,
            candidate_addr,
            &packet,
            PacketLoopTime::from_millis(5),
        );

        assert!(matches!(
            plan,
            DatagramDemuxPlan::Drop(RtcDatagramDropReason::SourceRateLimited)
        ));
    }

    #[test]
    fn pure_demux_plan_selects_remote_candidate_fallback_without_host_session() {
        let source_addr = test_source_addr();
        let candidate_addr = test_candidate_addr();
        let session_key = test_session_key(2);
        let mut state = PacketLoopState::default();
        let mut routing_state = PacketLoopRoutingState::new();
        let session_snapshot = DatagramDemuxSessionSnapshot {
            single_session_key: None,
        };
        state
            .remote_addr_demux
            .replace_session_remote_candidate_addrs(&session_key, [source_addr]);

        let plan = plan_datagram_demux(
            &state,
            &mut routing_state,
            &session_snapshot,
            source_addr,
            candidate_addr,
            valid_rtp_packet(),
            PacketLoopTime::ZERO,
        );

        assert!(
            matches!(&plan, DatagramDemuxPlan::Fallback { .. }),
            "expected fallback candidate plan"
        );
        let DatagramDemuxPlan::Fallback {
            candidates,
            route_resolution,
            ..
        } = plan
        else {
            return;
        };
        assert_eq!(route_resolution, "recovery-index");
        assert!(matches!(
            candidates,
            CandidateSource::RemoteCandidateAddr(candidate_source_addr)
                if candidate_source_addr == source_addr
        ));
    }

    #[test]
    fn pure_demux_success_updates_pins_and_clears_negative_state_without_host_session() {
        let source_addr = test_source_addr();
        let candidate_addr = test_candidate_addr();
        let session_key = test_session_key(3);
        let mut state = PacketLoopState::default();
        let mut routing_state = PacketLoopRoutingState::new();
        let mut effects = PacketLoopEffects::default();
        let packet = valid_rtp_packet();
        let miss_key = PacketLoopRoutingMissKey::new(source_addr, candidate_addr, packet);
        routing_state.record_miss(miss_key, packet, source_addr, PacketLoopTime::ZERO);
        let mut route = super::PacketRouteContext {
            effects: &mut effects,
            source_addr,
            candidate_addr,
            packet,
            received_at: Instant::now(),
            packet_time: PacketLoopTime::ZERO,
        };

        remember_routed_source_pin(&mut state, &mut route, &session_key, Some("test"));
        routing_state.record_fallback_route_success(miss_key, packet, source_addr);

        assert_eq!(
            state
                .remote_addr_demux
                .session_key_for_remote_addr(source_addr),
            Some(&session_key)
        );
        assert!(!routing_state.should_skip_scan(miss_key, packet));
        assert!(effects.iter().any(|effect| matches!(
            effect,
            PacketLoopEffect::RememberSnapshotRemoteAddr {
                source_addr: remembered_addr,
                session_key: remembered_session_key,
            } if *remembered_addr == source_addr && remembered_session_key == &session_key
        )));
    }

    #[test]
    fn pure_demux_topology_clear_removes_recent_miss_state_without_host_session() {
        let source_addr = test_source_addr();
        let candidate_addr = test_candidate_addr();
        let packet = valid_rtp_packet();
        let miss_key = PacketLoopRoutingMissKey::new(source_addr, candidate_addr, packet);
        let mut routing_state = PacketLoopRoutingState::new();
        routing_state.record_miss(miss_key, packet, source_addr, PacketLoopTime::ZERO);

        routing_state.clear_on_topology_change();

        assert!(!routing_state.should_skip_scan(miss_key, packet));
    }

    fn test_source_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_321)
    }

    fn test_candidate_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_322)
    }

    fn valid_rtp_packet() -> &'static [u8] {
        &[0x80, 0x60, 0x00, 0x01, 0x55]
    }

    fn varied_rtp_packet(offset: u64) -> [u8; 5] {
        [
            0x80,
            0x60,
            0x00,
            u8::try_from(offset).unwrap_or(u8::MAX),
            0x55,
        ]
    }

    fn test_session_key(connection_id: u64) -> TransportSessionKey {
        TransportSessionKey::new(
            RoomInstanceId::from_raw(81),
            0,
            ConnectionId::from_raw(connection_id),
            UserId::Integer(i64::try_from(connection_id).unwrap_or(i64::MAX)),
        )
    }

    fn no_live_sessions() -> DatagramDemuxSessionSnapshot {
        DatagramDemuxSessionSnapshot {
            single_session_key: None,
        }
    }
}
