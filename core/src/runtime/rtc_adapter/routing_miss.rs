//! Negative routing memory for packet-loop UDP demux fallback.
//!
//! `ingress_routing` owns the authoritative demux decision for one datagram. It
//! first checks pinned source-address state, then probes recovery indexes and
//! finally asks `str0m::Rtc::accepts()` before feeding a packet into a session.
//! This module does not decide ownership. It only remembers recent negative
//! results so the packet loop can avoid repeating expensive fallback work for
//! traffic that was already proven unrelated to any live session.
//!
//! # Invalidation contract
//!
//! The state here is a performance hint. It must be cleared whenever topology,
//! ICE credentials or candidate indexes can change. A stale negative result
//! could otherwise hide a packet that becomes valid after a join, answer or
//! source-address remap.
//!
//! # Hot-path contract
//!
//! Repeated unknown-source traffic must not force unbounded scans or allocator
//! churn. The recent-miss cache is bounded and preserves exact packet bytes, so
//! it can skip only packets that match byte-for-byte. The rate limiter is keyed
//! by source address and limits varied probe traffic that would otherwise bypass
//! the exact miss cache.

use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    time::{Duration, Instant},
};

/// Maximum exact negative route decisions retained by one packet-loop worker.
///
/// The limit is intentionally small and worker-local. A miss only helps with
/// very recent repeated traffic, while durable routing truth lives in
/// `RtcBootstrapState` and `RemoteAddrDemux`.
const RECENT_MISS_CACHE_LIMIT: usize = 256;

/// Maximum unknown source addresses tracked for defensive probe throttling.
///
/// This protects the packet-loop worker from a large set of spoofed or stale
/// source tuples. Eviction is best-effort because a source that falls out of
/// this map can only regain a small probe burst.
const UNKNOWN_SOURCE_RATE_LIMIT_CAPACITY: usize = 512;

/// Number of unresolved probes allowed before a source enters cooldown.
///
/// Legitimate sessions should normally become source-address pinned through
/// STUN before media packets depend on this path. The burst allows short
/// recovery windows without letting one source monopolize fallback work.
const UNKNOWN_SOURCE_MISS_BURST_LIMIT: usize = 4;

/// Cooldown applied after one source exhausts its unresolved probe burst.
///
/// The value is deliberately short. This is abuse resistance for the demux
/// fallback path, not a session lifecycle timeout.
const UNKNOWN_SOURCE_RATE_LIMIT_COOLDOWN: Duration = Duration::from_millis(200);

/// Compact lookup key for an exact recent demux miss.
///
/// The key narrows cache lookup using stable packet facts that are cheap to
/// carry through the packet loop. It is not sufficient by itself. Callers must
/// still compare the saved bytes because the fingerprint is intentionally small
/// and can collide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct PacketLoopRoutingMissKey {
    /// Remote UDP tuple that produced the packet.
    source_addr: SocketAddr,
    /// Local shard candidate address that received the packet.
    candidate_addr: SocketAddr,
    /// Packet length, kept outside the fingerprint so different sized packets
    /// never share a miss key accidentally.
    packet_len: usize,
    /// Cheap lossy fingerprint used only before the exact byte comparison.
    packet_fingerprint: u64,
}

impl PacketLoopRoutingMissKey {
    /// Builds the lookup key for one negative routing candidate.
    ///
    /// The caller must keep the original packet bytes available when checking
    /// or recording a miss. The key only avoids comparing every cached packet
    /// when the tuple, length and fingerprint clearly differ.
    pub(super) fn new(source_addr: SocketAddr, candidate_addr: SocketAddr, packet: &[u8]) -> Self {
        Self {
            source_addr,
            candidate_addr,
            packet_len: packet.len(),
            packet_fingerprint: packet_fingerprint(packet),
        }
    }
}

/// Computes a small fingerprint for routing-miss prefiltering.
///
/// This is not a hash table security boundary. It samples length, prefix and
/// suffix so common RTP or STUN variations usually differ before the exact byte
/// comparison. Empty and short packets are still handled deterministically.
fn packet_fingerprint(packet: &[u8]) -> u64 {
    fn load_u64(bytes: &[u8]) -> u64 {
        let mut buffer = [0_u8; 8];
        for (slot, byte) in buffer.iter_mut().zip(bytes.iter().copied()) {
            *slot = byte;
        }
        u64::from_le_bytes(buffer)
    }

    let len = u64::try_from(packet.len()).map_or(u64::MAX, |len| len);
    let prefix = load_u64(packet);
    let suffix = load_u64(
        packet
            .get(packet.len().saturating_sub(8)..)
            .unwrap_or(packet),
    );
    len.rotate_left(17) ^ prefix.rotate_left(29) ^ suffix.rotate_left(43)
}

/// Exact packet bytes for one cached negative route decision.
///
/// The packet is stored in a `Vec<u8>` rather than `Box<[u8]>` on purpose.
/// Once the bounded cache is full, eviction reuses the oldest record by
/// clearing and extending this vector. That keeps capacity available for the
/// next miss and avoids the steady allocation churn a boxed slice would create
/// under sustained unknown-source traffic.
#[derive(Debug, Clone)]
struct PacketLoopRoutingMissRecord {
    /// Coarse tuple and fingerprint used before exact byte comparison.
    key: PacketLoopRoutingMissKey,
    /// Exact packet bytes required to prove that a later packet is the same
    /// negative routing case.
    packet: Vec<u8>,
}

impl PacketLoopRoutingMissRecord {
    /// Allocates storage for a new miss record.
    ///
    /// This runs during warmup or after the cache was cleared. Once the cache
    /// reaches its limit, `overwrite` is preferred so the worker can reuse the
    /// existing packet buffer.
    fn new(key: PacketLoopRoutingMissKey, packet: &[u8]) -> Self {
        Self {
            key,
            packet: packet.to_vec(),
        }
    }

    /// Replaces one evicted miss while retaining packet buffer capacity.
    ///
    /// If the new packet fits in the previous allocation, the packet loop only
    /// copies bytes into existing storage. Larger packets may grow the vector
    /// once, then that larger buffer remains reusable for later evictions.
    fn overwrite(&mut self, key: PacketLoopRoutingMissKey, packet: &[u8]) {
        self.key = key;
        self.packet.clear();
        self.packet.extend_from_slice(packet);
    }
}

/// Bounded cache of exact packets that no session accepted recently.
///
/// This cache answers one narrow question for `ingress_routing`: "can we skip
/// recovery work because this exact datagram already failed against the current
/// topology". It does not track malformed packets and it does not replace
/// `Rtc::accepts()` for packets that reach a candidate session.
#[derive(Default)]
struct PacketLoopRoutingMissCache {
    /// Oldest misses sit at the front so cache pressure reuses the coldest
    /// record first.
    entries: VecDeque<PacketLoopRoutingMissRecord>,
}

impl PacketLoopRoutingMissCache {
    /// Drops all negative routing memory while keeping cache allocations.
    ///
    /// This is required after topology or ICE changes because a previous miss
    /// can become valid when a session joins or candidate indexes are refreshed.
    fn clear(&mut self) {
        self.entries.clear();
    }

    /// Returns true only when both the miss key and full packet bytes match.
    ///
    /// The exact byte comparison is the safety guard that lets the fingerprint
    /// stay cheap. A collision can cost one comparison but cannot suppress a
    /// different packet.
    fn contains(&self, key: PacketLoopRoutingMissKey, packet: &[u8]) -> bool {
        self.entries
            .iter()
            .any(|candidate| candidate.key == key && candidate.packet.as_slice() == packet)
    }

    /// Records a packet that failed fallback routing under the current topology.
    ///
    /// Duplicate misses are ignored. A full cache reuses the oldest record so
    /// sustained unknown-source traffic does not allocate a fresh boxed packet
    /// for every retained negative decision.
    fn record(&mut self, key: PacketLoopRoutingMissKey, packet: &[u8]) {
        if self.contains(key, packet) {
            return;
        }
        if self.entries.len() == RECENT_MISS_CACHE_LIMIT
            && let Some(mut record) = self.entries.pop_front()
        {
            record.overwrite(key, packet);
            self.entries.push_back(record);
            return;
        }
        self.entries
            .push_back(PacketLoopRoutingMissRecord::new(key, packet));
    }

    /// Removes one miss after a later packet from the same source routes.
    ///
    /// Route success means the packet loop learned something new about that
    /// source tuple. Forgetting the matching negative record avoids carrying a
    /// stale "no session accepted this" result next to a fresh source pin.
    fn forget(&mut self, key: PacketLoopRoutingMissKey, packet: &[u8]) {
        let Some(position) = self
            .entries
            .iter()
            .position(|candidate| candidate.key == key && candidate.packet.as_slice() == packet)
        else {
            return;
        };
        let _ = self.entries.remove(position);
    }
}

/// Per-source cooldown state for unresolved fallback probes.
///
/// This is separate from the exact miss cache because an abusive or stale
/// source can vary sequence numbers, SSRCs or random payload bytes enough to
/// avoid exact cache hits. The limiter bounds those varied probes by source
/// address instead of by packet identity.
#[derive(Debug, Clone, Copy)]
struct UnknownSourceRateLimitEntry {
    /// Number of unresolved probes since the last cooldown decision.
    miss_count: usize,
    /// End of the current cooldown window, if the source is blocked.
    blocked_until: Option<Instant>,
}

impl UnknownSourceRateLimitEntry {
    /// Starts a source in the allowed state.
    fn new() -> Self {
        Self {
            miss_count: 0,
            blocked_until: None,
        }
    }

    /// Returns whether the source may attempt fallback recovery at `now`.
    ///
    /// Expired cooldowns are cleared lazily so the hot path does not need a
    /// background cleanup pass.
    fn allow_probe(&mut self, now: Instant) -> bool {
        if self
            .blocked_until
            .is_some_and(|blocked_until| blocked_until > now)
        {
            return false;
        }
        self.blocked_until = None;
        true
    }

    /// Accounts for one fallback probe that found no owner.
    ///
    /// After the burst is exhausted, the source enters a short cooldown and the
    /// burst counter resets. That keeps repeated abuse bounded while allowing a
    /// later legitimate ICE update to recover without waiting for long state.
    fn record_miss(&mut self, now: Instant) {
        self.miss_count = self.miss_count.saturating_add(1);
        if self.miss_count < UNKNOWN_SOURCE_MISS_BURST_LIMIT {
            return;
        }
        self.miss_count = 0;
        self.blocked_until = Some(now + UNKNOWN_SOURCE_RATE_LIMIT_COOLDOWN);
    }
}

/// Bounded source-address throttle for varied unknown traffic.
///
/// The limiter is worker-local and defensive. It should not be used to infer
/// whether a session exists, only whether the packet loop should spend fallback
/// work on a source that keeps missing.
#[derive(Default)]
struct UnknownSourceRateLimiter {
    /// Current cooldown state by source address.
    entries: HashMap<SocketAddr, UnknownSourceRateLimitEntry>,
    /// Best-effort insertion order used to cap memory under many sources.
    insertion_order: VecDeque<SocketAddr>,
}

impl UnknownSourceRateLimiter {
    /// Clears source throttling after topology changes.
    ///
    /// Source misses are only meaningful for the topology that produced them.
    /// Clearing both maps avoids blocking a source whose candidate set just
    /// became valid.
    fn clear(&mut self) {
        self.entries.clear();
        self.insertion_order.clear();
    }

    /// Returns whether fallback recovery may inspect this source now.
    fn allow_probe(&mut self, source_addr: SocketAddr, now: Instant) -> bool {
        self.entry_mut(source_addr).allow_probe(now)
    }

    /// Records that fallback recovery found no matching session for a source.
    ///
    /// Capacity enforcement happens after the miss so the source that triggered
    /// growth is represented before older entries are evicted.
    fn record_miss(&mut self, source_addr: SocketAddr, now: Instant) {
        self.entry_mut(source_addr).record_miss(now);
        self.enforce_capacity();
    }

    /// Drops throttling state once the source successfully routes.
    ///
    /// A successful route proves the source is no longer unknown. The stale
    /// address may remain in insertion order until capacity pressure reaches
    /// it, which is harmless because `entries` is authoritative.
    fn forget_source(&mut self, source_addr: SocketAddr) {
        self.entries.remove(&source_addr);
    }

    /// Creates or returns the mutable cooldown entry for one source.
    ///
    /// `insertion_order` is allowed to contain duplicates after a source was
    /// forgotten and later seen again. Capacity enforcement treats that queue as
    /// a best-effort eviction hint and removes from `entries` only when a key is
    /// still live.
    fn entry_mut(&mut self, source_addr: SocketAddr) -> &mut UnknownSourceRateLimitEntry {
        if !self.entries.contains_key(&source_addr) {
            self.insertion_order.push_back(source_addr);
        }
        self.entries
            .entry(source_addr)
            .or_insert_with(UnknownSourceRateLimitEntry::new)
    }

    /// Keeps the source throttle bounded under high-cardinality misses.
    fn enforce_capacity(&mut self) {
        while self.entries.len() > UNKNOWN_SOURCE_RATE_LIMIT_CAPACITY {
            let Some(evicted_source_addr) = self.insertion_order.pop_front() else {
                break;
            };
            self.entries.remove(&evicted_source_addr);
        }
    }

    #[cfg(test)]
    fn contains_source(&self, source_addr: SocketAddr) -> bool {
        self.entries.contains_key(&source_addr)
    }
}

/// Packet-loop routing hints for UDP datagrams that miss the fast path.
///
/// `PacketLoopRoutingState` is owned by the packet-loop task, next to
/// `RtcBootstrapState`. It has no async work and no authority over routing. Its
/// only job is to help `ingress_routing` decide whether fallback recovery should
/// run for an unknown source tuple.
///
/// # Invariants
///
/// Callers must clear this state whenever worker topology, ICE credentials or
/// demux indexes change. Callers must also pair `record_miss` with only packets
/// that completed fallback recovery and found no session. A packet that routes
/// successfully must call `record_route_success` so stale miss and rate-limit
/// state do not outlive the learned source tuple.
pub(super) struct PacketLoopRoutingState {
    /// Exact recent packets that failed fallback routing.
    miss_cache: PacketLoopRoutingMissCache,
    /// Source-address throttle for varied traffic that keeps missing.
    source_rate_limiter: UnknownSourceRateLimiter,
    #[cfg(test)]
    /// Test-only counter proving when fallback recovery was attempted.
    fallback_attempts: usize,
}

impl PacketLoopRoutingState {
    /// Creates empty routing hints for one packet-loop worker.
    pub(super) fn new() -> Self {
        Self {
            miss_cache: PacketLoopRoutingMissCache::default(),
            source_rate_limiter: UnknownSourceRateLimiter::default(),
            #[cfg(test)]
            fallback_attempts: 0,
        }
    }

    /// Invalidates all negative routing memory after a topology change.
    ///
    /// This should be called after worker commands that can add, remove or
    /// retarget sessions, candidates or ICE ufrags. The next packet will pay
    /// fallback cost again, which is safer than trusting stale negative state.
    pub(super) fn clear_on_topology_change(&mut self) {
        self.miss_cache.clear();
        self.source_rate_limiter.clear();
    }

    /// Returns whether fallback recovery can be skipped for this exact packet.
    ///
    /// A true result means the same packet already failed against the current
    /// topology. It does not say anything about other packets from the same
    /// source, which is why varied traffic is handled by the source limiter.
    pub(super) fn should_skip_scan(
        &self,
        miss_key: PacketLoopRoutingMissKey,
        packet: &[u8],
    ) -> bool {
        self.miss_cache.contains(miss_key, packet)
    }

    /// Returns whether the source is currently over its unknown-probe budget.
    ///
    /// This mutates limiter state because expired cooldowns are cleared lazily
    /// when the source is next seen.
    pub(super) fn should_rate_limit_source(
        &mut self,
        source_addr: SocketAddr,
        now: Instant,
    ) -> bool {
        !self.source_rate_limiter.allow_probe(source_addr, now)
    }

    /// Records that fallback recovery found no session for this packet.
    ///
    /// The exact miss cache handles repeated identical packets. The source
    /// limiter handles varied packets from the same unresolved address.
    pub(super) fn record_miss(
        &mut self,
        miss_key: PacketLoopRoutingMissKey,
        packet: &[u8],
        source_addr: SocketAddr,
        now: Instant,
    ) {
        self.miss_cache.record(miss_key, packet);
        self.source_rate_limiter.record_miss(source_addr, now);
    }

    /// Clears negative state for a source after a packet routes successfully.
    ///
    /// This keeps the hints aligned with the fast-path demux state. Once a
    /// source is accepted, later packets should use the learned source pin or
    /// revalidate normally instead of inheriting old fallback failures.
    pub(super) fn record_route_success(
        &mut self,
        miss_key: PacketLoopRoutingMissKey,
        packet: &[u8],
        source_addr: SocketAddr,
    ) {
        self.miss_cache.forget(miss_key, packet);
        self.source_rate_limiter.forget_source(source_addr);
    }

    #[cfg(test)]
    pub(super) fn record_fallback_attempt(&mut self) {
        self.fallback_attempts = self.fallback_attempts.saturating_add(1);
    }

    #[cfg(test)]
    pub(super) fn fallback_attempts(&self) -> usize {
        self.fallback_attempts
    }

    #[cfg(test)]
    pub(super) fn source_is_tracked(&self, source_addr: SocketAddr) -> bool {
        self.source_rate_limiter.contains_source(source_addr)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        time::{Duration, Instant},
    };

    use super::{PacketLoopRoutingMissKey, PacketLoopRoutingState, UnknownSourceRateLimiter};

    #[test]
    fn unknown_source_rate_limiter_blocks_after_burst_and_recovers_after_cooldown() {
        let source_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 44_000));
        let mut limiter = UnknownSourceRateLimiter::default();
        let start = Instant::now();

        for offset in 0..4 {
            let now = start + Duration::from_millis(offset);
            assert!(limiter.allow_probe(source_addr, now));
            limiter.record_miss(source_addr, now);
        }

        assert!(!limiter.allow_probe(source_addr, start + Duration::from_millis(4)));
        assert!(!limiter.allow_probe(source_addr, start + Duration::from_millis(199)));
        assert!(limiter.allow_probe(source_addr, start + Duration::from_millis(203)));
    }

    #[test]
    fn route_success_clears_source_rate_limit_state() {
        let source_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 44_010));
        let candidate_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 44_011));
        let mut routing_state = PacketLoopRoutingState::new();
        let start = Instant::now();
        let packet = [0x80, 0x60, 0x00, 0x01];
        let miss_key = PacketLoopRoutingMissKey::new(source_addr, candidate_addr, &packet);

        for offset in 0..4 {
            let now = start + Duration::from_millis(offset);
            routing_state.record_miss(miss_key, &packet, source_addr, now);
        }

        assert!(routing_state.source_is_tracked(source_addr));
        assert!(
            routing_state.should_rate_limit_source(source_addr, start + Duration::from_millis(4),)
        );

        routing_state.record_route_success(miss_key, &packet, source_addr);

        assert!(!routing_state.source_is_tracked(source_addr));
        assert!(
            !routing_state.should_rate_limit_source(source_addr, start + Duration::from_millis(5),)
        );
    }
}
