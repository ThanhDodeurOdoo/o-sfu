//! negative routing memory for packet-loop UDP demux fallback
//!
//! `ingress_routing` owns the authoritative demux decision for one datagram. It
//! first checks pinned source-address state, then probes recovery indexes and
//! finally asks `str0m::Rtc::accepts()` before feeding a packet into a session
//! this module does not decide ownership. It only remembers recent negative
//! results so the packet loop can avoid repeating expensive fallback work for
//! traffic that was already proven unrelated to any live session
//!
//! # invalidation contract
//!
//! this state is a performance hint and must be cleared whenever topology, ICE
//! credentials or candidate indexes can change. a stale negative result could
//! otherwise hide a packet that becomes valid after a join, answer or
//! source-address remap
//!
//! # hot-path contract
//!
//! repeated unknown-source traffic must not force unbounded scans or allocator
//! churn. the recent-miss cache is bounded and preserves exact packet bytes, so
//! it skips only byte-for-byte repeats. the rate limiter is keyed by source
//! address and bounds varied probe traffic that would bypass the exact cache

mod fingerprint;

use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    time::{Duration, Instant},
};

use self::fingerprint::packet_fingerprint;

/// maximum exact negative route decisions retained by one packet-loop worker
///
/// the limit is small and worker-local. a miss only helps with
/// very recent repeated traffic, while durable routing truth lives in
/// `PacketLoopState` and `RemoteAddrDemux`
const RECENT_MISS_CACHE_LIMIT: usize = 256;

/// maximum unknown source addresses tracked for defensive probe throttling
///
/// this protects the packet-loop worker from a large set of spoofed or stale
/// source tuples. eviction is best-effort because a removed source can only
/// regain a small probe burst
const UNKNOWN_SOURCE_RATE_LIMIT_CAPACITY: usize = 512;

/// unresolved probes allowed before a source enters cooldown
///
/// legitimate sessions should normally become source-address pinned through
/// STUN before media packets depend on this path
const UNKNOWN_SOURCE_MISS_BURST_LIMIT: usize = 4;

/// cooldown after one source exhausts its unresolved probe burst
///
/// this is abuse resistance for the demux fallback path, not a session timeout
const UNKNOWN_SOURCE_RATE_LIMIT_COOLDOWN: Duration = Duration::from_millis(200);

/// compact lookup key for an exact recent demux miss
///
/// callers must still compare saved packet bytes because the fingerprint is
/// small and can collide
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct PacketLoopRoutingMissKey {
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    /// kept outside the fingerprint so different sized packets never collide
    packet_len: usize,
    /// lossy prefilter before exact byte comparison
    packet_fingerprint: u64,
}

impl PacketLoopRoutingMissKey {
    /// build the lookup key for one negative routing candidate
    ///
    /// the caller must keep the original packet bytes available when checking
    /// or recording a miss. The key only avoids comparing every cached packet
    /// when the tuple, length and fingerprint clearly differ
    pub(super) fn new(source_addr: SocketAddr, candidate_addr: SocketAddr, packet: &[u8]) -> Self {
        Self {
            source_addr,
            candidate_addr,
            packet_len: packet.len(),
            packet_fingerprint: packet_fingerprint(packet),
        }
    }
}

#[cfg(feature = "internal-benchmarks")]
#[must_use]
pub fn packet_fingerprint_for_benchmark(packet: &[u8]) -> u64 {
    packet_fingerprint(packet)
}

/// exact packet bytes for one cached negative route decision
///
/// the packet is stored in a `Vec<u8>` rather than `Box<[u8]>` so evictions can
/// reuse the old allocation under sustained unknown-source traffic
#[derive(Debug, Clone)]
struct PacketLoopRoutingMissRecord {
    key: PacketLoopRoutingMissKey,
    /// exact packet bytes required before skipping fallback
    packet: Vec<u8>,
}

impl PacketLoopRoutingMissRecord {
    fn new(key: PacketLoopRoutingMissKey, packet: &[u8]) -> Self {
        Self {
            key,
            packet: packet.to_vec(),
        }
    }

    /// replace one evicted miss while retaining packet buffer capacity
    ///
    /// if the new packet fits in the previous allocation, the packet loop only
    /// copies bytes into existing storage. Larger packets may grow the vector
    /// once, then that larger buffer remains reusable for later evictions
    fn overwrite(&mut self, key: PacketLoopRoutingMissKey, packet: &[u8]) {
        self.key = key;
        self.packet.clear();
        self.packet.extend_from_slice(packet);
    }
}

/// bounded cache of exact packets that no session accepted recently
///
/// this cache answers one narrow question for `ingress_routing`: can this exact
/// datagram skip recovery because it already failed against the current topology
#[derive(Default)]
struct PacketLoopRoutingMissCache {
    entries: VecDeque<PacketLoopRoutingMissRecord>,
}

impl PacketLoopRoutingMissCache {
    fn clear(&mut self) {
        self.entries.clear();
    }

    /// return true only when both the miss key and full packet bytes match
    ///
    /// the exact byte comparison is the safety guard that lets the fingerprint
    /// stay cheap. A collision can cost one comparison but cannot suppress a
    /// different packet
    fn contains(&self, key: PacketLoopRoutingMissKey, packet: &[u8]) -> bool {
        self.entries
            .iter()
            .any(|candidate| candidate.key == key && candidate.packet.as_slice() == packet)
    }

    /// record a packet that failed fallback routing under the current topology
    ///
    /// duplicate misses are ignored. a full cache reuses the oldest record so
    /// sustained unknown-source traffic does not allocate a fresh boxed packet
    /// for every retained negative decision
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

    /// remove one miss after fallback routing later accepts the same source
    ///
    /// fallback route success means the packet loop learned something new about that
    /// source tuple. Forgetting the matching negative record avoids carrying a
    /// stale "no session accepted this" result next to a fresh source pin
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

/// per-source cooldown state for unresolved fallback probes
///
/// this is separate from the exact miss cache because an abusive or stale
/// source can vary sequence numbers, SSRCs or random payload bytes enough to
/// avoid exact cache hits. The limiter bounds those varied probes by source
/// address instead of by packet identity
#[derive(Debug, Clone, Copy)]
struct UnknownSourceRateLimitEntry {
    miss_count: usize,
    blocked_until: Option<Instant>,
}

impl UnknownSourceRateLimitEntry {
    fn new() -> Self {
        Self {
            miss_count: 0,
            blocked_until: None,
        }
    }

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

    /// account for one fallback probe that found no owner
    ///
    /// after the burst is exhausted, the source enters a short cooldown and the
    /// burst counter resets
    fn record_miss(&mut self, now: Instant) -> bool {
        self.miss_count = self.miss_count.saturating_add(1);
        if self.miss_count < UNKNOWN_SOURCE_MISS_BURST_LIMIT {
            return false;
        }
        self.miss_count = 0;
        self.blocked_until = Some(now + UNKNOWN_SOURCE_RATE_LIMIT_COOLDOWN);
        true
    }
}

/// bounded source-address throttle for varied unknown traffic
///
/// the limiter is worker-local and defensive. it should not be used to infer
/// whether a session exists, only whether the packet loop should spend fallback
/// work on a source that keeps missing
#[derive(Default)]
struct UnknownSourceRateLimiter {
    entries: HashMap<SocketAddr, UnknownSourceRateLimitEntry>,
    insertion_order: VecDeque<SocketAddr>,
}

impl UnknownSourceRateLimiter {
    fn clear(&mut self) {
        self.entries.clear();
        self.insertion_order.clear();
    }

    fn allow_probe(&mut self, source_addr: SocketAddr, now: Instant) -> bool {
        self.entry_mut(source_addr).allow_probe(now)
    }

    /// check cooldown state without allocating for unseen sources
    fn is_blocked(&mut self, source_addr: SocketAddr, now: Instant) -> bool {
        self.entries
            .get_mut(&source_addr)
            .is_some_and(|entry| !entry.allow_probe(now))
    }

    fn record_miss(&mut self, source_addr: SocketAddr, now: Instant) -> bool {
        let entered_cooldown = self.entry_mut(source_addr).record_miss(now);
        self.enforce_capacity();
        entered_cooldown
    }

    fn forget_source(&mut self, source_addr: SocketAddr) {
        self.entries.remove(&source_addr);
    }

    /// return the mutable cooldown entry for one source
    ///
    /// `insertion_order` is allowed to contain duplicates after a source was
    /// forgotten and later seen again. Capacity enforcement treats that queue as
    /// a best-effort eviction hint and removes from `entries` only when a key is
    /// still live
    fn entry_mut(&mut self, source_addr: SocketAddr) -> &mut UnknownSourceRateLimitEntry {
        if !self.entries.contains_key(&source_addr) {
            self.insertion_order.push_back(source_addr);
        }
        self.entries
            .entry(source_addr)
            .or_insert_with(UnknownSourceRateLimitEntry::new)
    }

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

/// packet-loop demux recovery hints for datagrams that miss the fast path
///
/// `DemuxRecoveryState` is owned by the packet-loop task, next to
/// `PacketLoopState`. It has no async work and no authority over routing. Its
/// only job is to help `ingress_routing` decide whether fallback recovery should
/// run for an unknown source tuple
///
/// # invariants
///
/// callers must clear this state whenever worker topology, ICE credentials or
/// demux indexes change. Callers must also pair `record_miss` with only packets
/// that completed fallback recovery and found no session. A packet that routes
/// successfully through fallback must call `record_fallback_route_success` so
/// stale miss and rate-limit state do not outlive the learned source tuple
pub(super) struct DemuxRecoveryState {
    miss_cache: PacketLoopRoutingMissCache,
    source_rate_limiter: UnknownSourceRateLimiter,
    #[cfg(test)]
    fallback_attempts: usize,
}

impl DemuxRecoveryState {
    pub(super) fn new() -> Self {
        Self {
            miss_cache: PacketLoopRoutingMissCache::default(),
            source_rate_limiter: UnknownSourceRateLimiter::default(),
            #[cfg(test)]
            fallback_attempts: 0,
        }
    }

    /// invalidate all negative demux recovery memory after a topology change
    ///
    /// call this after worker commands that can add, remove or retarget
    /// sessions, candidates or ICE ufrags
    pub(super) fn clear_on_topology_change(&mut self) {
        self.miss_cache.clear();
        self.source_rate_limiter.clear();
    }

    /// return whether fallback recovery can be skipped for this exact packet
    ///
    /// a true result means the same packet already failed against the current
    /// topology. It does not say anything about other packets from the same
    /// source, which is why varied traffic is handled by the source limiter
    pub(super) fn should_skip_scan(
        &self,
        miss_key: PacketLoopRoutingMissKey,
        packet: &[u8],
    ) -> bool {
        self.miss_cache.contains(miss_key, packet)
    }

    /// return whether the source is currently over its unknown-probe budget
    ///
    /// this mutates limiter state because expired cooldowns are cleared lazily
    /// when the source is next seen
    pub(super) fn should_rate_limit_source(
        &mut self,
        source_addr: SocketAddr,
        now: Instant,
    ) -> bool {
        !self.source_rate_limiter.allow_probe(source_addr, now)
    }

    /// check whether a source is blocked before packet fingerprinting
    pub(super) fn is_source_blocked(&mut self, source_addr: SocketAddr, now: Instant) -> bool {
        self.source_rate_limiter.is_blocked(source_addr, now)
    }

    /// record that fallback recovery found no session for this packet
    ///
    /// the exact miss cache handles repeated identical packets
    /// the source limiter handles varied packets from the same unresolved address
    pub(super) fn record_miss(
        &mut self,
        miss_key: PacketLoopRoutingMissKey,
        packet: &[u8],
        source_addr: SocketAddr,
        now: Instant,
    ) -> bool {
        self.miss_cache.record(miss_key, packet);
        self.source_rate_limiter.record_miss(source_addr, now)
    }

    /// clear negative state for a source after fallback routing succeeds
    ///
    /// once fallback accepts a source, later packets should use the learned
    /// source pin or revalidate normally instead of inheriting old failures
    pub(super) fn record_fallback_route_success(
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
    pub(super) fn is_tracking_source(&self, source_addr: SocketAddr) -> bool {
        self.source_rate_limiter.contains_source(source_addr)
    }
}

#[cfg(test)]
#[path = "TESTS/routing_miss.rs"]
mod tests;
