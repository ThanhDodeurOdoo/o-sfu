use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    time::{Duration, Instant},
};

const RECENT_MISS_CACHE_LIMIT: usize = 256;
const UNKNOWN_SOURCE_RATE_LIMIT_CAPACITY: usize = 512;
const UNKNOWN_SOURCE_MISS_BURST_LIMIT: usize = 4;
const UNKNOWN_SOURCE_RATE_LIMIT_COOLDOWN: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct PacketLoopRoutingMissKey {
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet_len: usize,
    packet_fingerprint: u64,
}

impl PacketLoopRoutingMissKey {
    pub(super) fn new(source_addr: SocketAddr, candidate_addr: SocketAddr, packet: &[u8]) -> Self {
        Self {
            source_addr,
            candidate_addr,
            packet_len: packet.len(),
            packet_fingerprint: packet_fingerprint(packet),
        }
    }
}

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

#[derive(Debug, Clone)]
struct PacketLoopRoutingMissRecord {
    key: PacketLoopRoutingMissKey,
    packet: Box<[u8]>,
}

#[derive(Default)]
struct PacketLoopRoutingMissCache {
    entries: VecDeque<PacketLoopRoutingMissRecord>,
}

impl PacketLoopRoutingMissCache {
    fn clear(&mut self) {
        self.entries.clear();
    }

    fn contains(&self, key: PacketLoopRoutingMissKey, packet: &[u8]) -> bool {
        self.entries
            .iter()
            .any(|candidate| candidate.key == key && candidate.packet.as_ref() == packet)
    }

    fn record(&mut self, key: PacketLoopRoutingMissKey, packet: &[u8]) {
        if self.contains(key, packet) {
            return;
        }
        self.entries.push_back(PacketLoopRoutingMissRecord {
            key,
            packet: packet.to_vec().into_boxed_slice(),
        });
        while self.entries.len() > RECENT_MISS_CACHE_LIMIT {
            let Some(_) = self.entries.pop_front() else {
                break;
            };
        }
    }

    fn forget(&mut self, key: PacketLoopRoutingMissKey, packet: &[u8]) {
        let Some(position) = self
            .entries
            .iter()
            .position(|candidate| candidate.key == key && candidate.packet.as_ref() == packet)
        else {
            return;
        };
        let _ = self.entries.remove(position);
    }
}

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

    fn record_miss(&mut self, now: Instant) {
        self.miss_count = self.miss_count.saturating_add(1);
        if self.miss_count < UNKNOWN_SOURCE_MISS_BURST_LIMIT {
            return;
        }
        self.miss_count = 0;
        self.blocked_until = Some(now + UNKNOWN_SOURCE_RATE_LIMIT_COOLDOWN);
    }
}

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

    fn record_miss(&mut self, source_addr: SocketAddr, now: Instant) {
        self.entry_mut(source_addr).record_miss(now);
        self.enforce_capacity();
    }

    fn forget_source(&mut self, source_addr: SocketAddr) {
        self.entries.remove(&source_addr);
    }

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

pub(super) struct PacketLoopRoutingState {
    miss_cache: PacketLoopRoutingMissCache,
    source_rate_limiter: UnknownSourceRateLimiter,
    #[cfg(test)]
    fallback_attempts: usize,
}

impl PacketLoopRoutingState {
    pub(super) fn new() -> Self {
        Self {
            miss_cache: PacketLoopRoutingMissCache::default(),
            source_rate_limiter: UnknownSourceRateLimiter::default(),
            #[cfg(test)]
            fallback_attempts: 0,
        }
    }

    pub(super) fn clear_on_topology_change(&mut self) {
        self.miss_cache.clear();
        self.source_rate_limiter.clear();
    }

    pub(super) fn should_skip_scan(
        &self,
        miss_key: PacketLoopRoutingMissKey,
        packet: &[u8],
    ) -> bool {
        self.miss_cache.contains(miss_key, packet)
    }

    pub(super) fn should_rate_limit_source(
        &mut self,
        source_addr: SocketAddr,
        now: Instant,
    ) -> bool {
        !self.source_rate_limiter.allow_probe(source_addr, now)
    }

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
