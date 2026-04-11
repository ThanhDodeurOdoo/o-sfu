//! Benchmark-only helpers for transport hot-path measurements.
//!
//! This module is intentionally hidden behind the `internal-benchmarks`
//! feature as we only benchmark if we need to verify something

use std::{
    hint::black_box,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use tokio::runtime::Builder;

use super::{
    recording::MediaTap,
    rtc_adapter::RtcTransportAdapter,
    transport_adapter::{TransportAdapterError, TransportSessionKey},
};
use crate::{config::RtcPortRange, signaling::shared::SessionId};
use o_sfu_router::RtpCapabilities as RouterRtpCapabilities;

const BENCHMARK_PUBLIC_IP: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
const BENCHMARK_PORT_RANGE: RtcPortRange = RtcPortRange::new(40_000, 49_999);
const BENCHMARK_CHANNEL_RUNTIME_ID: u64 = 1;
const BENCHMARK_MEDIA_WORKER_ID: usize = 0;
const BENCHMARK_FIRST_CONNECTION_ID: u64 = 1;
const BENCHMARK_FIRST_REMOTE_PORT: u16 = 10_000;

/// Prepared benchmark fixture for the steady-state UDP demux lookup path.
///
/// The fixture exercises the worker-local remote-address cache against the
/// old reverse-scan shape over the same prepared session set.intentionally
/// isolates the lookup portion of the hot path so benchmark noise from socket
/// I/O, packet parsing, or `Rtc::accepts(...)` does not drown the index cost.
#[derive(Debug)]
pub struct RtcUdpDemuxBenchmarkFixture {
    adapter: RtcTransportAdapter,
    probe_addrs: Vec<SocketAddr>,
}

impl RtcUdpDemuxBenchmarkFixture {
    #[must_use]
    pub fn new(session_count: usize) -> Option<Self> {
        if session_count == 0 {
            return None;
        }
        let runtime = Builder::new_current_thread().enable_all().build().ok()?;
        let _runtime_guard = runtime.enter();
        let adapter = RtcTransportAdapter::new(
            BENCHMARK_PUBLIC_IP,
            BENCHMARK_PORT_RANGE,
            Arc::new(MediaTap::default()),
        );
        let mut session_keys = Vec::with_capacity(session_count);
        let mut probe_addrs = Vec::with_capacity(session_count);
        for idx in 0..session_count {
            let session_key = benchmark_session_key(idx)?;
            let remote_addr = benchmark_remote_addr(idx)?;
            session_keys.push(session_key);
            probe_addrs.push(remote_addr);
        }
        let router_capabilities = RouterRtpCapabilities::new(vec![], vec![]);
        runtime
            .block_on(async {
                for session_key in &session_keys {
                    let _payload = adapter
                        .transport_bootstrap_payload(session_key, &router_capabilities)
                        .await?;
                }
                Ok::<(), TransportAdapterError>(())
            })
            .ok()?;
        for (session_key, remote_addr) in session_keys.iter().zip(probe_addrs.iter().copied()) {
            let register_result = runtime.block_on(async {
                adapter
                    .benchmark_register_remote_addr(remote_addr, session_key)
                    .await
            });
            if register_result.is_err() {
                return None;
            }
        }
        Some(Self {
            adapter,
            probe_addrs,
        })
    }

    #[must_use]
    pub fn lookup_count_u64(&self) -> u64 {
        u64::try_from(self.probe_addrs.len()).unwrap_or(u64::MAX)
    }

    #[must_use]
    pub fn cached_lookup_cycle(&self) -> usize {
        let mut hits = 0_usize;
        for remote_addr in &self.probe_addrs {
            if self
                .adapter
                .benchmark_cached_remote_addr_lookup(*remote_addr)
            {
                hits = hits.saturating_add(1);
            }
        }
        black_box(hits)
    }

    #[must_use]
    pub fn linear_scan_cycle(&self) -> usize {
        let mut hits = 0_usize;
        for remote_addr in &self.probe_addrs {
            if self
                .adapter
                .benchmark_linear_remote_addr_lookup(*remote_addr)
            {
                hits = hits.saturating_add(1);
            }
        }
        black_box(hits)
    }
}

fn benchmark_session_key(idx: usize) -> Option<TransportSessionKey> {
    let session_id = SessionId::Integer(i64::try_from(idx).ok()?);
    let connection_id = BENCHMARK_FIRST_CONNECTION_ID.saturating_add(u64::try_from(idx).ok()?);
    Some(TransportSessionKey::new(
        BENCHMARK_CHANNEL_RUNTIME_ID,
        BENCHMARK_MEDIA_WORKER_ID,
        connection_id,
        session_id,
    ))
}

fn benchmark_remote_addr(idx: usize) -> Option<SocketAddr> {
    let offset = u16::try_from(idx).ok()?;
    let port = BENCHMARK_FIRST_REMOTE_PORT.checked_add(offset)?;
    Some(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}
