#[cfg(any(test, feature = "testing-transport"))]
use {
    super::{MediaTransport, TransportMediaId, TransportSessionHealth, TransportSessionKey},
    crate::engine::sync::lock_unpoisoned,
    std::{
        collections::BTreeSet,
        env, fs,
        io::ErrorKind,
        net::{SocketAddrV4, UdpSocket},
        path::{Path, PathBuf},
        sync::{Mutex, OnceLock},
        time::{Duration, Instant, SystemTime},
    },
    str0m::media::Mid,
};
#[cfg(test)]
use {
    super::{MediaTransportBuilder, TransportAdapterError},
    o_sfu_router::MediaStream as RouterRtpParameters,
};
#[cfg(any(test, feature = "internal-benchmarks"))]
use {
    super::{MediaTransportConfig, MediaTransportDeps},
    crate::{
        Bitrate, CodecPreferences, MediaCodecFlags, RtcUdpIoBackend, SessionBitrateLimits,
        VideoBitrateLimits,
        engine::{
            diagnostics::DiagnosticsStore, metrics::RuntimeMetrics,
            packet_sink_registry::RoomPacketSinkRegistry,
        },
    },
    std::{net::IpAddr, sync::Arc},
};
#[cfg(any(test, feature = "internal-benchmarks", feature = "testing-transport"))]
use {crate::RtcPortRange, std::net::Ipv4Addr};

#[cfg(any(test, feature = "testing-transport"))]
pub use super::rtc::{ForwardedPacket, test_support::*};

#[cfg(any(test, feature = "testing-transport"))]
static RESERVED_RTC_TEST_PORTS: OnceLock<Mutex<BTreeSet<u16>>> = OnceLock::new();
#[cfg(any(test, feature = "testing-transport"))]
const RTC_PORT_RESERVATION_ATTEMPTS: usize = 256;
#[cfg(any(test, feature = "testing-transport"))]
const RTC_PORT_LOCK_DIR: &str = "o-sfu-rtc-test-ports";
#[cfg(any(test, feature = "testing-transport"))]
const RTC_PORT_LOCK_STALE_AFTER: Duration = Duration::from_hours(1);

#[derive(Debug, Clone, Copy)]
#[cfg(any(test, feature = "testing-transport"))]
pub struct MediaTransportTestApi<'a> {
    transport: &'a MediaTransport,
}

#[cfg(any(test, feature = "testing-transport"))]
impl MediaTransport {
    #[must_use]
    pub fn test_api(&self) -> MediaTransportTestApi<'_> {
        MediaTransportTestApi { transport: self }
    }
}

#[cfg(any(test, feature = "testing-transport"))]
impl MediaTransportTestApi<'_> {
    #[cfg(test)]
    pub(crate) async fn negotiated_producer_parameters(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.transport
            .worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .negotiated_producer_parameters(session_key, transport_media_id)
            .await
    }

    /// Overrides a real RTC session health snapshot in test builds.
    ///
    /// This is a route-test hook for failure injection and is not a production
    /// control-plane operation.
    pub fn set_session_transport_health(
        self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        if let Some(worker) = self.transport.worker_for_user(session_key) {
            worker.debug_set_session_transport_health(session_key, health);
        }
    }

    pub async fn route_entry(
        self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.transport
            .worker_for_user(source_session_key)?
            .debug_route_entry(source_session_key, source_mid)
            .await
    }

    /// Inspects a real RTC route by consumer mid in test builds.
    ///
    /// This is exposed for integration assertions that need to prove routing
    /// state without exposing worker internals to production callers.
    pub async fn route_entry_by_consumer_mid(
        self,
        consumer_session_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        for worker in self.transport.all_workers() {
            if let Some(entry) = worker
                .debug_route_entry_by_consumer_mid(consumer_session_key, consumer_mid)
                .await
            {
                return Some(entry);
            }
        }
        None
    }

    pub async fn route_entry_by_media_id(
        self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        for worker in self.transport.all_workers() {
            if let Some(entry) = worker
                .debug_route_entry_by_media_id(source_transport_media_id)
                .await
            {
                return Some(entry);
            }
        }
        None
    }

    pub async fn session_receiver_bwe_target(
        self,
        session_key: &TransportSessionKey,
    ) -> Option<crate::Bitrate> {
        self.transport
            .worker_for_user(session_key)?
            .debug_session_receiver_bwe_target(session_key)
            .await
    }

    pub async fn observe_audio_activity_with_level(
        self,
        transport_media_id: TransportMediaId,
        audio_level_dbov: i8,
        now: Instant,
    ) {
        for worker in self.transport.all_workers() {
            worker
                .debug_observe_audio_activity(
                    transport_media_id,
                    Some(true),
                    Some(audio_level_dbov),
                    now,
                )
                .await;
        }
    }
}

/// returns a process-unique contiguous UDP port range for RTC integration tests
///
/// asks the operating system for an ephemeral starting port and keeps selected
/// ranges out of later test transports in the same process. sockets are
/// released before the transport starts so the real RTC worker still owns the
/// final bind path
#[cfg(any(test, feature = "testing-transport"))]
#[must_use]
pub fn test_rtc_port_range(worker_count: usize) -> Option<RtcPortRange> {
    let worker_count = u16::try_from(worker_count).ok()?;
    if worker_count == 0 {
        return None;
    }
    let ports = RESERVED_RTC_TEST_PORTS.get_or_init(|| Mutex::new(BTreeSet::new()));
    for _ in 0..RTC_PORT_RESERVATION_ATTEMPTS {
        let used_ports = {
            let ports = lock_unpoisoned(ports);
            ports.clone()
        };
        let Some((range, _sockets)) = reserve_contiguous_rtc_ports(worker_count, &used_ports)
        else {
            continue;
        };
        let Some(port_locks) = reserve_rtc_port_locks(range) else {
            continue;
        };
        let inserted = {
            let mut ports = lock_unpoisoned(ports);
            if range.ports().any(|port| ports.contains(&port)) {
                false
            } else {
                ports.extend(range.ports());
                true
            }
        };
        if !inserted {
            release_rtc_port_locks(&port_locks);
            continue;
        }
        return Some(range);
    }
    None
}

#[cfg(any(test, feature = "testing-transport"))]
fn reserve_contiguous_rtc_ports(
    worker_count: u16,
    used_ports: &BTreeSet<u16>,
) -> Option<(RtcPortRange, Vec<UdpSocket>)> {
    let first_socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    let first_port = first_socket.local_addr().ok()?.port();
    let last_port = first_port.checked_add(worker_count.checked_sub(1)?)?;
    let range = RtcPortRange::new(first_port, last_port);
    if range.ports().any(|port| used_ports.contains(&port)) {
        return None;
    }
    let mut sockets = Vec::with_capacity(usize::from(worker_count));
    sockets.push(first_socket);
    for port in (first_port.saturating_add(1))..=last_port {
        sockets.push(UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).ok()?);
    }
    Some((range, sockets))
}

#[cfg(any(test, feature = "testing-transport"))]
fn reserve_rtc_port_locks(range: RtcPortRange) -> Option<Vec<PathBuf>> {
    let lock_root = env::temp_dir().join(RTC_PORT_LOCK_DIR);
    fs::create_dir_all(&lock_root).ok()?;
    let mut locked_paths = Vec::with_capacity(usize::from(range.port_count()));
    for port in range.ports() {
        let lock_path = lock_root.join(port.to_string());
        if !reserve_rtc_port_lock(&lock_path) {
            release_rtc_port_locks(&locked_paths);
            return None;
        }
        locked_paths.push(lock_path);
    }
    Some(locked_paths)
}

#[cfg(any(test, feature = "testing-transport"))]
fn reserve_rtc_port_lock(path: &Path) -> bool {
    match fs::create_dir(path) {
        Ok(()) => true,
        Err(error)
            if error.kind() == ErrorKind::AlreadyExists && remove_stale_rtc_port_lock(path) =>
        {
            fs::create_dir(path).is_ok()
        }
        Err(_) => false,
    }
}

#[cfg(any(test, feature = "testing-transport"))]
fn remove_stale_rtc_port_lock(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    let Ok(modified_at) = metadata.modified() else {
        return false;
    };
    let Ok(age) = SystemTime::now().duration_since(modified_at) else {
        return false;
    };
    age > RTC_PORT_LOCK_STALE_AFTER && fs::remove_dir(path).is_ok()
}

#[cfg(any(test, feature = "testing-transport"))]
fn release_rtc_port_locks(paths: &[PathBuf]) {
    for path in paths {
        let _ = fs::remove_dir(path);
    }
}

#[cfg(test)]
pub(crate) fn test_media_transport_builder(rtc_port_range: RtcPortRange) -> MediaTransportBuilder {
    MediaTransport::builder()
        .transport_config(test_media_transport_config(rtc_port_range))
        .deps(test_media_transport_deps())
}

#[cfg(any(test, feature = "internal-benchmarks"))]
pub(crate) fn test_media_transport_config(rtc_port_range: RtcPortRange) -> MediaTransportConfig {
    MediaTransportConfig {
        public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        bitrate_limits: SessionBitrateLimits::new(Bitrate::from_mbps(8), Bitrate::from_mbps(10)),
        video_bitrate_limits: VideoBitrateLimits::default(),
        rtc_port_range,
        rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
        codec_flags: MediaCodecFlags::default(),
        codec_preferences: CodecPreferences::default(),
        media_quality_interval: None,
    }
}

#[cfg(any(test, feature = "internal-benchmarks"))]
pub(crate) fn test_media_transport_deps() -> MediaTransportDeps {
    MediaTransportDeps {
        diagnostics: Arc::new(DiagnosticsStore::default()),
        packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
        metrics: Arc::new(RuntimeMetrics::default()),
    }
}
