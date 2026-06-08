//! Construction inputs for the media transport boundary.
//!
//! These types are cold-path configuration and dependency carriers. They are
//! consumed when the runtime builds the transport service and must not be
//! consulted by packet-loop code after startup. The split keeps startup policy
//! separate from long-lived service handles:
//! transport config describes operator policy, while transport deps describe
//! process services shared with diagnostics, metrics and recording.

use std::{net::IpAddr, sync::Arc, time::Duration};

use crate::{
    CodecPreferences, CoreOptions, MediaCodecFlags, RtcPortRange, RtcUdpIoBackend,
    SessionBitrateLimits, VideoBitrateLimits,
    engine::{
        diagnostics::DiagnosticsStore, metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

/// Operator-facing RTC transport policy used to build each RTC worker.
///
/// This is still RTC-specific because it describes the concrete server-side
/// WebRTC transport. Application code should normally pass
/// [`CoreOptions`] into
/// [`MediaTransport`](super::MediaTransport) construction instead of
/// assembling this type directly.
///
/// Values are immutable after construction. Per-worker port splitting derives
/// smaller configs from this one without changing bitrate, codec or public IP
/// policy.
#[derive(Debug, Clone)]
pub struct MediaTransportConfig {
    /// Public address advertised in ICE candidates.
    ///
    /// This is deployment policy, not a local bind address. A wrong value can
    /// make otherwise valid sessions unreachable from browsers.
    pub public_ip: IpAddr,
    /// Per-user ingress and egress bitrate ceilings enforced by the transport.
    pub bitrate_limits: SessionBitrateLimits,
    /// Default video bitrate policy used while building negotiated offers.
    pub video_bitrate_limits: VideoBitrateLimits,
    /// UDP port range available to this transport config.
    ///
    /// The top-level config covers the whole process. Worker configs carry only
    /// the sub-range assigned to one media worker.
    pub rtc_port_range: RtcPortRange,
    /// UDP I/O implementation used by RTC packet-loop workers.
    pub rtc_udp_io_backend: RtcUdpIoBackend,
    /// Enabled codec set for offer generation and capability projection.
    pub codec_flags: MediaCodecFlags,
    /// Codec preference order preserved while constructing router capabilities.
    pub codec_preferences: CodecPreferences,
    /// str0m stats interval used for sampled transport-quality events.
    pub media_quality_interval: Option<Duration>,
}

impl MediaTransportConfig {
    /// Projects neutral core options into RTC transport policy.
    #[must_use]
    pub fn from_core_options(options: &CoreOptions) -> Self {
        Self {
            public_ip: options.media.public_ip,
            bitrate_limits: options.media.bitrate_limits,
            video_bitrate_limits: options.media.video_bitrate_limits,
            rtc_port_range: options.media.rtc_port_range,
            rtc_udp_io_backend: options.media.rtc_udp_io_backend,
            codec_flags: options.codecs.flags,
            codec_preferences: options.codecs.preferences,
            media_quality_interval: options.observability.media_quality_interval,
        }
    }

    /// Returns a copy scoped to one worker UDP port range.
    ///
    /// This is used only during media transport construction. Callers should
    /// validate the original range once through the media transport builder
    /// instead of slicing it themselves.
    #[must_use]
    pub(super) fn with_rtc_port_range(&self, rtc_port_range: RtcPortRange) -> Self {
        Self {
            public_ip: self.public_ip,
            bitrate_limits: self.bitrate_limits,
            video_bitrate_limits: self.video_bitrate_limits,
            rtc_port_range,
            rtc_udp_io_backend: self.rtc_udp_io_backend,
            codec_flags: self.codec_flags,
            codec_preferences: self.codec_preferences,
            media_quality_interval: self.media_quality_interval,
        }
    }
}

/// Long-lived services injected into media transport construction.
///
/// # Resource split
///
/// The transport owns no global telemetry registry or recording service by
/// itself. It receives handles to the process stores it must update while
/// executing media work. Keeping this dependency bag neutral prevents the
/// server runtime from importing RTC-specific construction names.
#[derive(Debug, Clone)]
pub struct MediaTransportDeps {
    /// Operator diagnostics store used for session and transport lifecycle
    /// events.
    pub diagnostics: Arc<DiagnosticsStore>,
    /// Room packet-sink registry used by the packet path to fan out recording
    /// and non-local destinations.
    pub packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    /// Process-local metrics catalog updated by transport lifecycle and media
    /// counters.
    pub metrics: Arc<RuntimeMetrics>,
}

impl MediaTransportDeps {
    #[must_use]
    pub fn packet_sink_registry(&self) -> Arc<RoomPacketSinkRegistry> {
        Arc::clone(&self.packet_sink_registry)
    }

    #[must_use]
    pub fn diagnostics(&self) -> Arc<DiagnosticsStore> {
        Arc::clone(&self.diagnostics)
    }

    #[must_use]
    pub fn metrics(&self) -> Arc<RuntimeMetrics> {
        Arc::clone(&self.metrics)
    }
}

#[cfg(test)]
#[path = "TESTS/config.rs"]
mod tests;
