//! Construction inputs for the media transport boundary.
//!
//! These types are cold-path configuration and dependency carriers. They are
//! consumed when the runtime builds the transport service and must not be
//! consulted by packet-loop code after startup. The split is intentional:
//! transport config describes operator policy, while transport deps describe
//! process-owned services shared with diagnostics, metrics and recording.

use std::{net::IpAddr, sync::Arc};

use crate::{
    CodecPreferences, MediaCodecFlags, RtcPortRange, SessionBitrateLimits, VideoBitrateLimits,
    runtime::{
        diagnostics::DiagnosticsStore, metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

/// Operator-facing RTC transport policy used to build each worker shard.
///
/// # Boundary role
///
/// This is still RTC-specific because it describes the concrete server-side
/// WebRTC transport. The orchestration layer should normally pass
/// [`CoreOptions`](crate::CoreOptions) into
/// [`MediaTransport`](super::MediaTransport) construction instead of
/// assembling this type directly.
///
/// Values are immutable after construction. Per-shard port splitting derives
/// smaller configs from this one without changing bitrate, codec or public IP
/// policy.
#[derive(Debug, Clone)]
pub struct RtcTransportConfig {
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
    /// The top-level config covers the whole process. Shard configs carry only
    /// the sub-range assigned to one media worker.
    pub rtc_port_range: RtcPortRange,
    /// Enabled codec set for offer generation and capability projection.
    pub codec_flags: MediaCodecFlags,
    /// Codec preference order preserved while constructing router capabilities.
    pub codec_preferences: CodecPreferences,
}

impl RtcTransportConfig {
    /// Returns a copy scoped to one worker-owned UDP port range.
    ///
    /// This is used only by shard-set construction. Callers outside the shard
    /// set should validate the original range once through the media transport
    /// builder instead of slicing it themselves.
    #[must_use]
    pub(super) fn with_rtc_port_range(&self, rtc_port_range: RtcPortRange) -> Self {
        Self {
            public_ip: self.public_ip,
            bitrate_limits: self.bitrate_limits,
            video_bitrate_limits: self.video_bitrate_limits,
            rtc_port_range,
            codec_flags: self.codec_flags,
            codec_preferences: self.codec_preferences,
        }
    }

    #[must_use]
    pub const fn public_ip(&self) -> IpAddr {
        self.public_ip
    }

    #[must_use]
    pub const fn max_bitrate_in_bps(&self) -> u64 {
        self.bitrate_limits.max_bitrate_in_bps()
    }

    #[must_use]
    pub const fn max_bitrate_out_bps(&self) -> u64 {
        self.bitrate_limits.max_bitrate_out_bps()
    }

    #[must_use]
    pub const fn video_bitrate_limits(&self) -> VideoBitrateLimits {
        self.video_bitrate_limits
    }

    #[must_use]
    pub const fn rtc_port_range(&self) -> RtcPortRange {
        self.rtc_port_range
    }

    #[must_use]
    pub const fn codec_flags(&self) -> MediaCodecFlags {
        self.codec_flags
    }

    #[must_use]
    pub const fn codec_preferences(&self) -> CodecPreferences {
        self.codec_preferences
    }
}

/// Long-lived services injected into media transport construction.
///
/// # Ownership split
///
/// The transport owns no global telemetry registry or recording service by
/// itself. It receives handles to the process-owned stores it must update while
/// executing media work. Keeping this dependency bag neutral prevents the
/// server runtime from importing RTC-specific construction names.
#[derive(Debug, Clone)]
pub struct MediaTransportDeps {
    /// Operator diagnostics store used for session and transport lifecycle
    /// events.
    pub diagnostics: Arc<DiagnosticsStore>,
    /// Room packet-sink registry used by the packet path to fan out recording
    /// and future non-local destinations.
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

/// Internal shard-set construction input.
///
/// # Design note
///
/// Public runtime construction goes through `MediaTransport::from_core_options`
/// or `RtcTransport::builder()`, which validate worker and port policy before
/// creating transport state. This struct remains as the narrow handoff from the
/// builder to the shard set, where worker-local RTC shards are actually
/// created.
#[derive(Debug, Clone)]
pub struct RtcTransportShardSetConfig {
    /// Number of media workers that should receive transport shards.
    pub worker_count: usize,
    /// Shared operator policy before shard-local port splitting.
    pub transport: RtcTransportConfig,
    /// Shared process services cloned into each shard.
    pub deps: MediaTransportDeps,
}

impl RtcTransportShardSetConfig {
    #[must_use]
    pub fn new(
        transport: RtcTransportConfig,
        deps: MediaTransportDeps,
        worker_count: usize,
    ) -> Self {
        Self {
            worker_count,
            transport,
            deps,
        }
    }

    #[must_use]
    pub(super) fn worker_count(&self) -> usize {
        self.worker_count
    }

    #[must_use]
    pub(super) fn transport_config(&self) -> &RtcTransportConfig {
        &self.transport
    }

    #[must_use]
    pub(super) fn transport_deps(&self) -> &MediaTransportDeps {
        &self.deps
    }

    #[must_use]
    pub(super) fn shard_config_with_port_range(
        &self,
        rtc_port_range: RtcPortRange,
    ) -> RtcTransportConfig {
        self.transport.with_rtc_port_range(rtc_port_range)
    }
}
