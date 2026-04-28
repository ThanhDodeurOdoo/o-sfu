use std::{net::IpAddr, sync::Arc};

use crate::{
    CodecPreferences, MediaCodecFlags, RtcPortRange, SessionBitrateLimits, VideoBitrateLimits,
    runtime::{
        diagnostics::DiagnosticsStore, metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

#[derive(Debug, Clone)]
pub struct RtcTransportAdapterConfig {
    pub public_ip: IpAddr,
    pub bitrate_limits: SessionBitrateLimits,
    pub video_bitrate_limits: VideoBitrateLimits,
    pub rtc_port_range: RtcPortRange,
    pub codec_flags: MediaCodecFlags,
    pub codec_preferences: CodecPreferences,
}

impl RtcTransportAdapterConfig {
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

#[derive(Debug, Clone)]
pub struct RtcTransportAdapterDeps {
    pub diagnostics: Arc<DiagnosticsStore>,
    pub packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    pub metrics: Arc<RuntimeMetrics>,
}

impl RtcTransportAdapterDeps {
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

#[derive(Debug, Clone)]
pub struct RtcTransportAdapterShardSetConfig {
    pub worker_count: usize,
    pub transport: RtcTransportAdapterConfig,
    pub deps: RtcTransportAdapterDeps,
}

impl RtcTransportAdapterShardSetConfig {
    #[must_use]
    pub fn new(
        transport: RtcTransportAdapterConfig,
        deps: RtcTransportAdapterDeps,
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
    pub(super) fn adapter_config(&self) -> &RtcTransportAdapterConfig {
        &self.transport
    }

    #[must_use]
    pub(super) fn adapter_deps(&self) -> &RtcTransportAdapterDeps {
        &self.deps
    }

    #[must_use]
    pub(super) fn shard_config_with_port_range(
        &self,
        rtc_port_range: RtcPortRange,
    ) -> RtcTransportAdapterConfig {
        self.transport.with_rtc_port_range(rtc_port_range)
    }
}
