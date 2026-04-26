use std::{net::IpAddr, sync::Arc};

use crate::{
    MediaCodecFlags, RtcPortRange, SessionBitrateLimits,
    runtime::{
        diagnostics::DiagnosticsStore, metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

#[derive(Debug, Clone)]
pub struct RtcTransportAdapterConfig {
    public_ip: IpAddr,
    bitrate_limits: SessionBitrateLimits,
    rtc_port_range: RtcPortRange,
    codec_flags: MediaCodecFlags,
    diagnostics: Arc<DiagnosticsStore>,
    packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    metrics: Arc<RuntimeMetrics>,
}

impl RtcTransportAdapterConfig {
    #[must_use]
    pub fn new(
        public_ip: IpAddr,
        bitrate_limits: SessionBitrateLimits,
        rtc_port_range: RtcPortRange,
        codec_flags: MediaCodecFlags,
        diagnostics: Arc<DiagnosticsStore>,
        packet_sink_registry: Arc<RoomPacketSinkRegistry>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            public_ip,
            bitrate_limits,
            rtc_port_range,
            codec_flags,
            diagnostics,
            packet_sink_registry,
            metrics,
        }
    }

    #[must_use]
    pub(super) fn with_rtc_port_range(&self, rtc_port_range: RtcPortRange) -> Self {
        Self {
            public_ip: self.public_ip,
            bitrate_limits: self.bitrate_limits,
            rtc_port_range,
            codec_flags: self.codec_flags,
            diagnostics: Arc::clone(&self.diagnostics),
            packet_sink_registry: Arc::clone(&self.packet_sink_registry),
            metrics: Arc::clone(&self.metrics),
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
    pub const fn rtc_port_range(&self) -> RtcPortRange {
        self.rtc_port_range
    }

    #[must_use]
    pub const fn codec_flags(&self) -> MediaCodecFlags {
        self.codec_flags
    }

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
    worker_count: usize,
    adapter: RtcTransportAdapterConfig,
}

impl RtcTransportAdapterShardSetConfig {
    #[allow(
        clippy::too_many_arguments,
        reason = "transport shard-set construction keeps network, codec, and shared runtime services explicit"
    )]
    #[must_use]
    pub fn new(
        public_ip: IpAddr,
        bitrate_limits: SessionBitrateLimits,
        rtc_port_range: RtcPortRange,
        worker_count: usize,
        codec_flags: MediaCodecFlags,
        diagnostics: Arc<DiagnosticsStore>,
        packet_sink_registry: Arc<RoomPacketSinkRegistry>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            worker_count,
            adapter: RtcTransportAdapterConfig::new(
                public_ip,
                bitrate_limits,
                rtc_port_range,
                codec_flags,
                diagnostics,
                packet_sink_registry,
                metrics,
            ),
        }
    }

    #[must_use]
    pub(super) fn worker_count(&self) -> usize {
        self.worker_count
    }

    #[must_use]
    pub(super) fn adapter_config(&self) -> &RtcTransportAdapterConfig {
        &self.adapter
    }

    #[must_use]
    pub(super) fn shard_config_with_port_range(
        &self,
        rtc_port_range: RtcPortRange,
    ) -> RtcTransportAdapterConfig {
        self.adapter.with_rtc_port_range(rtc_port_range)
    }
}
