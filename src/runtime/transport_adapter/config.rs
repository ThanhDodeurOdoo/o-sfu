use std::{net::IpAddr, sync::Arc};

use crate::config::{MediaCodecFlags, RtcPortRange};
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::MediaTap;

/// transport bitrate limit per session
///
/// `MAX_BITRATE_IN` is enforced by requesting REMB on inbound recieve streams so
/// the remote sender sees a capped receive budget while `MAX_BITRATE_OUT` is
/// enforced by enabling `str0m` BWE and setting the local desired send bitrate.
/// This stays transport-native on purpose and does not imply packet-loop hard
/// htrottling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionBitrateLimits {
    max_bitrate_in_bps: u64,
    max_bitrate_out_bps: u64,
}

impl SessionBitrateLimits {
    #[must_use]
    pub(crate) const fn new(max_bitrate_in_bps: u64, max_bitrate_out_bps: u64) -> Self {
        Self {
            max_bitrate_in_bps,
            max_bitrate_out_bps,
        }
    }

    #[must_use]
    pub(crate) const fn max_bitrate_in_bps(&self) -> u64 {
        self.max_bitrate_in_bps
    }

    #[must_use]
    pub(crate) const fn max_bitrate_out_bps(&self) -> u64 {
        self.max_bitrate_out_bps
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RtcTransportAdapterConfig {
    public_ip: IpAddr,
    bitrate_limits: SessionBitrateLimits,
    rtc_port_range: RtcPortRange,
    codec_flags: MediaCodecFlags,
    media_tap: Arc<MediaTap>,
    metrics: Arc<RuntimeMetrics>,
}

impl RtcTransportAdapterConfig {
    #[must_use]
    pub(crate) fn new(
        public_ip: IpAddr,
        bitrate_limits: SessionBitrateLimits,
        rtc_port_range: RtcPortRange,
        codec_flags: MediaCodecFlags,
        media_tap: Arc<MediaTap>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            public_ip,
            bitrate_limits,
            rtc_port_range,
            codec_flags,
            media_tap,
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
            media_tap: Arc::clone(&self.media_tap),
            metrics: Arc::clone(&self.metrics),
        }
    }

    #[must_use]
    pub(crate) const fn public_ip(&self) -> IpAddr {
        self.public_ip
    }

    pub(crate) const fn max_bitrate_in_bps(&self) -> u64 {
        self.bitrate_limits.max_bitrate_in_bps()
    }

    #[must_use]
    pub(crate) const fn max_bitrate_out_bps(&self) -> u64 {
        self.bitrate_limits.max_bitrate_out_bps()
    }

    #[must_use]
    pub(crate) const fn rtc_port_range(&self) -> RtcPortRange {
        self.rtc_port_range
    }

    #[must_use]
    pub(crate) const fn codec_flags(&self) -> MediaCodecFlags {
        self.codec_flags
    }

    #[must_use]
    pub(crate) fn media_tap(&self) -> Arc<MediaTap> {
        Arc::clone(&self.media_tap)
    }

    #[must_use]
    pub(crate) fn metrics(&self) -> Arc<RuntimeMetrics> {
        Arc::clone(&self.metrics)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RtcTransportAdapterShardSetConfig {
    worker_count: usize,
    adapter: RtcTransportAdapterConfig,
}

impl RtcTransportAdapterShardSetConfig {
    #[must_use]
    pub(crate) fn new(
        public_ip: IpAddr,
        bitrate_limits: SessionBitrateLimits,
        rtc_port_range: RtcPortRange,
        worker_count: usize,
        codec_flags: MediaCodecFlags,
        media_tap: Arc<MediaTap>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            worker_count,
            adapter: RtcTransportAdapterConfig::new(
                public_ip,
                bitrate_limits,
                rtc_port_range,
                codec_flags,
                media_tap,
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
