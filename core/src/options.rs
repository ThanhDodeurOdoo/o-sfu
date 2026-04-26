use std::net::IpAddr;

use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct MediaCodecSet: u16 {
        const OPUS = 1 << 0;
        const PCMU = 1 << 1;
        const PCMA = 1 << 2;
        const VP8 = 1 << 3;
        const H264 = 1 << 4;
        const H265 = 1 << 5;
        const VP9 = 1 << 6;
        const AV1 = 1 << 7;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoreOptions {
    pub media: MediaOptions,
    pub routing: RoutingOptions,
    pub codecs: CodecOptions,
    pub observability: ObservabilityOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaOptions {
    pub public_ip: IpAddr,
    pub rtc_port_range: RtcPortRange,
    pub bitrate_limits: SessionBitrateLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutingOptions {
    pub media_worker_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecOptions {
    pub flags: MediaCodecFlags,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservabilityOptions {
    pub transport_diagnostics_enabled: bool,
    pub transport_metrics_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtcPortRange {
    min: u16,
    max: u16,
}

impl RtcPortRange {
    #[must_use]
    pub const fn new(min: u16, max: u16) -> Self {
        Self { min, max }
    }

    #[must_use]
    pub const fn min(self) -> u16 {
        self.min
    }

    #[must_use]
    pub const fn max(self) -> u16 {
        self.max
    }

    #[must_use]
    pub const fn port_count(self) -> u16 {
        self.max - self.min + 1
    }

    pub fn ports(self) -> impl Iterator<Item = u16> {
        self.min..=self.max
    }

    #[must_use]
    pub fn split_for_workers(self, worker_count: usize) -> Option<Vec<Self>> {
        if worker_count == 0 || worker_count > usize::from(self.port_count()) {
            return None;
        }
        let total_ports = usize::from(self.port_count());
        let base_ports_per_worker = total_ports / worker_count;
        let extra_ports = total_ports % worker_count;
        let mut next_min = u32::from(self.min);
        let mut ranges = Vec::with_capacity(worker_count);
        for worker_idx in 0..worker_count {
            let worker_port_count = base_ports_per_worker + usize::from(worker_idx < extra_ports);
            let worker_port_count = u32::try_from(worker_port_count).ok()?;
            let max_inclusive = next_min + worker_port_count - 1;
            ranges.push(Self::new(
                u16::try_from(next_min).ok()?,
                u16::try_from(max_inclusive).ok()?,
            ));
            next_min = max_inclusive + 1;
        }
        Some(ranges)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionBitrateLimits {
    max_bitrate_in_bps: u64,
    max_bitrate_out_bps: u64,
}

impl SessionBitrateLimits {
    #[must_use]
    pub const fn new(max_bitrate_in_bps: u64, max_bitrate_out_bps: u64) -> Self {
        Self {
            max_bitrate_in_bps,
            max_bitrate_out_bps,
        }
    }

    #[must_use]
    pub const fn max_bitrate_in_bps(&self) -> u64 {
        self.max_bitrate_in_bps
    }

    #[must_use]
    pub const fn max_bitrate_out_bps(&self) -> u64 {
        self.max_bitrate_out_bps
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaCodecFlags {
    enabled: MediaCodecSet,
}

macro_rules! media_codec_accessors {
    ($($enabled:ident => $with:ident => $flag:ident),+ $(,)?) => {
        $(
            #[must_use]
            pub fn $enabled(self) -> bool {
                self.enabled.contains(MediaCodecSet::$flag)
            }

            #[must_use]
            pub fn $with(self, enabled: bool) -> Self {
                self.with_flag(MediaCodecSet::$flag, enabled)
            }
        )+
    };
}

impl MediaCodecFlags {
    #[must_use]
    fn with_flag(mut self, flag: MediaCodecSet, enabled: bool) -> Self {
        if enabled {
            self.enabled.insert(flag);
        } else {
            self.enabled.remove(flag);
        }
        self
    }

    media_codec_accessors!(
        opus_enabled => with_opus => OPUS,
        pcmu_enabled => with_pcmu => PCMU,
        pcma_enabled => with_pcma => PCMA,
        vp8_enabled => with_vp8 => VP8,
        h264_enabled => with_h264 => H264,
        h265_enabled => with_h265 => H265,
        vp9_enabled => with_vp9 => VP9,
        av1_enabled => with_av1 => AV1,
    );
}

impl Default for MediaCodecFlags {
    fn default() -> Self {
        Self {
            enabled: MediaCodecSet::OPUS | MediaCodecSet::VP8,
        }
    }
}

impl CoreOptions {
    #[must_use]
    pub const fn new(
        media: MediaOptions,
        routing: RoutingOptions,
        codecs: CodecOptions,
        observability: ObservabilityOptions,
    ) -> Self {
        Self {
            media,
            routing,
            codecs,
            observability,
        }
    }
}
