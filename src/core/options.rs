use std::net::IpAddr;

use crate::{
    config::{MediaCodecFlags, RtcPortRange},
    runtime::SessionBitrateLimits,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CoreOptions {
    pub(crate) media: MediaOptions,
    pub(crate) routing: RoutingOptions,
    pub(crate) codecs: CodecOptions,
    pub(crate) _observability: ObservabilityOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MediaOptions {
    pub(crate) public_ip: IpAddr,
    pub(crate) rtc_port_range: RtcPortRange,
    pub(crate) bitrate_limits: SessionBitrateLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoutingOptions {
    pub(crate) media_worker_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CodecOptions {
    pub(crate) flags: MediaCodecFlags,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservabilityOptions {
    pub(crate) transport_diagnostics_enabled: bool,
    pub(crate) transport_metrics_enabled: bool,
}

impl CoreOptions {
    #[must_use]
    pub(crate) const fn new(
        media: MediaOptions,
        routing: RoutingOptions,
        codecs: CodecOptions,
        observability: ObservabilityOptions,
    ) -> Self {
        Self {
            media,
            routing,
            codecs,
            _observability: observability,
        }
    }
}
