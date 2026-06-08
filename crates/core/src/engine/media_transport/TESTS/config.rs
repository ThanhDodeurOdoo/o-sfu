use std::net::{IpAddr, Ipv4Addr};

use super::*;
use crate::{
    Bitrate,
    prelude::{CodecOptions, CoreOptions, MediaOptions, ObservabilityOptions, RoutingOptions},
};

#[test]
fn media_transport_config_preserves_udp_io_backend_from_core_options() {
    let options = CoreOptions::new(
        MediaOptions {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            rtc_port_range: RtcPortRange::new(40_000, 40_099),
            rtc_udp_io_backend: RtcUdpIoBackend::IoUring,
            bitrate_limits: SessionBitrateLimits::new(
                Bitrate::from_mbps(8),
                Bitrate::from_mbps(10),
            ),
            video_bitrate_limits: VideoBitrateLimits::default(),
        },
        RoutingOptions::new(1),
        CodecOptions {
            flags: MediaCodecFlags::default(),
            preferences: CodecPreferences::default(),
        },
        ObservabilityOptions {
            transport_diagnostics_enabled: false,
            transport_metrics_enabled: false,
            media_quality_interval: None,
        },
    );

    let config = MediaTransportConfig::from_core_options(&options);

    assert_eq!(config.rtc_udp_io_backend, RtcUdpIoBackend::IoUring);
}
