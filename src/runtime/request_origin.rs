use std::{net::SocketAddr, str};

use axum::http::HeaderMap;

use crate::config::Config;

const UNKNOWN_REMOTE_ADDRESS: &str = "unknown";

pub(crate) fn resolve_remote_address(
    headers: &HeaderMap,
    config: &Config,
    connect_info: Option<SocketAddr>,
) -> String {
    trusted_forwarded_header(headers, config, "x-forwarded-for")
        .map(str::to_owned)
        .or_else(|| connect_info.map(|addr| addr.ip().to_string()))
        .unwrap_or_else(|| UNKNOWN_REMOTE_ADDRESS.to_owned())
}

pub(crate) fn trusted_forwarded_header<'headers>(
    headers: &'headers HeaderMap,
    config: &Config,
    name: &str,
) -> Option<&'headers str> {
    if !config.trust_proxy_headers {
        return None;
    }
    forwarded_header(headers, name)
}

fn forwarded_header<'headers>(headers: &'headers HeaderMap, name: &str) -> Option<&'headers str> {
    let value = headers.get(name)?.to_str().ok()?;
    value.split(',').next().map(str::trim)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use axum::http::{HeaderMap, HeaderValue};

    use super::{resolve_remote_address, trusted_forwarded_header};
    use crate::config::{
        Config, DiagnosticsConfig, MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags,
        TelemetryConfig,
    };

    fn test_config(trust_proxy_headers: bool) -> Config {
        Config {
            auth_key: "dGVzdC1rZXk=".to_owned(),
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8070)),
            authentication_timeout_ms: 10_000,
            channel_size: 100,
            session_timeout_ms: 10_000,
            ping_interval_ms: 60_000,
            trust_proxy_headers,
            feature_flags: RuntimeFeatureFlags::default(),
            codec_flags: MediaCodecFlags::default(),
            diagnostics: DiagnosticsConfig::default(),
            telemetry: TelemetryConfig::default(),
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            max_bitrate_in_bps: 8_000_000,
            max_bitrate_out_bps: 10_000_000,
            rtc_port_range: RtcPortRange::new(40_000, 49_999),
            rtc_media_worker_count: 1,
        }
    }

    #[test]
    fn resolve_remote_address_prefers_trusted_forwarded_for_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.24, 203.0.113.8"),
        );

        let remote_address = resolve_remote_address(
            &headers,
            &test_config(true),
            Some(SocketAddr::from(([127, 0, 0, 1], 8070))),
        );

        assert_eq!(remote_address, "198.51.100.24");
    }

    #[test]
    fn resolve_remote_address_uses_socket_ip_when_proxy_headers_are_untrusted() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.24, 203.0.113.8"),
        );

        let remote_address = resolve_remote_address(
            &headers,
            &test_config(false),
            Some(SocketAddr::from(([127, 0, 0, 1], 8070))),
        );

        assert_eq!(remote_address, "127.0.0.1");
    }

    #[test]
    fn trusted_forwarded_header_selects_the_first_forwarded_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-host",
            HeaderValue::from_static("sfu.internal, edge.internal"),
        );

        assert_eq!(
            trusted_forwarded_header(&headers, &test_config(true), "x-forwarded-host"),
            Some("sfu.internal")
        );
    }
}
