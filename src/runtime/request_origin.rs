use std::{net::SocketAddr, str};

use axum::http::HeaderMap;

const UNKNOWN_REMOTE_ADDRESS: &str = "unknown";

pub(crate) fn resolve_remote_address(
    headers: &HeaderMap,
    trust_proxy_headers: bool,
    connect_info: Option<SocketAddr>,
) -> String {
    trusted_forwarded_header(headers, trust_proxy_headers, "x-forwarded-for")
        .map(str::to_owned)
        .or_else(|| connect_info.map(|addr| addr.ip().to_string()))
        .unwrap_or_else(|| UNKNOWN_REMOTE_ADDRESS.to_owned())
}

pub(crate) fn trusted_forwarded_header<'headers>(
    headers: &'headers HeaderMap,
    trust_proxy_headers: bool,
    name: &str,
) -> Option<&'headers str> {
    if !trust_proxy_headers {
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
    use std::net::SocketAddr;

    use axum::http::{HeaderMap, HeaderValue};

    use super::{resolve_remote_address, trusted_forwarded_header};
    #[test]
    fn resolve_remote_address_prefers_trusted_forwarded_for_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.24, 203.0.113.8"),
        );

        let remote_address = resolve_remote_address(
            &headers,
            true,
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
            false,
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
            trusted_forwarded_header(&headers, true, "x-forwarded-host"),
            Some("sfu.internal")
        );
    }
}
