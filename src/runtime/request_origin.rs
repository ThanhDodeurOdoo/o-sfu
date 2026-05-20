use std::{convert::Infallible, net::SocketAddr, str};

use axum::{
    extract::{ConnectInfo, FromRequestParts},
    http::{HeaderMap, header, request::Parts},
};

use crate::runtime::RuntimeState;

const UNKNOWN_REMOTE_ADDRESS: &str = "unknown";

/// Proxy-aware request origin derived by the HTTP edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestOrigin {
    pub base_url: String,
    pub remote_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedRequestOrigin(pub RequestOrigin);

impl FromRequestParts<RuntimeState> for ResolvedRequestOrigin {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &RuntimeState,
    ) -> Result<Self, Self::Rejection> {
        let connect_info = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| *addr);
        Ok(Self(resolve_request_origin(
            &parts.headers,
            state.config.http.trust_proxy_headers,
            state.config.http.bind_address,
            connect_info,
        )))
    }
}

#[must_use]
pub fn resolve_request_origin(
    headers: &HeaderMap,
    trust_proxy_headers: bool,
    fallback_bind_address: SocketAddr,
    connect_info: Option<SocketAddr>,
) -> RequestOrigin {
    RequestOrigin {
        base_url: request_base_url(headers, trust_proxy_headers, fallback_bind_address),
        remote_address: resolve_remote_address(headers, trust_proxy_headers, connect_info),
    }
}

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

pub(crate) fn request_base_url(
    headers: &HeaderMap,
    trust_proxy_headers: bool,
    fallback_bind_address: SocketAddr,
) -> String {
    let scheme = trusted_forwarded_header(headers, trust_proxy_headers, "x-forwarded-proto")
        .unwrap_or("http");
    let host = trusted_forwarded_header(headers, trust_proxy_headers, "x-forwarded-host")
        .map(str::to_owned)
        .or_else(|| {
            headers
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| fallback_bind_address.to_string());
    format!("{scheme}://{host}")
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use axum::http::{HeaderMap, HeaderValue};

    use super::{request_base_url, resolve_remote_address, trusted_forwarded_header};
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

    #[test]
    fn request_base_url_uses_trusted_forwarded_host_and_proto() {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("sfu.example.com"));
        headers.insert(
            "x-forwarded-host",
            HeaderValue::from_static("edge.example.com"),
        );
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));

        assert_eq!(
            request_base_url(&headers, true, SocketAddr::from(([127, 0, 0, 1], 8070))),
            "https://edge.example.com"
        );
    }
}
