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

impl FromRequestParts<RuntimeState> for RequestOrigin {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &RuntimeState,
    ) -> Result<Self, Self::Rejection> {
        let connect_info = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| *addr);
        Ok(resolve_request_origin(
            &parts.headers,
            state.config.http.trust_proxy_headers,
            state.config.http.bind_address,
            connect_info,
        ))
    }
}

/// Resolves proxy headers only when `trust_proxy_headers` is set.
///
/// Set `trust_proxy_headers` only when every request reaches this listener
/// through a proxy that strips or overwrites client-supplied `x-forwarded-*`
/// values.
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
