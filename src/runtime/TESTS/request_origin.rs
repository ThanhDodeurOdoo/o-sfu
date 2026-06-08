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
