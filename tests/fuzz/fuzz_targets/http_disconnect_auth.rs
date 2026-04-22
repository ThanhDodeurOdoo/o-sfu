#![no_main]

//! Fuzz target for the HTTP auth and forwarded-header boundary.
//!
//! The `/v1/channel` and `/v1/disconnect` routes both verify route-specific JWT
//! claims, and the channel route also derives base URL and remote address from
//! proxy-aware forwarding headers.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use libfuzzer_sys::{
    arbitrary,
    arbitrary::Arbitrary,
    fuzz_target,
};
use o_sfu::testing::{
    auth::{HttpChannelClaims, HttpDisconnectClaims, verify},
    http::resolve_request_origin,
};

const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

#[derive(Debug, Arbitrary)]
struct HttpRouteInput {
    token: String,
    host: Option<String>,
    forwarded_host: Option<String>,
    forwarded_proto: Option<String>,
    forwarded_for: Option<String>,
    trust_proxy_headers: bool,
    connect_info: Option<SocketAddrInput>,
}

#[derive(Debug, Clone, Copy, Arbitrary)]
struct SocketAddrInput {
    ip: [u8; 4],
    port: u16,
}

impl SocketAddrInput {
    fn into_socket_addr(self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::from(self.ip)), self.port)
    }
}

fuzz_target!(|input: HttpRouteInput| {
    let _ = verify::<HttpChannelClaims>(&input.token, TEST_AUTH_KEY);
    let _ = verify::<HttpDisconnectClaims>(&input.token, TEST_AUTH_KEY);
    let _ = resolve_request_origin(
        input.host.as_deref(),
        input.forwarded_host.as_deref(),
        input.forwarded_proto.as_deref(),
        input.forwarded_for.as_deref(),
        input.trust_proxy_headers,
        input.connect_info.map(SocketAddrInput::into_socket_addr),
    );
});
