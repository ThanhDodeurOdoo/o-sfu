use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use str0m::ice::{StunMessage, TransId};

use super::{PacketIndexProbe, packet_index_probe};
use crate::engine::media_transport::rtc::test_support::serialize_stun_message;

const STUN_TEST_PASSWORD: &[u8] = b"probe-password";

#[test]
fn packet_index_probe_extracts_the_local_ice_ufrag_from_binding_requests() {
    let packet = serialize_stun_message(
        &StunMessage::binding_request(
            "local-ufrag:remote-ufrag",
            TransId::new(),
            true,
            1,
            1,
            false,
        ),
        Some(STUN_TEST_PASSWORD),
    );

    assert!(matches!(
        packet
            .as_deref()
            .and_then(|packet| packet_index_probe(test_source_addr(), packet)),
        Some(PacketIndexProbe::LocalIceUfrag(local_ice_ufrag))
            if local_ice_ufrag == "local-ufrag"
    ));
}

#[test]
fn packet_index_probe_uses_the_source_addr_when_stun_has_no_username() {
    let source_addr = test_source_addr();
    let packet = serialize_stun_message(
        &StunMessage::binding_reply(TransId::new(), source_addr),
        Some(STUN_TEST_PASSWORD),
    );

    assert!(matches!(
        packet
            .as_deref()
            .and_then(|packet| packet_index_probe(source_addr, packet)),
        Some(PacketIndexProbe::RemoteCandidateAddr(probed_source_addr))
            if probed_source_addr == source_addr
    ));
}

fn test_source_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_321)
}
