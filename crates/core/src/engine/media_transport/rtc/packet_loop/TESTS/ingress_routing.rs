use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use str0m::ice::{StunMessage, TransId};

use super::{PacketIndexProbe, packet_index_probe};
use crate::engine::media_transport::rtc::{
    state::{
        RTCP_INGRESS_BUDGET_CAPACITY_BYTES, RTCP_INGRESS_BUDGET_REFILL_BYTES_PER_SECOND,
        RtcpIngressBudget,
    },
    test_support::serialize_stun_message,
};

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

#[test]
fn rtcp_ingress_budget_rejects_without_spending_credit() {
    let now = Instant::now();
    let mut budget = RtcpIngressBudget::new(now);

    assert!(!budget.try_charge(RTCP_INGRESS_BUDGET_CAPACITY_BYTES + 1, now));
    assert!(budget.try_charge(RTCP_INGRESS_BUDGET_CAPACITY_BYTES, now));
    assert!(!budget.try_charge(1, now));
}

#[test]
fn rtcp_ingress_budget_refills_at_the_configured_rate() {
    let now = Instant::now();
    let mut budget = RtcpIngressBudget::new(now);

    assert!(budget.try_charge(RTCP_INGRESS_BUDGET_CAPACITY_BYTES, now));
    assert!(!budget.try_charge(1, now));
    assert!(budget.try_charge(
        RTCP_INGRESS_BUDGET_REFILL_BYTES_PER_SECOND,
        now + Duration::from_secs(1),
    ));
    assert!(!budget.try_charge(1, now + Duration::from_secs(1)));
}

#[test]
fn rtcp_ingress_budget_retains_only_fractional_refill_below_capacity() {
    let now = Instant::now();
    let mut budget = RtcpIngressBudget::new(now);

    assert!(budget.try_charge(RTCP_INGRESS_BUDGET_CAPACITY_BYTES, now));
    assert!(!budget.try_charge(1, now + Duration::from_micros(125)));
    assert!(budget.try_charge(1, now + Duration::from_micros(250)));

    let saturated_at = now + Duration::from_secs(3) + Duration::from_micros(375);
    assert!(budget.try_charge(RTCP_INGRESS_BUDGET_CAPACITY_BYTES, saturated_at));
    assert!(!budget.try_charge(1, saturated_at + Duration::from_micros(125)));
    assert!(budget.try_charge(1, saturated_at + Duration::from_micros(250)));
}

fn test_source_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_321)
}
