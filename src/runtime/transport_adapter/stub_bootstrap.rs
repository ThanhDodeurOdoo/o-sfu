use std::net::{IpAddr, Ipv4Addr};

use crate::runtime::transport_bootstrap::{
    SessionTransportBootstrap, TransportDtlsFingerprint, TransportDtlsFingerprintAlgorithm,
    TransportDtlsParameters, TransportDtlsRole, TransportEndpointBootstrap, TransportIceCandidate,
    TransportIceCandidateType, TransportIceParameters, TransportIceProtocol,
    TransportSctpParameters,
};

const STUB_STC_TRANSPORT_ID: &str = "stc-stub";
const STUB_CTS_TRANSPORT_ID: &str = "cts-stub";

pub(super) fn transport_bootstrap_payload(
    router_capabilities: &o_sfu_router::RtpCapabilities,
) -> SessionTransportBootstrap {
    SessionTransportBootstrap::new(
        router_capabilities,
        stub_transport_bootstrap(STUB_STC_TRANSPORT_ID),
        stub_transport_bootstrap(STUB_CTS_TRANSPORT_ID),
    )
}

fn stub_transport_bootstrap(id: &str) -> TransportEndpointBootstrap {
    TransportEndpointBootstrap {
        id: id.to_owned(),
        ice_parameters: TransportIceParameters {
            username_fragment: String::from("ufrag"),
            password: String::from("pwd"),
            ice_lite: true,
        },
        ice_candidates: vec![TransportIceCandidate {
            foundation: String::from("foundation"),
            priority: 1,
            ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            protocol: TransportIceProtocol::Udp,
            port: 40_000,
            candidate_type: TransportIceCandidateType::Host,
        }],
        dtls_parameters: TransportDtlsParameters {
            role: TransportDtlsRole::Auto,
            fingerprints: vec![TransportDtlsFingerprint {
                algorithm: TransportDtlsFingerprintAlgorithm::Sha256,
                value: String::from("AA:BB:CC"),
            }],
        },
        sctp_parameters: default_sctp_parameters(),
    }
}

fn default_sctp_parameters() -> TransportSctpParameters {
    super::super::transport_bootstrap::default_sctp_parameters()
}
