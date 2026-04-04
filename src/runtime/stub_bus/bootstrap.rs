use serde_json::json;

use crate::{
    rfc::webrtc,
    runtime::transport_bootstrap,
    signaling::{
        current_protocol::CurrentTransportBootstrapPayload,
        webrtc::{
            DtlsFingerprint, DtlsParameters, IceCandidate, IceParameters, TransportBootstrap,
        },
    },
};

const STUB_STC_TRANSPORT_ID: &str = "stc-stub";
const STUB_CTS_TRANSPORT_ID: &str = "cts-stub";

pub(super) fn transport_bootstrap_payload(
    router_capabilities: &o_sfu_router::RtpCapabilities,
) -> CurrentTransportBootstrapPayload {
    transport_bootstrap::transport_bootstrap_payload(
        router_capabilities,
        stub_transport_bootstrap(STUB_STC_TRANSPORT_ID),
        stub_transport_bootstrap(STUB_CTS_TRANSPORT_ID),
    )
}

fn stub_transport_bootstrap(id: &str) -> TransportBootstrap {
    TransportBootstrap {
        id: id.to_owned(),
        ice_parameters: IceParameters(json!({
            "usernameFragment": "ufrag",
            "password": "pwd",
            "iceLite": true
        })),
        ice_candidates: vec![IceCandidate {
            foundation: String::from("foundation"),
            priority: 1,
            ip: String::from("203.0.113.10"),
            protocol: String::from(webrtc::ICE_TRANSPORT_UDP),
            port: 40_000,
            candidate_type: String::from(webrtc::ICE_CANDIDATE_TYPE_HOST),
        }],
        dtls_parameters: DtlsParameters {
            role: String::from("auto"),
            fingerprints: vec![DtlsFingerprint {
                algorithm: String::from("sha-256"),
                value: String::from("AA:BB:CC"),
            }],
        },
        sctp_parameters: transport_bootstrap::default_sctp_parameters(),
    }
}
