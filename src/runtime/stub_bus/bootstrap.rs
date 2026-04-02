use serde_json::json;

use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    webrtc::{
        DtlsParameters, IceCandidate, IceParameters, PublishOptions, PublishOptionsByMediaKind,
        RtpCapabilities, SctpParameters, TransportBootstrap,
    },
};

pub(super) const STUB_SERVER_BUS_ID: u64 = 0;
const STUB_STC_TRANSPORT_ID: &str = "stc-stub";
const STUB_CTS_TRANSPORT_ID: &str = "cts-stub";

pub(super) fn stub_transport_bootstrap_payload() -> CurrentTransportBootstrapPayload {
    CurrentTransportBootstrapPayload {
        router_capabilities: RtpCapabilities(json!({
            "codecs": [],
            "headerExtensions": []
        })),
        download_transport: stub_transport_bootstrap(STUB_STC_TRANSPORT_ID),
        upload_transport: stub_transport_bootstrap(STUB_CTS_TRANSPORT_ID),
        publish_options_by_media_kind: PublishOptionsByMediaKind {
            audio: PublishOptions(json!({
                "stopTracks": false
            })),
            video: PublishOptions(json!({
                "stopTracks": false,
                "zeroRtpOnPause": true
            })),
        },
    }
}

fn stub_transport_bootstrap(id: &str) -> TransportBootstrap {
    TransportBootstrap {
        id: id.to_owned(),
        ice_parameters: IceParameters(json!({
            "usernameFragment": "ufrag",
            "password": "pwd",
            "iceLite": true
        })),
        ice_candidates: vec![IceCandidate(json!({
            "foundation": "foundation",
            "priority": 1,
            "ip": "203.0.113.10",
            "protocol": "udp",
            "port": 40000,
            "type": "host"
        }))],
        dtls_parameters: DtlsParameters(json!({
            "role": "auto",
            "fingerprints": [{
                "algorithm": "sha-256",
                "value": "AA:BB:CC"
            }]
        })),
        sctp_parameters: SctpParameters(json!({
            "port": 5000,
            "OS": 1024,
            "MIS": 1024,
            "maxMessageSize": 262_144
        })),
    }
}
