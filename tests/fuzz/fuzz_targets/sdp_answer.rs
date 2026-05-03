#![no_main]

//! Fuzz target for native SDP answer capability projection.
//!
//! The protocol/runtime answer path must reject malformed answers cleanly and
//! never panic while projecting router-native client capabilities from an
//! answered SDP.

use std::str;

use libfuzzer_sys::fuzz_target;
use o_sfu_core::server::transport::client_rtp_capabilities_from_answer;

fuzz_target!(|data: &[u8]| {
    let Ok(answer_sdp) = str::from_utf8(data) else {
        return;
    };
    let _ = client_rtp_capabilities_from_answer(answer_sdp);
});
