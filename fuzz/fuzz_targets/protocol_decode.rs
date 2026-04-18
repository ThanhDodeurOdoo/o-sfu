#![no_main]

//! Fuzz target for the signaling layer's primary input paths.
//!
//! This target ensures that untrusted data from the WebSocket connection
//! does not cause panics or resource leaks during decoding or authentication.

use std::str;

use libfuzzer_sys::fuzz_target;
use o_sfu::{
    public::auth::{WebSocketConnectClaims, verify},
    public::client_batch::decode_client_batch,
};

const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

fuzz_target!(|data: &[u8]| {
    if let Ok(payload) = str::from_utf8(data) {
        // Tests the robustness of the signaling protocol parser against malformed JSON or
        // unexpected message structures. This is a critical entry point for all client messages.
        let _ = decode_client_batch(payload);

        // Tests the JWT verification pipeline, including Base64 decoding of segments,
        // header/claims parsing, and timestamp validation.
        let _ = verify::<WebSocketConnectClaims>(payload, TEST_AUTH_KEY);
    }
});
