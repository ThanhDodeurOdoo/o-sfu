#![no_main]

use std::str;

use libfuzzer_sys::fuzz_target;
use o_sfu::{
    runtime::testing::fuzz_decode_native_client_batch,
    signaling::auth::{WebSocketConnectClaims, verify},
};

const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

fuzz_target!(|data: &[u8]| {
    if let Ok(payload) = str::from_utf8(data) {
        let _ = fuzz_decode_native_client_batch(payload);
        let _ = verify::<WebSocketConnectClaims>(payload, TEST_AUTH_KEY);
    }
});
