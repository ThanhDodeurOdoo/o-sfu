#![no_main]

//! Fuzz target for the `/v1/disconnect` body and JWT verification boundary.
//!
//! The HTTP route first rejects non-UTF-8 bodies, then verifies the raw body
//! as `HttpDisconnectClaims`.

use std::str;

use libfuzzer_sys::fuzz_target;
use o_sfu::public::auth::{HttpDisconnectClaims, verify};

const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

fuzz_target!(|body: &[u8]| {
    let Ok(token) = str::from_utf8(body) else {
        return;
    };
    let _ = verify::<HttpDisconnectClaims>(token, TEST_AUTH_KEY);
});
