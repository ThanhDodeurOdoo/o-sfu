//! Compatibility shim for RTC-edge simulcast helpers.
//!
//! Codec-specific simulcast ownership lives under `rtc_engine::simulcast`.
//! This module preserves the previous worker import path while negotiation and
//! publication callers move at their own pace.

pub(super) use super::simulcast::{
    NegotiatedRid, bootstrap_recv_simulcast, bootstrap_upload_encodings,
    publish_recv_simulcast_or_default, publish_upload_encodings_or_default, send_rids_for_mid,
};
