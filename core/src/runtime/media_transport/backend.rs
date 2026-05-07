//! Backend selection hidden behind [`MediaTransport`](super::MediaTransport).
//!
//! The opaque media transport handle is the only type above this module that
//! implements the transport concern traits. This enum keeps production RTC
//! shard ownership and deterministic fake transport selection below that
//! orchestration boundary.

#[cfg(any(test, feature = "testing-transport"))]
use std::sync::Arc;

use super::runtime_adapter::RtcTransport;
#[cfg(any(test, feature = "testing-transport"))]
use crate::runtime::media_transport::test_support::FakeMediaTransport;

#[derive(Debug, Clone)]
pub(super) enum MediaTransportBackend {
    Rtc(RtcTransport),
    #[cfg(any(test, feature = "testing-transport"))]
    Fake(Arc<FakeMediaTransport>),
}

impl MediaTransportBackend {
    pub(super) const fn from_rtc(transport: RtcTransport) -> Self {
        Self::Rtc(transport)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn from_fake(transport: Arc<FakeMediaTransport>) -> Self {
        Self::Fake(transport)
    }
}
