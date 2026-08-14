//! pure routed topology plus the RTP models used at its boundary
#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod diagnostic;
pub(crate) mod error;
pub(crate) mod ids;
pub(crate) mod rtp;
pub(crate) mod rtp_negotiation;
pub(crate) mod topology;

pub use o_sfu_rfc::webrtc::MediaKind;

pub use self::{
    error::RouterError,
    ids::{ConnectionId, ConsumerId, MediaWorkerId, ProducerId, RouterId},
    rtp::{
        CodecSetting, HeaderExtension, HeaderExtensionUri, MediaCapabilities, MediaCodec,
        MediaCodecCapability, MediaFormat, MediaStream, PayloadType, RtcpFeedback,
        RtcpFeedbackKind,
    },
    topology::Router,
};
