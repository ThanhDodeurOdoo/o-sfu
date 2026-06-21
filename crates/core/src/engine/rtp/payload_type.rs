//! local payload type bindings advertised to browser peers
//!
//! values must stay valid for RTP/RTCP muxed sessions

use o_sfu_rfc::rtp::{AvpStaticPayloadType, PayloadType};

pub const PCMU: PayloadType = PayloadType::new(AvpStaticPayloadType::Pcmu.as_u8());
pub const PCMA: PayloadType = PayloadType::new(AvpStaticPayloadType::Pcma.as_u8());
pub const OPUS: PayloadType = PayloadType::new(111);
pub const VP8: PayloadType = PayloadType::new(96);
pub const H265: PayloadType = PayloadType::new(115);
pub const VP9_PROFILE_0: PayloadType = PayloadType::new(116);
pub const VP9_PROFILE_0_RTX: PayloadType = PayloadType::new(117);
pub const VP9_PROFILE_2: PayloadType = PayloadType::new(118);
pub const VP9_PROFILE_2_RTX: PayloadType = PayloadType::new(119);
pub const AV1: PayloadType = PayloadType::new(120);
