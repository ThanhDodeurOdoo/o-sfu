//! Runtime-native source, encoding and selection vocabulary for published media.
//!
//! This module defines the room-domain source identity that room state,
//! transport projection, diagnostics and recording metadata are expected to
//! share. It is the vocabulary above SDP, browser APIs and worker-local media
//! handles: those layers may attach facts to a source, but they must not define
//! the source identity.
//!
//! A published media stream is modeled as one [`PublishedSourceId`] plus one or
//! more [`SourceEncodingId`] values. [`o_sfu_router::rtp::Mid`],
//! [`o_sfu_router::rtp::Rid`] and [`o_sfu_router::rtp::Ssrc`] stay as negotiated
//! or transport-facing attachment points. Keeping those identities separate
//! lets later same-room spillover and recording consume the same source
//! inventory without redefining it around local worker placement.
//!
//! Application layers should express stream-specific behavior by constructing
//! [`SourcePublishIntent`] values. Core policy reads the source
//! [`SourcePolicy`] carried by each source, never compatibility stream labels.
//!
//! # Upload layer profiles
//!
//! The server-defined upload ladder lives at the RTC offer edge as
//! upload-slot metadata, while this module stores the negotiated source
//! encodings that result from the answer.

mod descriptor;
mod diagnostics;
mod ids;
mod intent;
mod policy;
mod selection;

pub use descriptor::{
    PublishedSourceDescriptor, PublishedSourceDescriptorParts, SourceEncodingDescriptor,
    SourceEncodingDescriptorParts, SourceModelError,
};
pub use diagnostics::{OverBudgetExceptionReason, ReceiverVideoBudgetDiagnostics};
pub use ids::{
    PublishedSourceId, PublishedSourceOwner, SourceEncodingId, SourceOperatingPoint,
    SourceTemporalLayerId, UserStreamId,
};
pub use intent::{SourcePublishIntent, SourceSubscriptionIntent, SourceUnpublishIntent};
pub use policy::{
    ActiveSpeakerGroup, ActiveSpeakerPolicy, ActiveSpeakerSourceRole, PolicyPauseReason,
    SourceAdaptationPolicy, SourceLayoutPolicy, SourcePolicy, SourceRoomPolicySelector,
    SourceRoutePriority, UploadLayerPolicyRole,
};
pub use selection::{ConsumerSourceSelection, SourceSelector};

#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/support.rs"]
pub mod test_support;

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
