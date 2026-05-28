use std::fmt::{self, Display, Formatter};

use o_sfu_rfc::rtp::frame_marking;
use serde::{Deserialize, Serialize};

use crate::engine::UserId;

/// User-scoped stream identity supplied with a publish intent.
///
/// The room treats this value as an opaque slot name scoped by the publishing
/// user. Callers allocate the slot before entering core and pass media kind
/// plus room-policy metadata through the publish intent.
///
/// # Invariant
///
/// `UserStreamId` is not globally unique. It is unique only together with the
/// publishing user id. Room indexes that need publication identity must pair it
/// with the publishing user or use [`PublishedSourceId`] after a publish
/// commits.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UserStreamId(String);

impl UserStreamId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for UserStreamId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<&str> for UserStreamId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for UserStreamId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Stable room-domain identity for one logical published source.
///
/// A source id identifies the publication itself, not the SDP media section,
/// negotiated RID, RTP SSRC or transport media handle currently realizing it.
/// Room state should allocate it when a publish becomes a room-domain source
/// and keep using it across renegotiation or transport reattachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublishedSourceId(u64);

impl PublishedSourceId {
    #[must_use]
    pub fn allocate(next_source_id: &mut u64) -> Self {
        let source_id = Self(*next_source_id);
        *next_source_id = next_source_id.saturating_add(1);
        source_id
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for PublishedSourceId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "source-{}", self.0)
    }
}

/// Stable room-domain identity for one advertised source encoding
///
/// The id names an operating point of a source such as a simulcast `lo` or
/// `hi` layer. It does not encode a RID string or worker-local
/// route so selectors can keep pointing at the same encoding after transport
/// details are refreshed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceEncodingId(u64);

impl SourceEncodingId {
    #[must_use]
    pub fn allocate(next_encoding_id: &mut u64) -> Self {
        let encoding_id = Self(*next_encoding_id);
        *next_encoding_id = next_encoding_id.saturating_add(1);
        encoding_id
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for SourceEncodingId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "encoding-{}", self.0)
    }
}

/// Codec-native temporal layer id used by SVC operating points.
///
/// The current representation follows the RFC 9626 frame-marking
/// temporal-id range. Spatial identity remains modeled by the source encoding,
/// which keeps hybrid simulcast plus SVC as one selected encoding plus one
/// temporal ceiling instead of a second parallel source graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceTemporalLayerId(u8);

impl SourceTemporalLayerId {
    #[allow(
        dead_code,
        reason = "production SVC descriptors will construct temporal ids after RFC 9626 negotiation lands"
    )]
    #[must_use]
    pub const fn new(value: u8) -> Option<Self> {
        if frame_marking::is_valid_temporal_layer_id(value) {
            Some(Self(value))
        } else {
            None
        }
    }

    #[allow(
        dead_code,
        reason = "base-layer operating-point diagnostics are staged until production SVC selection is reachable"
    )]
    #[must_use]
    pub const fn base() -> Self {
        Self(frame_marking::BASE_LAYER_ID)
    }

    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self.0
    }
}

/// One source operating point selected by room or receiver policy.
///
/// The selected encoding carries the simulcast or spatial choice. The temporal
/// layer ceiling carries the first codec-native SVC dimension that can be
/// projected into transport packet gates when frame-marking metadata is present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceOperatingPoint {
    encoding_id: SourceEncodingId,
    max_temporal_layer_id: SourceTemporalLayerId,
}

impl SourceOperatingPoint {
    #[allow(
        dead_code,
        reason = "operating-point selectors are staged until negotiated temporal metadata is production-reachable"
    )]
    #[must_use]
    pub const fn new(
        encoding_id: SourceEncodingId,
        max_temporal_layer_id: SourceTemporalLayerId,
    ) -> Self {
        Self {
            encoding_id,
            max_temporal_layer_id,
        }
    }

    #[must_use]
    pub const fn encoding_id(self) -> SourceEncodingId {
        self.encoding_id
    }

    #[must_use]
    pub const fn max_temporal_layer_id(self) -> SourceTemporalLayerId {
        self.max_temporal_layer_id
    }
}

/// Publishing user authority attached to a source descriptor.
///
/// The user identifies the logical owner visible to room policy. Connection
/// freshness is tracked by producer and transport indexes, because source
/// descriptors are room-domain metadata rather than async commit guards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedSourceOwner {
    user_id: UserId,
}

impl PublishedSourceOwner {
    #[must_use]
    pub fn new(user_id: UserId) -> Self {
        Self { user_id }
    }

    #[must_use]
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }
}
