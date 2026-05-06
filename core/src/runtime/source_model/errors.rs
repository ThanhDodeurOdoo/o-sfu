use thiserror::Error;

use super::{PublishedSourceId, SourceEncodingId};

/// Rejection returned while assembling a source descriptor.
///
/// # Error handling guidance
///
/// These are construction-time domain errors. They should be handled before a
/// publish becomes authoritative in room state. They are not transport
/// failures and should not be retried without rebuilding the source descriptor
/// from valid runtime facts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SourceModelError {
    #[error("published source {source_id} has no advertised encoding")]
    SourceWithoutEncodings { source_id: PublishedSourceId },
    #[error(
        "encoding {encoding_id} belongs to {encoding_source_id}, not published source {source_id}"
    )]
    EncodingSourceMismatch {
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        encoding_source_id: PublishedSourceId,
    },
}
