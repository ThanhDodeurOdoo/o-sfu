use o_sfu_rfc::webrtc::MediaKind;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MediaOutcome {
    signals: Vec<MediaSignal>,
    end_reason: Option<MediaEndReason>,
}

impl MediaOutcome {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            signals: Vec::new(),
            end_reason: None,
        }
    }

    #[must_use]
    pub fn with_signal(mut self, signal: MediaSignal) -> Self {
        self.signals.push(signal);
        self
    }

    #[must_use]
    pub fn with_end_reason(mut self, reason: MediaEndReason) -> Self {
        self.end_reason = Some(reason);
        self
    }

    #[must_use]
    pub fn signals(&self) -> &[MediaSignal] {
        &self.signals
    }

    #[must_use]
    pub const fn end_reason(&self) -> Option<MediaEndReason> {
        self.end_reason
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaSignal {
    InitialOffer(MediaNegotiation),
    RenegotiationOffer(MediaNegotiation),
    RouteUpdate(MediaRouteUpdate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaNegotiation {
    pub description: MediaSessionDescription,
    pub upload_slots: Vec<MediaUploadSlot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaSessionDescription {
    pub sdp: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaUploadSlot {
    pub kind: MediaKind,
    pub encodings: Vec<MediaUploadEncoding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaUploadEncoding {
    pub rid: Option<String>,
    pub max_bitrate_bps: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaRouteUpdate {
    pub affected_publication: MediaPublicationId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MediaPublicationId(String);

impl MediaPublicationId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaEndReason {
    EndpointRemoved,
    NegotiationFailed,
    TransportDisconnected,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_outcome_preserves_signal_order() {
        let initial = MediaSignal::InitialOffer(MediaNegotiation {
            description: MediaSessionDescription {
                sdp: String::from("v=0"),
            },
            upload_slots: vec![MediaUploadSlot {
                kind: MediaKind::Video,
                encodings: vec![MediaUploadEncoding {
                    rid: Some(String::from("h")),
                    max_bitrate_bps: Some(1_000_000),
                }],
            }],
        });
        let route_update = MediaSignal::RouteUpdate(MediaRouteUpdate {
            affected_publication: MediaPublicationId::new("publication-1"),
        });

        let outcome = MediaOutcome::new()
            .with_signal(initial.clone())
            .with_signal(route_update.clone());

        assert_eq!(outcome.signals(), &[initial, route_update]);
    }

    #[test]
    fn media_outcome_records_end_reason() {
        let outcome = MediaOutcome::new().with_end_reason(MediaEndReason::NegotiationFailed);

        assert_eq!(
            outcome.end_reason(),
            Some(MediaEndReason::NegotiationFailed)
        );
        assert!(outcome.signals().is_empty());
    }
}
