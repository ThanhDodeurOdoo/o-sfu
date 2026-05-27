use super::{CodecOptions, MediaOptions, ObservabilityOptions, RoutingOptions};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoreOptions {
    pub media: MediaOptions,
    pub routing: RoutingOptions,
    pub codecs: CodecOptions,
    pub observability: ObservabilityOptions,
}

impl CoreOptions {
    #[must_use]
    pub const fn new(
        media: MediaOptions,
        routing: RoutingOptions,
        codecs: CodecOptions,
        observability: ObservabilityOptions,
    ) -> Self {
        Self {
            media,
            routing,
            codecs,
            observability,
        }
    }
}
