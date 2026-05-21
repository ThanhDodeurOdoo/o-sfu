//! Application-facing media-core imports.
//!
//! This is the supported front door for configuration, sessions, negotiation,
//! source intent and caller-facing outcomes. Concrete room, diagnostics and
//! transport integration remain under [`crate::server`].

pub use crate::{
    Bitrate, ConnectionId, RoomInstanceId,
    options::{
        AudioCodecPreference, CodecOptions, CodecPreferences, CoreOptions, LocalSpilloverPolicy,
        LocalSpilloverPolicyError, LocalSpilloverPolicyParts, MediaCodecFlags, MediaOptions,
        ObservabilityOptions, RoomMediaLimits, RoomMediaLimitsError, RoomSpilloverMode,
        RoomWorkerPolicy, RoutingOptions, RtcPortRange, RuntimeFeatureFlags, SessionBitrateLimits,
        VideoBitrateLimits, VideoCodecPreference,
    },
    room::{
        MediaSessionContext, PublicationActivity, PublicationActivityOutcome, PublishStageOutcome,
        RollbackStagedPublishOutcome, SessionNegotiationOutcome, SubscriptionUpdateOutcome,
        TransportEffectOutcome, UnpublishOutcome, UserInfoRefresh,
    },
    runtime::{
        media_transport::TransportSessionHealth,
        source_model::{
            ActiveSpeakerGroup, ActiveSpeakerPolicy, ActiveSpeakerSourceRole,
            SourceAdaptationPolicy, SourceLayoutPolicy, SourcePolicy, SourcePublishIntent,
            SourceRoomPolicySelector, SourceSubscriptionIntent, UploadLayerPolicyRole,
            UserStreamId,
        },
    },
    sfu::{
        InitialOffer, MediaSession, NegotiationOffer, SfuCore, SfuCoreError, UploadEncoding,
        UploadSlot,
    },
};
