use o_sfu_protocol::shared::{AvailableFeatures, SessionId};
use uuid::Uuid;

use crate::config::RuntimeFeatureFlags;
use crate::runtime::transport_adapter::TransportSessionKey;

use super::{ChannelConfig, ChannelRuntimeContext, ChannelRuntimePolicy};

#[derive(Debug, Clone)]
struct ChannelIdentity {
    uuid: String,
    issuer: String,
    key: Option<String>,
}

impl ChannelIdentity {
    fn new(issuer: String, key: Option<String>) -> Self {
        Self {
            uuid: Uuid::new_v4().to_string(),
            issuer,
            key,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ChannelDefinition {
    runtime_id: u64,
    media_worker_id: usize,
    identity: ChannelIdentity,
    config: ChannelConfig,
    feature_flags: RuntimeFeatureFlags,
}

impl ChannelDefinition {
    #[must_use]
    pub(crate) fn new(
        runtime_context: ChannelRuntimeContext,
        runtime_policy: &ChannelRuntimePolicy,
        issuer: String,
        key: Option<String>,
        config: ChannelConfig,
    ) -> Self {
        Self {
            runtime_id: runtime_context.runtime,
            media_worker_id: runtime_context.media_worker,
            identity: ChannelIdentity::new(issuer, key),
            config,
            feature_flags: runtime_policy.feature_flags,
        }
    }

    #[must_use]
    pub(crate) fn uuid(&self) -> &str {
        &self.identity.uuid
    }

    #[must_use]
    pub(crate) fn issuer(&self) -> &str {
        &self.identity.issuer
    }

    #[must_use]
    pub(crate) fn key(&self) -> Option<&str> {
        self.identity.key.as_deref()
    }

    #[must_use]
    pub(crate) fn available_features(&self) -> AvailableFeatures {
        AvailableFeatures {
            rtc: self.config.web_rtc_enabled,
            transcription: self.feature_flags.transcription,
            audio_recording: self.feature_flags.audio_recording,
            video_recording: self.feature_flags.video_recording,
        }
    }

    #[must_use]
    pub(crate) fn transport_session_key(
        &self,
        session_id: &SessionId,
        connection_id: u64,
    ) -> TransportSessionKey {
        TransportSessionKey::new(
            self.runtime_id,
            self.media_worker_id,
            connection_id,
            session_id.clone(),
        )
    }

    #[must_use]
    pub(crate) const fn web_rtc_enabled(&self) -> bool {
        self.config.web_rtc_enabled
    }

    #[must_use]
    pub(crate) const fn recording_enabled(&self) -> bool {
        self.config.recording_address.is_some()
    }

    #[must_use]
    pub(crate) const fn feature_flags(&self) -> RuntimeFeatureFlags {
        self.feature_flags
    }

    #[must_use]
    pub(crate) const fn media_worker_id(&self) -> usize {
        self.media_worker_id
    }

    #[must_use]
    pub(crate) const fn runtime_id(&self) -> u64 {
        self.runtime_id
    }
}
