use crate::config::{
    AuthConfig, Config, DiagnosticsConfig, HttpConfig, RuntimeFeatureFlags, UserConfig,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeConfig {
    pub(crate) auth: AuthConfig,
    pub(crate) http: HttpConfig,
    pub(crate) user: UserConfig,
    pub(crate) diagnostics: DiagnosticsConfig,
}

impl RuntimeConfig {
    #[must_use]
    pub(crate) fn from_config(config: &Config) -> Self {
        Self {
            auth: config.auth.clone(),
            http: config.http.clone(),
            user: config.user,
            diagnostics: config.diagnostics.clone(),
        }
    }
}

pub(crate) const fn effective_feature_flags(features: RuntimeFeatureFlags) -> RuntimeFeatureFlags {
    RuntimeFeatureFlags {
        transcription: features.transcription
            && (features.audio_recording || features.video_recording),
        audio_recording: features.audio_recording,
        video_recording: features.video_recording,
    }
}

#[cfg(test)]
#[path = "TESTS/options.rs"]
mod tests;
