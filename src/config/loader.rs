use std::env;

use anyhow::Result;

use super::{
    CodecConfig, auth::load_auth_config, codec_flags::load_media_codec_flags,
    codec_preferences::load_codec_preferences, diagnostics::load_diagnostics_config,
    feature_flags::load_runtime_feature_flags, http::load_http_config, settings::Config,
    telemetry::load_telemetry_config, transport::load_transport_config, user::load_user_config,
};

impl Config {
    /// # Errors
    ///
    /// Returns an error when a required operator environment variable is
    /// missing, an environment value cannot be parsed, a constrained value is
    /// outside its accepted range, telemetry export is requested without the
    /// required cargo feature, or transport cross-field validation rejects the
    /// advertised address, RTC port range, worker count, or room policy.
    pub fn from_env() -> Result<Self> {
        Self::from_var_lookup(|key| env::var(key).ok())
    }

    fn from_var_lookup(mut get_var: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let http = load_http_config(&mut get_var)?;
        let auth = load_auth_config(&mut get_var)?;
        let user = load_user_config(&mut get_var)?;
        let feature_flags = load_runtime_feature_flags(&mut get_var)?;
        let codec_flags = load_media_codec_flags(&mut get_var)?;
        let codec_preferences = load_codec_preferences(&mut get_var)?;
        let diagnostics = load_diagnostics_config(&mut get_var)?;
        let telemetry = load_telemetry_config(&mut get_var)?;
        let transport = load_transport_config(&mut get_var)?;
        Ok(Self {
            auth,
            http,
            user,
            transport,
            codecs: CodecConfig {
                flags: codec_flags,
                preferences: codec_preferences,
            },
            features: feature_flags,
            telemetry,
            diagnostics,
        })
    }
}

#[cfg(test)]
#[path = "TESTS/loader.rs"]
mod tests;
