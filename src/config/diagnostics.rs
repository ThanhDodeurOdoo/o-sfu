use anyhow::Result;
use axum::http::HeaderValue;

use super::env::{Env, non_empty};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DiagnosticsConfig {
    /// Bearer token required on every listener when configured.
    pub auth_token: Option<String>,
}

impl DiagnosticsConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        let auth_token = env
            .var("DIAGNOSTICS_AUTH_TOKEN")
            .check(non_empty)
            .check(|key, value| {
                anyhow::ensure!(
                    HeaderValue::try_from(&value).is_ok() && value.is_ascii(),
                    "{key} contains invalid HTTP header-value characters"
                );
                Ok(value)
            })
            .optional()?;
        Ok(Self { auth_token })
    }
}

#[cfg(test)]
#[path = "TESTS/diagnostics.rs"]
mod tests;
