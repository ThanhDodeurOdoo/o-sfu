use anyhow::Result;

use super::env::{Env, non_empty};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DiagnosticsConfig {
    pub auth_token: Option<String>,
}

impl DiagnosticsConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        Ok(Self {
            auth_token: env
                .var("DIAGNOSTICS_AUTH_TOKEN")
                .check(non_empty)
                .optional()?,
        })
    }
}

#[cfg(test)]
#[path = "TESTS/diagnostics.rs"]
mod tests;
