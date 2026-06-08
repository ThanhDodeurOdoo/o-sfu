use anyhow::Result;

use super::env::{env_block, non_empty};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DiagnosticsConfig {
    pub auth_token: Option<String>,
}

env_block! {
    struct DiagnosticsEnv {
        auth_token: Option<String> = optional("DIAGNOSTICS_AUTH_TOKEN").check(non_empty);
    }
}

pub(super) fn load_diagnostics_config(
    get_var: impl FnMut(&str) -> Option<String>,
) -> Result<DiagnosticsConfig> {
    let env = DiagnosticsEnv::load(get_var)?;
    Ok(DiagnosticsConfig {
        auth_token: env.auth_token,
    })
}

#[cfg(test)]
#[path = "TESTS/diagnostics.rs"]
mod tests;
