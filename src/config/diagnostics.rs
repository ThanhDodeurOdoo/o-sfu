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
mod tests {
    use super::load_diagnostics_config;

    #[test]
    fn load_diagnostics_config_accepts_trimmed_bearer_token() -> anyhow::Result<()> {
        let config = load_diagnostics_config(|key| match key {
            "DIAGNOSTICS_AUTH_TOKEN" => Some("  bearer-token  ".to_owned()),
            _ => None,
        })?;
        assert_eq!(config.auth_token.as_deref(), Some("bearer-token"));
        Ok(())
    }

    #[test]
    fn load_diagnostics_config_rejects_empty_token() {
        let error = load_diagnostics_config(|key| match key {
            "DIAGNOSTICS_AUTH_TOKEN" => Some("   ".to_owned()),
            _ => None,
        })
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("DIAGNOSTICS_AUTH_TOKEN must not be empty")
        );
    }
}
