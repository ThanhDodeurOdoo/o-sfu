use anyhow::Result;

use super::parsing::parse_optional_non_empty_env;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DiagnosticsConfig {
    pub auth_token: Option<String>,
}

pub(super) fn load_diagnostics_config(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<DiagnosticsConfig> {
    Ok(DiagnosticsConfig {
        auth_token: parse_optional_non_empty_env(&mut get_var, "DIAGNOSTICS_AUTH_TOKEN")?,
    })
}

#[cfg(test)]
mod tests {
    use super::load_diagnostics_config;

    #[test]
    fn load_diagnostics_config_accepts_trimmed_bearer_token() {
        let config = load_diagnostics_config(|key| match key {
            "DIAGNOSTICS_AUTH_TOKEN" => Some("  bearer-token  ".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.auth_token.as_deref(), Some("bearer-token"));
    }

    #[test]
    fn load_diagnostics_config_rejects_empty_token() {
        let config = load_diagnostics_config(|key| match key {
            "DIAGNOSTICS_AUTH_TOKEN" => Some("   ".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }
}
