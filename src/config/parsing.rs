use std::str::FromStr;

use anyhow::{Result, anyhow, ensure};

pub(super) fn parse_optional_env<T>(
    mut get_var: impl FnMut(&str) -> Option<String>,
    key: &str,
    error_message: &str,
) -> Result<Option<T>>
where
    T: FromStr,
{
    get_var(key)
        .map(|value| {
            value
                .parse()
                .map_err(|_error| anyhow!(error_message.to_owned()))
        })
        .transpose()
}

pub(super) fn parse_optional_non_empty_env(
    mut get_var: impl FnMut(&str) -> Option<String>,
    key: &str,
) -> Result<Option<String>> {
    match get_var(key) {
        Some(value) => {
            let trimmed = value.trim();
            ensure!(!trimmed.is_empty(), "{key} must not be empty");
            Ok(Some(trimmed.to_owned()))
        }
        None => Ok(None),
    }
}
