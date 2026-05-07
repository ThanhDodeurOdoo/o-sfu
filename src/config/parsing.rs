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

pub(super) fn parse_env_or_default<T>(
    get_var: &mut impl FnMut(&str) -> Option<String>,
    key: &str,
    type_name: &str,
    default: T,
) -> Result<T>
where
    T: FromStr,
{
    let Some(value) = get_var(key) else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_error| anyhow!("{key} must be a valid {type_name}"))
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
