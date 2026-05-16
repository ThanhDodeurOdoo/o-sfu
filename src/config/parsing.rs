use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

use anyhow::{Context, Result, anyhow, ensure};

pub(super) trait EnvValue: FromStr {
    const TYPE_NAME: &'static str;
}

macro_rules! impl_env_value {
    ($type:ty, $name:literal) => {
        impl EnvValue for $type {
            const TYPE_NAME: &'static str = $name;
        }
    };
}

impl_env_value!(IpAddr, "IP address");
impl_env_value!(SocketAddr, "socket address");
impl_env_value!(u8, "u8");
impl_env_value!(u16, "u16");
impl_env_value!(u64, "u64");
impl_env_value!(usize, "usize");

pub(super) fn required_env(
    get_var: &mut impl FnMut(&str) -> Option<String>,
    key: &str,
) -> Result<String> {
    get_var(key).with_context(|| format!("{key} env variable is required"))
}

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

pub(super) fn parse_required_env<T>(
    get_var: &mut impl FnMut(&str) -> Option<String>,
    key: &str,
) -> Result<T>
where
    T: EnvValue,
{
    let value = required_env(get_var, key)?;
    parse_env_value(key, &value)
}

pub(super) fn parse_env_or_default<T>(
    get_var: &mut impl FnMut(&str) -> Option<String>,
    key: &str,
    default: T,
) -> Result<T>
where
    T: EnvValue,
{
    let Some(value) = get_var(key) else {
        return Ok(default);
    };
    parse_env_value(key, &value)
}

pub(super) fn parse_bool_env_or_default(
    get_var: &mut impl FnMut(&str) -> Option<String>,
    key: &str,
    default: bool,
) -> Result<bool> {
    Ok(parse_optional_env(
        get_var,
        key,
        &format!("{key} must be either `true` or `false`"),
    )?
    .unwrap_or(default))
}

pub(super) fn parse_positive_env_or_default<T>(
    get_var: &mut impl FnMut(&str) -> Option<String>,
    key: &str,
    default: T,
) -> Result<T>
where
    T: EnvValue + Copy + From<u8> + PartialOrd,
{
    let value = parse_env_or_default(get_var, key, default)?;
    ensure!(value > T::from(0), "{key} must be greater than zero");
    Ok(value)
}

fn parse_env_value<T>(key: &str, value: &str) -> Result<T>
where
    T: EnvValue,
{
    value
        .parse()
        .map_err(|_error| anyhow!("{key} must be a valid {}", T::TYPE_NAME))
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
