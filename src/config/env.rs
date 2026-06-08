use std::net::{IpAddr, SocketAddr};

use anyhow::{Context, Result, anyhow, ensure};

pub trait EnvParse: Sized {
    fn parse(key: &'static str, value: String) -> Result<Self>;
}

macro_rules! parse_from_str {
    ($type:ty, $name:literal) => {
        impl EnvParse for $type {
            fn parse(key: &'static str, value: String) -> Result<Self> {
                value
                    .parse()
                    .map_err(|_error| anyhow!("{key} must be a valid {}", $name))
            }
        }
    };
}

parse_from_str!(IpAddr, "IP address");
parse_from_str!(SocketAddr, "socket address");
parse_from_str!(u8, "u8");
parse_from_str!(u16, "u16");
parse_from_str!(u64, "u64");
parse_from_str!(usize, "usize");

impl EnvParse for bool {
    fn parse(key: &'static str, value: String) -> Result<Self> {
        value
            .parse()
            .map_err(|_error| anyhow!("{key} must be either `true` or `false`"))
    }
}

impl EnvParse for String {
    fn parse(_key: &'static str, value: String) -> Result<Self> {
        Ok(value)
    }
}

pub fn required<T>(get_var: &mut impl FnMut(&str) -> Option<String>, key: &'static str) -> Result<T>
where
    T: EnvParse,
{
    let value = get_var(key).with_context(|| format!("{key} env variable is required"))?;
    T::parse(key, value)
}

pub fn default<T>(
    get_var: &mut impl FnMut(&str) -> Option<String>,
    key: &'static str,
    default: T,
) -> Result<T>
where
    T: EnvParse,
{
    let Some(value) = get_var(key) else {
        return Ok(default);
    };
    T::parse(key, value)
}

pub fn optional<T>(
    get_var: &mut impl FnMut(&str) -> Option<String>,
    key: &'static str,
) -> Result<Option<T>>
where
    T: EnvParse,
{
    get_var(key).map_or_else(|| Ok(None), |value| Ok(Some(T::parse(key, value)?)))
}

pub fn positive<T>(key: &'static str, value: T) -> Result<T>
where
    T: From<u8> + PartialOrd,
{
    ensure!(value > T::from(0), "{key} must be greater than zero");
    Ok(value)
}

pub fn non_empty(key: &'static str, value: Option<String>) -> Result<Option<String>> {
    match value {
        Some(value) => {
            let trimmed = value.trim();
            ensure!(!trimmed.is_empty(), "{key} must not be empty");
            if trimmed.len() == value.len() {
                Ok(Some(value))
            } else {
                Ok(Some(trimmed.to_owned()))
            }
        }
        None => Ok(None),
    }
}

macro_rules! env_block {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $field:ident : $ty:ty = $kind:ident ($($args:tt)*) $(.check($check:path))*;
            )+
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, PartialEq, Eq)]
        $vis struct $name {
            $(
                $field: $ty,
            )+
        }

        impl $name {
            fn load(mut get_var: impl FnMut(&str) -> Option<String>) -> anyhow::Result<Self> {
                Ok(Self {
                    $(
                        $field: {
                            let _key = env_block!(@key $kind($($args)*));
                            let value = env_block!(@read &mut get_var, $ty, $kind($($args)*))?;
                            env_block!(@checks value, _key, $($check),*)
                        },
                    )+
                })
            }
        }
    };
    (@key required($key:expr)) => {
        $key
    };
    (@key default($key:expr, $default:expr)) => {
        $key
    };
    (@key optional($key:expr)) => {
        $key
    };
    (@read $get:expr, $ty:ty, required($key:expr)) => {
        crate::config::env::required::<$ty>($get, $key)
    };
    (@read $get:expr, $ty:ty, default($key:expr, $default:expr)) => {
        crate::config::env::default::<$ty>($get, $key, $default)
    };
    (@read $get:expr, $ty:ty, optional($key:expr)) => {
        crate::config::env::optional($get, $key)
    };
    (@checks $value:expr, $key:expr,) => {
        $value
    };
    (@checks $value:expr, $key:expr, $check:path $(, $tail:path)*) => {{
        let value = $check($key, $value)?;
        env_block!(@checks value, $key, $($tail),*)
    }};
}

pub(super) use env_block;

#[cfg(test)]
#[path = "TESTS/env.rs"]
mod tests;
