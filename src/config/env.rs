use std::net::{IpAddr, SocketAddr};

use anyhow::{Context, Result, anyhow, ensure};

pub(in crate::config) trait EnvParse: Sized {
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

pub(in crate::config) fn required<T>(
    get_var: &mut impl FnMut(&str) -> Option<String>,
    key: &'static str,
) -> Result<T>
where
    T: EnvParse,
{
    let value = get_var(key).with_context(|| format!("{key} env variable is required"))?;
    T::parse(key, value)
}

pub(in crate::config) fn default<T>(
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

pub(in crate::config) fn optional<T>(
    get_var: &mut impl FnMut(&str) -> Option<String>,
    key: &'static str,
) -> Result<Option<T>>
where
    T: EnvParse,
{
    get_var(key).map_or_else(|| Ok(None), |value| Ok(Some(T::parse(key, value)?)))
}

pub(in crate::config) fn positive<T>(key: &'static str, value: T) -> Result<T>
where
    T: From<u8> + PartialOrd,
{
    ensure!(value > T::from(0), "{key} must be greater than zero");
    Ok(value)
}

pub(in crate::config) fn non_empty(
    key: &'static str,
    value: Option<String>,
) -> Result<Option<String>> {
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

pub(in crate::config) use env_block;

#[cfg(test)]
mod tests {
    use super::{non_empty, positive};

    fn local_check(key: &'static str, value: String) -> anyhow::Result<String> {
        anyhow::ensure!(value == "local", "{key} must pass the local check");
        Ok(value)
    }

    env_block! {
        struct TestEnv {
            required: String = required("REQUIRED_ENV");
            flag: bool = default("FLAG_ENV", false);
            count: usize = default("COUNT_ENV", 1).check(positive);
            token: Option<String> = optional("TOKEN_ENV").check(non_empty);
        }
    }

    env_block! {
        struct LocalCheckEnv {
            value: String = required("LOCAL_CHECK_ENV").check(local_check);
        }
    }

    #[test]
    fn env_block_loads_required_default_and_optional_values() {
        let env = TestEnv::load(|key| match key {
            "REQUIRED_ENV" => Some("value".to_owned()),
            "FLAG_ENV" => Some("true".to_owned()),
            "COUNT_ENV" => Some("4".to_owned()),
            "TOKEN_ENV" => Some("  token  ".to_owned()),
            _ => None,
        });

        assert_eq!(
            env.ok(),
            Some(TestEnv {
                required: "value".to_owned(),
                flag: true,
                count: 4,
                token: Some("token".to_owned()),
            })
        );
    }

    #[test]
    fn env_block_reports_missing_required_values() {
        let error = TestEnv::load(|_| None).err().map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("REQUIRED_ENV env variable is required")
        );
    }

    #[test]
    fn env_block_reports_invalid_bools() {
        let error = TestEnv::load(|key| match key {
            "REQUIRED_ENV" => Some("value".to_owned()),
            "FLAG_ENV" => Some("yes".to_owned()),
            _ => None,
        })
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("FLAG_ENV must be either `true` or `false`")
        );
    }

    #[test]
    fn env_block_applies_positive_validation() {
        let error = TestEnv::load(|key| match key {
            "REQUIRED_ENV" => Some("value".to_owned()),
            "COUNT_ENV" => Some("0".to_owned()),
            _ => None,
        })
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("COUNT_ENV must be greater than zero")
        );
    }

    #[test]
    fn env_block_rejects_empty_non_empty_options() {
        let error = TestEnv::load(|key| match key {
            "REQUIRED_ENV" => Some("value".to_owned()),
            "TOKEN_ENV" => Some("   ".to_owned()),
            _ => None,
        })
        .err()
        .map(|error| error.to_string());

        assert_eq!(error.as_deref(), Some("TOKEN_ENV must not be empty"));
    }

    #[test]
    fn env_block_accepts_call_site_validators() {
        let env = LocalCheckEnv::load(|key| match key {
            "LOCAL_CHECK_ENV" => Some("local".to_owned()),
            _ => None,
        });

        assert_eq!(
            env.ok(),
            Some(LocalCheckEnv {
                value: "local".to_owned()
            })
        );
    }
}
