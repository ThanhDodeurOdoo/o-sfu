use std::{
    fmt,
    net::{IpAddr, SocketAddr},
};

use anyhow::{Context, Result, anyhow, ensure};

type Lookup<'a> = dyn Fn(&str) -> Option<String> + 'a;

#[derive(Clone, Copy)]
pub(super) struct EnvKey(&'static str);

impl EnvKey {
    fn new(key: &'static str) -> Self {
        Self(key)
    }

    fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for EnvKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

pub(super) struct EnvValue {
    key: EnvKey,
    raw: String,
}

impl EnvValue {
    pub(super) fn key(&self) -> EnvKey {
        self.key
    }

    pub(super) fn as_str(&self) -> &str {
        &self.raw
    }

    fn into_raw(self) -> String {
        self.raw
    }
}

pub(super) struct Env<'a> {
    lookup: Box<Lookup<'a>>,
}

impl<'a> Env<'a> {
    pub(super) fn new(get_var: impl Fn(&str) -> Option<String> + 'a) -> Self {
        Self {
            lookup: Box::new(get_var),
        }
    }

    pub(super) fn var<T>(&self, key: &'static str) -> Var<'a, '_, T> {
        Var {
            lookup: self.lookup.as_ref(),
            key: EnvKey::new(key),
            checks: Vec::new(),
            aliases: Vec::new(),
        }
    }
}

pub(super) struct Var<'env, 'lookup, T> {
    lookup: &'lookup Lookup<'env>,
    key: EnvKey,
    checks: Vec<fn(EnvKey, T) -> Result<T>>,
    aliases: Vec<EnvKey>,
}

impl<T> Var<'_, '_, T>
where
    T: EnvParse,
{
    pub(super) fn check(mut self, check: fn(EnvKey, T) -> Result<T>) -> Self {
        self.checks.push(check);
        self
    }

    pub(super) fn alias(mut self, alias: &'static str) -> Self {
        self.aliases.push(EnvKey::new(alias));
        self
    }

    pub(super) fn required(self) -> Result<T> {
        let value = self
            .load()
            .with_context(|| format!("{} env variable is required", self.key))?;
        self.parse(value)
    }

    pub(super) fn default(self, default: T) -> Result<T> {
        let Some(value) = self.load() else {
            return self.validate(self.key, default);
        };
        self.parse(value)
    }

    pub(super) fn optional(self) -> Result<Option<T>> {
        self.load().map(|value| self.parse(value)).transpose()
    }

    fn load(&self) -> Option<EnvValue> {
        self.load_key(self.key).or_else(|| {
            self.aliases
                .iter()
                .copied()
                .find_map(|alias| self.load_key(alias))
        })
    }

    fn load_key(&self, key: EnvKey) -> Option<EnvValue> {
        (self.lookup)(key.as_str()).map(|raw| EnvValue { key, raw })
    }

    fn parse(&self, value: EnvValue) -> Result<T> {
        let key = value.key();
        self.validate(key, T::parse(value)?)
    }

    fn validate(&self, key: EnvKey, mut value: T) -> Result<T> {
        for check in &self.checks {
            value = check(key, value)?;
        }
        Ok(value)
    }
}

pub(super) trait EnvParse: Sized {
    fn parse(value: EnvValue) -> Result<Self>;
}

macro_rules! parse_from_str {
    ($type:ty, $name:literal) => {
        impl EnvParse for $type {
            fn parse(value: EnvValue) -> Result<Self> {
                let key = value.key();
                value
                    .into_raw()
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
    fn parse(value: EnvValue) -> Result<Self> {
        let key = value.key();
        value
            .into_raw()
            .parse()
            .map_err(|_error| anyhow!("{key} must be either `true` or `false`"))
    }
}

impl EnvParse for String {
    fn parse(value: EnvValue) -> Result<Self> {
        Ok(value.into_raw())
    }
}

pub(super) fn positive<T>(key: EnvKey, value: T) -> Result<T>
where
    T: From<u8> + PartialOrd,
{
    ensure!(value > T::from(0), "{key} must be greater than zero");
    Ok(value)
}

pub(super) fn non_empty(key: EnvKey, value: String) -> Result<String> {
    let trimmed = value.trim();
    ensure!(!trimmed.is_empty(), "{key} must not be empty");
    if trimmed.len() == value.len() {
        Ok(value)
    } else {
        Ok(trimmed.to_owned())
    }
}

#[cfg(test)]
#[path = "TESTS/env.rs"]
mod tests;
