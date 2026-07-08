use std::net::SocketAddr;

use anyhow::Result;

use super::{HttpConfig, env::Env};

impl HttpConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        Ok(Self {
            bind_address: env
                .var("BIND_ADDRESS")
                .default(SocketAddr::from(([0, 0, 0, 0], 8070)))?,
            trust_proxy_headers: env.var("PROXY").default(false)?,
        })
    }
}
