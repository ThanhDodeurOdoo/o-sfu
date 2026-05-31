use std::net::SocketAddr;

use anyhow::Result;

use super::{HttpConfig, env::env_block};

env_block! {
    struct HttpEnv {
        bind_address: SocketAddr = default(
            "BIND_ADDRESS",
            SocketAddr::from(([0, 0, 0, 0], 8070))
        );
        trust_proxy_headers: bool = default("PROXY", false);
    }
}

pub(super) fn load_http_config(get_var: impl FnMut(&str) -> Option<String>) -> Result<HttpConfig> {
    let env = HttpEnv::load(get_var)?;
    Ok(HttpConfig {
        bind_address: env.bind_address,
        trust_proxy_headers: env.trust_proxy_headers,
    })
}
