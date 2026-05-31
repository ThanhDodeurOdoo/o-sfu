use anyhow::Result;

use super::{
    AuthConfig, DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
    DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN,
    env::{env_block, positive},
};

env_block! {
    struct AuthEnv {
        key: String = required("AUTH_KEY");
        authentication_timeout_ms: u64 = default("AUTHENTICATION_TIMEOUT_MS", 10_000);
        max_pre_auth_websocket_sessions: usize = default(
            "MAX_PRE_AUTH_WEBSOCKET_SESSIONS",
            DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS
        ).check(positive);
        max_pre_auth_websocket_sessions_per_origin: usize = default(
            "MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN",
            DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN
        ).check(positive);
    }
}

pub(super) fn load_auth_config(get_var: impl FnMut(&str) -> Option<String>) -> Result<AuthConfig> {
    let env = AuthEnv::load(get_var)?;
    Ok(AuthConfig {
        key: env.key,
        authentication_timeout_ms: env.authentication_timeout_ms,
        max_pre_auth_websocket_sessions: env.max_pre_auth_websocket_sessions,
        max_pre_auth_websocket_sessions_per_origin: env.max_pre_auth_websocket_sessions_per_origin,
    })
}
