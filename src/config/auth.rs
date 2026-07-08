use anyhow::Result;

use super::{
    AuthConfig, DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
    DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN,
    env::{Env, positive},
};

impl AuthConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        Ok(Self {
            key: env.var("AUTH_KEY").required()?,
            authentication_timeout_ms: env.var("AUTHENTICATION_TIMEOUT_MS").default(10_000)?,
            max_pre_auth_websocket_sessions: env
                .var("MAX_PRE_AUTH_WEBSOCKET_SESSIONS")
                .check(positive)
                .default(DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS)?,
            max_pre_auth_websocket_sessions_per_origin: env
                .var("MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN")
                .check(positive)
                .default(DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN)?,
        })
    }
}
