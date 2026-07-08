use anyhow::Result;

use super::{
    UserConfig,
    env::{Env, positive},
};
use crate::core::server::room::{
    DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
};

impl UserConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        Ok(Self {
            room_size: env.var("ROOM_SIZE").check(positive).default(100)?,
            timeout_ms: env.var("USER_TIMEOUT_MS").check(positive).default(10_000)?,
            ping_interval_ms: env
                .var("PING_INTERVAL_MS")
                .check(positive)
                .default(60_000)?,
            outbound_queue_capacity: env
                .var("USER_OUTBOUND_QUEUE_CAPACITY")
                .check(positive)
                .default(DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY)?,
            outbound_queue_byte_capacity: env
                .var("USER_OUTBOUND_QUEUE_BYTE_CAPACITY")
                .check(positive)
                .default(DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY)?,
        })
    }
}
