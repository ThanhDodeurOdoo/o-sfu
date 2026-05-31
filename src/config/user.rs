use anyhow::Result;

use super::{
    UserConfig,
    env::{env_block, positive},
};
use crate::core::server::room::{
    DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
};

env_block! {
    struct UserEnv {
        room_size: usize = default("ROOM_SIZE", 100).check(positive);
        timeout_ms: u64 = default("USER_TIMEOUT_MS", 10_000).check(positive);
        ping_interval_ms: u64 = default("PING_INTERVAL_MS", 60_000).check(positive);
        outbound_queue_capacity: usize = default(
            "USER_OUTBOUND_QUEUE_CAPACITY",
            DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY
        ).check(positive);
        outbound_queue_byte_capacity: usize = default(
            "USER_OUTBOUND_QUEUE_BYTE_CAPACITY",
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY
        ).check(positive);
    }
}

pub(super) fn load_user_config(get_var: impl FnMut(&str) -> Option<String>) -> Result<UserConfig> {
    let env = UserEnv::load(get_var)?;
    Ok(UserConfig {
        room_size: env.room_size,
        timeout_ms: env.timeout_ms,
        ping_interval_ms: env.ping_interval_ms,
        outbound_queue_capacity: env.outbound_queue_capacity,
        outbound_queue_byte_capacity: env.outbound_queue_byte_capacity,
    })
}
