use crate::{config::Config, signaling::CURRENT_WIRE_PROTOCOL_VERSION};

#[derive(Debug)]
pub struct Runtime {
    pub config: Config,
    pub current_wire_protocol_version: u16,
}

impl Runtime {
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            current_wire_protocol_version: CURRENT_WIRE_PROTOCOL_VERSION,
        }
    }
}

/// # Errors
///
/// Returns an error once runtime bootstrap starts performing fallible I/O.
pub fn run() -> anyhow::Result<()> {
    let _runtime = Runtime::new(Config::from_env());
    Ok(())
}
