use crate::{config::Config, signaling::PROTOCOL_VERSION};

#[derive(Debug)]
pub struct Runtime {
    pub config: Config,
    pub protocol_version: u16,
}

impl Runtime {
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            protocol_version: PROTOCOL_VERSION,
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
