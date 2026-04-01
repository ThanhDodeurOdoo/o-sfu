use o_sfu_router::Router;

use crate::{config::Config, signaling::PROTOCOL_VERSION};

#[derive(Debug)]
pub struct Runtime {
    pub config: Config,
    pub router: Router,
    pub protocol_version: u16,
}

impl Runtime {
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            router: Router::new(),
            protocol_version: PROTOCOL_VERSION,
        }
    }
}

pub async fn run() -> anyhow::Result<()> {
    let _runtime = Runtime::new(Config::from_env());
    Ok(())
}
