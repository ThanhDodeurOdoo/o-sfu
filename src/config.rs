#[derive(Debug, Clone)]
pub struct Config {
    pub bind_address: String,
}

impl Config {
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".to_owned(),
        }
    }
}
