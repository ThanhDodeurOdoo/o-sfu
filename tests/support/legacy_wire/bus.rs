use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurrentBusOrigin {
    Client,
    Server,
}

impl CurrentBusOrigin {
    const fn as_prefix(self) -> &'static str {
        match self {
            Self::Client => "c",
            Self::Server => "s",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CurrentBusRequestId(String);

impl CurrentBusRequestId {
    #[must_use]
    pub fn new(origin: CurrentBusOrigin, bus_id: u64, counter: u64) -> Self {
        Self(format!("{}_{}_{}", origin.as_prefix(), bus_id, counter))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CurrentBusEnvelope {
    pub message: Value,
    #[serde(rename = "needResponse", skip_serializing_if = "Option::is_none")]
    pub need_response: Option<CurrentBusRequestId>,
    #[serde(rename = "responseTo", skip_serializing_if = "Option::is_none")]
    pub response_to: Option<CurrentBusRequestId>,
}

pub type CurrentBusBatch = Vec<CurrentBusEnvelope>;
