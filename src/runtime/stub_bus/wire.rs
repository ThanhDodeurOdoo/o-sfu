use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct LegacyRequestId(String);

impl LegacyRequestId {
    #[must_use]
    pub(super) fn server(bus_id: u64, counter: u64) -> Self {
        Self(format!("s_{bus_id}_{counter}"))
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn client(bus_id: u64, counter: u64) -> Self {
        Self(format!("c_{bus_id}_{counter}"))
    }

    #[must_use]
    pub(super) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct LegacyEnvelope {
    pub(super) message: Value,
    #[serde(rename = "needResponse", skip_serializing_if = "Option::is_none")]
    pub(super) need_response: Option<LegacyRequestId>,
    #[serde(rename = "responseTo", skip_serializing_if = "Option::is_none")]
    pub(super) response_to: Option<LegacyRequestId>,
}

pub(super) type LegacyBatch = Vec<LegacyEnvelope>;
