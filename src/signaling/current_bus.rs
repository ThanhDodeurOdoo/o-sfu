//! Current bundle/server batching envelope used by the deployed SFU.
//! This is a compatibility reference layer, not a permanent architectural boundary.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const CURRENT_WIRE_BATCH_DELAY_MS: u64 = 200;
pub const CURRENT_WIRE_REQUEST_TIMEOUT_MS: u64 = 5_000;

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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        CURRENT_WIRE_BATCH_DELAY_MS, CURRENT_WIRE_REQUEST_TIMEOUT_MS, CurrentBusBatch,
        CurrentBusEnvelope, CurrentBusOrigin, CurrentBusRequestId,
    };

    #[test]
    fn request_id_format_matches_current_bus_contract() {
        assert_eq!(
            CurrentBusRequestId::new(CurrentBusOrigin::Client, 7, 9).as_str(),
            "c_7_9"
        );
        assert_eq!(
            CurrentBusRequestId::new(CurrentBusOrigin::Server, 2, 0).as_str(),
            "s_2_0"
        );
        assert_eq!(CURRENT_WIRE_BATCH_DELAY_MS, 200);
        assert_eq!(CURRENT_WIRE_REQUEST_TIMEOUT_MS, 5_000);
    }

    #[test]
    fn current_bus_envelopes_round_trip() -> serde_json::Result<()> {
        let request = CurrentBusEnvelope {
            message: json!({
                "name": "PING"
            }),
            need_response: Some(CurrentBusRequestId::new(CurrentBusOrigin::Server, 1, 3)),
            response_to: None,
        };
        let expected_request = json!({
            "message": {
                "name": "PING"
            },
            "needResponse": "s_1_3"
        });
        assert_eq!(serde_json::to_value(&request)?, expected_request);
        assert_eq!(
            serde_json::from_value::<CurrentBusEnvelope>(expected_request)?,
            request
        );

        let response = CurrentBusEnvelope {
            message: json!({
                "id": "producer-1"
            }),
            need_response: None,
            response_to: Some(CurrentBusRequestId::new(CurrentBusOrigin::Client, 4, 8)),
        };
        let batch: CurrentBusBatch = vec![request, response.clone()];
        let expected_batch = json!([
            {
                "message": {
                    "name": "PING"
                },
                "needResponse": "s_1_3"
            },
            {
                "message": {
                    "id": "producer-1"
                },
                "responseTo": "c_4_8"
            }
        ]);
        assert_eq!(serde_json::to_value(&batch)?, expected_batch);
        assert_eq!(
            serde_json::from_value::<CurrentBusBatch>(expected_batch)?,
            batch
        );
        assert_eq!(
            response.response_to,
            Some(CurrentBusRequestId::new(CurrentBusOrigin::Client, 4, 8))
        );

        Ok(())
    }
}
