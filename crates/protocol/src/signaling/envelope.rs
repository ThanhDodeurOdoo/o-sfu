use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type EnvelopeBatch = Vec<Envelope>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(String);

impl RequestId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "t")]
    pub tag: String,
    #[serde(rename = "p", skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(rename = "q", skip_serializing_if = "Option::is_none")]
    pub request_id: Option<RequestId>,
    #[serde(rename = "r", skip_serializing_if = "Option::is_none")]
    pub response_to: Option<RequestId>,
}

impl Envelope {
    pub(crate) fn message(tag: &str, payload: Option<Value>) -> Self {
        Self {
            tag: tag.to_owned(),
            payload,
            request_id: None,
            response_to: None,
        }
    }

    pub(crate) fn request(tag: &str, request_id: RequestId, payload: Option<Value>) -> Self {
        Self {
            tag: tag.to_owned(),
            payload,
            request_id: Some(request_id),
            response_to: None,
        }
    }

    pub(crate) fn response(tag: &str, response_to: RequestId, payload: Option<Value>) -> Self {
        Self {
            tag: tag.to_owned(),
            payload,
            request_id: None,
            response_to: Some(response_to),
        }
    }
}
