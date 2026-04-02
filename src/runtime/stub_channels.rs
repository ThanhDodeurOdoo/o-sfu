use std::collections::BTreeMap;

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::signaling::http::CreateChannelQuery;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StubChannel {
    pub issuer: String,
    pub key: Option<String>,
    pub uuid: String,
    pub web_rtc_enabled: bool,
    pub recording_address: Option<String>,
}

#[derive(Debug, Default)]
pub struct StubChannelRegistry {
    channels_by_issuer: RwLock<BTreeMap<String, StubChannel>>,
}

impl StubChannelRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn create_or_get(
        &self,
        issuer: &str,
        key: Option<&str>,
        query: &CreateChannelQuery,
    ) -> StubChannel {
        {
            let channels_by_issuer = self.channels_by_issuer.read().await;
            if let Some(channel) = channels_by_issuer.get(issuer) {
                return channel.clone();
            }
        }
        let mut channels_by_issuer = self.channels_by_issuer.write().await;
        let channel = StubChannel {
            issuer: issuer.to_owned(),
            key: key.map(str::to_owned),
            uuid: Uuid::new_v4().to_string(),
            web_rtc_enabled: query.web_rtc_enabled(),
            recording_address: query.recording_address.clone(),
        };
        channels_by_issuer
            .entry(issuer.to_owned())
            .or_insert_with(|| channel.clone())
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::StubChannelRegistry;
    use crate::signaling::http::CreateChannelQuery;

    #[tokio::test]
    async fn create_or_get_is_idempotent_by_issuer() {
        let registry = StubChannelRegistry::new();
        let query = CreateChannelQuery::default();
        let first = registry.create_or_get("issuer-a", None, &query).await;
        let second = registry
            .create_or_get("issuer-a", Some("ignored"), &query)
            .await;
        let third = registry.create_or_get("issuer-b", None, &query).await;
        assert_eq!(first.uuid, second.uuid);
        assert_ne!(first.uuid, third.uuid);
    }
}
