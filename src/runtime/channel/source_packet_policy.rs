use tracing::warn;

use crate::runtime::transport_adapter::RuntimeTransportAdapter;

use super::Channel;

impl Channel {
    pub(super) async fn sync_source_packet_selection_policy(
        &self,
        transport_adapter: Option<&RuntimeTransportAdapter>,
    ) {
        let Some(transport_adapter) = transport_adapter else {
            return;
        };
        let active_speaker_sources = transport_adapter
            .active_speaker_source_snapshot(self.runtime_id)
            .await;
        let updates = {
            let state = self.state.read().await;
            state.source_packet_selection_updates(&active_speaker_sources)
        };
        if updates.is_empty() {
            return;
        }
        let mut applied_updates = Vec::with_capacity(updates.len());
        for update in updates {
            if transport_adapter
                .set_source_packet_selection(
                    &self.transport_session_key(
                        update.owner_session_id(),
                        update.owner_connection_id(),
                    ),
                    update.transport_media_id(),
                    update.selection().cloned(),
                )
                .await
                .is_err()
            {
                warn!(
                    session_id = ?update.owner_session_id(),
                    connection_id = update.owner_connection_id(),
                    transport_media_id = ?update.transport_media_id(),
                    "transport adapter rejected the room-owned source packet selection update"
                );
                continue;
            }
            applied_updates.push(update);
        }
        if applied_updates.is_empty() {
            return;
        }
        let mut state = self.state.write().await;
        state.commit_source_packet_selection_updates(&applied_updates);
    }
}
