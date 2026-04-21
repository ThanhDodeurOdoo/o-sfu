use tracing::warn;

use crate::runtime::transport_adapter::{RuntimeTransportAdapter, SourcePacketGate};

use super::Channel;

impl Channel {
    pub(super) async fn sync_source_packet_selection_policy(
        &self,
        transport_adapter: Option<&RuntimeTransportAdapter>,
    ) {
        let Some(transport_adapter) = transport_adapter else {
            return;
        };
        let active_speaker_sources = transport_adapter.active_speaker_source_snapshot().await;
        let (source_packet_updates, featured_session_updates) = {
            let state = self.state.read().await;
            (
                state.source_packet_selection_updates(&active_speaker_sources),
                state.featured_session_updates(&active_speaker_sources),
            )
        };
        if source_packet_updates.is_empty() && featured_session_updates.is_empty() {
            return;
        }
        let mut applied_source_packet_updates = Vec::with_capacity(source_packet_updates.len());
        for update in source_packet_updates {
            if transport_adapter
                .set_source_packet_gate(
                    &self.transport_session_key(
                        update.owner_session_id(),
                        update.owner_connection_id(),
                    ),
                    update.transport_media_id(),
                    update.selection().map(SourcePacketGate::from),
                )
                .await
                .is_err()
            {
                warn!(
                    session_id = ?update.owner_session_id(),
                    connection_id = ?update.owner_connection_id(),
                    transport_media_id = ?update.transport_media_id(),
                    "transport adapter rejected the room-owned source packet selection update"
                );
                continue;
            }
            applied_source_packet_updates.push(update);
        }
        let info_fanout = {
            let mut state = self.state.write().await;
            state.commit_source_packet_selection_updates(&applied_source_packet_updates);
            state.commit_featured_session_updates(&featured_session_updates)
        };
        if let Some(info_fanout) = info_fanout {
            info_fanout.emit();
        }
    }
}
