use o_sfu_protocol::{shared::StreamType, signaling::WebSocketCloseCode};
use tracing::{info, instrument};

use crate::runtime::telemetry::schema::event as telemetry_event;
use crate::runtime::websocket_server::WsWriter;

use super::super::controller::SessionProtocolOutcome;
use super::controller::PostAuthSessionProtocol;

impl PostAuthSessionProtocol {
    #[instrument(
        name = "publish.intent",
        skip_all,
        fields(
            channel_uuid = %self.channel.uuid(),
            session_id = ?self.session_id,
            connection_id = ?self.connection_id,
            stream_type = ?stream_type
        )
    )]
    pub(super) async fn handle_publish_intent(
        &mut self,
        writer: &mut WsWriter,
        stream_type: StreamType,
    ) -> SessionProtocolOutcome {
        if self.state.has_queued_publish(stream_type)
            || self
                .channel
                .has_staged_publish(&self.session_id, self.connection_id, stream_type)
                .await
        {
            return SessionProtocolOutcome::Continue;
        }
        if self
            .channel
            .is_stream_published(&self.session_id, stream_type)
            .await
        {
            self.channel
                .set_publication_active_runtime(
                    &self.session_id,
                    self.connection_id,
                    stream_type,
                    true,
                    &self.transport_adapter,
                )
                .await;
            return SessionProtocolOutcome::Continue;
        }
        if self.negotiation.awaiting_answer() {
            self.state.queue_publish_stream(stream_type);
            let _disposition = self.negotiation.request_renegotiation();
            return SessionProtocolOutcome::Continue;
        }
        if !self.stage_publish_stream(stream_type).await {
            return SessionProtocolOutcome::Continue;
        }
        match self.request_renegotiation(writer).await {
            Ok(_sent) => SessionProtocolOutcome::Continue,
            Err(code) => SessionProtocolOutcome::Close(code),
        }
    }

    pub(super) async fn handle_unpublish_intent_with_writer(
        &mut self,
        stream_type: StreamType,
        writer: Option<&mut WsWriter>,
    ) -> SessionProtocolOutcome {
        if self.state.clear_queued_publish(stream_type) {
            return SessionProtocolOutcome::Continue;
        }
        if self
            .channel
            .rollback_staged_publish(
                &self.session_id,
                self.connection_id,
                stream_type,
                &self.transport_adapter,
            )
            .await
        {
            let _disposition = self.negotiation.request_renegotiation();
            return SessionProtocolOutcome::Continue;
        }
        if !self
            .channel
            .unpublish_track(
                &self.session_id,
                self.connection_id,
                stream_type,
                &self.transport_adapter,
            )
            .await
        {
            return SessionProtocolOutcome::Continue;
        }
        let Some(writer) = writer else {
            return SessionProtocolOutcome::Continue;
        };
        handle_unpublish_renegotiation_result(self.request_renegotiation(writer).await)
    }

    async fn stage_publish_stream(&self, stream_type: StreamType) -> bool {
        let staged = self
            .channel
            .stage_negotiated_publish(
                &self.session_id,
                self.connection_id,
                stream_type,
                &self.transport_adapter,
            )
            .await;
        if staged {
            info!(
                event = telemetry_event::PUBLISH_PREPARED,
                operation = "publish_prepare",
                outcome = "staged",
                stream_type = ?stream_type,
                "staged publish stream for negotiation"
            );
        } else {
            info!(
                event = telemetry_event::PUBLISH_ABORTED,
                operation = "publish_prepare",
                outcome = "ignored",
                stream_type = ?stream_type,
                "publish intent did not stage new media"
            );
        }
        staged
    }

    pub(super) async fn stage_queued_publish_streams(&mut self) -> bool {
        let queued_publish_streams = self.state.take_queued_publish_streams();
        let mut staged_any = false;
        for stream_type in queued_publish_streams {
            if self.stage_publish_stream(stream_type).await {
                staged_any = true;
            }
        }
        staged_any
    }

    #[instrument(
        name = "publish.commit",
        skip_all,
        fields(
            channel_uuid = %self.channel.uuid(),
            session_id = ?self.session_id,
            connection_id = ?self.connection_id
        )
    )]
    pub(super) async fn commit_staged_publishes(&self) {
        self.channel
            .commit_staged_publishes(
                &self.session_id,
                self.connection_id,
                &self.transport_adapter,
            )
            .await;
        info!(
            event = telemetry_event::PUBLISH_COMMITTED,
            operation = "publish_commit",
            outcome = "applied",
            "committed staged publish streams"
        );
    }
}

fn handle_unpublish_renegotiation_result(
    result: Result<bool, WebSocketCloseCode>,
) -> SessionProtocolOutcome {
    match result {
        Ok(_sent) => SessionProtocolOutcome::Continue,
        Err(code) => SessionProtocolOutcome::Close(code),
    }
}
