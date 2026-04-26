//! Websocket-side publish and unpublish intent handling
//!
//! This file owns the control-flow decisions around queued publish intents,
//! staged publish transactions and when renegotiation must happen. The
//! staged publish lifecycle is in `room/media_transaction.rs`.

use o_sfu_protocol::{shared::StreamType, signaling::WebSocketCloseCode};
use tracing::{info, instrument};

use super::controller::PostAuthSessionProtocol;
use crate::{
    application::outcomes::CallOutcome,
    runtime::{
        telemetry::schema::event as telemetry_event, transport_adapter::AppliedSessionAnswer,
    },
};

impl PostAuthSessionProtocol {
    #[instrument(
        name = "publish.intent",
        skip_all,
        fields(
            room_id = %self.room.uuid(),
            user_id = ?self.user_id,
            connection_id = ?self.connection_id,
            stream_type = ?stream_type
        )
    )]
    pub(super) async fn handle_publish_intent(
        &mut self,
        stream_type: StreamType,
    ) -> Result<CallOutcome, WebSocketCloseCode> {
        // If the same stream is already queued or staged, treat the new intent
        // as idempotent. The room transaction layer is strict about one
        // staged publish per `(user, connection, stream_type)`
        if self.flow_state.has_queued_publish(stream_type)
            || self
                .room
                .has_staged_publish(&self.user_id, self.connection_id, stream_type)
                .await
        {
            return Ok(CallOutcome::new());
        }
        if self
            .room
            .is_stream_published(&self.user_id, stream_type)
            .await
        {
            // Once a stream is already live publish intent is just a resume of
            // producer activity. No new transport media or renegotiation is
            // needed here
            self.room
                .set_publication_active_runtime(
                    &self.user_id,
                    self.connection_id,
                    stream_type,
                    true,
                    &self.transport_adapter,
                )
                .await;
            return Ok(CallOutcome::new());
        }
        if self.flow_state.awaiting_answer() {
            // A publish intent that arrive while another answer is in flight
            // cannot stage transport media yet. Queue it so the follow-up
            // renegotiation stages it against the latest user state.
            self.flow_state.queue_publish_stream(stream_type);
            let _disposition = self.flow_state.request_renegotiation();
            return Ok(CallOutcome::new());
        }
        if !self.stage_publish_stream(stream_type).await {
            return Ok(CallOutcome::new());
        }
        self.request_renegotiation().await
    }

    pub(super) async fn handle_unpublish_intent(
        &mut self,
        stream_type: StreamType,
    ) -> Result<CallOutcome, WebSocketCloseCode> {
        // Queued publish means the stream never staged transport media, so
        // clearing the queued intent is enough.
        if self.flow_state.clear_queued_publish(stream_type) {
            return Ok(CallOutcome::new());
        }
        // If a publish was staged but not committed yet, unpublish becomes a
        // pure rollback of the staged transaction.
        if self
            .room
            .rollback_staged_publish(
                &self.user_id,
                self.connection_id,
                stream_type,
                &self.transport_adapter,
            )
            .await
        {
            let _disposition = self.flow_state.request_renegotiation();
            return Ok(CallOutcome::new());
        }
        if !self
            .room
            .unpublish_track(
                &self.user_id,
                self.connection_id,
                stream_type,
                &self.transport_adapter,
            )
            .await
        {
            return Ok(CallOutcome::new());
        }
        self.request_renegotiation().await
    }

    async fn stage_publish_stream(&self, stream_type: StreamType) -> bool {
        let staged = self
            .room
            .stage_negotiated_publish(
                &self.user_id,
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
        // Follow-up renegotiation drains the queued intents into real staged
        // publish transactions once the previous answer is no longer in flight.
        let queued_publish_streams = self.flow_state.take_queued_publish_streams();
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
            room_id = %self.room.uuid(),
            user_id = ?self.user_id,
            connection_id = ?self.connection_id
        )
    )]
    pub(super) async fn commit_staged_publishes(&self, applied_answer: &AppliedSessionAnswer) {
        // Answer handling already proved that the transport layer accepted the
        // negotiated user update. The room-side transaction now finishes
        // every staged publish that belongs to this connection.
        self.room
            .commit_staged_publishes(
                &self.user_id,
                self.connection_id,
                applied_answer,
                &self.transport_adapter,
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
