use o_sfu_router::RtpParameters as RouterRtpParameters;

use crate::runtime::{channel::NegotiatedPublish, stub_bus::WsWriter};
use crate::signaling::{shared::StreamType, webrtc::MediaKind as SignalingMediaKind};

use super::super::controller::SessionProtocolOutcome;
use super::{controller::NativeSessionProtocol, state::PendingPublishCommit};

impl NativeSessionProtocol {
    pub(super) async fn handle_publish_intent(
        &mut self,
        writer: &mut WsWriter,
        stream_type: StreamType,
    ) -> SessionProtocolOutcome {
        if self.state.contains_publish_transition(stream_type) {
            return SessionProtocolOutcome::Continue;
        }
        if self
            .channel
            .is_stream_published(&self.session_id, stream_type)
            .await
        {
            self.channel
                .update_upload_state(&self.session_id, stream_type, true, &self.transport_adapter)
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

    pub(super) async fn handle_unpublish_intent(&mut self, stream_type: StreamType) {
        if self.state.remove_queued_publish_stream(stream_type) {
            return;
        }
        if let Some(pending_publish) = self.state.take_pending_publish_for_stream(stream_type) {
            let session_key = self
                .channel
                .transport_session_key(&self.session_id, self.connection_id);
            let _result = self
                .transport_adapter
                .remove_media(&session_key, pending_publish.transport_media_id)
                .await;
            let _disposition = self.negotiation.request_renegotiation();
            return;
        }
        self.channel
            .update_upload_state(
                &self.session_id,
                stream_type,
                false,
                &self.transport_adapter,
            )
            .await;
    }

    async fn stage_publish_stream(&mut self, stream_type: StreamType) -> bool {
        let media_kind = media_kind_for_stream_type(stream_type);
        let session_key = self
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        let transport_media_id = match self
            .transport_adapter
            .publish_media(&session_key, media_kind, &pending_publish_parameters())
            .await
        {
            Ok(transport_media_id) => transport_media_id,
            Err(_error) => return false,
        };
        self.state.push_pending_publish(PendingPublishCommit {
            stream_type,
            media_kind,
            transport_media_id,
        });
        true
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

    pub(super) async fn commit_pending_publishes(&mut self) {
        let pending_publish_commits = self.state.take_pending_publish_commits();
        let session_key = self
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        for pending_publish in pending_publish_commits {
            let consumable_rtp_parameters = match self
                .transport_adapter
                .negotiated_producer_parameters(&session_key, pending_publish.transport_media_id)
                .await
            {
                Ok(rtp_parameters) => rtp_parameters,
                Err(_error) => {
                    let _result = self
                        .transport_adapter
                        .remove_media(&session_key, pending_publish.transport_media_id)
                        .await;
                    continue;
                }
            };
            if self
                .channel
                .publish_negotiated_track(
                    &self.session_id,
                    NegotiatedPublish {
                        connection_id: self.connection_id,
                        stream_type: pending_publish.stream_type,
                        media_kind: pending_publish.media_kind,
                        transport_media_id: pending_publish.transport_media_id,
                        consumable_rtp_parameters,
                    },
                    &self.transport_adapter,
                )
                .await
                .is_none()
            {
                let _result = self
                    .transport_adapter
                    .remove_media(&session_key, pending_publish.transport_media_id)
                    .await;
            }
        }
    }
}

fn media_kind_for_stream_type(stream_type: StreamType) -> SignalingMediaKind {
    match stream_type {
        StreamType::Audio => SignalingMediaKind::Audio,
        StreamType::Camera | StreamType::Screen => SignalingMediaKind::Video,
    }
}

fn pending_publish_parameters() -> RouterRtpParameters {
    RouterRtpParameters::new(vec![], vec![], vec![])
}
