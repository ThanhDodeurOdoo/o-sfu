use crate::runtime::transport_adapter::{RuntimeTransportAdapter, TransportSessionKey};
use o_sfu_router::RtpParameters as RouterRtpParameters;

use crate::runtime::{channel::NegotiatedPublish, websocket_server::WsWriter};
use crate::signaling::{shared::StreamType, webrtc::MediaKind as SignalingMediaKind};

use super::super::controller::SessionProtocolOutcome;
use super::{
    controller::NativeSessionProtocol,
    state::{ClearedPublishTransition, StagedPublishTransaction},
};

impl StagedPublishTransaction {
    async fn abort(
        self,
        transport_adapter: &RuntimeTransportAdapter,
        session_key: &TransportSessionKey,
    ) {
        let _result = transport_adapter
            .remove_media(session_key, self.transport_media_id)
            .await;
    }

    async fn commit(self, session: &NativeSessionProtocol, session_key: &TransportSessionKey) {
        let consumable_rtp_parameters = match session
            .transport_adapter
            .negotiated_producer_parameters(session_key, self.transport_media_id)
            .await
        {
            Ok(rtp_parameters) => rtp_parameters,
            Err(_error) => {
                self.abort(&session.transport_adapter, session_key).await;
                return;
            }
        };
        if session
            .channel
            .publish_negotiated_track(
                &session.session_id,
                NegotiatedPublish {
                    connection_id: session.connection_id,
                    stream_type: self.stream_type,
                    media_kind: self.media_kind,
                    transport_media_id: self.transport_media_id,
                    consumable_rtp_parameters,
                },
                &session.transport_adapter,
            )
            .await
            .is_none()
        {
            self.abort(&session.transport_adapter, session_key).await;
        }
    }
}

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
                .set_publication_active(
                    &self.session_id,
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
    ) {
        match self.state.clear_publish_transition(stream_type) {
            Some(ClearedPublishTransition::Queued) => return,
            Some(ClearedPublishTransition::Staged(staged_publish)) => {
                let session_key = self
                    .channel
                    .transport_session_key(&self.session_id, self.connection_id);
                staged_publish
                    .abort(&self.transport_adapter, &session_key)
                    .await;
                let _disposition = self.negotiation.request_renegotiation();
                return;
            }
            None => {}
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
            return;
        }
        let Some(writer) = writer else {
            return;
        };
        let _result = self.request_renegotiation(writer).await;
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
        self.state
            .stage_publish_transaction(StagedPublishTransaction {
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

    pub(super) async fn commit_staged_publishes(&mut self) {
        let staged_publishes = self.state.take_staged_publish_transactions();
        let session_key = self
            .channel
            .transport_session_key(&self.session_id, self.connection_id);
        for staged_publish in staged_publishes {
            staged_publish.commit(self, &session_key).await;
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
