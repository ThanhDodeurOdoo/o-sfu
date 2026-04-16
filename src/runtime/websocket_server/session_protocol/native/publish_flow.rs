use std::future::Future;

use crate::runtime::transport_adapter::{TransportAdapterError, TransportMediaId};
use o_sfu_router::MediaKind;
use o_sfu_router::RtpParameters as RouterRtpParameters;

use crate::runtime::{channel::NegotiatedPublish, websocket_server::WsWriter};
use crate::signaling::shared::StreamType;

use super::super::controller::SessionProtocolOutcome;
use super::{
    controller::NativeSessionProtocol,
    state::{ClearedPublishTransition, StagedPublishTransaction},
};

struct PublishTransactionGuard {
    staged_publish: StagedPublishTransaction,
}

impl PublishTransactionGuard {
    fn new(staged_publish: StagedPublishTransaction) -> Self {
        Self { staged_publish }
    }

    async fn commit<
        LoadParameters,
        LoadParametersFuture,
        PublishTrack,
        PublishTrackFuture,
        PublishTrackOutput,
        Cleanup,
        CleanupFuture,
    >(
        self,
        connection_id: u64,
        load_consumable_parameters: LoadParameters,
        publish_track: PublishTrack,
        cleanup_media: Cleanup,
    ) -> bool
    where
        LoadParameters: FnOnce(TransportMediaId) -> LoadParametersFuture,
        LoadParametersFuture: Future<Output = Result<RouterRtpParameters, TransportAdapterError>>,
        PublishTrack: FnOnce(NegotiatedPublish) -> PublishTrackFuture,
        PublishTrackFuture: Future<Output = Option<PublishTrackOutput>>,
        Cleanup: FnOnce(TransportMediaId) -> CleanupFuture,
        CleanupFuture: Future<Output = ()>,
    {
        let transport_media_id = self.staged_publish.transport_media_id;
        let consumable_rtp_parameters = match load_consumable_parameters(transport_media_id).await {
            Ok(rtp_parameters) => rtp_parameters,
            Err(_error) => {
                cleanup_media(transport_media_id).await;
                return false;
            }
        };
        if publish_track(NegotiatedPublish {
            connection_id,
            stream_type: self.staged_publish.stream_type,
            media_kind: self.staged_publish.media_kind,
            transport_media_id,
            consumable_rtp_parameters,
        })
        .await
        .is_none()
        {
            cleanup_media(transport_media_id).await;
            return false;
        }
        true
    }

    async fn rollback<Cleanup, CleanupFuture>(self, cleanup_media: Cleanup)
    where
        Cleanup: FnOnce(TransportMediaId) -> CleanupFuture,
        CleanupFuture: Future<Output = ()>,
    {
        cleanup_media(self.staged_publish.transport_media_id).await;
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
                let transport_adapter = &self.transport_adapter;
                PublishTransactionGuard::new(staged_publish)
                    .rollback(|transport_media_id| async move {
                        let _result = transport_adapter
                            .remove_media(&session_key, transport_media_id)
                            .await;
                    })
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
        let transport_adapter = &self.transport_adapter;
        let channel = &self.channel;
        let session_id = &self.session_id;
        let session_key = &session_key;
        for staged_publish in staged_publishes {
            PublishTransactionGuard::new(staged_publish)
                .commit(
                    self.connection_id,
                    |transport_media_id| async move {
                        transport_adapter
                            .negotiated_producer_parameters(session_key, transport_media_id)
                            .await
                    },
                    |publish| async move {
                        channel
                            .publish_negotiated_track(session_id, publish, transport_adapter)
                            .await
                    },
                    |transport_media_id| async move {
                        let _result = transport_adapter
                            .remove_media(session_key, transport_media_id)
                            .await;
                    },
                )
                .await;
        }
    }
}

fn media_kind_for_stream_type(stream_type: StreamType) -> MediaKind {
    match stream_type {
        StreamType::Audio => MediaKind::Audio,
        StreamType::Camera | StreamType::Screen => MediaKind::Video,
    }
}

fn pending_publish_parameters() -> RouterRtpParameters {
    RouterRtpParameters::new(vec![], vec![], vec![])
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "these unit tests use poisoned-mutex failures as explicit test invariants"
    )]

    use std::sync::{Arc, Mutex};

    use super::{PublishTransactionGuard, StagedPublishTransaction};
    use crate::{
        runtime::transport_adapter::{TransportAdapterError, TransportMediaId},
        signaling::shared::StreamType,
    };
    use o_sfu_router::{MediaKind, RtpParameters as RouterRtpParameters};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Step {
        Load(u64),
        Publish(u64),
        Cleanup(u64),
    }

    #[tokio::test]
    async fn publish_transaction_guard_aborts_when_parameter_lookup_fails() {
        let steps = Arc::new(Mutex::new(Vec::new()));
        let committed = PublishTransactionGuard::new(staged_publish_transaction())
            .commit(
                9,
                {
                    let steps = Arc::clone(&steps);
                    move |transport_media_id| {
                        let steps = Arc::clone(&steps);
                        async move {
                            steps
                                .lock()
                                .expect("steps lock")
                                .push(Step::Load(transport_media_id.as_u64()));
                            Err(TransportAdapterError::InvalidInput)
                        }
                    }
                },
                {
                    let steps = Arc::clone(&steps);
                    move |publish| {
                        let steps = Arc::clone(&steps);
                        async move {
                            steps
                                .lock()
                                .expect("steps lock")
                                .push(Step::Publish(publish.transport_media_id.as_u64()));
                            Some(())
                        }
                    }
                },
                {
                    let steps = Arc::clone(&steps);
                    move |transport_media_id| {
                        let steps = Arc::clone(&steps);
                        async move {
                            steps
                                .lock()
                                .expect("steps lock")
                                .push(Step::Cleanup(transport_media_id.as_u64()));
                        }
                    }
                },
            )
            .await;

        assert!(!committed);
        assert_eq!(
            steps.lock().expect("steps lock").as_slice(),
            &[Step::Load(17), Step::Cleanup(17)]
        );
    }

    #[tokio::test]
    async fn publish_transaction_guard_aborts_after_publish_rejection() {
        let steps = Arc::new(Mutex::new(Vec::new()));
        let committed = PublishTransactionGuard::new(staged_publish_transaction())
            .commit(
                9,
                {
                    let steps = Arc::clone(&steps);
                    move |transport_media_id| {
                        let steps = Arc::clone(&steps);
                        async move {
                            steps
                                .lock()
                                .expect("steps lock")
                                .push(Step::Load(transport_media_id.as_u64()));
                            Ok(RouterRtpParameters::new(vec![], vec![], vec![]))
                        }
                    }
                },
                {
                    let steps = Arc::clone(&steps);
                    move |publish| {
                        let steps = Arc::clone(&steps);
                        async move {
                            steps
                                .lock()
                                .expect("steps lock")
                                .push(Step::Publish(publish.transport_media_id.as_u64()));
                            None::<()>
                        }
                    }
                },
                {
                    let steps = Arc::clone(&steps);
                    move |transport_media_id| {
                        let steps = Arc::clone(&steps);
                        async move {
                            steps
                                .lock()
                                .expect("steps lock")
                                .push(Step::Cleanup(transport_media_id.as_u64()));
                        }
                    }
                },
            )
            .await;

        assert!(!committed);
        assert_eq!(
            steps.lock().expect("steps lock").as_slice(),
            &[Step::Load(17), Step::Publish(17), Step::Cleanup(17)]
        );
    }

    #[tokio::test]
    async fn publish_transaction_guard_keeps_successful_publishes() {
        let steps = Arc::new(Mutex::new(Vec::new()));
        let committed = PublishTransactionGuard::new(staged_publish_transaction())
            .commit(
                9,
                {
                    let steps = Arc::clone(&steps);
                    move |transport_media_id| {
                        let steps = Arc::clone(&steps);
                        async move {
                            steps
                                .lock()
                                .expect("steps lock")
                                .push(Step::Load(transport_media_id.as_u64()));
                            Ok(RouterRtpParameters::new(vec![], vec![], vec![]))
                        }
                    }
                },
                {
                    let steps = Arc::clone(&steps);
                    move |publish| {
                        let steps = Arc::clone(&steps);
                        async move {
                            steps
                                .lock()
                                .expect("steps lock")
                                .push(Step::Publish(publish.transport_media_id.as_u64()));
                            Some(())
                        }
                    }
                },
                {
                    let steps = Arc::clone(&steps);
                    move |transport_media_id| {
                        let steps = Arc::clone(&steps);
                        async move {
                            steps
                                .lock()
                                .expect("steps lock")
                                .push(Step::Cleanup(transport_media_id.as_u64()));
                        }
                    }
                },
            )
            .await;

        assert!(committed);
        assert_eq!(
            steps.lock().expect("steps lock").as_slice(),
            &[Step::Load(17), Step::Publish(17)]
        );
    }

    fn staged_publish_transaction() -> StagedPublishTransaction {
        StagedPublishTransaction {
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            transport_media_id: TransportMediaId::new(17),
        }
    }
}
