use std::collections::BTreeMap;

use o_sfu_router::derive_consumable_rtp_parameters;
use tracing::{error, warn};

use crate::runtime::transport_adapter::RuntimeTransportAdapter;
use crate::signaling::{
    bundle_api::bundle_session_info_key,
    current_protocol::{CurrentServerMessage, CurrentSessionInfoSnapshotById},
    shared::{DownloadStates, SessionId, StreamType},
    webrtc::{MediaKind as SignalingMediaKind, RtpParameters},
};

use super::{
    Channel,
    outbound::send_to_all,
    state::{ConsumerBootstrapOrigin, PendingConsumerBootstrapTarget},
};

impl Channel {
    pub async fn bootstrap_late_join_consumers(
        &self,
        session_id: &SessionId,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let targets = {
            let state = self.state.read().await;
            state.late_join_consumer_targets(session_id)
        };

        for target in targets {
            self.bootstrap_consumer_target(
                &target,
                transport_adapter,
                ConsumerBootstrapOrigin::LateJoin,
            )
            .await;
        }
    }

    pub async fn publish_track(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
        media_kind: SignalingMediaKind,
        rtp_parameters: RtpParameters,
        transport_adapter: &RuntimeTransportAdapter,
    ) -> Option<String> {
        let (publisher_connection_id, router_capabilities) = {
            let state = self.state.read().await;
            let session = state.sessions.get(session_id)?;
            if !session.upload_transport_connected {
                return None;
            }
            (
                session.connection_id,
                state.router.rtp_capabilities().clone(),
            )
        };

        let parsed_rtp_parameters = super::rtp_conversion::parse_rtp_parameters(&rtp_parameters.0)
            .or_else(|| {
                warn!(
                    ?session_id,
                    "failed to parse producer RTP parameters from wire format"
                );
                None
            })?;
        let consumable_rtp_parameters =
            derive_consumable_rtp_parameters(&parsed_rtp_parameters, &router_capabilities)
                .map_err(|error| {
                    warn!(
                        ?session_id,
                        ?error,
                        "failed to derive consumable RTP parameters for producer"
                    );
                })
                .ok()?;

        let producer_id = {
            let mut state = self.state.write().await;
            state.reserve_published_track(
                session_id,
                publisher_connection_id,
                stream_type,
                media_kind,
                consumable_rtp_parameters,
            )?
        };

        let transport_media_id = match transport_adapter
            .publish_media(session_id, stream_type, media_kind, &parsed_rtp_parameters)
            .await
        {
            Ok(id) => id,
            Err(_error) => {
                self.state
                    .write()
                    .await
                    .rollback_published_track(&producer_id);
                warn!(
                    ?session_id,
                    "transport adapter rejected publish media declaration"
                );
                return None;
            }
        };

        let consumer_targets = {
            let mut state = self.state.write().await;
            state.finalize_published_track(
                session_id,
                publisher_connection_id,
                &producer_id,
                transport_media_id,
            )
        };
        let Some(consumer_targets) = consumer_targets else {
            let _result = transport_adapter
                .remove_media(session_id, transport_media_id)
                .await;
            self.state
                .write()
                .await
                .rollback_published_track(&producer_id);
            return None;
        };

        for target in consumer_targets {
            self.bootstrap_consumer_target(
                &target,
                transport_adapter,
                ConsumerBootstrapOrigin::Publish,
            )
            .await;
        }
        Some(producer_id)
    }

    async fn bootstrap_consumer_target(
        &self,
        target: &PendingConsumerBootstrapTarget,
        transport_adapter: &RuntimeTransportAdapter,
        origin: ConsumerBootstrapOrigin,
    ) {
        let Some(prepared) = ({
            let state = self.state.read().await;
            state.prepare_consumer_bootstrap(target)
        }) else {
            return;
        };
        let Some(reserved) = ({
            let mut state = self.state.write().await;
            state.reserve_consumer_bootstrap(target, &prepared)
        }) else {
            return;
        };
        let consumer_transport_media_id = match transport_adapter
            .consume_media(
                &target.consumer_session_id,
                target.media_kind,
                &target.producer_session_id,
                target.transport_media_id,
                &prepared.consumer_rtp_parameters,
            )
            .await
        {
            Ok(transport_media_id) => transport_media_id,
            Err(_error) => {
                self.state
                    .write()
                    .await
                    .rollback_reserved_consumer_bootstrap(&reserved);
                warn!(
                    consumer_session_id = ?target.consumer_session_id,
                    producer_session_id = ?target.producer_session_id,
                    ?origin,
                    "transport adapter rejected consume media declaration"
                );
                return;
            }
        };
        let outbound = {
            let mut state = self.state.write().await;
            state.finalize_reserved_consumer_bootstrap(
                target,
                &prepared,
                &reserved,
                consumer_transport_media_id,
            )
        };
        let Some((sender, request)) = outbound else {
            let _result = transport_adapter
                .remove_media(&target.consumer_session_id, consumer_transport_media_id)
                .await;
            self.state
                .write()
                .await
                .rollback_reserved_consumer_bootstrap(&reserved);
            return;
        };
        let _ = sender.send(super::SessionOutbound::Request(Box::new(request)));
    }

    #[allow(
        clippy::cognitive_complexity,
        reason = "the production-change transition intentionally keeps router updates, session-info sync, broadcast, and transport activity in one explicit sequence"
    )]
    pub async fn update_upload_state(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
        active: bool,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let transport_media_id = {
            let mut state = self.state.write().await;
            let producer = state.producers.values_mut().find(|producer| {
                producer.owner_session_id == *session_id && producer.stream_type == stream_type
            });
            let Some(producer) = producer else {
                return;
            };
            producer.active = active;
            let router_producer_id = producer.router_producer_id;
            let Some(transport_media_id) = producer.transport_media_id else {
                return;
            };
            let paused = !active;
            if state
                .router
                .set_producer_paused(router_producer_id, paused)
                .is_err()
            {
                error!(
                    ?session_id,
                    ?stream_type,
                    "failed to set producer pause state in channel router"
                );
                return;
            }
            let Some(session) = state.sessions.get_mut(session_id) else {
                return;
            };
            match stream_type {
                StreamType::Camera => session.info.is_camera_on = Some(active),
                StreamType::Screen => session.info.is_screen_sharing_on = Some(active),
                StreamType::Audio => {}
            }
            let updated_info = session.info.clone();
            if state
                .router
                .update_session_info(session_id, &updated_info)
                .is_err()
            {
                error!(
                    ?session_id,
                    "failed to mirror session info update into channel router after production change"
                );
            }
            let snapshot: CurrentSessionInfoSnapshotById =
                BTreeMap::from([(bundle_session_info_key(session_id), updated_info)]);
            send_to_all(
                &state.sessions,
                &CurrentServerMessage::SessionInfoChanged(snapshot),
            );
            transport_media_id
        };
        if transport_adapter
            .set_producer_active(session_id, transport_media_id, active)
            .await
            .is_err()
        {
            warn!(
                ?session_id,
                ?stream_type,
                active,
                "transport adapter failed to update producer route activity"
            );
        }
    }

    pub async fn update_download_state(
        &self,
        session_id: &SessionId,
        target_session_id: &SessionId,
        states: &DownloadStates,
        transport_adapter: &RuntimeTransportAdapter,
    ) {
        let mut route_updates = Vec::new();
        let mut state = self.state.write().await;
        for (stream_type, active) in states.iter() {
            let key = super::state::ConsumerKey {
                consumer_session_id: session_id.clone(),
                producer_session_id: target_session_id.clone(),
                stream_type,
            };
            let Some(consumer_state) = state.consumer_index.get(&key).copied() else {
                continue;
            };
            let paused = !active;
            if state
                .router
                .set_consumer_paused(consumer_state.router_consumer, paused)
                .is_err()
            {
                error!(
                    ?session_id,
                    ?target_session_id,
                    ?stream_type,
                    "failed to set consumer pause state in channel router"
                );
                continue;
            }
            route_updates.push((consumer_state, stream_type, active));
        }
        drop(state);
        for (consumer_state, stream_type, active) in route_updates {
            if transport_adapter
                .set_consumer_active(
                    session_id,
                    consumer_state.consumer_media,
                    target_session_id,
                    consumer_state.source_media,
                    active,
                )
                .await
                .is_err()
            {
                warn!(
                    ?session_id,
                    ?target_session_id,
                    ?stream_type,
                    active,
                    "transport adapter failed to update consumer route activity"
                );
            }
        }
    }
}
