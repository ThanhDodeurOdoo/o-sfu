//! Runtime transport adapter facade for the `rtc` WebRTC backend.

#[cfg(test)]
use std::collections::BTreeMap;
use std::{
    fmt,
    net::IpAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use crate::config::{MediaCodecFlags, RtcPortRange};
#[cfg(test)]
use crate::runtime::transport_adapter::TransportConnectRequest;
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::runtime::transport_bootstrap::SessionTransportBootstrap;
use crate::runtime::{
    metrics::RuntimeMetrics,
    recording::MediaTap,
    transport_adapter::{
        RtcTransportAdapterConfig, SessionOffer, SourcePacketGate, TransportAdapterError,
        TransportMediaId, TransportSessionKey,
    },
};
use o_sfu_router::RtpParameters as RouterRtpParameters;
use str0m::media::MediaKind;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
#[cfg(test)]
use tracing::debug;

#[cfg(test)]
use super::super::state::{TransportLifecycleState, TransportStateKey};
#[cfg(any(test, feature = "internal-benchmarks"))]
use super::super::validation;
use super::super::{
    commands::{
        CloseSessionOutcome, CloseSessionState, RemoteSourceControl, RemoveMediaOutcome,
        RtcWorkerCommand,
    },
    relay_registry::{RelayPacketMailbox, RelayRegistry, RelayTargetId},
    state::{RtcBitrateState, RtcSnapshotState},
};

static NEXT_RELAY_TARGET_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub(crate) struct RtcWorkerHandle {
    pub(super) command_tx: mpsc::Sender<RtcWorkerCommand>,
    pub(super) relay_mailbox: RelayPacketMailbox,
    pub(crate) bitrate_state: Arc<Mutex<RtcBitrateState>>,
    pub(crate) snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    pub(super) shutdown_token: CancellationToken,
}

impl fmt::Debug for RtcWorkerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RtcWorkerHandle")
            .field("command_tx", &self.command_tx)
            .field("relay_mailbox", &self.relay_mailbox)
            .finish_non_exhaustive()
    }
}

pub(crate) struct RtcTransportAdapter {
    pub(super) relay_target_id: RelayTargetId,
    pub(super) public_ip: IpAddr,
    pub(super) rtc_port_range: RtcPortRange,
    pub(super) codec_flags: MediaCodecFlags,
    pub(super) media_tap: Arc<MediaTap>,
    pub(super) relay_registry: Arc<RelayRegistry>,
    pub(crate) metrics: Arc<RuntimeMetrics>,
    pub(super) worker_handle: Mutex<Option<RtcWorkerHandle>>,
    #[cfg(test)]
    pub(super) transport_states: Arc<Mutex<BTreeMap<TransportStateKey, TransportLifecycleState>>>,
    pub(crate) packet_loop_started: Arc<AtomicBool>,
}

#[derive(Clone, Copy)]
pub(crate) struct RtcTransportNegotiationFacade<'a> {
    adapter: &'a RtcTransportAdapter,
}

#[derive(Clone, Copy)]
pub(crate) struct RtcTransportMediaFacade<'a> {
    adapter: &'a RtcTransportAdapter,
}

#[derive(Clone, Copy)]
pub(crate) struct RtcTransportSessionFacade<'a> {
    adapter: &'a RtcTransportAdapter,
}

#[derive(Clone, Copy)]
pub(crate) struct RtcTransportObservabilityFacade<'a> {
    pub(super) adapter: &'a RtcTransportAdapter,
}

impl RtcTransportAdapter {
    pub(crate) fn new(config: &RtcTransportAdapterConfig) -> Self {
        Self {
            relay_target_id: RelayTargetId::new(
                NEXT_RELAY_TARGET_ID.fetch_add(1, Ordering::Relaxed),
            ),
            public_ip: config.public_ip(),
            rtc_port_range: config.rtc_port_range(),
            codec_flags: config.codec_flags(),
            media_tap: config.media_tap(),
            relay_registry: Arc::new(RelayRegistry::default()),
            metrics: config.metrics(),
            worker_handle: Mutex::new(None),
            #[cfg(test)]
            transport_states: Arc::new(Mutex::new(BTreeMap::new())),
            packet_loop_started: Arc::new(AtomicBool::new(false)),
        }
    }

    #[must_use]
    pub(crate) const fn negotiation(&self) -> RtcTransportNegotiationFacade<'_> {
        RtcTransportNegotiationFacade { adapter: self }
    }

    #[must_use]
    pub(crate) const fn media(&self) -> RtcTransportMediaFacade<'_> {
        RtcTransportMediaFacade { adapter: self }
    }

    #[must_use]
    pub(crate) const fn sessions(&self) -> RtcTransportSessionFacade<'_> {
        RtcTransportSessionFacade { adapter: self }
    }

    #[must_use]
    pub(crate) const fn observability(&self) -> RtcTransportObservabilityFacade<'_> {
        RtcTransportObservabilityFacade { adapter: self }
    }

    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub(crate) async fn transport_bootstrap_payload(
        &self,
        session_key: &TransportSessionKey,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<SessionTransportBootstrap, TransportAdapterError> {
        self.negotiation()
            .transport_bootstrap_payload(session_key, router_capabilities)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.negotiation()
            .create_initial_session_offer(session_key)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.negotiation()
            .create_session_renegotiation_offer(session_key)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<(), TransportAdapterError> {
        self.negotiation()
            .apply_session_answer(session_key, answer_sdp)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn connect_transport(
        &self,
        session_key: &TransportSessionKey,
        request: TransportConnectRequest<'_>,
    ) -> Result<(), TransportAdapterError> {
        if let Some(sdp_offer) = request.sdp_offer() {
            validation::validate_sdp_offer(sdp_offer)?;
        }
        let parsed_dtls_parameters = validation::parse_dtls_parameters(request.dtls_parameters())?;
        let remote_ice_credentials =
            validation::parse_remote_ice_credentials(request.ice_parameters())?;
        self.ensure_connect_transition(session_key, request.direction())?;
        debug!(
            direction = ?request.direction(),
            session_id = ?session_key.session_id(),
            media_worker_id = session_key.media_worker_id(),
            "validated DTLS parameters and transport lifecycle state before rtc transport connect"
        );
        self.request_worker(|response| RtcWorkerCommand::ConnectTransport {
            session_key: session_key.clone(),
            direction: request.direction(),
            parsed_dtls_parameters,
            remote_ice_credentials,
            response,
        })
        .await?;
        self.mark_connected(session_key, request.direction())?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.sessions().close_session(session_key).await
    }

    #[cfg(test)]
    pub(crate) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .remove_media(session_key, transport_media_id)
            .await
    }

    #[allow(
        dead_code,
        reason = "protocol publish commit wiring is landing incrementally and this lookup is already exercised by negotiation tests"
    )]
    #[cfg(test)]
    pub(crate) async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.media()
            .negotiated_producer_parameters(session_key, transport_media_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn add_recv_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.media()
            .add_recv_media(session_key, media_kind, rtp_parameters)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn add_send_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        remote_source_control: Option<RemoteSourceControl>,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.media()
            .add_send_media(
                consumer_session_key,
                media_kind,
                source_session_key,
                source_transport_media_id,
                remote_source_control,
                consumer_rtp_parameters,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .set_producer_active(session_key, transport_media_id, active)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .set_consumer_active(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                active,
            )
            .await
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Phase 6 introduces the server-owned source gate before the channel/runtime policy caller lands, so this adapter entry point is intentionally staged"
    )]
    pub(super) async fn set_route_control_source_packet_gate(
        &self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<super::super::route_control::PacketLayerGate>,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .set_route_control_source_packet_gate(
                source_session_key,
                source_transport_media_id,
                packet_gate,
            )
            .await
    }

    #[cfg(test)]
    pub(crate) async fn set_source_packet_gate(
        &self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<SourcePacketGate>,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .set_source_packet_gate(source_session_key, source_transport_media_id, packet_gate)
            .await
    }

    #[cfg(test)]
    pub(crate) fn activate_relay_route(
        &self,
        source_transport_media_id: TransportMediaId,
        target: &Self,
    ) -> Result<(), TransportAdapterError> {
        self.media()
            .activate_relay_route(source_transport_media_id, target)
    }

    #[cfg(test)]
    pub(crate) fn deactivate_relay_route(
        &self,
        source_transport_media_id: TransportMediaId,
        target: &Self,
    ) {
        self.media()
            .deactivate_relay_route(source_transport_media_id, target);
    }

    #[cfg(test)]
    pub(crate) fn set_relay_route_active(
        &self,
        source_transport_media_id: TransportMediaId,
        target: &Self,
        active: bool,
    ) {
        self.media()
            .set_relay_route_active(source_transport_media_id, target, active);
    }
}

impl RtcTransportNegotiationFacade<'_> {
    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub(crate) async fn transport_bootstrap_payload(
        self,
        session_key: &TransportSessionKey,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<SessionTransportBootstrap, TransportAdapterError> {
        let payload = self
            .adapter
            .request_worker(|response| RtcWorkerCommand::BuildBootstrap {
                session_key: session_key.clone(),
                router_capabilities: router_capabilities.clone(),
                response,
            })
            .await?;
        validation::validate_bootstrap_payload(&payload)?;
        #[cfg(test)]
        self.adapter.mark_bootstrap_sent(session_key)?;
        Ok(payload)
    }

    pub(crate) async fn create_initial_session_offer(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.adapter
            .request_worker(|response| RtcWorkerCommand::CreateInitialSessionOffer {
                session_key: session_key.clone(),
                response,
            })
            .await
    }

    pub(crate) async fn create_session_renegotiation_offer(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.adapter
            .request_worker(
                |response| RtcWorkerCommand::CreateSessionRenegotiationOffer {
                    session_key: session_key.clone(),
                    response,
                },
            )
            .await
    }

    pub(crate) async fn apply_session_answer(
        self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<(), TransportAdapterError> {
        self.adapter
            .request_worker(|response| RtcWorkerCommand::ApplySessionAnswer {
                session_key: session_key.clone(),
                answer_sdp: answer_sdp.to_owned(),
                response,
            })
            .await
    }
}

impl RtcTransportSessionFacade<'_> {
    #[cfg(test)]
    pub(crate) async fn close_session(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let _ = self.close_session_with_outcome(session_key).await?;
        Ok(())
    }

    pub(crate) async fn close_session_with_outcome(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<CloseSessionOutcome, TransportAdapterError> {
        #[cfg(test)]
        {
            let Ok(mut transport_states) = self.adapter.transport_states.lock() else {
                return Err(TransportAdapterError::TransportUnavailable);
            };
            transport_states.retain(|key, _| key.session_key != *session_key);
        }
        let Some(worker_handle) = self.adapter.worker_handle()? else {
            return Ok(CloseSessionOutcome::new(
                CloseSessionState::SessionClosed,
                Vec::new(),
            ));
        };
        let close_outcome = self
            .adapter
            .send_worker_command(&worker_handle, |response| RtcWorkerCommand::CloseSession {
                session_key: session_key.clone(),
                response,
            })
            .await?;
        if close_outcome.state() == CloseSessionState::WorkerDrained {
            worker_handle.shutdown_token.cancel();
            if let Ok(mut worker_slot) = self.adapter.worker_handle.lock() {
                *worker_slot = None;
            }
            self.adapter
                .packet_loop_started
                .store(false, Ordering::Release);
        }
        Ok(close_outcome)
    }
}

impl RtcTransportMediaFacade<'_> {
    #[cfg(test)]
    pub(crate) async fn remove_media(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let _ = self
            .remove_media_with_outcome(session_key, transport_media_id)
            .await?;
        Ok(())
    }

    pub(crate) async fn remove_media_with_outcome(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RemoveMediaOutcome, TransportAdapterError> {
        self.adapter
            .request_worker(|response| RtcWorkerCommand::RemoveMedia {
                session_key: session_key.clone(),
                transport_media_id,
                response,
            })
            .await
    }

    #[allow(
        dead_code,
        reason = "protocol publish commit wiring is landing incrementally and this lookup is already exercised by negotiation tests"
    )]
    pub(crate) async fn negotiated_producer_parameters(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.adapter
            .request_worker(
                |response| RtcWorkerCommand::ResolveNegotiatedProducerParameters {
                    session_key: session_key.clone(),
                    transport_media_id,
                    response,
                },
            )
            .await
    }

    pub(crate) async fn add_recv_media(
        self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.adapter
            .request_worker(|response| RtcWorkerCommand::AddRecvMedia {
                session_key: session_key.clone(),
                media_kind,
                rtp_parameters: rtp_parameters.clone(),
                response,
            })
            .await
    }

    pub(crate) async fn add_send_media(
        self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        remote_source_control: Option<RemoteSourceControl>,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.adapter
            .request_worker(|response| RtcWorkerCommand::AddSendMedia {
                consumer_session_key: consumer_session_key.clone(),
                media_kind,
                source_session_key: source_session_key.clone(),
                source_transport_media_id,
                remote_source_control,
                consumer_rtp_parameters: consumer_rtp_parameters.clone(),
                response,
            })
            .await
    }

    pub(crate) async fn set_producer_active(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.adapter
            .request_worker(|response| RtcWorkerCommand::SetProducerActive {
                session_key: session_key.clone(),
                transport_media_id,
                active,
                response,
            })
            .await
    }

    pub(crate) async fn set_consumer_active(
        self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.adapter
            .request_worker(|response| RtcWorkerCommand::SetConsumerActive {
                consumer_session_key: consumer_session_key.clone(),
                consumer_transport_media_id,
                source_session_key: source_session_key.clone(),
                source_transport_media_id,
                active,
                response,
            })
            .await
    }

    pub(crate) async fn transport_media_mid(
        self,
        transport_media_id: TransportMediaId,
    ) -> Result<Option<String>, TransportAdapterError> {
        self.adapter
            .request_worker(|response| RtcWorkerCommand::ResolveMediaMid {
                transport_media_id,
                response,
            })
            .await
    }

    #[allow(
        dead_code,
        reason = "Phase 6 introduces the server-owned source gate before the channel/runtime policy caller lands, so this adapter entry point is intentionally staged"
    )]
    pub(super) async fn set_route_control_source_packet_gate(
        self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<super::super::route_control::PacketLayerGate>,
    ) -> Result<(), TransportAdapterError> {
        self.adapter
            .request_worker(|response| RtcWorkerCommand::SetSourcePacketGate {
                source_session_key: source_session_key.clone(),
                source_transport_media_id,
                packet_gate,
                response,
            })
            .await
    }

    pub(crate) async fn set_source_packet_gate(
        self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: Option<SourcePacketGate>,
    ) -> Result<(), TransportAdapterError> {
        let packet_gate = packet_gate.map(|packet_gate| match packet_gate {
            SourcePacketGate::Rid(rid) => {
                super::super::route_control::PacketLayerGate::Rid(rid.as_str().into())
            }
        });
        self.set_route_control_source_packet_gate(
            source_session_key,
            source_transport_media_id,
            packet_gate,
        )
        .await
    }

    pub(crate) fn remote_source_control(
        self,
        target: &RtcTransportAdapter,
    ) -> Result<RemoteSourceControl, TransportAdapterError> {
        let worker_handle = self.adapter.ensure_packet_loop_started()?;
        Ok(RemoteSourceControl::new(
            worker_handle.command_tx,
            target.relay_target_id,
        ))
    }

    pub(crate) fn activate_relay_route(
        self,
        source_transport_media_id: TransportMediaId,
        target: &RtcTransportAdapter,
    ) -> Result<(), TransportAdapterError> {
        let mailbox = target.ensure_packet_loop_started()?.relay_mailbox;
        self.adapter.relay_registry.activate_source_target(
            source_transport_media_id,
            target.relay_target_id,
            super::super::relay_registry::RelayTargetTransport::from(mailbox),
        );
        Ok(())
    }

    pub(crate) fn deactivate_relay_route(
        self,
        source_transport_media_id: TransportMediaId,
        target: &RtcTransportAdapter,
    ) {
        self.adapter
            .relay_registry
            .deactivate_source_target(source_transport_media_id, target.relay_target_id);
    }

    pub(crate) fn set_relay_route_active(
        self,
        source_transport_media_id: TransportMediaId,
        target: &RtcTransportAdapter,
        active: bool,
    ) {
        self.adapter.relay_registry.set_source_target_active(
            source_transport_media_id,
            target.relay_target_id,
            active,
        );
    }
}

impl fmt::Debug for RtcTransportAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RtcTransportAdapter")
            .field("public_ip", &self.public_ip)
            .field("rtc_port_range", &self.rtc_port_range)
            .finish_non_exhaustive()
    }
}
