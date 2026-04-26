//! Facades for the RTC transport adapter.
//!
//! ### Key Structures
//!
//! * [`RtcTransportAdapter`]: The central handle for an RTC worker shard. It own
//!   the relay registry and the handle to the background worker loop.
//! * [`RtcTransportNegotiationFacade`]: Handles SDP-related operations like creating
//!   offers and applying answers.
//! * [`RtcTransportMediaFacade`]: Handles media-level operations like adding/removing
//!   send/recv tracks and controlling producer/consumer activity
//! * [`RtcTransportSessionFacade`]: Handles user-level lifeccyle like closing
//!   users and draining workers.
//! * [`RtcTransportObservabilityFacade`]: Provide read-only access to transport
//!   health, bitrates, and active speaker information.

use std::{
    fmt,
    net::IpAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::MediaKind;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::super::{
    bitrate::RtcBitrateState,
    commands::{
        CloseSessionOutcome, CloseSessionState, ConsumerPacketGateCommand, RemoteSourceControl,
        RemoveMediaOutcome, RtcWorkerCommand,
    },
    relay_registry::{RelayPacketMailbox, RelayRegistry, RelayTargetId},
    state::RtcSnapshotState,
};
use crate::{
    MediaCodecFlags, RtcPortRange,
    runtime::{
        diagnostics::DiagnosticsStore,
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
        transport_adapter::{
            AppliedSessionAnswer, ConsumerPacketGateUpdate, RtcTransportAdapterConfig,
            SessionOffer, SourcePacketGate, SourcePolicySignal, TransportAdapterError,
            TransportMediaId, TransportResult, TransportSessionKey,
        },
    },
};

static NEXT_RELAY_TARGET_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct RtcWorkerHandle {
    pub(super) command_tx: mpsc::Sender<RtcWorkerCommand>,
    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) debug_tx: mpsc::Sender<super::super::commands::debug::DebugRtcWorkerCommand>,
    pub(super) relay_mailbox: RelayPacketMailbox,
    pub bitrate_state: Arc<Mutex<RtcBitrateState>>,
    pub snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    pub(super) shutdown_token: CancellationToken,
}

impl fmt::Debug for RtcWorkerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("RtcWorkerHandle");
        debug.field("command_tx", &self.command_tx);
        #[cfg(any(test, feature = "testing-transport"))]
        debug.field("debug_tx", &self.debug_tx);
        debug.field("relay_mailbox", &self.relay_mailbox);
        debug.finish_non_exhaustive()
    }
}

pub struct RtcTransportAdapter {
    pub(super) relay_target_id: RelayTargetId,
    pub(super) public_ip: IpAddr,
    pub(super) max_bitrate_in_bps: u64,
    pub(super) max_bitrate_out_bps: u64,
    pub(super) rtc_port_range: RtcPortRange,
    pub(super) codec_flags: MediaCodecFlags,
    pub(super) diagnostics: Arc<DiagnosticsStore>,
    pub(super) packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    pub(super) relay_registry: Arc<RelayRegistry>,
    pub(super) source_policy_signal: Arc<SourcePolicySignal>,
    pub metrics: Arc<RuntimeMetrics>,
    pub(super) worker_handle: Mutex<super::runtime::WorkerHandleSlot<RtcWorkerHandle>>,
}

#[derive(Clone, Copy)]
pub struct RtcTransportNegotiationFacade<'a> {
    adapter: &'a RtcTransportAdapter,
}

#[derive(Clone, Copy)]
pub struct RtcTransportMediaFacade<'a> {
    adapter: &'a RtcTransportAdapter,
}

#[derive(Clone, Copy)]
pub struct RtcTransportSessionFacade<'a> {
    adapter: &'a RtcTransportAdapter,
}

#[derive(Clone, Copy)]
pub struct RtcTransportObservabilityFacade<'a> {
    pub(super) adapter: &'a RtcTransportAdapter,
}

impl RtcTransportAdapter {
    pub fn new(
        config: &RtcTransportAdapterConfig,
        source_policy_signal: Arc<SourcePolicySignal>,
    ) -> Self {
        Self {
            relay_target_id: RelayTargetId::new(
                NEXT_RELAY_TARGET_ID.fetch_add(1, Ordering::Relaxed),
            ),
            public_ip: config.public_ip(),
            max_bitrate_in_bps: config.max_bitrate_in_bps(),
            max_bitrate_out_bps: config.max_bitrate_out_bps(),
            rtc_port_range: config.rtc_port_range(),
            codec_flags: config.codec_flags(),
            diagnostics: config.diagnostics(),
            packet_sink_registry: config.packet_sink_registry(),
            relay_registry: Arc::new(RelayRegistry::default()),
            source_policy_signal,
            metrics: config.metrics(),
            worker_handle: Mutex::new(super::runtime::WorkerHandleSlot::default()),
        }
    }

    #[must_use]
    pub const fn negotiation(&self) -> RtcTransportNegotiationFacade<'_> {
        RtcTransportNegotiationFacade { adapter: self }
    }

    #[must_use]
    pub const fn media(&self) -> RtcTransportMediaFacade<'_> {
        RtcTransportMediaFacade { adapter: self }
    }

    #[must_use]
    pub const fn users(&self) -> RtcTransportSessionFacade<'_> {
        RtcTransportSessionFacade { adapter: self }
    }

    #[must_use]
    pub const fn observability(&self) -> RtcTransportObservabilityFacade<'_> {
        RtcTransportObservabilityFacade { adapter: self }
    }
}

impl RtcTransportNegotiationFacade<'_> {
    pub async fn create_initial_session_offer(
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

    pub async fn create_session_renegotiation_offer(
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

    pub async fn apply_session_answer(
        self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
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
    pub async fn close_session_with_outcome(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<CloseSessionOutcome, TransportAdapterError> {
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
                worker_slot.clear();
            }
        }
        Ok(close_outcome)
    }
}

impl RtcTransportMediaFacade<'_> {
    pub async fn remove_media_with_outcome(
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

    #[cfg(any(test, feature = "testing-transport"))]
    pub async fn negotiated_producer_parameters(
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

    pub async fn add_recv_media(
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

    pub async fn add_send_media(
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

    pub async fn set_producer_active(
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

    pub async fn set_consumer_active(
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

    pub async fn set_consumer_packet_gate(
        self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        let packet_gate = packet_layer_gate(packet_gate);
        self.adapter
            .request_worker(|response| RtcWorkerCommand::SetConsumerPacketGate {
                consumer_session_key: consumer_session_key.clone(),
                consumer_transport_media_id,
                source_session_key: source_session_key.clone(),
                source_transport_media_id,
                packet_gate,
                response,
            })
            .await
    }

    pub async fn set_consumer_packet_gates<'a>(
        self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        updates: impl IntoIterator<Item = &'a ConsumerPacketGateUpdate>,
    ) -> Result<Vec<TransportResult<()>>, TransportAdapterError> {
        let updates = updates
            .into_iter()
            .map(|update| {
                ConsumerPacketGateCommand::new(
                    update.consumer_session_key().clone(),
                    update.consumer_transport_media_id(),
                    packet_layer_gate(update.packet_gate().clone()),
                )
            })
            .collect();
        self.adapter
            .request_worker(|response| RtcWorkerCommand::SetConsumerPacketGateBatch {
                source_session_key: source_session_key.clone(),
                source_transport_media_id,
                updates,
                response,
            })
            .await
    }

    pub async fn request_consumer_keyframe(
        self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.adapter
            .request_worker(|response| RtcWorkerCommand::RequestConsumerKeyframe {
                consumer_session_key: consumer_session_key.clone(),
                consumer_transport_media_id,
                source_session_key: source_session_key.clone(),
                source_transport_media_id,
                response,
            })
            .await
    }

    pub async fn transport_media_mid(
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

    pub fn remote_source_control(
        self,
        target: &RtcTransportAdapter,
    ) -> Result<RemoteSourceControl, TransportAdapterError> {
        let worker_handle = self.adapter.ensure_packet_loop_started()?;
        Ok(RemoteSourceControl::new(
            worker_handle.command_tx,
            target.relay_target_id,
        ))
    }

    pub fn activate_relay_route(
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

    pub fn deactivate_relay_route(
        self,
        source_transport_media_id: TransportMediaId,
        target: &RtcTransportAdapter,
    ) {
        self.adapter
            .relay_registry
            .deactivate_source_target(source_transport_media_id, target.relay_target_id);
    }

    pub fn set_relay_route_active(
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

fn packet_layer_gate(
    packet_gate: SourcePacketGate,
) -> super::super::route_control::PacketLayerGate {
    match packet_gate {
        SourcePacketGate::Open => super::super::route_control::PacketLayerGate::Open,
        SourcePacketGate::Rid(rid) => {
            super::super::route_control::PacketLayerGate::Rid(rid.as_str().into())
        }
        SourcePacketGate::OperatingPoint(operating_point) => {
            super::super::route_control::PacketLayerGate::OperatingPoint(
                super::super::route_control::PacketOperatingPointGate::new(
                    operating_point.rid().map(Into::into),
                    operating_point.max_temporal_layer_id(),
                ),
            )
        }
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
