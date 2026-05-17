//! concern-scoped facades for one RTC transport worker
//!
//! this file is the narrow backend surface used by the media transport worker
//! manager after it has selected the worker that owns a session
//! callers express transport intent through negotiation, media, session and
//! observability facades while the worker keeps mutable RTC state inside its
//! packet-loop task
//!
//! facade methods are cold-path orchestration
//! they clone typed identifiers, build mailbox commands and await worker
//! responses
//! the packet loop remains the only owner of WebRTC sessions, media handles,
//! route-control state and relay fanout
//!
//! the split is intentionally small:
//!
//! ```text
//! media transport worker manager
//!   |
//!   v
//! selected rtc worker
//!   |-- negotiation()  offer and answer state
//!   |-- media()        producer, consumer and relay mutations
//!   |-- users()        session teardown and worker draining
//!   `-- observability() best-effort transport snapshots
//! ```
//!
//! [`TransportMediaId`] values allocated by a worker must stay process-wide
//! distinct
//! source route maps and relay maps use them as packet-path keys even when a
//! source is forwarded to another worker
//! the worker manager assigns each worker a media-id range before the packet
//! loop starts so the hot path keeps one compact media key instead of carrying
//! a composite worker key

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
    bitrate::BitrateRegistry,
    commands::{
        CloseSessionOutcome, CloseSessionState, ConsumerPacketGateCommand, RemoteSourceControl,
        RtcWorkerCommand,
    },
    packet_loop::PacketLoopLagSnapshot,
    relay_registry::{RelayPacketMailbox, RelayTargetId, RelayTargetTransport},
    state::RtcSnapshotState,
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, VideoBitrateLimits,
    runtime::{
        diagnostics::DiagnosticsStore,
        media_transport::{
            AppliedSessionAnswer, ConsumerPacketGateUpdate, MediaTransportDeps, RtcTransportConfig,
            SessionOffer, SourcePacketGate, SourcePolicySignal, TransportAdapterError,
            TransportConsumerRoute, TransportMediaId, TransportResult, TransportSessionKey,
        },
        metrics::{RtcMetricsRecorder, RtpMetricsRecorder, RuntimeMetrics},
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

static NEXT_RELAY_TARGET_ID: AtomicU64 = AtomicU64::new(1);

/// published handle for a lazily booted packet-loop worker
///
/// the handle is cloned by cold-path facade methods so they can enqueue work
/// without holding the worker publication lock across `.await`
/// it contains command channels plus the read-mostly snapshot state that can be
/// observed without entering the packet-loop task
///
/// dropping this handle does not stop the worker
/// shutdown is explicit through `shutdown_token` after the worker reports that
/// its last session has been closed
#[derive(Clone)]
pub struct RtcWorkerHandle {
    pub(super) command_tx: mpsc::Sender<RtcWorkerCommand>,
    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) debug_handle: super::super::test_support::RtcWorkerDebugHandle,
    pub(super) relay_mailbox: RelayPacketMailbox,
    pub bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    pub snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    pub(super) packet_loop_lag: Arc<PacketLoopLagSnapshot>,
    pub(super) shutdown_token: CancellationToken,
}

impl fmt::Debug for RtcWorkerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("RtcWorkerHandle");
        debug.field("command_tx", &self.command_tx);
        #[cfg(any(test, feature = "testing-transport"))]
        debug.field("debug_handle", &self.debug_handle);
        debug.field("relay_mailbox", &self.relay_mailbox);
        debug.finish_non_exhaustive()
    }
}

/// worker-local RTC transport facade
///
/// a worker owns one packet loop plus the process-local services that feed it
/// the surrounding worker manager decides which transport sessions live here
/// this type keeps worker state hidden behind negotiation, media, session and
/// observability facades
///
/// all mutation methods eventually enter the same worker mailbox
/// that means the packet-loop task serializes WebRTC, media registry and route
/// state updates without exposing a shared mutable state lock to room code
///
/// observability reads are best-effort by design
/// snapshots may race with packet processing or teardown and must not be used
/// as room-membership authority
pub struct RtcTransportWorker {
    pub(super) relay_target_id: RelayTargetId,
    /// first transport media id reserved for this worker's packet loop
    pub(super) media_id_base: u64,
    pub(super) public_ip: IpAddr,
    pub(super) max_bitrate_in: Bitrate,
    pub(super) max_bitrate_out: Bitrate,
    pub(super) video_bitrate_limits: VideoBitrateLimits,
    pub(super) rtc_port_range: RtcPortRange,
    pub(super) codec_flags: MediaCodecFlags,
    pub(super) codec_preferences: CodecPreferences,
    pub(super) diagnostics: Arc<DiagnosticsStore>,
    pub(super) packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    pub(super) source_policy_signal: Arc<SourcePolicySignal>,
    pub metrics: Arc<RuntimeMetrics>,
    pub(super) rtp_metrics: Arc<RtpMetricsRecorder>,
    pub(super) rtc_metrics: Arc<RtcMetricsRecorder>,
    pub(super) worker_handle: Mutex<super::runtime::WorkerHandleSlot<RtcWorkerHandle>>,
}

/// facade for offer and answer operations on one worker
///
/// callers must pass session keys that the worker manager has already mapped
/// to this worker
/// the worker enforces SDP state such as the one-outstanding-offer rule
#[derive(Clone, Copy)]
pub struct RtcTransportNegotiationFacade<'a> {
    worker: &'a RtcTransportWorker,
}

/// facade for producer, consumer, packet-gate and relay mutations
///
/// this surface is still cold-path orchestration
/// packet fanout and RTP forwarding stay in the worker-owned packet loop after
/// commands have installed the needed state
#[derive(Clone, Copy)]
pub struct RtcTransportMediaFacade<'a> {
    worker: &'a RtcTransportWorker,
}

/// facade for session lifecycle operations
///
/// closing a session may drain the whole worker
/// the facade owns the post-close decision that clears the lazy handle and
/// requests packet-loop shutdown once no session state remains
#[derive(Clone, Copy)]
pub struct RtcTransportSessionFacade<'a> {
    worker: &'a RtcTransportWorker,
}

/// facade for read-only transport observations
///
/// these methods expose packet-loop side channels to worker-manager policy and
/// diagnostics
/// every result is best-effort and should be interpreted as a current transport
/// observation rather than authoritative room state
#[derive(Clone, Copy)]
pub struct RtcTransportObservabilityFacade<'a> {
    pub(super) worker: &'a RtcTransportWorker,
}

impl RtcTransportWorker {
    /// builds one worker facade from validated RTC transport config
    ///
    /// construction does not start sockets or spawn the packet loop
    /// the first command that needs worker-owned state triggers lazy boot
    ///
    /// `media_id_base` must identify the media-id range reserved for this
    /// worker
    /// callers get that value from the worker manager so media ids remain
    /// process-wide unique across local relay routes
    pub fn new(
        config: &RtcTransportConfig,
        deps: &MediaTransportDeps,
        source_policy_signal: Arc<SourcePolicySignal>,
        media_id_base: u64,
        media_worker_id: usize,
    ) -> Self {
        let metrics = deps.metrics();
        Self {
            relay_target_id: RelayTargetId::new(
                NEXT_RELAY_TARGET_ID.fetch_add(1, Ordering::Relaxed),
            ),
            media_id_base,
            public_ip: config.public_ip(),
            max_bitrate_in: config.max_bitrate_in(),
            max_bitrate_out: config.max_bitrate_out(),
            video_bitrate_limits: config.video_bitrate_limits(),
            rtc_port_range: config.rtc_port_range(),
            codec_flags: config.codec_flags(),
            codec_preferences: config.codec_preferences(),
            diagnostics: deps.diagnostics(),
            packet_sink_registry: deps.packet_sink_registry(),
            source_policy_signal,
            metrics: Arc::clone(&metrics),
            rtp_metrics: metrics.register_rtp_worker_for_media_worker(media_worker_id),
            rtc_metrics: metrics.register_rtc_worker(),
            worker_handle: Mutex::new(super::runtime::WorkerHandleSlot::default()),
        }
    }

    #[must_use]
    pub const fn negotiation(&self) -> RtcTransportNegotiationFacade<'_> {
        RtcTransportNegotiationFacade { worker: self }
    }

    #[must_use]
    pub const fn media(&self) -> RtcTransportMediaFacade<'_> {
        RtcTransportMediaFacade { worker: self }
    }

    #[must_use]
    pub const fn users(&self) -> RtcTransportSessionFacade<'_> {
        RtcTransportSessionFacade { worker: self }
    }

    #[must_use]
    pub const fn observability(&self) -> RtcTransportObservabilityFacade<'_> {
        RtcTransportObservabilityFacade { worker: self }
    }
}

impl RtcTransportNegotiationFacade<'_> {
    /// creates the first transport offer for a worker-owned session
    ///
    /// this boots the packet loop if needed and asks the worker to create the
    /// bootstrap offer used for WebRTC setup and capability probing
    /// the session must not already have registered media or an earlier
    /// outstanding initial offer
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the worker
    /// cannot be booted or addressed
    ///
    /// returns [`TransportAdapterError::InvalidInput`] when the session already
    /// has an unresolved local offer
    ///
    /// returns [`TransportAdapterError::UnsupportedFeature`] when the session is
    /// no longer in the initial negotiation phase
    pub async fn create_initial_session_offer(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.worker
            .request_worker(|response| RtcWorkerCommand::CreateInitialSessionOffer {
                session_key: session_key.clone(),
                response,
            })
            .await
    }

    /// creates the next local offer after media topology changed
    ///
    /// the worker owns the staged offer and the one-outstanding-offer rule
    /// callers should request this after producer or consumer setup has staged
    /// sdp work for the session
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the session
    /// no longer exists on this worker
    ///
    /// returns [`TransportAdapterError::InvalidInput`] when the initial answer
    /// has not landed or another offer is still awaiting an answer
    ///
    /// returns [`TransportAdapterError::UnsupportedFeature`] when no
    /// renegotiation offer is currently staged
    pub async fn create_session_renegotiation_offer(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.worker
            .request_worker(
                |response| RtcWorkerCommand::CreateSessionRenegotiationOffer {
                    session_key: session_key.clone(),
                    response,
                },
            )
            .await
    }

    /// applies the browser answer that matches the current pending local offer
    ///
    /// answer application commits worker-local SDP state, refreshes negotiated
    /// producer parameters and returns the transport-derived facts needed by
    /// room code to commit staged publications
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the session
    /// is gone or the worker cannot be reached
    ///
    /// returns [`TransportAdapterError::InvalidInput`] when the SDP cannot be
    /// parsed or no matching pending offer exists
    pub async fn apply_session_answer(
        self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        self.worker
            .request_worker(|response| RtcWorkerCommand::ApplySessionAnswer {
                session_key: session_key.clone(),
                answer_sdp: answer_sdp.to_owned(),
                response,
            })
            .await
    }
}

impl RtcTransportSessionFacade<'_> {
    /// closes a session and reports whether the worker still owns live state
    ///
    /// if the packet loop was never started, the session is treated as already
    /// closed
    /// when the worker reports [`CloseSessionState::WorkerDrained`], this
    /// method clears the published worker handle and cancels the worker
    /// shutdown token
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the worker
    /// handle lock is poisoned or the worker mailbox cannot answer
    pub async fn close_session_with_outcome(
        self,
        session_key: &TransportSessionKey,
    ) -> Result<CloseSessionOutcome, TransportAdapterError> {
        let Some(worker_handle) = self.worker.worker_handle()? else {
            return Ok(CloseSessionOutcome::new(CloseSessionState::SessionClosed));
        };
        let close_outcome = self
            .worker
            .send_worker_command(&worker_handle, |response| RtcWorkerCommand::CloseSession {
                session_key: session_key.clone(),
                response,
            })
            .await?;
        if close_outcome.state() == CloseSessionState::WorkerDrained {
            worker_handle.shutdown_token.cancel();
            if let Ok(mut worker_slot) = self.worker.worker_handle.lock() {
                worker_slot.clear();
            }
        }
        Ok(close_outcome)
    }
}

impl RtcTransportMediaFacade<'_> {
    /// removes one producer or consumer media handle from a session
    ///
    /// the worker validates that the handle belongs to the supplied session
    /// removal may stage SDP cleanup, clear route-control state, drop bitrate
    /// counters and mark the session dirty for the next packet-loop turn
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the session
    /// or media handle is not owned by the worker
    ///
    /// returns [`TransportAdapterError::InvalidInput`] when the handle belongs
    /// to another session or cannot be represented in the current SDP state
    pub async fn remove_media(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.worker
            .request_worker(|response| RtcWorkerCommand::RemoveMedia {
                session_key: session_key.clone(),
                transport_media_id,
                response,
            })
            .await
    }

    /// reads answer-derived producer RTP parameters in tests
    ///
    /// this keeps adapter tests at the transport boundary while still checking
    /// the negotiated state that production code uses after answer application
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when the worker cannot resolve the
    /// requested producer media
    #[cfg(test)]
    pub async fn negotiated_producer_parameters(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.worker
            .request_worker(
                |response| RtcWorkerCommand::ResolveNegotiatedProducerParameters {
                    session_key: session_key.clone(),
                    transport_media_id,
                    response,
                },
            )
            .await
    }

    /// registers browser-uploaded media as a worker-owned producer
    ///
    /// before the initial answer, recv state can be declared directly on the
    /// worker session
    /// after negotiation, this stages a recv-only m-section and returns the
    /// transport media id that room state can bind to its producer
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the session
    /// is gone or worker command dispatch fails
    ///
    /// returns [`TransportAdapterError::InvalidInput`] when a new offer would
    /// violate the worker-owned one-outstanding-offer rule
    pub async fn add_recv_media(
        self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.worker
            .request_worker(|response| RtcWorkerCommand::AddRecvMedia {
                session_key: session_key.clone(),
                media_kind,
                rtp_parameters: rtp_parameters.clone(),
                response,
            })
            .await
    }

    /// registers browser-downloaded media as a worker-owned consumer
    ///
    /// the source may be local to the same packet loop or represented by a
    /// remote-source control handle from another worker
    /// the command creates the consumer media handle and installs the route that
    /// lets packet-loop fanout deliver RTP to the consumer session
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the
    /// consumer session or source route cannot be resolved
    ///
    /// returns [`TransportAdapterError::InvalidInput`] when the requested
    /// consumer setup violates worker SDP or route ownership rules
    pub async fn add_send_media(
        self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        remote_source_control: Option<RemoteSourceControl>,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.worker
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

    /// toggles packet fanout from one producer media handle
    ///
    /// this preserves producer registration and consumer routes
    /// it only changes whether packets from the source can leave the worker
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when the worker cannot validate or
    /// mutate the producer route
    pub async fn set_producer_active(
        self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.worker
            .request_worker(|response| RtcWorkerCommand::SetProducerActive {
                session_key: session_key.clone(),
                transport_media_id,
                active,
                response,
            })
            .await
    }

    /// toggles packet fanout to one consumer route
    ///
    /// the route must already have been validated by the worker manager for
    /// room ownership
    /// the worker revalidates route state before changing the destination
    /// activity flag
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when the consumer route cannot be found
    /// or cannot be updated
    pub async fn set_consumer_active(
        self,
        route: &TransportConsumerRoute,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.worker
            .request_worker(|response| RtcWorkerCommand::SetConsumerActive {
                route: route.clone(),
                active,
                response,
            })
            .await
    }

    /// applies one source-selection packet gate to a consumer route
    ///
    /// packet gates are room-owned policy projected into transport execution
    /// the facade translates the public transport gate into the worker
    /// route-control vocabulary before the command enters the packet loop
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when the route cannot be validated or
    /// the gate cannot become effective for the current source state
    pub async fn set_consumer_packet_gate(
        self,
        route: &TransportConsumerRoute,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        let packet_gate = packet_layer_gate(packet_gate);
        self.worker
            .request_worker(|response| RtcWorkerCommand::SetConsumerPacketGate {
                route: route.clone(),
                packet_gate,
                response,
            })
            .await
    }

    /// applies several packet gates for one source media id in one worker turn
    ///
    /// batching keeps dense-room source-policy changes as one mailbox command
    /// and one source-gate refresh
    /// the outer result reports whether the batch reached the worker
    /// each inner result reports validation for the matching update
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when the worker cannot receive or
    /// answer the batch command
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
                    update.route().consumer_session_key().clone(),
                    update.route().consumer_transport_media_id(),
                    packet_layer_gate(update.packet_gate().clone()),
                )
            })
            .collect();
        self.worker
            .request_worker(|response| RtcWorkerCommand::SetConsumerPacketGateBatch {
                source_session_key: source_session_key.clone(),
                source_transport_media_id,
                updates,
                response,
            })
            .await
    }

    /// asks the worker to request a keyframe for one consumer route
    ///
    /// the worker maps the consumer route back to the producer source
    /// local sources are requested directly, remote sources are forwarded
    /// through their [`RemoteSourceControl`]
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when the route cannot be validated or
    /// the source worker cannot be reached
    pub async fn request_consumer_keyframe(
        self,
        route: &TransportConsumerRoute,
    ) -> Result<(), TransportAdapterError> {
        self.worker
            .request_worker(|response| RtcWorkerCommand::RequestConsumerKeyframe {
                route: route.clone(),
                response,
            })
            .await
    }

    /// resolves the browser-visible MID for a transport media id
    ///
    /// this is a best-effort compatibility and diagnostic lookup
    /// `Ok(None)` can mean the media id is unknown, already removed or not yet
    /// negotiated
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the worker
    /// command path cannot answer
    pub async fn transport_media_mid(
        self,
        transport_media_id: TransportMediaId,
    ) -> Result<Option<String>, TransportAdapterError> {
        self.worker
            .request_worker(|response| RtcWorkerCommand::ResolveMediaMid {
                transport_media_id,
                response,
            })
            .await
    }

    /// builds the control handle a consumer worker stores for a remote source
    ///
    /// the returned handle lets the consumer worker send best-effort keyframe
    /// and packet-gate updates back to the worker that owns the producer
    /// this starts the source worker if it is not already running because the
    /// handle needs its command mailbox
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the source
    /// worker cannot be booted
    pub fn remote_source_control(
        self,
        target: &RtcTransportWorker,
    ) -> Result<RemoteSourceControl, TransportAdapterError> {
        let worker_handle = self.worker.ensure_packet_loop_started()?;
        Ok(RemoteSourceControl::new(
            worker_handle.command_tx,
            target.relay_target_id,
        ))
    }

    /// installs cross-worker relay delivery from this source worker to a target
    ///
    /// the target worker is booted first so its relay mailbox can be registered
    /// on the source worker before packets need to flow
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when either worker cannot be booted or
    /// the source worker rejects the relay target
    pub async fn activate_relay_route(
        self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target: &RtcTransportWorker,
    ) -> Result<(), TransportAdapterError> {
        let mailbox = target.ensure_packet_loop_started()?.relay_mailbox;
        self.worker
            .request_worker(|response| RtcWorkerCommand::AddRelayTarget {
                source_session_key: source_session_key.clone(),
                source_transport_media_id,
                target_id: target.relay_target_id,
                target: RelayTargetTransport::from(mailbox),
                response,
            })
            .await
    }

    /// removes cross-worker relay delivery from this source worker to a target
    ///
    /// relay removal is part of room-owned cleanup and should be safe to retry
    /// when the target was already removed by earlier teardown
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when the source worker cannot receive
    /// or answer the removal command
    pub async fn deactivate_relay_route(
        self,
        source_transport_media_id: TransportMediaId,
        target: &RtcTransportWorker,
    ) -> Result<(), TransportAdapterError> {
        self.worker
            .request_worker(|response| RtcWorkerCommand::RemoveRelayTarget {
                source_transport_media_id,
                target_id: target.relay_target_id,
                response,
            })
            .await
    }

    /// applies the active state of one cross-worker relay target
    ///
    /// relay target activity is separate from relay target registration
    /// inactive targets keep their mailbox identity but source fanout stops
    /// selecting them
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when the source worker cannot validate
    /// or update the relay target
    pub async fn apply_relay_target_activity(
        self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        target: &RtcTransportWorker,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.worker
            .request_worker(|response| RtcWorkerCommand::SetRelayTargetActive {
                source_session_key: source_session_key.clone(),
                source_transport_media_id,
                target_id: target.relay_target_id,
                active,
                response,
            })
            .await
    }
}

/// translates public source-selection policy into packet-loop route-control policy
///
/// the conversion stays at this boundary so room and media-transport code do
/// not import worker-private route-control types
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

impl fmt::Debug for RtcTransportWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RtcTransportWorker")
            .field("public_ip", &self.public_ip)
            .field("rtc_port_range", &self.rtc_port_range)
            .finish_non_exhaustive()
    }
}
