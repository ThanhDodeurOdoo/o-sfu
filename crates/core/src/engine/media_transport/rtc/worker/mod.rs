//! RTC worker state and mailbox access for media transport
//!
//! this module owns lazy packet-loop boot, worker handle publication, relay
//! request construction, remote-source control handles and observability reads
//!
//! production transport operations build `RtcWorkerCommand` values at the
//! `MediaTransport` boundary and enqueue them through lifecycle command helpers
//! packet-loop command handling remains explicit inside `handlers::dispatcher`
//!
//! cfg-gated tests and benchmarks keep a small direct worker harness for setup
//! paths that would be less readable through the full transport facade
//!
//! ```text
//! MediaTransport
//!   |
//!   v
//! RtcWorker mailbox
//!   |
//!   v
//! packet-loop dispatcher
//!   |-- offer and answer state
//!   |-- producer, consumer and relay mutations
//!   `-- session teardown and worker draining
//! ```
//!
//! [`TransportMediaId`] values allocated by a worker must stay process-wide
//! distinct
//! source route maps and relay maps use them as packet-path keys even when a
//! source is forwarded to another worker
//! the worker manager assigns each worker a media-id range before the packet
//! loop starts so the hot path keeps one compact media key instead of carrying
//! a composite worker key

mod handlers;
mod lifecycle;

#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/support.rs"]
mod test_support;

use std::{
    fmt,
    net::IpAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[cfg(feature = "internal-benchmarks")]
pub use handlers::worker_set_consumer_pkt_gates_for_bench;
pub(super) use handlers::{
    KeyframeRequestMode, KeyframeRequestTarget, WorkerCommandContext, apply_src_rid_ready,
    drain_due_rid_kf_refreshes, handle_worker_command, request_kf_for_target,
};
use lifecycle::WorkerHandleSlot;
#[cfg(any(test, feature = "internal-benchmarks"))]
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
#[cfg(any(test, feature = "internal-benchmarks"))]
use str0m::media::MediaKind;
use tokio::sync::mpsc;

#[cfg(any(test, feature = "internal-benchmarks"))]
use super::commands::CloseSessionState;
#[cfg(test)]
use super::commands::{ConsumerPacketGateCommand, RtcMediaControlCommand};
use super::{
    bitrate::BitrateRegistry,
    commands::{RemoteSourceControl, RouteControlRequest, RtcWorkerCommand},
    packet_loop::PacketLoopLagSnapshot,
    relay_registry::{RelayPacketMailbox, RelayTargetId},
    state::RtcSnapshotState,
};
#[cfg(test)]
use crate::engine::media_transport::{
    AppliedSessionAnswer, ReceiverBweTargetUpdate, SourcePacketGate, TransportConsumerRoute,
    TransportResult,
};
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::engine::media_transport::{SessionOffer, TransportSessionKey};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, MediaWorkerId, RtcPortRange, RtcUdpIoBackend,
    VideoBitrateLimits,
    engine::{
        diagnostics::DiagnosticsStore,
        media_transport::{
            MediaTransportConfig, MediaTransportDeps, SourcePolicySignal, TransportAdapterError,
            TransportMediaId, TransportSourceKey,
        },
        metrics::{RtcMetricsRecorder, RtpMetricsRecorder, RuntimeMetrics},
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

static NEXT_RELAY_TARGET_ID: AtomicU64 = AtomicU64::new(1);

/// published handle for a lazily booted packet-loop worker
///
/// the handle is cloned by cold-path worker methods so they can enqueue work
/// without holding the worker publication lock across `.await`
/// it contains command channels plus the read-mostly snapshot state that can be
/// observed without entering the packet-loop task
///
/// dropping the last handle stops the worker by closing the command channel
/// drained workers keep this handle so a new session can reuse the idle packet
/// loop without racing the previous socket teardown
#[derive(Clone)]
pub struct RtcWorkerHandle {
    pub(super) command_tx: mpsc::Sender<RtcWorkerCommand>,
    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) debug_handle: super::test_support::RtcWorkerDebugHandle,
    pub(super) relay_mailbox: RelayPacketMailbox,
    pub bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    pub snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    pub(super) packet_loop_lag: Arc<PacketLoopLagSnapshot>,
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

/// worker-local RTC transport API
///
/// a worker owns one packet loop plus the process-local services that feed it
/// the surrounding worker manager decides which transport sessions live here
/// this type keeps worker state hidden behind mailbox-backed methods
///
/// all mutation methods eventually enter the same worker mailbox
/// that means the packet-loop task serializes WebRTC, media registry and route
/// state updates without exposing a shared mutable state lock to room code
///
/// observability reads are best-effort by design
/// snapshots may race with packet processing or teardown and must not be used
/// as room-membership authority
pub struct RtcWorker {
    pub(super) relay_target_id: RelayTargetId,
    /// first transport media id reserved for this worker's packet loop
    pub(super) media_id_base: u64,
    pub(super) public_ip: IpAddr,
    pub(super) max_bitrate_in: Bitrate,
    pub(super) max_bitrate_out: Bitrate,
    pub(super) video_bitrate_limits: VideoBitrateLimits,
    pub(super) rtc_port_range: RtcPortRange,
    pub(super) rtc_udp_io_backend: RtcUdpIoBackend,
    pub(super) codec_flags: MediaCodecFlags,
    pub(super) codec_preferences: CodecPreferences,
    pub(super) media_quality_interval: Option<Duration>,
    pub(super) diagnostics: Arc<DiagnosticsStore>,
    pub(super) packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    pub(super) source_policy_signal: Arc<SourcePolicySignal>,
    pub metrics: Arc<RuntimeMetrics>,
    pub(super) rtp_metrics: Arc<RtpMetricsRecorder>,
    pub(super) rtc_metrics: Arc<RtcMetricsRecorder>,
    pub(super) worker_handle: Mutex<WorkerHandleSlot<RtcWorkerHandle>>,
}

impl RtcWorker {
    /// builds one worker from validated RTC transport config
    ///
    /// construction does not start sockets or spawn the packet loop
    /// the first command that needs worker-local state triggers lazy boot
    ///
    /// `media_id_base` must identify the media-id range reserved for this
    /// worker
    /// callers get that value from the worker manager so media ids remain
    /// process-wide unique across local relay routes
    pub fn new(
        config: &MediaTransportConfig,
        deps: &MediaTransportDeps,
        source_policy_signal: Arc<SourcePolicySignal>,
        media_id_base: u64,
        media_worker_id: MediaWorkerId,
    ) -> Self {
        let metrics = deps.metrics();
        Self {
            relay_target_id: RelayTargetId::new(
                NEXT_RELAY_TARGET_ID.fetch_add(1, Ordering::Relaxed),
            ),
            media_id_base,
            public_ip: config.public_ip,
            max_bitrate_in: config.bitrate_limits.max_bitrate_in(),
            max_bitrate_out: config.bitrate_limits.max_bitrate_out(),
            video_bitrate_limits: config.video_bitrate_limits,
            rtc_port_range: config.rtc_port_range,
            rtc_udp_io_backend: config.rtc_udp_io_backend,
            codec_flags: config.codec_flags,
            codec_preferences: config.codec_preferences,
            media_quality_interval: config.media_quality_interval,
            diagnostics: deps.diagnostics(),
            packet_sink_registry: deps.packet_sink_registry(),
            source_policy_signal,
            metrics: Arc::clone(&metrics),
            rtp_metrics: metrics.register_rtp_worker_for_media_worker(media_worker_id.as_usize()),
            rtc_metrics: metrics.register_rtc_worker(),
            worker_handle: Mutex::new(WorkerHandleSlot::default()),
        }
    }
}

impl RtcWorker {
    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::CreateInitialSessionOffer {
            session_key: session_key.clone(),
            response,
        })
        .await
    }

    #[cfg(test)]
    pub async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.request_worker(
            |response| RtcWorkerCommand::CreateSessionRenegotiationOffer {
                session_key: session_key.clone(),
                response,
            },
        )
        .await
    }

    #[cfg(test)]
    pub async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::ApplySessionAnswer {
            session_key: session_key.clone(),
            answer_sdp: answer_sdp.to_owned(),
            response,
        })
        .await
    }
}

impl RtcWorker {
    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<CloseSessionState, TransportAdapterError> {
        let Some(worker_handle) = self.worker_handle()? else {
            return Ok(CloseSessionState::SessionClosed);
        };
        self.send_worker_command(&worker_handle, |response| RtcWorkerCommand::CloseSession {
            session_key: session_key.clone(),
            response,
        })
        .await
    }
}

impl RtcWorker {
    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::RemoveMedia {
            session_key: session_key.clone(),
            transport_media_id,
            response,
        })
        .await
    }

    #[cfg(test)]
    pub async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.request_worker(
            |response| RtcWorkerCommand::ResolveNegotiatedProducerParameters {
                session_key: session_key.clone(),
                transport_media_id,
                response,
            },
        )
        .await
    }

    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub async fn add_recv_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::AddRecvMedia {
            session_key: session_key.clone(),
            media_kind,
            rtp_parameters: rtp_parameters.clone(),
            response,
        })
        .await
    }

    #[cfg(test)]
    pub async fn add_send_media(
        &self,
        consumer_key: &TransportSessionKey,
        media_kind: MediaKind,
        source: TransportSourceKey,
        consumer_rtp_parameters: &RouterRtpParameters,
        active: bool,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::AddSendMedia {
            consumer_key: consumer_key.clone(),
            media_kind,
            source,
            remote_source_control: None,
            consumer_rtp_parameters: consumer_rtp_parameters.clone(),
            active,
            response,
        })
        .await
    }

    #[cfg(test)]
    async fn request_media_control(
        &self,
        request: RouteControlRequest,
    ) -> Result<(), TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::media_control(request, response))
            .await
    }

    #[cfg(test)]
    pub async fn set_producer_active(
        &self,
        source: &TransportSourceKey,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.request_media_control(RouteControlRequest::SetProducerActive {
            source: source.clone(),
            active,
        })
        .await
    }

    #[cfg(test)]
    pub async fn set_consumer_active(
        &self,
        route: &TransportConsumerRoute,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.request_media_control(RouteControlRequest::SetConsumerActive {
            route: route.clone(),
            active,
        })
        .await
    }

    #[cfg(test)]
    pub async fn set_consumer_pkt_gate(
        &self,
        route: &TransportConsumerRoute,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        let mut results = self
            .request_worker(|response| {
                RtcWorkerCommand::MediaControl(RtcMediaControlCommand::SetConsumerPacketGateBatch {
                    source: route.source().clone(),
                    updates: vec![ConsumerPacketGateCommand::from_route(route, &packet_gate)],
                    response,
                })
            })
            .await?;
        results
            .pop()
            .unwrap_or(Err(TransportAdapterError::InvalidInput))
    }

    #[cfg(test)]
    pub async fn activate_relay_route(
        &self,
        source: &TransportSourceKey,
        target: &Self,
    ) -> Result<(), TransportAdapterError> {
        self.request_media_control(target.relay_install_request(source.clone())?)
            .await
    }

    #[cfg(test)]
    pub async fn deactivate_relay_route(
        &self,
        src_media: TransportMediaId,
        target: &Self,
    ) -> Result<(), TransportAdapterError> {
        self.request_media_control(target.relay_release_request(src_media))
            .await
    }

    #[cfg(test)]
    pub async fn apply_relay_target_activity(
        &self,
        source: &TransportSourceKey,
        target: &Self,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.request_media_control(target.relay_activity_request(source.clone(), active))
            .await
    }

    #[cfg(test)]
    pub async fn set_receiver_bwe_targets<'a>(
        &self,
        updates: impl IntoIterator<Item = &'a ReceiverBweTargetUpdate>,
    ) -> Result<Vec<TransportResult<()>>, TransportAdapterError> {
        let updates = updates.into_iter().cloned().collect();
        self.request_worker(|response| RtcWorkerCommand::SetReceiverBweTargetBatch {
            updates,
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
        &self,
        target: &Self,
    ) -> Result<RemoteSourceControl, TransportAdapterError> {
        let worker_handle = self.ensure_packet_loop_started()?;
        Ok(RemoteSourceControl::with_metrics(
            worker_handle.command_tx,
            target.relay_target_id,
            Arc::clone(&target.rtc_metrics),
        ))
    }

    pub fn relay_install_request(
        &self,
        source: TransportSourceKey,
    ) -> Result<RouteControlRequest, TransportAdapterError> {
        Ok(RouteControlRequest::AddRelayTarget {
            source,
            target_id: self.relay_target_id,
            target: self.ensure_packet_loop_started()?.relay_mailbox,
        })
    }

    pub fn relay_release_request(&self, src_media: TransportMediaId) -> RouteControlRequest {
        RouteControlRequest::RemoveRelayTarget {
            src_media,
            target_id: self.relay_target_id,
        }
    }

    pub fn relay_activity_request(
        &self,
        source: TransportSourceKey,
        active: bool,
    ) -> RouteControlRequest {
        RouteControlRequest::SetRelayTargetActive {
            source,
            target_id: self.relay_target_id,
            active,
        }
    }
}

impl fmt::Debug for RtcWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RtcWorker")
            .field("public_ip", &self.public_ip)
            .field("rtc_port_range", &self.rtc_port_range)
            .field("rtc_udp_io_backend", &self.rtc_udp_io_backend)
            .finish_non_exhaustive()
    }
}
