//! RTC worker state and mailbox access for media transport
//!
//! this module owns lazy packet-loop boot, worker handle publication, relay
//! request construction, remote-source control handles and observability reads
//!
//! production transport operations enter through `MediaTransport` and enqueue
//! worker commands through lifecycle helpers
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
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

#[cfg(feature = "internal-benchmarks")]
pub use handlers::apply_media_control_batch;
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

use super::{
    RtcWorkerConfig, RtpProfile,
    bitrate::BitrateRegistry,
    commands::{RemoteSourceControl, RouteControlRequest, RtcWorkerCommand},
    packet_loop::PacketLoopLagSnapshot,
    relay_registry::{RelayPacketMailbox, RelayTargetId},
    state::RtcSnapshotState,
};
#[cfg(test)]
use crate::engine::media_transport::AppliedSessionAnswer;
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::engine::media_transport::{SessionOffer, TransportMediaId, TransportSessionKey};
use crate::{
    MediaWorkerId, RtcPortRange,
    engine::{
        diagnostics::DiagnosticsStore,
        media_transport::{
            MediaTransportConfig, MediaTransportDeps, SourcePolicySignal, TransportAdapterError,
            TransportRelayRouteAction, TransportSourceKey,
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
    config: RtcWorkerConfig,
    pub(super) diagnostics: Arc<DiagnosticsStore>,
    pub(super) packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    pub(super) source_policy_signal: SourcePolicySignal,
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
        profile: Arc<RtpProfile>,
        rtc_port_range: RtcPortRange,
        deps: &MediaTransportDeps,
        source_policy_signal: SourcePolicySignal,
        media_id_base: u64,
        media_worker_id: MediaWorkerId,
    ) -> Self {
        let metrics = &deps.metrics;
        Self {
            relay_target_id: RelayTargetId::new(
                NEXT_RELAY_TARGET_ID.fetch_add(1, Ordering::Relaxed),
            ),
            config: RtcWorkerConfig {
                announced_ip: config.announced_ip,
                bitrate_limits: config.bitrate_limits,
                video_bitrate_limits: config.video_bitrate_limits,
                rtc_port_range,
                rtc_udp_io_backend: config.rtc_udp_io_backend,
                profile,
                media_quality_interval: config.media_quality_interval,
                media_id_base,
            },
            diagnostics: Arc::clone(&deps.diagnostics),
            packet_sink_registry: Arc::clone(&deps.packet_sink_registry),
            source_policy_signal,
            metrics: Arc::clone(metrics),
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
    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let Some(worker_handle) = self.worker_handle()? else {
            return Ok(());
        };
        self.send_worker_command(&worker_handle, |response| RtcWorkerCommand::CloseSession {
            session_key: session_key.clone(),
            response,
        })
        .await
    }
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

    pub fn relay_route_request(
        &self,
        source: TransportSourceKey,
        action: TransportRelayRouteAction,
    ) -> Result<RouteControlRequest, TransportAdapterError> {
        Ok(match action {
            TransportRelayRouteAction::Install => RouteControlRequest::AddRelayTarget {
                source,
                target_id: self.relay_target_id,
                target: self.ensure_packet_loop_started()?.relay_mailbox,
            },
            TransportRelayRouteAction::Release => RouteControlRequest::RemoveRelayTarget {
                source,
                target_id: self.relay_target_id,
            },
            TransportRelayRouteAction::SetActivity(activity) => {
                RouteControlRequest::SetRelayTargetActive {
                    source,
                    target_id: self.relay_target_id,
                    active: activity.is_active(),
                }
            }
        })
    }
}

impl fmt::Debug for RtcWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RtcWorker")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
