//! RTC worker state and mailbox access for media transport
//!
//! this module owns the ready packet-loop handle, relay request construction,
//! remote-source control handles and observability reads
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
    sync::{Arc, Mutex, atomic::AtomicU64},
    thread,
};

#[cfg(feature = "internal-benchmarks")]
pub use handlers::apply_media_control_batch;
pub(super) use handlers::{
    KeyframeRequestMode, KeyframeRequestTarget, WorkerCommandContext, apply_src_rid_ready,
    drain_due_rid_kf_refreshes, handle_worker_command, request_kf_for_target,
};
#[cfg(any(test, feature = "internal-benchmarks"))]
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
#[cfg(any(test, feature = "internal-benchmarks"))]
use str0m::media::MediaKind;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    bitrate::BitrateRegistry,
    commands::{RemoteSourceControl, RouteControlRequest, RtcWorkerCommand},
    packet_loop::PacketLoopLagSnapshot,
    relay_registry::{RelayPacketMailbox, RelayTargetId},
    state::RtcSnapshotState,
};
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::engine::media_transport::TransportAdapterError;
#[cfg(test)]
use crate::engine::media_transport::{AppliedSessionAnswer, SourcePolicySignal};
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::engine::media_transport::{SessionOffer, TransportMediaId, TransportSessionKey};
#[cfg(any(test, feature = "testing-transport"))]
use crate::engine::metrics::RuntimeMetrics;
use crate::engine::{
    media_transport::{TransportRelayRouteAction, TransportSourceKey},
    metrics::RtcMetricsRecorder,
};

static NEXT_RELAY_TARGET_ID: AtomicU64 = AtomicU64::new(1);

/// command and observation handle for a ready packet-loop worker
///
/// it contains command channels plus the read-mostly snapshot state that can be
/// observed without entering the packet-loop task
pub(super) struct RtcWorkerHandle {
    pub(super) command_tx: mpsc::Sender<RtcWorkerCommand>,
    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) debug_handle: super::test_support::RtcWorkerDebugHandle,
    pub(super) relay_mailbox: RelayPacketMailbox,
    pub(super) bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    pub(super) snapshot_state: Arc<Mutex<RtcSnapshotState>>,
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
    relay_target_id: RelayTargetId,
    handle: RtcWorkerHandle,
    shutdown: CancellationToken,
    thread: Option<thread::JoinHandle<()>>,
    #[cfg(any(test, feature = "testing-transport"))]
    pub metrics: Arc<RuntimeMetrics>,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    #[cfg(test)]
    pub(super) source_policy_signal: SourcePolicySignal,
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
        self.request_worker(|response| RtcWorkerCommand::CloseSession {
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
    pub fn remote_source_control(&self, target: &Self) -> RemoteSourceControl {
        RemoteSourceControl::with_metrics(
            self.handle.command_tx.clone(),
            target.relay_target_id,
            Arc::clone(&target.rtc_metrics),
        )
    }

    pub fn relay_route_request(
        &self,
        source: TransportSourceKey,
        action: TransportRelayRouteAction,
    ) -> RouteControlRequest {
        match action {
            TransportRelayRouteAction::Install => RouteControlRequest::AddRelayTarget {
                source,
                target_id: self.relay_target_id,
                target: self.handle.relay_mailbox.clone(),
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
        }
    }
}

impl fmt::Debug for RtcWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RtcWorker")
            .field("relay_target_id", &self.relay_target_id)
            .finish_non_exhaustive()
    }
}
