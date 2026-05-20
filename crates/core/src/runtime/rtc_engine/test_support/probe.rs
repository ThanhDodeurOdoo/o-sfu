//! a probe is a cfg-gated request that runs on the packet-loop worker and
//! returns one typed observation or setup outcome
//! it is the test-support
//! boundary for facts that live inside [`PacketLoopState`] and cannot be read
//! from snapshots without losing ordering against worker commands
//!
//! use a probe when a test needs worker-owned route, demux, session, bitrate,
//! relay or route-control state
//! keep the readable helper method in `api/test_support.rs`
//! implement one small [`DebugProbe`] type here with its request fields and
//! exact [`DebugProbe::Output`] type
//!
//! ```text
//! rtc test helper
//!   -> RtcWorkerDebugHandle::probe(P)
//!   -> packet-loop control input
//!   -> P::inspect(PacketLoopState, WorkerCommandContext)
//!   -> typed response
//! ```
//!
//! prefer read-only probes
//! a mutating probe is only appropriate when the test
//! needs deterministic packet-loop setup that cannot be expressed through the
//! production transport facade, such as seeding demux state or injecting an
//! audio activity observation

use std::fmt;
#[cfg(all(not(test), feature = "testing-transport"))]
use std::time::Instant;

use str0m::media::Mid;
use tokio::sync::{mpsc, oneshot};
#[cfg(test)]
use {
    crate::Bitrate,
    std::{net::SocketAddr, time::Instant},
};

use crate::runtime::{
    media_transport::{TransportMediaId, TransportSessionKey},
    rtc_engine::{
        packet_loop::PacketLoopInputReceivers, route_control::PacketLayerGate,
        state::PacketLoopState, worker::WorkerCommandContext,
    },
};

const DEBUG_PROBE_CHANNEL_CAPACITY: usize = 64;

/// sender-side handle for cfg-gated worker probes
///
/// the handle belongs to tests and `testing-transport` helpers that need to
/// inspect packet-loop state through the worker task that owns it
/// cloning the handle clones only the mailbox sender, not the worker state
#[derive(Clone)]
pub(in crate::runtime::rtc_engine) struct RtcWorkerDebugHandle {
    tx: mpsc::Sender<DebugProbeRequest>,
}

impl fmt::Debug for RtcWorkerDebugHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("RtcWorkerDebugHandle").finish()
    }
}

impl RtcWorkerDebugHandle {
    /// runs one typed probe on the packet-loop worker
    ///
    /// callers pass a concrete probe value and receive that probe's exact
    /// output type
    /// `None` means the worker was unavailable, the probe could
    /// not be queued or the worker task dropped the response before answering
    pub async fn probe<P>(&self, probe: P) -> Option<P::Output>
    where
        P: DebugProbe,
    {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(DebugProbeRequest::new(probe, response_tx))
            .await
            .ok()?;
        response_rx.await.ok()
    }
}

/// paired sender and receiver used during test-capable worker startup
///
/// construction stays outside production worker boot
/// installing the receiver extends the packet-loop input bundle with a probe
/// mailbox while normal commands keep their existing priority
pub(in crate::runtime::rtc_engine) struct RtcWorkerDebugChannels {
    handle: RtcWorkerDebugHandle,
    rx: mpsc::Receiver<DebugProbeRequest>,
}

impl RtcWorkerDebugChannels {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(DEBUG_PROBE_CHANNEL_CAPACITY);
        Self {
            handle: RtcWorkerDebugHandle { tx },
            rx,
        }
    }

    pub fn handle(&self) -> RtcWorkerDebugHandle {
        self.handle.clone()
    }

    pub fn install(self, inputs: PacketLoopInputReceivers) -> PacketLoopInputReceivers {
        inputs.with_probe_receiver(self.rx)
    }
}

/// contract implemented by one packet-loop probe
///
/// a probe owns the request data needed for one inspection or setup action
/// [`Self::Output`] is the typed response returned to the test helper
///
/// [`Self::inspect`] runs on the packet-loop worker with exclusive access to
/// [`PacketLoopState`]
/// it may also use [`WorkerCommandContext`] for shared
/// cold-path side stores such as snapshots or bitrate registries
pub(in crate::runtime::rtc_engine) trait DebugProbe:
    Send + 'static
{
    type Output: Send + 'static;

    /// reads or prepares worker-owned state for one test probe
    fn inspect(
        self,
        state: &mut PacketLoopState,
        context: &WorkerCommandContext<'_>,
    ) -> Self::Output;
}

/// object-safe wrapper for carrying different probe types through one mailbox
///
/// type erasure is confined to the mailbox edge
/// the call site and the concrete probe implementation keep the exact response
/// type through [`DebugProbe::Output`]
trait ErasedDebugProbe: Send {
    fn inspect(self: Box<Self>, state: &mut PacketLoopState, context: &WorkerCommandContext<'_>);
}

struct DebugProbeEnvelope<P>
where
    P: DebugProbe,
{
    probe: P,
    response: oneshot::Sender<P::Output>,
}

impl<P> ErasedDebugProbe for DebugProbeEnvelope<P>
where
    P: DebugProbe,
{
    fn inspect(self: Box<Self>, state: &mut PacketLoopState, context: &WorkerCommandContext<'_>) {
        let Self { probe, response } = *self;
        let _ = response.send(probe.inspect(state, context));
    }
}

/// erased probe envelope consumed by packet-loop control input
pub(in crate::runtime::rtc_engine) struct DebugProbeRequest {
    probe: Box<dyn ErasedDebugProbe>,
}

impl DebugProbeRequest {
    fn new<P>(probe: P, response: oneshot::Sender<P::Output>) -> Self
    where
        P: DebugProbe,
    {
        Self {
            probe: Box::new(DebugProbeEnvelope { probe, response }),
        }
    }

    pub fn inspect(self, state: &mut PacketLoopState, context: &WorkerCommandContext<'_>) {
        self.probe.inspect(state, context);
    }
}

/// dispatches one test probe against the authoritative worker state
pub(in crate::runtime::rtc_engine) fn handle_debug_probe(
    state: &mut PacketLoopState,
    context: &WorkerCommandContext<'_>,
    probe: DebugProbeRequest,
) {
    probe.inspect(state, context);
}

#[cfg(test)]
pub(in crate::runtime::rtc_engine) struct ResolveMidProbe {
    pub transport_media_id: TransportMediaId,
}

#[cfg(test)]
impl DebugProbe for ResolveMidProbe {
    type Output = Option<Mid>;

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        state.resolve_mid(self.transport_media_id)
    }
}

#[cfg(test)]
pub(in crate::runtime::rtc_engine) struct RemoteAddrOwnerProbe {
    pub source_addr: SocketAddr,
}

#[cfg(test)]
impl DebugProbe for RemoteAddrOwnerProbe {
    type Output = Option<TransportSessionKey>;

    fn inspect(
        self,
        _state: &mut PacketLoopState,
        context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        context.snapshot_state.lock().ok().and_then(|snapshot| {
            snapshot
                .remote_addr_demux
                .session_key_for_remote_addr(self.source_addr)
                .cloned()
        })
    }
}

#[cfg(test)]
pub(in crate::runtime::rtc_engine) struct HasAnyRemoteAddrSessionProbe;

#[cfg(test)]
impl DebugProbe for HasAnyRemoteAddrSessionProbe {
    type Output = bool;

    fn inspect(
        self,
        _state: &mut PacketLoopState,
        context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        context
            .snapshot_state
            .lock()
            .ok()
            .is_some_and(|snapshot| !snapshot.remote_addr_demux.is_empty())
    }
}

#[cfg(test)]
pub(in crate::runtime::rtc_engine) struct RememberRemoteAddrProbe {
    pub source_addr: SocketAddr,
    pub session_key: TransportSessionKey,
}

#[cfg(test)]
impl DebugProbe for RememberRemoteAddrProbe {
    type Output = ();

    fn inspect(
        self,
        state: &mut PacketLoopState,
        context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        if state
            .remote_addr_demux
            .remember_remote_addr(self.source_addr, &self.session_key)
            && let Ok(mut snapshot) = context.snapshot_state.lock()
        {
            let _ = snapshot
                .remote_addr_demux
                .remember_remote_addr(self.source_addr, &self.session_key);
        }
    }
}

#[cfg(test)]
pub(in crate::runtime::rtc_engine) struct SessionStreamRxSsrcProbe {
    pub session_key: TransportSessionKey,
    pub mid: Mid,
}

#[cfg(test)]
impl DebugProbe for SessionStreamRxSsrcProbe {
    type Output = Option<u32>;

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        state
            .users
            .get_mut(&self.session_key)
            .and_then(|session_state| {
                let mut direct_api = session_state.rtc.direct_api();
                direct_api
                    .stream_rx_by_mid(self.mid, None)
                    .map(|stream_rx| *stream_rx.ssrc())
            })
    }
}

#[cfg(test)]
pub(in crate::runtime::rtc_engine) struct SessionStreamTxSsrcProbe {
    pub session_key: TransportSessionKey,
    pub mid: Mid,
}

#[cfg(test)]
impl DebugProbe for SessionStreamTxSsrcProbe {
    type Output = Option<u32>;

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        state
            .users
            .get_mut(&self.session_key)
            .and_then(|session_state| {
                let mut direct_api = session_state.rtc.direct_api();
                direct_api
                    .stream_tx_by_mid(self.mid, None)
                    .map(|stream_tx| *stream_tx.ssrc())
            })
    }
}

#[cfg(test)]
pub(in crate::runtime::rtc_engine) struct SessionMaxBitrateInProbe {
    pub session_key: TransportSessionKey,
}

#[cfg(test)]
impl DebugProbe for SessionMaxBitrateInProbe {
    type Output = Option<Bitrate>;

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        state
            .users
            .get(&self.session_key)
            .and_then(|session_state| session_state.max_bitrate_in)
    }
}

#[cfg(test)]
pub(in crate::runtime::rtc_engine) struct SessionMaxBitrateOutProbe {
    pub session_key: TransportSessionKey,
}

#[cfg(test)]
impl DebugProbe for SessionMaxBitrateOutProbe {
    type Output = Option<Bitrate>;

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        state
            .users
            .get(&self.session_key)
            .and_then(|session_state| session_state.max_bitrate_out)
    }
}

#[cfg(any(test, feature = "testing-transport"))]
pub(in crate::runtime::rtc_engine) struct RouteEntryProbe {
    pub source_session_key: TransportSessionKey,
    pub source_mid: Mid,
}

#[cfg(any(test, feature = "testing-transport"))]
impl DebugProbe for RouteEntryProbe {
    type Output = Option<DebugRouteEntry>;

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        state
            .source_transport_media_id_for_mid(&self.source_session_key, self.source_mid)
            .and_then(|source_transport_media_id| {
                debug_route_entry(state, source_transport_media_id)
            })
    }
}

pub(in crate::runtime::rtc_engine) struct RouteEntryByConsumerMidProbe {
    pub consumer_session_key: TransportSessionKey,
    pub consumer_mid: Mid,
}

impl DebugProbe for RouteEntryByConsumerMidProbe {
    type Output = Option<DebugRouteEntry>;

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        state
            .consumer_source_transport_media_id_for_mid(
                &self.consumer_session_key,
                self.consumer_mid,
            )
            .and_then(|source_transport_media_id| {
                debug_route_entry(state, source_transport_media_id)
            })
    }
}

#[cfg(any(test, feature = "testing-transport"))]
pub(in crate::runtime::rtc_engine) struct RouteEntryByMediaIdProbe {
    pub source_transport_media_id: TransportMediaId,
}

#[cfg(any(test, feature = "testing-transport"))]
impl DebugProbe for RouteEntryByMediaIdProbe {
    type Output = Option<DebugRouteEntry>;

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        debug_route_entry(state, self.source_transport_media_id)
    }
}

#[cfg(test)]
pub(in crate::runtime::rtc_engine) struct RecordIncomingMediaProbe {
    pub session_key: TransportSessionKey,
    pub transport_media_id: TransportMediaId,
    pub payload_bytes: usize,
    pub now: Instant,
}

#[cfg(test)]
impl DebugProbe for RecordIncomingMediaProbe {
    type Output = ();

    fn inspect(
        self,
        state: &mut PacketLoopState,
        context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        if state
            .record_incoming_bitrate(self.transport_media_id, self.now, self.payload_bytes)
            .is_none()
            && let Ok(mut bitrate) = context.bitrate_registry.lock()
        {
            let counter = bitrate.register_incoming_media(
                &self.session_key,
                self.transport_media_id,
                self.now,
            );
            counter.record(self.now, self.payload_bytes);
            state.register_incoming_bitrate_counter(self.transport_media_id, counter);
        }
    }
}

#[cfg(any(test, feature = "testing-transport"))]
pub(in crate::runtime::rtc_engine) struct ObserveAudioActivityProbe {
    pub transport_media_id: TransportMediaId,
    pub voice_activity: Option<bool>,
    pub audio_level_dbov: Option<i8>,
    pub now: Instant,
}

#[cfg(any(test, feature = "testing-transport"))]
impl DebugProbe for ObserveAudioActivityProbe {
    type Output = ();

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        state.route_control.observe_audio_activity(
            self.transport_media_id,
            self.voice_activity,
            self.audio_level_dbov,
            self.now,
        );
    }
}

#[cfg(test)]
pub(in crate::runtime::rtc_engine) struct RelayTargetCountProbe {
    pub source_transport_media_id: TransportMediaId,
}

#[cfg(test)]
impl DebugProbe for RelayTargetCountProbe {
    type Output = usize;

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        state.relay_target_count_for_source(self.source_transport_media_id)
    }
}

#[cfg(test)]
pub(in crate::runtime::rtc_engine) struct ActiveRelayTargetCountProbe {
    pub source_transport_media_id: TransportMediaId,
}

#[cfg(test)]
impl DebugProbe for ActiveRelayTargetCountProbe {
    type Output = usize;

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &WorkerCommandContext<'_>,
    ) -> Self::Output {
        state.active_relay_target_count_for_source(self.source_transport_media_id)
    }
}

/// one downstream destination in a route snapshot
///
/// the snapshot is copied out of the packet-loop route index so tests can
/// assert ownership, negotiated media identity and route activity without
/// borrowing worker state after the probe returns
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebugRouteDestination {
    pub dest_session: TransportSessionKey,
    pub dest_transport_media_id: TransportMediaId,
    pub dest_mid: Mid,
    pub active: bool,
}

/// route-control snapshot for one source media route
///
/// tests use this value to assert the source route, destination fanout and
/// effective packet gate chosen by the packet loop
/// it is intentionally a snapshot DTO so assertions cannot mutate worker-owned
/// route state
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebugRouteEntry {
    pub source_transport_media_id: TransportMediaId,
    pub source_active: bool,
    pub active_destination_count: usize,
    pub effective_packet_gate: DebugPacketGate,
    pub destinations: Vec<DebugRouteDestination>,
}

/// packet-gate shape exposed by route probes
///
/// this mirrors the gate semantics that matter to tests while keeping the
/// packet-loop route-control type private to the transport implementation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DebugPacketGate {
    Open,
    Block,
    Rid(String),
    OperatingPoint {
        rid: Option<String>,
        max_temporal_layer_id: u8,
    },
}

fn debug_route_entry(
    state: &PacketLoopState,
    source_transport_media_id: TransportMediaId,
) -> Option<DebugRouteEntry> {
    state
        .media_route_index
        .get(&source_transport_media_id)
        .map(|entry| DebugRouteEntry {
            source_transport_media_id,
            source_active: entry.source_active,
            active_destination_count: entry.active_destination_count,
            effective_packet_gate: state
                .route_control
                .effective_packet_gate(source_transport_media_id)
                .as_ref()
                .map_or(DebugPacketGate::Open, debug_packet_gate),
            destinations: entry
                .destinations
                .iter()
                .map(|destination| DebugRouteDestination {
                    dest_session: destination.dest_session.clone(),
                    dest_transport_media_id: destination.dest_transport_media_id,
                    dest_mid: destination.dest_mid,
                    active: destination.active,
                })
                .collect(),
        })
}

fn debug_packet_gate(packet_gate: &PacketLayerGate) -> DebugPacketGate {
    match packet_gate {
        PacketLayerGate::Open => DebugPacketGate::Open,
        PacketLayerGate::Block => DebugPacketGate::Block,
        PacketLayerGate::Rid(rid) => DebugPacketGate::Rid(rid.to_string()),
        PacketLayerGate::OperatingPoint(operating_point) => DebugPacketGate::OperatingPoint {
            rid: operating_point.rid().map(|rid| rid.to_string()),
            max_temporal_layer_id: operating_point.max_temporal_layer_id(),
        },
    }
}
