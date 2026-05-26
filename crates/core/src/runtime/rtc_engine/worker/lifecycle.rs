//! lazy lifecycle and mailbox runtime for one RTC transport worker
//!
//! this module owns the publication contract for the worker handle and the
//! request-response helpers used by the worker methods in [`super`]
//! it is the only place where worker calls boot the packet loop, publish its
//! mailboxes and translate a closed worker into [`TransportAdapterError`]
//!
//! the packet loop is started lazily so unused RTC workers do not bind sockets
//! or allocate worker-local registries
//! once a handle is published, callers clone it out of the slot before any
//! `.await`
//! this keeps the boot lock cold-path only and prevents mailbox sends from
//! holding the publication lock
//!
//! command dispatch follows one pattern:
//!
//! ```text
//! worker method
//!   |
//!   v
//! ensure worker handle exists
//!   |
//!   v
//! build RtcWorkerCommand with oneshot response
//!   |
//!   v
//! send through worker command mailbox
//!   |
//!   v
//! await worker response
//! ```
//!
//! observability methods in this file deliberately avoid lazy boot
//! a worker that has never started has no packet observations, so the snapshot
//! surface returns empty or default values instead of creating transport state
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::{
    super::{
        bitrate::BitrateRegistry,
        commands::{RtcWorkerCommand, RtcWorkerResponse},
        packet_loop::{self, PacketLoopConfig},
        relay_registry::{RELAY_MAILBOX_CAPACITY, RelayPacketMailbox, sender_backlog_depth},
        state::TransportSessionHealth,
    },
    RtcWorker, RtcWorkerHandle,
};
use crate::{
    Bitrate,
    runtime::{
        RoomInstanceId,
        media_transport::{
            ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, ReceiverBandwidthSnapshot,
            TransportAdapterError, TransportBitrateSnapshot, TransportPlacementPressureSnapshot,
            TransportQualitySnapshot, TransportSessionKey, TransportWorkerPressureSnapshot,
        },
    },
};

/// publication slot for the lazily booted packet-loop handle
///
/// the RTC worker publishes a fully constructed worker handle into this slot
/// before any caller can start sending commands
/// loom reuses the same slot logic with modeled synchronization primitives to
/// check the publication contract
#[derive(Debug, Clone)]
pub struct WorkerHandleSlot<T> {
    handle: Option<T>,
}

impl<T> Default for WorkerHandleSlot<T> {
    fn default() -> Self {
        Self { handle: None }
    }
}

impl<T: Clone> WorkerHandleSlot<T> {
    pub fn worker_handle(&self) -> Option<T> {
        self.handle.clone()
    }

    /// publishes a fully constructed worker handle and returns a clone
    ///
    /// callers use the returned handle after releasing the slot lock so command
    /// dispatch never awaits while the publication lock is held
    pub fn store(&mut self, handle: T) -> T {
        self.handle = Some(handle.clone());
        handle
    }

    /// clears the published handle after the worker reports it has drained
    ///
    /// this makes the next mutating worker call start a fresh packet loop
    pub fn clear(&mut self) {
        self.handle = None;
    }

    #[cfg(test)]
    pub fn is_started(&self) -> bool {
        self.handle.is_some()
    }
}

impl RtcWorker {
    /// clones the current worker handle if the packet loop has been started
    ///
    /// this method never starts the worker
    /// observability paths use it so read-only snapshots do not allocate
    /// transport state just because a caller asked for diagnostics
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the handle
    /// slot lock is poisoned
    pub fn worker_handle(&self) -> Result<Option<RtcWorkerHandle>, TransportAdapterError> {
        let Ok(worker_handle) = self.worker_handle.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        Ok(worker_handle.worker_handle())
    }

    /// reports whether this worker has started its packet loop in tests
    ///
    /// a poisoned slot lock is treated as not started because tests use this as
    /// a simple lifecycle observation rather than an error-reporting API
    #[cfg(test)]
    pub fn packet_loop_started(&self) -> bool {
        let Ok(worker_handle) = self.worker_handle.lock() else {
            return false;
        };
        worker_handle.is_started()
    }

    /// lazily boots the worker-local packet loop and returns its published handle
    ///
    /// this method is the publication boundary between cold-path worker calls
    /// and the hot packet loop
    /// it must publish exactly one complete handle before any caller can send
    /// commands, then move the receiver halves into the spawned packet-loop
    /// task
    ///
    /// boot order:
    ///
    /// ```text
    /// lock worker slot
    ///   |
    ///   +-- return existing handle when another caller already started it
    ///   +-- capture current Tokio runtime before building worker resources
    ///   +-- create command, relay and snapshot channels
    ///   +-- publish cloned sender-side handle in the slot
    ///   +-- release slot lock before spawning the task
    ///   `-- spawn packet loop with receiver-side inputs
    /// ```
    ///
    /// publishing before spawn is safe because commands sent immediately after
    /// publication queue in bounded mailboxes until the spawned task polls them
    /// publishing after spawn would leave a window where a second caller can
    /// start another packet loop for the same worker
    ///
    /// the published handle contains sender-side control, shared observations
    /// and the shutdown token
    /// authoritative RTC state is created and owned by the spawned packet-loop
    /// task
    ///
    /// this is a cold-path lifecycle method
    /// packet-path allocations and routing state are owned by the spawned task
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the handle
    /// slot is poisoned or the call is made outside a Tokio runtime
    pub(super) fn ensure_packet_loop_started(
        &self,
    ) -> Result<RtcWorkerHandle, TransportAdapterError> {
        // the slot lock is the single-start guard for this worker
        let Ok(mut worker_slot) = self.worker_handle.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        if let Some(worker_handle) = worker_slot.worker_handle() {
            return Ok(worker_handle);
        }
        // spawning must use the caller's current runtime so tests and embedded
        // runtimes keep ownership of the worker task
        let Ok(current_runtime) = Handle::try_current() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let (command_tx, command_rx) = mpsc::channel(64);
        #[cfg(any(test, feature = "testing-transport"))]
        let debug_channels = super::super::test_support::RtcWorkerDebugChannels::new();
        let (relay_tx, relay_rx) = mpsc::channel(RELAY_MAILBOX_CAPACITY);
        // observability reads these side channels without entering the packet
        // loop, while authoritative state stays owned by the worker task
        let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
        let snapshot_state = Arc::new(Mutex::new(super::super::state::RtcSnapshotState::default()));
        let packet_loop_lag = Arc::new(packet_loop::PacketLoopLagSnapshot::new(Instant::now()));
        let shutdown_token = CancellationToken::new();
        let worker_handle = RtcWorkerHandle {
            command_tx,
            #[cfg(any(test, feature = "testing-transport"))]
            debug_handle: debug_channels.handle(),
            relay_mailbox: RelayPacketMailbox::new(relay_tx),
            bitrate_registry: Arc::clone(&bitrate_registry),
            snapshot_state: Arc::clone(&snapshot_state),
            packet_loop_lag: Arc::clone(&packet_loop_lag),
            shutdown_token: shutdown_token.clone(),
        };
        // publication happens while the lock is held so competing callers see
        // one complete handle
        let worker_handle = worker_slot.store(worker_handle);
        // the lock protects publication only
        // task spawn, logging and packet-loop construction do not need to extend
        // the critical section
        drop(worker_slot);
        let packet_loop_inputs =
            packet_loop::PacketLoopInputReceivers::new(command_rx, relay_rx, shutdown_token);
        #[cfg(any(test, feature = "testing-transport"))]
        let packet_loop_inputs = debug_channels.install(packet_loop_inputs);
        info!(
            relay_target_id = ?self.relay_target_id,
            public_ip = %self.public_ip,
            max_bitrate_in_bps = self.max_bitrate_in.as_bps(),
            max_bitrate_out_bps = self.max_bitrate_out.as_bps(),
            rtc_port_range_min = self.rtc_port_range.min(),
            rtc_port_range_max = self.rtc_port_range.max(),
            "booted rtc packet loop worker"
        );
        current_runtime.spawn(packet_loop::run_packet_loop(
            PacketLoopConfig {
                public_ip: self.public_ip,
                max_bitrate_in: self.max_bitrate_in,
                max_bitrate_out: self.max_bitrate_out,
                video_bitrate_limits: self.video_bitrate_limits,
                rtc_port_range: self.rtc_port_range,
                codec_flags: self.codec_flags,
                codec_preferences: self.codec_preferences,
                media_quality_interval: self.media_quality_interval,
                media_id_base: self.media_id_base,
                diagnostics: Arc::clone(&self.diagnostics),
                packet_sink_registry: Arc::clone(&self.packet_sink_registry),
                source_policy_signal: Arc::clone(&self.source_policy_signal),
                metrics: Arc::clone(&self.metrics),
                rtp_metrics: Arc::clone(&self.rtp_metrics),
                rtc_metrics: Arc::clone(&self.rtc_metrics),
                packet_loop_lag,
            },
            bitrate_registry,
            snapshot_state,
            packet_loop_inputs,
        ));
        Ok(worker_handle)
    }

    /// sends a request command to the worker after starting it if needed
    ///
    /// use this for mutating worker operations where the absence of a worker
    /// means transport state must be created before the command can run
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when worker boot,
    /// command send or response receive fails
    pub(super) async fn request_worker<T, F>(
        &self,
        build_command: F,
    ) -> Result<T, TransportAdapterError>
    where
        F: FnOnce(RtcWorkerResponse<T>) -> RtcWorkerCommand,
    {
        let worker_handle = self.ensure_packet_loop_started()?;
        self.send_worker_command(&worker_handle, build_command)
            .await
    }

    /// sends a request command through an already acquired worker handle
    ///
    /// observability and close paths use this when they have intentionally
    /// decided whether a missing worker should be treated as empty state or
    /// should be booted first
    ///
    /// # Errors
    ///
    /// returns [`TransportAdapterError::TransportUnavailable`] when the command
    /// mailbox is closed or the response sender is dropped before answering
    pub(super) async fn send_worker_command<T, F>(
        &self,
        worker_handle: &RtcWorkerHandle,
        build_command: F,
    ) -> Result<T, TransportAdapterError>
    where
        F: FnOnce(RtcWorkerResponse<T>) -> RtcWorkerCommand,
    {
        let (response_tx, response_rx) = oneshot::channel();
        worker_handle
            .command_tx
            .send(build_command(response_tx))
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?
    }
}

impl RtcWorker {
    /// reads bitrate counters for the requested sessions without booting worker state
    ///
    /// a missing worker, unavailable registry or missing session contributes no
    /// bitrate
    /// callers must treat the result as a recent transport observation rather
    /// than an accounting source of truth
    pub fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return TransportBitrateSnapshot::default();
        };
        let Ok(bitrate_registry) = worker_handle.bitrate_registry.lock() else {
            return TransportBitrateSnapshot::default();
        };
        bitrate_registry.transport_bitrate_snapshot_at(session_keys, Instant::now())
    }

    /// reads receiver bandwidth estimates from the worker snapshot state
    ///
    /// an empty result means the worker has no current estimate or the snapshot
    /// side channel is unavailable
    /// it does not mean the room has no receivers
    pub fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return ReceiverBandwidthSnapshot::default();
        };
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return ReceiverBandwidthSnapshot::default();
        };
        snapshot_state.receiver_bandwidth_snapshot(session_keys)
    }

    /// reads sampled media quality from the worker snapshot state
    pub fn transport_quality_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportQualitySnapshot {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return TransportQualitySnapshot::default();
        };
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return TransportQualitySnapshot::default();
        };
        snapshot_state.transport_quality_snapshot(session_keys)
    }

    /// builds a placement-pressure snapshot for selected sessions
    ///
    /// egress bitrate is scoped to the supplied sessions while packet-loop lag
    /// and mailbox backlogs describe the whole worker because those resources
    /// are shared by every session on the packet loop
    pub fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return TransportPlacementPressureSnapshot::default();
        };
        let now = Instant::now();
        let egress_bitrate = match worker_handle.bitrate_registry.lock() {
            Ok(bitrate_registry) => bitrate_registry.egress_bitrate_snapshot_at(session_keys, now),
            Err(_error) => Bitrate::zero(),
        };
        pressure_snapshot(&worker_handle, egress_bitrate, now)
    }

    /// builds a pressure snapshot for the whole worker
    ///
    /// this is used by room placement to compare local workers
    /// the result is still best-effort and falls back to zero pressure when the
    /// worker has not started
    pub fn worker_pressure_snapshot(
        &self,
        media_worker_id: usize,
    ) -> TransportWorkerPressureSnapshot {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return TransportWorkerPressureSnapshot::new(
                media_worker_id,
                TransportPlacementPressureSnapshot::default(),
            );
        };
        let now = Instant::now();
        let egress_bitrate = match worker_handle.bitrate_registry.lock() {
            Ok(bitrate_registry) => bitrate_registry.total_egress_bitrate_snapshot_at(now),
            Err(_error) => Bitrate::zero(),
        };
        TransportWorkerPressureSnapshot::new(
            media_worker_id,
            pressure_snapshot(&worker_handle, egress_bitrate, now),
        )
    }

    /// reads the latest transport health side-channel entry for one session
    ///
    /// `None` means the worker is missing, the snapshot lock is unavailable or
    /// no health event has been observed for the session
    pub fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        let worker_handle = self.worker_handle().ok().flatten()?;
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return None;
        };
        snapshot_state.transport_health(session_key)
    }

    /// asks the packet loop for its current active-speaker source snapshot
    ///
    /// this command is read-only but still enters the worker mailbox because
    /// the source activity ordering lives beside route-control state
    /// dispatch failures return an empty snapshot
    pub async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return Vec::new();
        };
        self.send_worker_command(&worker_handle, |response| {
            RtcWorkerCommand::ActiveSpeakerSourceSnapshot { response }
        })
        .await
        .unwrap_or_default()
    }

    /// asks the packet loop for detailed active-speaker diagnostics
    ///
    /// diagnostics are read through the mailbox for the same ownership reason
    /// as source snapshots
    /// dispatch failures return an empty diagnostic set
    pub async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return Vec::new();
        };
        self.send_worker_command(&worker_handle, |response| {
            RtcWorkerCommand::ActiveSpeakerDiagnosticSnapshot { response }
        })
        .await
        .unwrap_or_default()
    }

    /// reads the next active-speaker expiry deadline from the packet loop
    ///
    /// `None` means no worker is running, no source has an expiry deadline or
    /// the worker could not answer the read command
    pub async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        let worker_handle = self.worker_handle().ok().flatten()?;
        self.send_worker_command(&worker_handle, |response| {
            RtcWorkerCommand::NextActiveSpeakerDeadline { response }
        })
        .await
        .ok()
        .flatten()
    }

    /// reads room ids whose transport-observed source activity expired by `now`
    ///
    /// the packet loop owns expiry calculation because the timestamps are
    /// produced by packet observation
    /// dispatch failures return an empty set so schedulers can retry on the
    /// next wakeup
    pub async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return BTreeSet::new();
        };
        self.send_worker_command(&worker_handle, |response| {
            RtcWorkerCommand::ExpiredActiveSpeakerRoomInstanceIds { now, response }
        })
        .await
        .unwrap_or_default()
    }
}

/// combines command and relay mailbox saturation into one pressure score
///
/// the score is intentionally the max of both queues
/// one saturated input path should make placement avoid the worker even if the
/// other path is idle
fn worker_pressure_score(
    command_backlog_depth: usize,
    command_capacity: usize,
    relay_mailbox_depth: usize,
    relay_mailbox_capacity: usize,
) -> u8 {
    backlog_pressure_score(command_backlog_depth, command_capacity).max(backlog_pressure_score(
        relay_mailbox_depth,
        relay_mailbox_capacity,
    ))
}

fn pressure_snapshot(
    worker_handle: &RtcWorkerHandle,
    egress_bitrate: Bitrate,
    now: Instant,
) -> TransportPlacementPressureSnapshot {
    let packet_loop_lag_ms = worker_handle.packet_loop_lag.packet_loop_lag_ms_at(now);
    let command_backlog_depth = sender_backlog_depth(&worker_handle.command_tx);
    let relay_mailbox_depth = worker_handle.relay_mailbox.backlog_depth();
    TransportPlacementPressureSnapshot {
        egress_bitrate,
        packet_loop_lag_ms,
        command_backlog_depth,
        relay_mailbox_depth,
        worker_pressure_score: worker_pressure_score(
            command_backlog_depth,
            worker_handle.command_tx.max_capacity(),
            relay_mailbox_depth,
            RELAY_MAILBOX_CAPACITY,
        ),
    }
}

/// converts one bounded mailbox depth into a percentage pressure score
///
/// zero capacity is treated as no pressure because there is no usable divisor
/// and current Tokio bounded mailboxes always report a positive capacity
fn backlog_pressure_score(backlog_depth: usize, capacity: usize) -> u8 {
    if capacity == 0 {
        return 0;
    }
    let score = backlog_depth.saturating_mul(100) / capacity;
    u8::try_from(score.min(100)).map_or(100, |value| value)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn placement_pressure_reads_packet_loop_lag_from_atomic_snapshot() -> Result<(), &'static str> {
        let adapter = RtcWorker::default();
        let now = Instant::now();
        let started_at = now.checked_sub(Duration::from_millis(200)).unwrap_or(now);
        let packet_loop_lag = Arc::new(packet_loop::PacketLoopLagSnapshot::new(started_at));
        packet_loop_lag.publish_for_test(37, started_at + Duration::from_millis(150));
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (relay_tx, _relay_rx) = mpsc::channel(RELAY_MAILBOX_CAPACITY);
        let debug_channels = super::super::super::test_support::RtcWorkerDebugChannels::new();

        let worker_handle = RtcWorkerHandle {
            command_tx,
            debug_handle: debug_channels.handle(),
            relay_mailbox: RelayPacketMailbox::new(relay_tx),
            bitrate_registry: Arc::new(Mutex::new(BitrateRegistry::default())),
            snapshot_state: Arc::new(Mutex::new(
                super::super::super::state::RtcSnapshotState::default(),
            )),
            packet_loop_lag,
            shutdown_token: CancellationToken::new(),
        };
        {
            let Ok(mut worker_slot) = adapter.worker_handle.lock() else {
                return Err("worker slot lock poisoned");
            };
            worker_slot.store(worker_handle);
        }

        let snapshot = adapter.placement_pressure_snapshot(&[]);

        assert_eq!(snapshot.packet_loop_lag_ms, 37);
        Ok(())
    }
}
