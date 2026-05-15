//! Lifecycle and communication runtime for the RTC transport worker.
//!
//! This module implements the internal machinery for managing the life of a
//! background packet loop worker and dispatching commands to it.
//!
//! ### Worker Bootstrapping
//!
//! Workers are started lazily via [`RtcTransportWorker::ensure_packet_loop_started`].
//! The first call to any facade method that requires worker interaction will
//! trigger the spawning of the background tokio task that runs the packet loop.
//!
//! ### Command Dispatching
//!
//! Communication with the worker follows a request/response pattern:
//! 1. The facade method constructs a command (e.g., `RtcWorkerCommand::CreateInitialSessionOffer`).
//! 2. It creates a `oneshot` room for the response.
//! 3. It sends the command + the response sender to the worker via an `mpsc` room.
//! 4. It waits for the response on the `oneshot` receiver.
//!
//! This pattern allows the facade methods to be `async` and return values from
//! the worker while keeping the worker itself synchronous and focused on the
//! media hot-path.
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
    facade::{RtcTransportObservabilityFacade, RtcTransportWorker, RtcWorkerHandle},
};
use crate::{
    Bitrate,
    runtime::{
        RoomInstanceId,
        media_transport::{
            ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, ReceiverBandwidthSnapshot,
            TransportAdapterError, TransportBitrateSnapshot, TransportPlacementPressureSnapshot,
            TransportSessionKey, TransportWorkerPressureSnapshot,
        },
    },
};

/// Publication slot for the lazily booted packet-loop handle.
///
/// The RTC worker publishes a fully constructed worker handle into this slot
/// before any caller can start sending commands. Loom reuses the same slot logic
/// with modeled synchronization primitives to check the publication contract.
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

    pub fn store(&mut self, handle: T) -> T {
        self.handle = Some(handle.clone());
        handle
    }

    pub fn clear(&mut self) {
        self.handle = None;
    }

    #[cfg(test)]
    pub fn is_started(&self) -> bool {
        self.handle.is_some()
    }
}

impl RtcTransportWorker {
    pub fn worker_handle(&self) -> Result<Option<RtcWorkerHandle>, TransportAdapterError> {
        let Ok(worker_handle) = self.worker_handle.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        Ok(worker_handle.worker_handle())
    }

    #[cfg(test)]
    pub fn packet_loop_started(&self) -> bool {
        let Ok(worker_handle) = self.worker_handle.lock() else {
            return false;
        };
        worker_handle.is_started()
    }

    /// Lazily boot the worker-local packet loop and return the handle that all
    /// facade operations use to talk to the worker.
    pub(super) fn ensure_packet_loop_started(
        &self,
    ) -> Result<RtcWorkerHandle, TransportAdapterError> {
        let Ok(mut worker_slot) = self.worker_handle.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        if let Some(worker_handle) = worker_slot.worker_handle() {
            return Ok(worker_handle);
        }
        let Ok(current_runtime) = Handle::try_current() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let (command_tx, command_rx) = mpsc::channel(64);
        #[cfg(any(test, feature = "testing-transport"))]
        let debug_channels = super::super::test_support::RtcWorkerDebugChannels::new();
        let (relay_tx, relay_rx) = mpsc::channel(RELAY_MAILBOX_CAPACITY);
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
        let worker_handle = worker_slot.store(worker_handle);
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

    pub fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        self.observability()
            .transport_bitrate_snapshot(session_keys)
    }

    pub fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        self.observability()
            .receiver_bandwidth_snapshot(session_keys)
    }

    pub fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        self.observability()
            .placement_pressure_snapshot(session_keys)
    }

    pub fn worker_pressure_snapshot(
        &self,
        media_worker_id: usize,
    ) -> TransportWorkerPressureSnapshot {
        self.observability()
            .worker_pressure_snapshot(media_worker_id)
    }

    #[cfg(test)]
    pub fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.observability().session_transport_health(session_key)
    }

    pub async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        self.observability().active_speaker_source_snapshot().await
    }

    pub async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.observability()
            .active_speaker_diagnostic_snapshot()
            .await
    }

    pub async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        self.observability().next_active_speaker_deadline().await
    }

    pub async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        self.observability()
            .expired_active_speaker_room_instance_ids(now)
            .await
    }
}

impl RtcTransportObservabilityFacade<'_> {
    pub fn transport_bitrate_snapshot(
        self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        let Some(worker_handle) = self.worker.worker_handle().ok().flatten() else {
            return TransportBitrateSnapshot::default();
        };
        let Ok(bitrate_registry) = worker_handle.bitrate_registry.lock() else {
            return TransportBitrateSnapshot::default();
        };
        bitrate_registry.transport_bitrate_snapshot_at(session_keys, Instant::now())
    }

    pub fn receiver_bandwidth_snapshot(
        self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        let Some(worker_handle) = self.worker.worker_handle().ok().flatten() else {
            return ReceiverBandwidthSnapshot::default();
        };
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return ReceiverBandwidthSnapshot::default();
        };
        snapshot_state.receiver_bandwidth_snapshot(session_keys)
    }

    pub fn placement_pressure_snapshot(
        self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        let Some(worker_handle) = self.worker.worker_handle().ok().flatten() else {
            return TransportPlacementPressureSnapshot::default();
        };
        let now = Instant::now();
        let egress_bitrate = match worker_handle.bitrate_registry.lock() {
            Ok(bitrate_registry) => bitrate_registry.egress_bitrate_snapshot_at(session_keys, now),
            Err(_error) => Bitrate::zero(),
        };
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

    pub fn worker_pressure_snapshot(
        self,
        media_worker_id: usize,
    ) -> TransportWorkerPressureSnapshot {
        let Some(worker_handle) = self.worker.worker_handle().ok().flatten() else {
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
        let packet_loop_lag_ms = worker_handle.packet_loop_lag.packet_loop_lag_ms_at(now);
        let command_backlog_depth = sender_backlog_depth(&worker_handle.command_tx);
        let relay_mailbox_depth = worker_handle.relay_mailbox.backlog_depth();
        TransportWorkerPressureSnapshot::new(
            media_worker_id,
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
            },
        )
    }

    pub fn session_transport_health(
        self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        let worker_handle = self.worker.worker_handle().ok().flatten()?;
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return None;
        };
        snapshot_state.transport_health(session_key)
    }

    pub async fn active_speaker_source_snapshot(self) -> Vec<ActiveSpeakerSource> {
        let Some(worker_handle) = self.worker.worker_handle().ok().flatten() else {
            return Vec::new();
        };
        self.worker
            .send_worker_command(&worker_handle, |response| {
                RtcWorkerCommand::ActiveSpeakerSourceSnapshot { response }
            })
            .await
            .unwrap_or_default()
    }

    pub async fn active_speaker_diagnostic_snapshot(self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        let Some(worker_handle) = self.worker.worker_handle().ok().flatten() else {
            return Vec::new();
        };
        self.worker
            .send_worker_command(&worker_handle, |response| {
                RtcWorkerCommand::ActiveSpeakerDiagnosticSnapshot { response }
            })
            .await
            .unwrap_or_default()
    }

    pub async fn next_active_speaker_deadline(self) -> Option<Instant> {
        let worker_handle = self.worker.worker_handle().ok().flatten()?;
        self.worker
            .send_worker_command(&worker_handle, |response| {
                RtcWorkerCommand::NextActiveSpeakerDeadline { response }
            })
            .await
            .ok()
            .flatten()
    }

    pub async fn expired_active_speaker_room_instance_ids(
        self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        let Some(worker_handle) = self.worker.worker_handle().ok().flatten() else {
            return BTreeSet::new();
        };
        self.worker
            .send_worker_command(&worker_handle, |response| {
                RtcWorkerCommand::ExpiredActiveSpeakerRoomInstanceIds { now, response }
            })
            .await
            .unwrap_or_default()
    }
}

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
        let adapter = RtcTransportWorker::default();
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
