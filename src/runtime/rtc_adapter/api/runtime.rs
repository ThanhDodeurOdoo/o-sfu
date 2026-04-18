use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::Instant,
};

use crate::runtime::transport_adapter::{
    ActiveSpeakerSource, TransportAdapterError, TransportBitrateSnapshot, TransportSessionKey,
};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::super::{
    commands::RtcWorkerCommand,
    packet_loop::{self, PacketLoopConfig},
    relay_registry::{RELAY_MAILBOX_CAPACITY, RelayPacketMailbox},
    state::TransportSessionHealth,
};
use super::facade::{RtcTransportAdapter, RtcTransportObservabilityFacade, RtcWorkerHandle};

impl RtcTransportAdapter {
    pub(crate) fn worker_handle(&self) -> Result<Option<RtcWorkerHandle>, TransportAdapterError> {
        let Ok(worker_handle) = self.worker_handle.lock() else {
            return Err(TransportAdapterError::TransportUnavailable);
        };
        Ok(worker_handle.clone())
    }

    /// Lazily boot the shard-local packet loop and return the handle that all
    /// facade operations use to talk to the worker.
    pub(super) fn ensure_packet_loop_started(
        &self,
    ) -> Result<RtcWorkerHandle, TransportAdapterError> {
        if let Some(worker_handle) = self.worker_handle()? {
            return Ok(worker_handle);
        }
        if self.packet_loop_started.swap(true, Ordering::AcqRel) {
            return self
                .worker_handle()?
                .ok_or(TransportAdapterError::TransportUnavailable);
        }
        let Ok(current_runtime) = Handle::try_current() else {
            self.packet_loop_started.store(false, Ordering::Release);
            return Err(TransportAdapterError::TransportUnavailable);
        };
        let (command_tx, command_rx) = mpsc::channel(64);
        let (relay_tx, relay_rx) = mpsc::channel(RELAY_MAILBOX_CAPACITY);
        let bitrate_state = Arc::new(Mutex::new(super::super::state::RtcBitrateState::default()));
        let snapshot_state = Arc::new(Mutex::new(super::super::state::RtcSnapshotState::default()));
        let shutdown_token = CancellationToken::new();
        let worker_handle = RtcWorkerHandle {
            command_tx,
            relay_mailbox: RelayPacketMailbox::new(relay_tx),
            bitrate_state: Arc::clone(&bitrate_state),
            snapshot_state: Arc::clone(&snapshot_state),
            shutdown_token: shutdown_token.clone(),
        };
        {
            let Ok(mut worker_slot) = self.worker_handle.lock() else {
                self.packet_loop_started.store(false, Ordering::Release);
                return Err(TransportAdapterError::TransportUnavailable);
            };
            *worker_slot = Some(worker_handle.clone());
        }
        info!(
            relay_target_id = ?self.relay_target_id,
            public_ip = %self.public_ip,
            rtc_port_range_min = self.rtc_port_range.min(),
            rtc_port_range_max = self.rtc_port_range.max(),
            "booted rtc packet loop shard"
        );
        current_runtime.spawn(packet_loop::run_packet_loop(
            PacketLoopConfig {
                public_ip: self.public_ip,
                rtc_port_range: self.rtc_port_range,
                codec_flags: self.codec_flags,
                media_tap: Arc::clone(&self.media_tap),
                relay_registry: Arc::clone(&self.relay_registry),
                metrics: Arc::clone(&self.metrics),
            },
            bitrate_state,
            snapshot_state,
            command_rx,
            relay_rx,
            shutdown_token,
        ));
        Ok(worker_handle)
    }

    pub(super) async fn request_worker<T, F>(
        &self,
        build_command: F,
    ) -> Result<T, TransportAdapterError>
    where
        F: FnOnce(oneshot::Sender<Result<T, TransportAdapterError>>) -> RtcWorkerCommand,
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
        F: FnOnce(oneshot::Sender<Result<T, TransportAdapterError>>) -> RtcWorkerCommand,
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

    pub(crate) fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        self.observability()
            .transport_bitrate_snapshot(session_keys)
    }

    #[cfg(test)]
    pub(crate) fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.observability().session_transport_health(session_key)
    }

    pub(crate) async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        self.observability().active_speaker_source_snapshot().await
    }
}

impl RtcTransportObservabilityFacade<'_> {
    pub(crate) fn transport_bitrate_snapshot(
        self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        let Some(worker_handle) = self.adapter.worker_handle().ok().flatten() else {
            return TransportBitrateSnapshot::default();
        };
        let Ok(bitrate_state) = worker_handle.bitrate_state.lock() else {
            return TransportBitrateSnapshot::default();
        };
        bitrate_state.transport_bitrate_snapshot_at(session_keys, Instant::now())
    }

    pub(crate) fn session_transport_health(
        self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        let worker_handle = self.adapter.worker_handle().ok().flatten()?;
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return None;
        };
        snapshot_state.transport_health(session_key)
    }

    pub(crate) async fn active_speaker_source_snapshot(self) -> Vec<ActiveSpeakerSource> {
        let Some(worker_handle) = self.adapter.worker_handle().ok().flatten() else {
            return Vec::new();
        };
        self.adapter
            .send_worker_command(&worker_handle, |response| {
                RtcWorkerCommand::ActiveSpeakerSourceSnapshot { response }
            })
            .await
            .unwrap_or_default()
    }
}
