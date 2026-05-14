//! Packet-loop worker input
//!
//! This module keeps receive-side mailbox wiring
//!
//! Production workers install only normal RTC commands. Test and
//! `testing-transport` builds may attach debug commands through
//! `rtc_engine::test_support`, but the driver still treats them as control
//! input. That keeps debug-only state mutation out of the production worker
//! contract while preserving deterministic inspection tests.

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[cfg(any(test, feature = "testing-transport"))]
use super::super::test_support::{DebugRtcWorkerCommand, handle_debug_worker_command};
use super::super::{
    commands::RtcWorkerCommand,
    forwarded_packet::ForwardedPacket,
    state::PacketLoopState,
    worker::{WorkerCommandContext, handle_worker_command},
};

/// Receive-side input bundle owned by one packet-loop worker.
///
/// `RtcTransportWorker` builds this bundle when it lazily boots a worker. After
/// that point the packet loop is the only receiver owner, while facade methods,
/// relay-control handles and test-support helpers retain sender-side handles.
///
/// Keeping these receivers together makes the loop driver depend on a single
/// worker-input contract. Production construction stays limited to commands,
/// relay packets and shutdown. Test construction can extend the bundle without
/// adding debug-channel branches to the driver.
pub(in crate::runtime::rtc_engine) struct PacketLoopInputReceivers {
    /// Facade-authored commands that mutate the authoritative RTC worker state.
    ///
    /// These commands are always checked before test debug commands so
    /// lifecycle work keeps priority when `testing-transport` is enabled.
    command_rx: mpsc::Receiver<RtcWorkerCommand>,
    /// Cross-worker relay packets drained during the pump phase.
    ///
    /// Relay input is intentionally separate from control input because it is
    /// bounded by the packet-loop turn budget and must not starve lifecycle
    /// commands or shutdown.
    relay_rx: mpsc::Receiver<ForwardedPacket>,
    /// First relay packet that woke the worker from the wait phase.
    woken_relay_packet: Option<ForwardedPacket>,
    /// Cancellation signal for the worker task.
    ///
    /// The token is cloned into the facade handle so session cleanup can stop a
    /// drained worker without closing ordinary command senders first.
    shutdown_token: CancellationToken,
    /// Test-only worker commands for deterministic inspection and state setup.
    ///
    /// The receiver is optional so production construction has no debug
    /// mailbox. When present, debug input is treated as control input because it
    /// can mutate the same worker-owned indexes as normal commands.
    #[cfg(any(test, feature = "testing-transport"))]
    debug_rx: Option<mpsc::Receiver<DebugRtcWorkerCommand>>,
}

/// Input that may mutate authoritative worker state.
///
/// The packet loop handles all control variants through one path because both
/// normal commands and cfg-gated debug commands can change transport topology,
/// source ownership or demux state. Socket datagrams and timeout wakes stay
/// outside this enum because they follow different routing rules.
pub(super) enum PacketLoopControlInput {
    /// Production worker command sent by RTC transport facades.
    Command(RtcWorkerCommand),
    /// Test-support command used for deterministic route inspection or setup.
    #[cfg(any(test, feature = "testing-transport"))]
    Debug(DebugRtcWorkerCommand),
}

/// Mailbox input that can wake the worker while it waits for external work.
pub(super) enum PacketLoopMailboxInput {
    Control(PacketLoopControlInput),
    Relay,
}

impl PacketLoopInputReceivers {
    /// Build the production receiver bundle for a newly booted packet loop.
    ///
    /// The returned bundle has no debug receiver. Test-support construction
    /// should attach one with [`Self::with_debug_receiver`] before passing the
    /// bundle to the loop.
    pub(in crate::runtime::rtc_engine) fn new(
        command_rx: mpsc::Receiver<RtcWorkerCommand>,
        relay_rx: mpsc::Receiver<ForwardedPacket>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self {
            command_rx,
            relay_rx,
            woken_relay_packet: None,
            shutdown_token,
            #[cfg(any(test, feature = "testing-transport"))]
            debug_rx: None,
        }
    }

    /// Attach the debug receiver used by deterministic RTC engine tests.
    ///
    /// This is a test-support extension point, not a second production control
    /// plane. The debug receiver is consumed into the same bundle so the loop
    /// driver does not need a separate debug wait or drain branch.
    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::runtime::rtc_engine) fn with_debug_receiver(
        mut self,
        debug_rx: mpsc::Receiver<DebugRtcWorkerCommand>,
    ) -> Self {
        self.debug_rx = Some(debug_rx);
        self
    }

    /// Borrow relay input for bounded pump-phase draining.
    ///
    /// Callers should only use this while they are already in the packet-loop
    /// turn that drains relay packets. Control waits use [`Self::recv_control`]
    /// so relay bursts cannot become lifecycle work.
    pub(super) fn relay_rx(&mut self) -> &mut mpsc::Receiver<ForwardedPacket> {
        &mut self.relay_rx
    }

    pub(super) fn shutdown_cancelled(&self) -> bool {
        self.shutdown_token.is_cancelled()
    }

    pub(super) fn take_woken_relay_packet(&mut self) -> Option<ForwardedPacket> {
        self.woken_relay_packet.take()
    }

    /// Drain already queued control input without awaiting.
    ///
    /// The loop calls this before media pumping so queued lifecycle work runs
    /// before packets are routed. Normal commands are checked before debug
    /// input to preserve production ordering even in test builds.
    pub(super) fn try_recv_control(&mut self) -> Option<PacketLoopControlInput> {
        if let Ok(command) = self.command_rx.try_recv() {
            return Some(PacketLoopControlInput::Command(command));
        }
        #[cfg(any(test, feature = "testing-transport"))]
        if let Some(debug_rx) = self.debug_rx.as_mut()
            && let Ok(command) = debug_rx.try_recv()
        {
            return Some(PacketLoopControlInput::Debug(command));
        }
        None
    }

    /// Wait for shutdown, control input or one relay packet.
    ///
    /// `None` means the worker should stop because shutdown fired or the owned
    /// command receiver closed. Relay receiver closure is ignored because relay
    /// handles are optional side inputs, not the worker lifetime owner.
    pub(super) async fn recv_control_or_relay(&mut self) -> Option<PacketLoopMailboxInput> {
        #[cfg(any(test, feature = "testing-transport"))]
        {
            if let Some(debug_rx) = self.debug_rx.as_mut() {
                return recv_mailbox_with_debug(
                    &mut self.command_rx,
                    &mut self.relay_rx,
                    &mut self.woken_relay_packet,
                    debug_rx,
                    &self.shutdown_token,
                )
                .await;
            }
        }
        recv_production_mailbox(
            &mut self.command_rx,
            &mut self.relay_rx,
            &mut self.woken_relay_packet,
            &self.shutdown_token,
        )
        .await
    }
}

impl PacketLoopControlInput {
    /// Apply this control input to authoritative worker state.
    ///
    /// The caller remains responsible for invalidating packet-routing hints
    /// after dispatch. Both variants may change ownership indexes that demux
    /// recovery relies on.
    pub(super) fn dispatch(self, state: &mut PacketLoopState, context: &WorkerCommandContext<'_>) {
        match self {
            Self::Command(command) => handle_worker_command(state, context, command),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Debug(command) => {
                handle_debug_worker_command(state, context, command);
            }
        }
    }
}

/// Wait on production mailbox inputs.
///
/// Shutdown is biased ahead of commands so teardown can interrupt an idle
/// worker. A closed command receiver ends the worker because facade command
/// delivery is the production lifetime owner. A closed relay receiver is
/// ignored because relay traffic is optional for single-worker rooms.
async fn recv_production_mailbox(
    command_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    woken_relay_packet: &mut Option<ForwardedPacket>,
    shutdown_token: &CancellationToken,
) -> Option<PacketLoopMailboxInput> {
    loop {
        tokio::select! {
            biased;
            () = shutdown_token.cancelled() => return None,
            maybe_command = command_rx.recv() => {
                return maybe_command
                    .map(PacketLoopControlInput::Command)
                    .map(PacketLoopMailboxInput::Control);
            }
            maybe_packet = relay_rx.recv() => {
                if let Some(packet) = maybe_packet {
                    *woken_relay_packet = Some(packet);
                    return Some(PacketLoopMailboxInput::Relay);
                }
            }
        }
    }
}

/// Wait on production and debug mailbox inputs in testing builds.
///
/// Production commands stay before debug commands in the biased wait. This
/// keeps lifecycle and cleanup behavior representative while still allowing
/// tests to inspect or prepare worker-local state deterministically.
#[cfg(any(test, feature = "testing-transport"))]
async fn recv_mailbox_with_debug(
    command_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    woken_relay_packet: &mut Option<ForwardedPacket>,
    debug_rx: &mut mpsc::Receiver<DebugRtcWorkerCommand>,
    shutdown_token: &CancellationToken,
) -> Option<PacketLoopMailboxInput> {
    loop {
        tokio::select! {
            biased;
            () = shutdown_token.cancelled() => return None,
            maybe_command = command_rx.recv() => {
                return maybe_command
                    .map(PacketLoopControlInput::Command)
                    .map(PacketLoopMailboxInput::Control);
            }
            maybe_command = debug_rx.recv() => {
                return maybe_command
                    .map(PacketLoopControlInput::Debug)
                    .map(PacketLoopMailboxInput::Control);
            }
            maybe_packet = relay_rx.recv() => {
                if let Some(packet) = maybe_packet {
                    *woken_relay_packet = Some(packet);
                    return Some(PacketLoopMailboxInput::Relay);
                }
            }
        }
    }
}
