use std::fmt;

use tokio::sync::{mpsc, oneshot};

use super::debug_command::DebugRtcWorkerCommand;
use crate::runtime::rtc_engine::packet_loop::PacketLoopInputReceivers;

const DEBUG_COMMAND_CHANNEL_CAPACITY: usize = 64;

#[derive(Clone)]
pub(in crate::runtime::rtc_engine) struct RtcWorkerDebugHandle {
    tx: mpsc::Sender<DebugRtcWorkerCommand>,
}

impl fmt::Debug for RtcWorkerDebugHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("RtcWorkerDebugHandle").finish()
    }
}

impl RtcWorkerDebugHandle {
    pub(in crate::runtime::rtc_engine) async fn request<T, F>(&self, build_command: F) -> Option<T>
    where
        F: FnOnce(oneshot::Sender<T>) -> DebugRtcWorkerCommand,
    {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx.send(build_command(response_tx)).await.ok()?;
        response_rx.await.ok()
    }
}

pub(in crate::runtime::rtc_engine) struct RtcWorkerDebugChannels {
    handle: RtcWorkerDebugHandle,
    rx: mpsc::Receiver<DebugRtcWorkerCommand>,
}

impl RtcWorkerDebugChannels {
    pub(in crate::runtime::rtc_engine) fn new() -> Self {
        let (tx, rx) = mpsc::channel(DEBUG_COMMAND_CHANNEL_CAPACITY);
        Self {
            handle: RtcWorkerDebugHandle { tx },
            rx,
        }
    }

    pub(in crate::runtime::rtc_engine) fn handle(&self) -> RtcWorkerDebugHandle {
        self.handle.clone()
    }

    pub(in crate::runtime::rtc_engine) fn install(
        self,
        inputs: PacketLoopInputReceivers,
    ) -> PacketLoopInputReceivers {
        inputs.with_debug_receiver(self.rx)
    }
}
