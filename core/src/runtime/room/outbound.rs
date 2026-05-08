use std::{future::pending, sync::Arc};

use tokio::sync::{
    mpsc::{self, error::TrySendError},
    watch,
};

use super::{RoomEventMessage, UserOutbound};
use crate::runtime::metrics::RuntimeMetrics;

pub const DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY: usize = 128;

pub(super) type OutboundSender = UserOutboundSender;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserOutboundOverflow {
    capacity: usize,
}

impl UserOutboundOverflow {
    const fn new(capacity: usize) -> Self {
        Self { capacity }
    }

    #[must_use]
    pub const fn capacity(self) -> usize {
        self.capacity
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserOutboundSendError {
    Full(UserOutboundOverflow),
    Closed,
}

#[derive(Debug)]
pub enum UserOutboundEvent {
    Message(UserOutbound),
    Overflow(UserOutboundOverflow),
    Closed,
}

#[derive(Debug, Clone)]
pub struct UserOutboundSender {
    messages: mpsc::Sender<UserOutbound>,
    overflow: watch::Sender<Option<UserOutboundOverflow>>,
    metrics: Arc<RuntimeMetrics>,
    capacity: usize,
}

#[derive(Debug)]
pub struct UserOutboundReceiver {
    messages: mpsc::Receiver<UserOutbound>,
    overflow: watch::Receiver<Option<UserOutboundOverflow>>,
    metrics: Arc<RuntimeMetrics>,
}

impl UserOutboundSender {
    #[must_use]
    pub fn channel(capacity: usize, metrics: Arc<RuntimeMetrics>) -> (Self, UserOutboundReceiver) {
        let capacity = capacity.max(1);
        let (messages_tx, messages_rx) = mpsc::channel(capacity);
        let (overflow_tx, overflow_rx) = watch::channel(None);
        (
            Self {
                messages: messages_tx,
                overflow: overflow_tx,
                metrics: Arc::clone(&metrics),
                capacity,
            },
            UserOutboundReceiver {
                messages: messages_rx,
                overflow: overflow_rx,
                metrics,
            },
        )
    }

    /// # Errors
    ///
    /// Returns `Full` when the bounded queue has reached capacity. Returns
    /// `Closed` when the receiver was dropped before the message could be
    /// queued.
    pub fn send(&self, outbound: UserOutbound) -> Result<(), UserOutboundSendError> {
        match self.messages.try_send(outbound) {
            Ok(()) => {
                self.metrics.add_ws_outbound_queued_messages(1);
                Ok(())
            }
            Err(TrySendError::Full(_outbound)) => {
                let overflow = UserOutboundOverflow::new(self.capacity);
                self.metrics.record_ws_outbound_queue_overflow();
                let _sent = self.overflow.send(Some(overflow));
                Err(UserOutboundSendError::Full(overflow))
            }
            Err(TrySendError::Closed(_outbound)) => Err(UserOutboundSendError::Closed),
        }
    }
}

impl UserOutboundReceiver {
    #[must_use]
    pub fn has_overflowed(&self) -> bool {
        self.overflow.borrow().is_some()
    }

    pub async fn recv(&mut self) -> Option<UserOutbound> {
        let message = self.messages.recv().await;
        self.record_received(message.is_some());
        message
    }

    /// # Errors
    ///
    /// Returns `Empty` when no queued message is immediately available. Returns
    /// `Disconnected` when every sender was dropped and the queue is empty.
    pub fn try_recv(&mut self) -> Result<UserOutbound, mpsc::error::TryRecvError> {
        let message = self.messages.try_recv();
        self.record_received(message.is_ok());
        message
    }

    pub async fn recv_event(&mut self) -> UserOutboundEvent {
        if let Some(overflow) = *self.overflow.borrow_and_update() {
            return UserOutboundEvent::Overflow(overflow);
        }
        tokio::select! {
            biased;
            overflow = wait_for_overflow(&mut self.overflow) => {
                UserOutboundEvent::Overflow(overflow)
            }
            message = self.messages.recv() => {
                self.record_received(message.is_some());
                message.map_or(UserOutboundEvent::Closed, UserOutboundEvent::Message)
            }
        }
    }

    fn record_received(&self, received: bool) {
        if received {
            self.metrics.add_ws_outbound_queued_messages(-1);
        }
    }
}

impl Drop for UserOutboundReceiver {
    fn drop(&mut self) {
        let pending = i64::try_from(self.messages.len()).unwrap_or(i64::MAX);
        if pending > 0 {
            self.metrics.add_ws_outbound_queued_messages(-pending);
        }
    }
}

async fn wait_for_overflow(
    overflow: &mut watch::Receiver<Option<UserOutboundOverflow>>,
) -> UserOutboundOverflow {
    loop {
        if let Some(overflow) = *overflow.borrow_and_update() {
            return overflow;
        }
        if overflow.changed().await.is_err() {
            pending::<()>().await;
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct MessageFanout {
    recipients: Vec<OutboundSender>,
    message: RoomEventMessage,
}

impl MessageFanout {
    pub(super) fn emit(self) {
        for recipient in self.recipients {
            let _ = recipient.send(UserOutbound::Message(self.message.clone()));
        }
    }
}

pub(super) fn fanout_all(
    recipients: impl IntoIterator<Item = OutboundSender>,
    message: &RoomEventMessage,
) -> MessageFanout {
    MessageFanout {
        recipients: recipients.into_iter().collect(),
        message: message.clone(),
    }
}
