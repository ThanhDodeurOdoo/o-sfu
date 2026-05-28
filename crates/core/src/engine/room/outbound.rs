use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::{
    mpsc::{self, error::TrySendError},
    watch,
};

use super::{
    events::{MAX_BROADCAST_PAYLOAD_BYTES, RoomEventMessage},
    lifecycle::UserCloseReason,
    media_graph::RemoteTrackBootstrap,
};
use crate::engine::{UserId, metrics::RuntimeMetrics, source_model::UserStreamId};

pub const DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY: usize = 128;
pub const DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY: usize =
    DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY * MAX_BROADCAST_PAYLOAD_BYTES;

pub(super) type OutboundSender = UserOutboundSender;

/// delta sent from room state to one post-auth user's wire track state
///
/// the room only names the publisher and user stream. the websocket
/// user maps that pair onto its own current browser-side binding so room state
/// stays independent from wire `mid` assignment and renegotiation details
#[derive(Debug, Clone)]
pub struct TrackBindingUpdate {
    /// publisher whose wire track set changed
    pub user_id: UserId,
    /// logical stream changed for that publisher
    pub stream_id: UserStreamId,
    /// active update for an existing binding or `None` when the binding ends
    pub active: Option<bool>,
}

/// outbound work the room wants one websocket user to perform
///
/// this is the main handoff from room state transitions to user
/// protocol handling. the room never writes websocket frames or serializes
/// protocol envelopes. it emits these values and leaves user-local wire state
/// to post-auth websocket code
#[derive(Debug, Clone)]
pub enum UserOutbound {
    /// fan-out payload that maps directly to server messages
    Message(RoomEventMessage),
    /// targeted bootstrap or renegotiation work for one live user
    Request(Box<RoomEventRequest>),
    /// minimal track-binding delta for the user's wire track state
    TrackBindingUpdate(TrackBindingUpdate),
    /// ask the user owner to close the websocket with the mapped reason
    Close(UserCloseReason),
}

/// user-local work requested by the room after a room-state transition
///
/// these requests are more specific than `RoomEventMessage` because they must
/// run in the context of one live websocket user
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomEventRequest {
    /// bootstrap one newly visible remote track on the targeted user
    BootstrapRemoteTrack(RemoteTrackBootstrap),
}

impl UserOutbound {
    #[must_use]
    pub(super) fn queued_bytes(&self) -> usize {
        match self {
            Self::Message(message) => message.queued_bytes(),
            Self::Request(_) | Self::TrackBindingUpdate(_) | Self::Close(_) => 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserOutboundOverflow {
    kind: UserOutboundOverflowKind,
    message_capacity: usize,
    byte_capacity: usize,
    queued_bytes: usize,
    message_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserOutboundOverflowKind {
    MessageCount,
    QueuedBytes,
}

impl UserOutboundOverflow {
    const fn new(
        kind: UserOutboundOverflowKind,
        message_capacity: usize,
        byte_capacity: usize,
        queued_bytes: usize,
        message_bytes: usize,
    ) -> Self {
        Self {
            kind,
            message_capacity,
            byte_capacity,
            queued_bytes,
            message_bytes,
        }
    }

    #[must_use]
    pub const fn capacity(self) -> usize {
        self.message_capacity
    }

    #[must_use]
    pub const fn kind(self) -> UserOutboundOverflowKind {
        self.kind
    }

    #[must_use]
    pub const fn byte_capacity(self) -> usize {
        self.byte_capacity
    }

    #[must_use]
    pub const fn queued_bytes(self) -> usize {
        self.queued_bytes
    }

    #[must_use]
    pub const fn message_bytes(self) -> usize {
        self.message_bytes
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserOutboundQueueLimits {
    message_capacity: usize,
    byte_capacity: usize,
}

impl UserOutboundQueueLimits {
    #[must_use]
    pub fn new(message_capacity: usize, byte_capacity: usize) -> Self {
        Self {
            message_capacity: message_capacity.max(1),
            byte_capacity: byte_capacity.max(1),
        }
    }

    #[must_use]
    pub const fn message_capacity(self) -> usize {
        self.message_capacity
    }

    #[must_use]
    pub const fn byte_capacity(self) -> usize {
        self.byte_capacity
    }
}

impl Default for UserOutboundQueueLimits {
    fn default() -> Self {
        Self::new(
            DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
        )
    }
}

#[derive(Debug)]
struct QueuedUserOutbound {
    outbound: UserOutbound,
    bytes: usize,
}

#[derive(Debug, Clone)]
pub struct UserOutboundSender {
    messages: mpsc::Sender<QueuedUserOutbound>,
    overflow: watch::Sender<Option<UserOutboundOverflow>>,
    metrics: Arc<RuntimeMetrics>,
    limits: UserOutboundQueueLimits,
    queued_bytes: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub struct UserOutboundReceiver {
    messages: mpsc::Receiver<QueuedUserOutbound>,
    overflow: watch::Receiver<Option<UserOutboundOverflow>>,
    metrics: Arc<RuntimeMetrics>,
    queued_bytes: Arc<AtomicUsize>,
}

impl UserOutboundSender {
    #[must_use]
    pub fn channel(capacity: usize, metrics: Arc<RuntimeMetrics>) -> (Self, UserOutboundReceiver) {
        Self::channel_with_limits(
            UserOutboundQueueLimits::new(capacity, DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY),
            metrics,
        )
    }

    #[must_use]
    pub fn channel_with_limits(
        limits: UserOutboundQueueLimits,
        metrics: Arc<RuntimeMetrics>,
    ) -> (Self, UserOutboundReceiver) {
        let (messages_tx, messages_rx) = mpsc::channel(limits.message_capacity());
        let (overflow_tx, overflow_rx) = watch::channel(None);
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        (
            Self {
                messages: messages_tx,
                overflow: overflow_tx,
                metrics: Arc::clone(&metrics),
                limits,
                queued_bytes: Arc::clone(&queued_bytes),
            },
            UserOutboundReceiver {
                messages: messages_rx,
                overflow: overflow_rx,
                metrics,
                queued_bytes,
            },
        )
    }

    /// # Errors
    ///
    /// Returns `Full` when the bounded queue has reached its message or byte
    /// capacity. Returns `Closed` when the receiver was dropped before the
    /// message could be queued.
    pub fn send(&self, outbound: UserOutbound) -> Result<(), UserOutboundSendError> {
        let bytes = outbound.queued_bytes();
        self.reserve_bytes(bytes)?;
        match self
            .messages
            .try_send(QueuedUserOutbound { outbound, bytes })
        {
            Ok(()) => {
                self.metrics.add_ws_outbound_queued_messages(1);
                Ok(())
            }
            Err(TrySendError::Full(_outbound)) => {
                self.release_bytes(bytes);
                let overflow = self.mark_overflow(
                    UserOutboundOverflowKind::MessageCount,
                    self.queued_bytes.load(Ordering::Acquire),
                    bytes,
                );
                Err(UserOutboundSendError::Full(overflow))
            }
            Err(TrySendError::Closed(_outbound)) => {
                self.release_bytes(bytes);
                Err(UserOutboundSendError::Closed)
            }
        }
    }

    fn reserve_bytes(&self, bytes: usize) -> Result<(), UserOutboundSendError> {
        let byte_capacity = self.limits.byte_capacity();
        let mut queued = self.queued_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = queued.checked_add(bytes) else {
                let overflow =
                    self.mark_overflow(UserOutboundOverflowKind::QueuedBytes, queued, bytes);
                return Err(UserOutboundSendError::Full(overflow));
            };
            if next > byte_capacity {
                let overflow =
                    self.mark_overflow(UserOutboundOverflowKind::QueuedBytes, queued, bytes);
                return Err(UserOutboundSendError::Full(overflow));
            }
            match self.queued_bytes.compare_exchange_weak(
                queued,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_previous) => return Ok(()),
                Err(current) => queued = current,
            }
        }
    }

    fn release_bytes(&self, bytes: usize) {
        self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    fn mark_overflow(
        &self,
        kind: UserOutboundOverflowKind,
        queued_bytes: usize,
        message_bytes: usize,
    ) -> UserOutboundOverflow {
        let overflow = UserOutboundOverflow::new(
            kind,
            self.limits.message_capacity(),
            self.limits.byte_capacity(),
            queued_bytes,
            message_bytes,
        );
        self.metrics.record_ws_outbound_queue_overflow();
        let _sent = self.overflow.send(Some(overflow));
        overflow
    }
}

impl UserOutboundReceiver {
    #[must_use]
    pub fn has_overflowed(&self) -> bool {
        self.overflow.borrow().is_some()
    }

    pub async fn recv(&mut self) -> Option<UserOutbound> {
        self.messages
            .recv()
            .await
            .map(|message| self.record_received(message))
    }

    /// # Errors
    ///
    /// Returns `Empty` when no queued message is immediately available. Returns
    /// `Disconnected` when every sender was dropped and the queue is empty.
    pub fn try_recv(&mut self) -> Result<UserOutbound, mpsc::error::TryRecvError> {
        self.messages
            .try_recv()
            .map(|message| self.record_received(message))
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
                message.map_or(UserOutboundEvent::Closed, |message| {
                    UserOutboundEvent::Message(self.record_received(message))
                })
            }
        }
    }

    fn record_received(&self, message: QueuedUserOutbound) -> UserOutbound {
        self.queued_bytes.fetch_sub(message.bytes, Ordering::AcqRel);
        self.metrics.add_ws_outbound_queued_messages(-1);
        message.outbound
    }
}

impl Drop for UserOutboundReceiver {
    fn drop(&mut self) {
        let mut pending = 0_i64;
        let mut bytes = 0_usize;
        while let Ok(message) = self.messages.try_recv() {
            pending = pending.saturating_add(1);
            bytes = bytes.saturating_add(message.bytes);
        }
        if pending > 0 {
            self.metrics.add_ws_outbound_queued_messages(-pending);
        }
        if bytes > 0 {
            self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
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
            let _result = recipient.send(UserOutbound::Message(self.message.clone()));
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
