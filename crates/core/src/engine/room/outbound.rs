use std::{
    collections::BTreeMap,
    future::pending,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::{
    mpsc::{self, error::TrySendError},
    watch,
};

use super::UserCloseReason;
use crate::engine::{
    JsonPayload, RecordingStateUpdate, UserId, UserInfo, metrics::RuntimeMetrics,
    source_model::UserStreamId,
};

pub const MAX_BROADCAST_PAYLOAD_BYTES: usize = 16 * 1024;

const ROOM_EVENT_QUEUE_BYTES: usize = 1024;
const BROADCAST_QUEUE_OVERHEAD_BYTES: usize = 256;
const TRACK_PROJECTION_QUEUE_OVERHEAD_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastPayload {
    message: Arc<JsonPayload>,
    byte_len: usize,
}

impl BroadcastPayload {
    /// # Errors
    ///
    /// Returns `TooLarge` when the serialized JSON payload exceeds
    /// [`MAX_BROADCAST_PAYLOAD_BYTES`].
    pub fn try_new(message: JsonPayload) -> Result<Self, BroadcastPayloadError> {
        let byte_len = serialized_json_len(&message)?;
        if byte_len > MAX_BROADCAST_PAYLOAD_BYTES {
            return Err(BroadcastPayloadError::TooLarge {
                actual: byte_len,
                limit: MAX_BROADCAST_PAYLOAD_BYTES,
            });
        }
        Ok(Self {
            message: Arc::new(message),
            byte_len,
        })
    }

    #[must_use]
    pub const fn byte_len(&self) -> usize {
        self.byte_len
    }

    #[must_use]
    pub fn to_json(&self) -> JsonPayload {
        self.message.as_ref().clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcastPayloadError {
    TooLarge { actual: usize, limit: usize },
    JsonSerialization,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomEventMessage {
    Broadcast {
        sender_id: UserId,
        message: BroadcastPayload,
    },
    UserJoined {
        user_id: UserId,
        info: UserInfo,
    },
    UserDeparted {
        user_id: UserId,
    },
    UserInfoChanged(BTreeMap<UserId, UserInfo>),
    RecordingStateChanged(RecordingStateUpdate),
}

impl RoomEventMessage {
    #[must_use]
    pub(super) fn queued_bytes(&self) -> usize {
        match self {
            Self::Broadcast { message, .. } => message
                .byte_len()
                .saturating_add(BROADCAST_QUEUE_OVERHEAD_BYTES),
            Self::UserInfoChanged(snapshot) => {
                ROOM_EVENT_QUEUE_BYTES.saturating_mul(snapshot.len())
            }
            Self::UserJoined { .. }
            | Self::UserDeparted { .. }
            | Self::RecordingStateChanged(_) => ROOM_EVENT_QUEUE_BYTES,
        }
    }
}

#[derive(Debug, Default)]
struct JsonByteCounter {
    len: usize,
}

impl io::Write for JsonByteCounter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.len = self.len.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_json_len(value: &JsonPayload) -> Result<usize, BroadcastPayloadError> {
    let mut counter = JsonByteCounter::default();
    serde_json::to_writer(&mut counter, value)
        .map_err(|_error| BroadcastPayloadError::JsonSerialization)?;
    Ok(counter.len)
}

pub const DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY: usize = 128;
pub const DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY: usize =
    DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY * MAX_BROADCAST_PAYLOAD_BYTES;

pub(super) type OutboundSender = UserOutboundSender;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTrackProjection {
    pub consumer_mid: String,
    pub user_id: UserId,
    pub stream_id: UserStreamId,
    pub producer_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTrackSnapshot {
    pub tracks: Vec<RemoteTrackProjection>,
    pub requires_negotiation: bool,
}

impl RemoteTrackSnapshot {
    fn queued_bytes(&self) -> usize {
        self.tracks
            .iter()
            .fold(ROOM_EVENT_QUEUE_BYTES, |bytes, track| {
                let user_id_bytes = match &track.user_id {
                    UserId::Integer(_) => 0,
                    UserId::String(value) => value.len(),
                };
                bytes
                    .saturating_add(TRACK_PROJECTION_QUEUE_OVERHEAD_BYTES)
                    .saturating_add(track.consumer_mid.len())
                    .saturating_add(user_id_bytes)
                    .saturating_add(track.stream_id.as_str().len())
            })
    }
}

/// room output that belongs to one connected user
#[derive(Debug, Clone)]
pub enum UserOutbound {
    Message(RoomEventMessage),
    RemoteTracks(RemoteTrackSnapshot),
    Close(UserCloseReason),
}

impl UserOutbound {
    #[must_use]
    pub(super) fn queued_bytes(&self) -> usize {
        match self {
            Self::Message(message) => message.queued_bytes(),
            Self::RemoteTracks(snapshot) => snapshot.queued_bytes(),
            Self::Close(_) => ROOM_EVENT_QUEUE_BYTES,
        }
    }
}

/// queue overflow details captured when a user cannot accept more outbound work
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserOutboundOverflow {
    kind: UserOutboundOverflowKind,
    message_capacity: usize,
    byte_capacity: usize,
    queued_bytes: usize,
    message_bytes: usize,
}

/// outbound queue limit that rejected a message
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

/// non-blocking outbound send failure
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserOutboundSendError {
    Full(UserOutboundOverflow),
    Closed,
}

/// receiver-side queue event for user-session loops
#[derive(Debug)]
pub enum UserOutboundEvent {
    Message(UserOutbound),
    Overflow(UserOutboundOverflow),
    Closed,
}

/// message and byte capacity for one user outbound queue
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
    /// returns `Full` when message or byte capacity is exhausted and `Closed`
    /// when the receiver has gone away
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
        let _ = self.overflow.send(Some(overflow));
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
    /// returns the underlying non-blocking receive error when the queue is
    /// empty or closed
    pub fn try_recv(&mut self) -> Result<UserOutbound, mpsc::error::TryRecvError> {
        self.messages
            .try_recv()
            .map(|message| self.record_received(message))
    }

    /// returns the overflow sentinel before queued output
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
