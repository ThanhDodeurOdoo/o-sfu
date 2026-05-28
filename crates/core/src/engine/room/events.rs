use std::{collections::BTreeMap, io, sync::Arc};

use crate::engine::{JsonPayload, RecordingStateUpdate, UserId, UserInfo};

pub const MAX_BROADCAST_PAYLOAD_BYTES: usize = 16 * 1024;

const ROOM_EVENT_QUEUE_BYTES: usize = 1024;
const BROADCAST_QUEUE_OVERHEAD_BYTES: usize = 256;

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

impl JsonByteCounter {
    const fn len(&self) -> usize {
        self.len
    }
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
    Ok(counter.len())
}
