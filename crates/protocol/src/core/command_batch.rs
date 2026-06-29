use std::{error::Error, fmt, ops::Deref, slice, vec::IntoIter};

use super::{Command, NegotiationKind, RECOVERY_TIMER_ID, timers::RequestTimeoutId};
use crate::signaling::{RequestId, SessionDescriptionPayload, WebSocketCloseCode};

/// ordered host side effects emitted by one protocol-core transition
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandBatch {
    commands: Vec<Command>,
}

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
#[path = "command_batch/TESTS/mod.rs"]
mod TESTS;

#[cfg(any(test, feature = "test-support"))]
#[path = "command_batch/TESTS/test_support.rs"]
pub mod test_support;

impl CommandBatch {
    pub(super) fn from_core_commands(commands: Vec<Command>) -> Self {
        assert!(
            Self::validate_commands(&commands).is_ok(),
            "protocol core emitted an invalid command batch"
        );
        Self { commands }
    }

    pub(super) fn close_for_protocol_error() -> Self {
        Self::from_core_commands(vec![Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }])
    }

    pub(super) fn initial_offer(request_id: RequestId, payload: SessionDescriptionPayload) -> Self {
        Self::from_core_commands(vec![
            Command::CreatePeerConnection,
            apply_negotiation(request_id, NegotiationKind::Offer, payload),
        ])
    }

    pub(super) fn renegotiation(request_id: RequestId, payload: SessionDescriptionPayload) -> Self {
        Self::from_core_commands(vec![apply_negotiation(
            request_id,
            NegotiationKind::Renegotiate,
            payload,
        )])
    }

    #[must_use]
    pub fn as_slice(&self) -> &[Command] {
        &self.commands
    }

    pub fn iter(&self) -> slice::Iter<'_, Command> {
        self.commands.iter()
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<Command> {
        self.commands
    }

    fn validate_commands(commands: &[Command]) -> Result<(), CommandBatchError> {
        let mut close_websocket_index = None;
        let mut close_peer_index = None;
        let mut recovery_index = None;
        for (index, command) in commands.iter().enumerate() {
            match command {
                Command::BeginPendingRequest { .. } if index != 0 => {
                    return Err(CommandBatchError::InvalidPendingRequestStart { index });
                }
                Command::ApplyNegotiation { kind, .. } => {
                    let previous = index
                        .checked_sub(1)
                        .and_then(|previous| commands.get(previous));
                    match (*kind, previous) {
                        (NegotiationKind::Offer, Some(Command::CreatePeerConnection)) => {}
                        (NegotiationKind::Offer, _) => {
                            return Err(CommandBatchError::InitialNegotiationWithoutPeerCreate {
                                index,
                            });
                        }
                        (NegotiationKind::Renegotiate, Some(Command::CreatePeerConnection)) => {
                            return Err(CommandBatchError::RenegotiationRecreatesPeer { index });
                        }
                        (NegotiationKind::Renegotiate, _) => {}
                    }
                }
                Command::CloseWebSocket { .. } => {
                    close_websocket_index.get_or_insert(index);
                }
                Command::ClosePeerConnection => {
                    close_peer_index.get_or_insert(index);
                }
                Command::ScheduleTimer {
                    id: RECOVERY_TIMER_ID,
                    ..
                } => {
                    recovery_index.get_or_insert(index);
                }
                _ => {}
            }
        }
        if let (Some(websocket_index), Some(peer_index)) = (close_websocket_index, close_peer_index)
            && websocket_index > peer_index
        {
            return Err(CommandBatchError::WebSocketCloseAfterPeerClose {
                websocket_index,
                peer_index,
            });
        }
        if let (Some(recovery_index), Some(peer_index)) = (recovery_index, close_peer_index)
            && recovery_index < peer_index
        {
            return Err(CommandBatchError::RecoveryScheduledBeforePeerClose {
                recovery_index,
                peer_index,
            });
        }
        validate_request_resolution(commands, close_peer_index)?;
        Ok(())
    }
}

impl Default for CommandBatch {
    fn default() -> Self {
        Self::from_core_commands(Vec::new())
    }
}

impl Deref for CommandBatch {
    type Target = [Command];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl IntoIterator for CommandBatch {
    type IntoIter = IntoIter<Command>;
    type Item = Command;

    fn into_iter(self) -> Self::IntoIter {
        self.commands.into_iter()
    }
}

impl<'a> IntoIterator for &'a CommandBatch {
    type IntoIter = slice::Iter<'a, Command>;
    type Item = &'a Command;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CommandBatchError {
    InitialNegotiationWithoutPeerCreate {
        index: usize,
    },
    RenegotiationRecreatesPeer {
        index: usize,
    },
    WebSocketCloseAfterPeerClose {
        websocket_index: usize,
        peer_index: usize,
    },
    RecoveryScheduledBeforePeerClose {
        recovery_index: usize,
        peer_index: usize,
    },
    UnknownResolvedRequest {
        request_id: RequestId,
        index: usize,
    },
    InvalidPendingRequestStart {
        index: usize,
    },
}

impl fmt::Display for CommandBatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitialNegotiationWithoutPeerCreate { index } => write!(
                formatter,
                "initial negotiation command at index {index} must immediately follow peer creation"
            ),
            Self::RenegotiationRecreatesPeer { index } => write!(
                formatter,
                "renegotiation command at index {index} must not recreate the peer connection"
            ),
            Self::WebSocketCloseAfterPeerClose {
                websocket_index,
                peer_index,
            } => write!(
                formatter,
                "websocket close at index {websocket_index} must precede peer close at index {peer_index}"
            ),
            Self::RecoveryScheduledBeforePeerClose {
                recovery_index,
                peer_index,
            } => write!(
                formatter,
                "recovery timer at index {recovery_index} must be scheduled after peer close at index {peer_index}"
            ),
            Self::UnknownResolvedRequest { request_id, index } => write!(
                formatter,
                "request resolution at index {index} references unknown pending request {request_id:?}"
            ),
            Self::InvalidPendingRequestStart { index } => {
                write!(formatter, "invalid pending request start at index {index}")
            }
        }
    }
}

impl Error for CommandBatchError {}

fn apply_negotiation(
    request_id: RequestId,
    kind: NegotiationKind,
    payload: SessionDescriptionPayload,
) -> Command {
    Command::ApplyNegotiation {
        request_id,
        kind,
        sdp: payload.sdp,
        upload_slots: payload.upload_slots,
    }
}

fn validate_request_resolution(
    commands: &[Command],
    close_peer_index: Option<usize>,
) -> Result<(), CommandBatchError> {
    for (index, command) in commands.iter().enumerate() {
        let Command::ResolvePendingRequest { request_id, .. } = command else {
            continue;
        };
        if close_peer_index.is_some_and(|close_index| close_index < index)
            || has_prior_timeout_cancel(commands, index)
        {
            continue;
        }
        return Err(CommandBatchError::UnknownResolvedRequest {
            request_id: request_id.clone(),
            index,
        });
    }
    Ok(())
}

fn has_prior_timeout_cancel(commands: &[Command], resolve_index: usize) -> bool {
    let Some(previous_index) = resolve_index.checked_sub(1) else {
        return false;
    };
    matches!(
        commands.get(previous_index),
        Some(Command::CancelTimer { id }) if RequestTimeoutId::try_from_raw(*id).is_some()
    )
}
