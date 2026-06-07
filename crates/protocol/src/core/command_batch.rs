use std::{error::Error, fmt, ops::Deref, slice, vec::IntoIter};

use super::{
    Command, Commands, NegotiationKind, RECOVERY_TIMER_ID, REQUEST_TIMEOUT_MS,
    request_tracker::RequestRegistration,
};
use crate::signaling::{RequestId, SessionDescriptionPayload, WebSocketCloseCode};

/// ordered host side effects emitted by one protocol-core transition
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandBatch {
    commands: Vec<Command>,
}

impl CommandBatch {
    pub(super) fn from_core_commands(commands: Vec<Command>) -> Self {
        debug_assert!(
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

    pub(super) fn start_pending_request(
        registration: RequestRegistration,
        outbound: Commands,
    ) -> Self {
        let mut commands = vec![
            Command::RegisterPendingRequest {
                request_id: registration.request_id,
                kind: registration.kind,
            },
            Command::ScheduleTimer {
                id: registration.timeout_timer_id,
                ms: REQUEST_TIMEOUT_MS,
            },
        ];
        commands.extend(outbound);
        Self::from_core_commands(commands)
    }

    /// builds a batch from manually assembled commands
    ///
    /// this is intended for tests and host bridges that need to validate an
    /// externally assembled batch before projecting it to host-specific work
    ///
    /// # Errors
    ///
    /// returns [`CommandBatchError`] when the command order can make the host
    /// execute negotiation, close, recovery, or request-resolution effects in an
    /// invalid sequence
    pub fn try_from_vec(commands: Vec<Command>) -> Result<Self, CommandBatchError> {
        Self::validate_commands(&commands)?;
        Ok(Self { commands })
    }

    /// validates this batch using the canonical Rust command-order contract
    ///
    /// # Errors
    ///
    /// returns [`CommandBatchError`] when the batch violates a host side-effect
    /// ordering rule
    pub fn validate(&self) -> Result<(), CommandBatchError> {
        Self::validate_commands(&self.commands)
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
        validate_request_resolution(commands, close_peer_index.is_some())?;
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

impl PartialEq<Vec<Command>> for CommandBatch {
    fn eq(&self, other: &Vec<Command>) -> bool {
        self.commands == *other
    }
}

impl PartialEq<CommandBatch> for Vec<Command> {
    fn eq(&self, other: &CommandBatch) -> bool {
        *self == other.commands
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandBatchError {
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
    closes_peer: bool,
) -> Result<(), CommandBatchError> {
    for (index, command) in commands.iter().enumerate() {
        let Command::ResolvePendingRequest { request_id, .. } = command else {
            continue;
        };
        if closes_peer || has_prior_resolution_evidence(commands, request_id, index) {
            continue;
        }
        return Err(CommandBatchError::UnknownResolvedRequest {
            request_id: request_id.clone(),
            index,
        });
    }
    Ok(())
}

fn has_prior_resolution_evidence(
    commands: &[Command],
    request_id: &RequestId,
    resolve_index: usize,
) -> bool {
    commands
        .iter()
        .take(resolve_index)
        .any(|command| match command {
            Command::CancelTimer { .. } => true,
            Command::RegisterPendingRequest { request_id: id, .. } => id == request_id,
            _ => false,
        })
}
