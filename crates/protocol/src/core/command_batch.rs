use std::{error::Error, fmt, ops::Deref, slice, vec::IntoIter};

use super::{Command, NegotiationKind, RECOVERY_TIMER_ID};
use crate::signaling::RequestId;

/// Ordered side effects emitted by one protocol-core transition.
///
/// `CommandBatch` is the canonical Rust contract for host-visible side
/// effects. Construction validates the ordering rules that would otherwise be
/// easy for native, WASM, fuzz, or test hosts to apply differently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandBatch {
    commands: Vec<Command>,
}

impl CommandBatch {
    /// Builds a batch from manually assembled commands.
    ///
    /// This is intended for tests and host bridges that need to validate an
    /// externally assembled batch before projecting it to host-specific work.
    ///
    /// # Errors
    ///
    /// Returns [`CommandBatchError`] when the command order can make the host
    /// execute negotiation, close, recovery, or request-resolution effects in an
    /// invalid sequence.
    pub fn try_from_vec(commands: Vec<Command>) -> Result<Self, CommandBatchError> {
        Self::validate_commands(&commands)?;
        Ok(Self { commands })
    }

    pub(super) fn from_core_commands(commands: Vec<Command>) -> Self {
        debug_assert!(
            Self::validate_commands(&commands).is_ok(),
            "protocol core emitted an invalid command batch"
        );
        Self { commands }
    }

    /// Validates this batch using the canonical Rust command-order contract.
    ///
    /// # Errors
    ///
    /// Returns [`CommandBatchError`] when the batch violates a host side-effect
    /// ordering rule.
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
        validate_negotiation_order(commands)?;
        validate_close_order(commands)?;
        validate_recovery_order(commands)?;
        validate_request_resolution(commands)?;
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

fn validate_negotiation_order(commands: &[Command]) -> Result<(), CommandBatchError> {
    for (index, command) in commands.iter().enumerate() {
        let Command::ApplyNegotiation { kind, .. } = command else {
            continue;
        };
        let previous = index
            .checked_sub(1)
            .and_then(|previous| commands.get(previous));
        match kind {
            NegotiationKind::Offer => {
                if !matches!(previous, Some(Command::CreatePeerConnection)) {
                    return Err(CommandBatchError::InitialNegotiationWithoutPeerCreate { index });
                }
            }
            NegotiationKind::Renegotiate => {
                if matches!(previous, Some(Command::CreatePeerConnection)) {
                    return Err(CommandBatchError::RenegotiationRecreatesPeer { index });
                }
            }
        }
    }
    Ok(())
}

fn validate_close_order(commands: &[Command]) -> Result<(), CommandBatchError> {
    let close_websocket_index = commands
        .iter()
        .position(|command| matches!(command, Command::CloseWebSocket { .. }));
    let close_peer_index = commands
        .iter()
        .position(|command| matches!(command, Command::ClosePeerConnection));
    if let (Some(websocket_index), Some(peer_index)) = (close_websocket_index, close_peer_index)
        && websocket_index > peer_index
    {
        return Err(CommandBatchError::WebSocketCloseAfterPeerClose {
            websocket_index,
            peer_index,
        });
    }
    Ok(())
}

fn validate_recovery_order(commands: &[Command]) -> Result<(), CommandBatchError> {
    let recovery_index = commands.iter().position(|command| {
        matches!(
            command,
            Command::ScheduleTimer {
                id: RECOVERY_TIMER_ID,
                ..
            }
        )
    });
    let close_peer_index = commands
        .iter()
        .position(|command| matches!(command, Command::ClosePeerConnection));
    if let (Some(recovery_index), Some(peer_index)) = (recovery_index, close_peer_index)
        && recovery_index < peer_index
    {
        return Err(CommandBatchError::RecoveryScheduledBeforePeerClose {
            recovery_index,
            peer_index,
        });
    }
    Ok(())
}

fn validate_request_resolution(commands: &[Command]) -> Result<(), CommandBatchError> {
    let has_close_peer = commands
        .iter()
        .any(|command| matches!(command, Command::ClosePeerConnection));
    for (index, command) in commands.iter().enumerate() {
        let Command::ResolvePendingRequest { request_id, .. } = command else {
            continue;
        };
        if has_close_peer || has_resolution_cause(commands, request_id, index) {
            continue;
        }
        return Err(CommandBatchError::UnknownResolvedRequest {
            request_id: request_id.clone(),
            index,
        });
    }
    Ok(())
}

fn has_resolution_cause(
    commands: &[Command],
    request_id: &RequestId,
    resolve_index: usize,
) -> bool {
    commands
        .iter()
        .take(resolve_index)
        .any(|command| match command {
            Command::CancelTimer { .. } => true,
            Command::RegisterPendingRequest {
                request_id: registered_request_id,
                ..
            } => registered_request_id == request_id,
            _ => false,
        })
}
