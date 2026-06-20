use super::*;
use crate::core::{CommandBatch, CommandBatchError};

macro_rules! assert_invalid {
    ($commands:expr, $error:pat) => {
        assert!(matches!(CommandBatch::try_from_vec($commands), Err($error)));
    };
}

#[test]
fn try_from_vec_rejects_invalid_command_order() {
    assert_invalid!(
        vec![apply_negotiation("offer-1", NegotiationKind::Offer)],
        CommandBatchError::InitialNegotiationWithoutPeerCreate { index: 0 }
    );
    assert_invalid!(
        vec![
            Command::CreatePeerConnection,
            apply_negotiation("renegotiate-1", NegotiationKind::Renegotiate),
        ],
        CommandBatchError::RenegotiationRecreatesPeer { index: 1 }
    );
    assert_invalid!(
        vec![Command::ClosePeerConnection, close_websocket()],
        CommandBatchError::WebSocketCloseAfterPeerClose {
            websocket_index: 1,
            peer_index: 0
        }
    );
    assert_invalid!(
        vec![
            Command::ClosePeerConnection,
            close_websocket(),
            Command::ClosePeerConnection,
        ],
        CommandBatchError::WebSocketCloseAfterPeerClose {
            websocket_index: 1,
            peer_index: 0
        }
    );
    assert_invalid!(
        vec![recovery_timer(), Command::ClosePeerConnection],
        CommandBatchError::RecoveryScheduledBeforePeerClose {
            recovery_index: 0,
            peer_index: 1
        }
    );
    assert_invalid!(
        vec![
            recovery_timer(),
            Command::ClosePeerConnection,
            recovery_timer(),
        ],
        CommandBatchError::RecoveryScheduledBeforePeerClose {
            recovery_index: 0,
            peer_index: 1
        }
    );
    assert_invalid!(
        vec![Command::ResolvePendingRequest {
            request_id: RequestId::new("missing"),
            ok: false,
        }],
        CommandBatchError::UnknownResolvedRequest {
            request_id: _,
            index: 0
        }
    );
}

#[test]
#[should_panic(expected = "protocol core emitted an invalid command batch")]
fn from_core_commands_panics_on_invalid_order() {
    CommandBatch::from_core_commands(vec![apply_negotiation("offer-1", NegotiationKind::Offer)]);
}

const fn close_websocket() -> Command {
    Command::CloseWebSocket { code: 1000 }
}

const fn recovery_timer() -> Command {
    Command::ScheduleTimer {
        id: RECOVERY_TIMER_ID,
        ms: 1_000,
    }
}

fn apply_negotiation(request_id: &str, kind: NegotiationKind) -> Command {
    Command::ApplyNegotiation {
        request_id: RequestId::new(request_id),
        kind,
        sdp: String::from("v=0"),
        upload_slots: Vec::new(),
    }
}
