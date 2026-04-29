use super::*;
use crate::core::{CommandBatch, CommandBatchError};

#[test]
fn command_batch_accepts_valid_initial_negotiation() {
    let batch = CommandBatch::try_from_vec(vec![
        Command::CreatePeerConnection,
        Command::ApplyNegotiation {
            request_id: RequestId::new("offer-1"),
            kind: NegotiationKind::Offer,
            sdp: String::from("v=0"),
            upload_slots: Vec::new(),
        },
    ]);

    assert!(batch.is_ok());
}

#[test]
fn command_batch_rejects_offer_without_immediate_peer_create() {
    let batch = CommandBatch::try_from_vec(vec![Command::ApplyNegotiation {
        request_id: RequestId::new("offer-1"),
        kind: NegotiationKind::Offer,
        sdp: String::from("v=0"),
        upload_slots: Vec::new(),
    }]);

    assert!(matches!(
        batch,
        Err(CommandBatchError::InitialNegotiationWithoutPeerCreate { index: 0 })
    ));
}

#[test]
fn command_batch_rejects_renegotiation_that_recreates_peer() {
    let batch = CommandBatch::try_from_vec(vec![
        Command::CreatePeerConnection,
        Command::ApplyNegotiation {
            request_id: RequestId::new("renegotiate-1"),
            kind: NegotiationKind::Renegotiate,
            sdp: String::from("v=0"),
            upload_slots: Vec::new(),
        },
    ]);

    assert!(matches!(
        batch,
        Err(CommandBatchError::RenegotiationRecreatesPeer { index: 1 })
    ));
}

#[test]
fn command_batch_rejects_peer_close_before_websocket_close() {
    let batch = CommandBatch::try_from_vec(vec![
        Command::ClosePeerConnection,
        Command::CloseWebSocket { code: 1000 },
    ]);

    assert!(matches!(
        batch,
        Err(CommandBatchError::WebSocketCloseAfterPeerClose {
            websocket_index: 1,
            peer_index: 0,
        })
    ));
}

#[test]
fn command_batch_rejects_recovery_before_peer_cleanup() {
    let batch = CommandBatch::try_from_vec(vec![
        Command::ScheduleTimer {
            id: RECOVERY_TIMER_ID,
            ms: 1_000,
        },
        Command::ClosePeerConnection,
    ]);

    assert!(matches!(
        batch,
        Err(CommandBatchError::RecoveryScheduledBeforePeerClose {
            recovery_index: 0,
            peer_index: 1,
        })
    ));
}

#[test]
fn command_batch_rejects_request_resolution_without_batch_evidence() {
    let batch = CommandBatch::try_from_vec(vec![Command::ResolvePendingRequest {
        request_id: RequestId::new("missing"),
        ok: false,
    }]);

    assert!(matches!(
        batch,
        Err(CommandBatchError::UnknownResolvedRequest { index: 0, .. })
    ));
}
