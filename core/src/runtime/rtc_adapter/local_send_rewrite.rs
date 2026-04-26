use std::collections::HashMap;

use str0m::{media::Rid, rtp::SeqNo};

use crate::runtime::transport_adapter::TransportMediaId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct LocalSendRewriteKey {
    transport_media_id: TransportMediaId,
    rid: Option<Rid>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct LocalSendRewriteState {
    next_seq_no: SeqNo,
}

impl LocalSendRewriteState {
    fn take_seq_no(&mut self) -> SeqNo {
        self.next_seq_no.inc()
    }
}

/// Return the next receiver-local RTP sequence number for a consumer stream
///
/// Fresh subscribers must not inherit the publisher sequence space because the
/// browser does not have the corresponding SRTP rollover info. The rewite
/// key is scoped by consumer transport media and RID so rebuilt and simulcast
/// routes each start from a receiver-safe local sequence space
/// Basically we keep track of packet ordering number locally
pub(super) fn next_rewritten_seq_no(
    rewrites: &mut HashMap<LocalSendRewriteKey, LocalSendRewriteState>,
    transport_media_id: TransportMediaId,
    rid: Option<Rid>,
) -> SeqNo {
    rewrites
        .entry(LocalSendRewriteKey {
            transport_media_id,
            rid,
        })
        .or_default()
        .take_seq_no()
}

/// Drop every local send rewrite entry owned by one consumer transport media.
///
/// consumer teardown and replacement must forget this state so any later
/// rebuilt consumer starts from a fresh local counter instead of continuing an
/// old stream identity.
pub(super) fn forget_transport_media_rewrites(
    rewrites: &mut HashMap<LocalSendRewriteKey, LocalSendRewriteState>,
    transport_media_id: TransportMediaId,
) {
    rewrites.retain(|key, _| key.transport_media_id != transport_media_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewritten_sequence_numbers_start_in_initial_roc_and_increment_per_stream() {
        let source_seq: SeqNo = 131_072.into();
        let transport_media_id = TransportMediaId::new(41);
        let mut rewrites = HashMap::new();

        let first = next_rewritten_seq_no(&mut rewrites, transport_media_id, None);
        let second = next_rewritten_seq_no(&mut rewrites, transport_media_id, None);

        assert_eq!(source_seq.roc(), 2);
        assert_eq!(first.roc(), 0);
        assert!(first.is_next(second));
    }

    #[test]
    fn rewritten_sequence_numbers_are_scoped_by_consumer_and_rid() {
        let transport_media_id = TransportMediaId::new(42);
        let rid = Rid::from("hi");
        let mut rewrites = HashMap::new();

        let plain = next_rewritten_seq_no(&mut rewrites, transport_media_id, None);
        let rid_first = next_rewritten_seq_no(&mut rewrites, transport_media_id, Some(rid));
        let rid_second = next_rewritten_seq_no(&mut rewrites, transport_media_id, Some(rid));
        let other = next_rewritten_seq_no(&mut rewrites, TransportMediaId::new(43), None);

        assert_eq!(plain.roc(), 0);
        assert_eq!(rid_first.roc(), 0);
        assert!(rid_first.is_next(rid_second));
        assert_eq!(other.roc(), 0);
    }

    #[test]
    fn forgetting_transport_media_rewrites_drops_all_stream_state_for_that_consumer() {
        let transport_media_id = TransportMediaId::new(44);
        let rid = Rid::from("hi");
        let mut rewrites = HashMap::new();

        let first = next_rewritten_seq_no(&mut rewrites, transport_media_id, None);
        let _ = next_rewritten_seq_no(&mut rewrites, transport_media_id, Some(rid));

        forget_transport_media_rewrites(&mut rewrites, transport_media_id);

        let reset = next_rewritten_seq_no(&mut rewrites, transport_media_id, None);
        assert_eq!(first.roc(), 0);
        assert_eq!(reset.roc(), 0);
        assert_eq!(reset.roc(), first.roc());
    }
}
