use str0m::{
    bwe::Bitrate as Str0mBitrate,
    change::DirectApi,
    media::{Mid, Rid},
    rtp::Ssrc,
};
use tracing::debug;

use crate::Bitrate;

/// Selects the authoritative SSRC when str0m already has a `(mid, rid)` binding.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum StaleSsrcPolicy {
    /// Preserves str0m's current binding and refreshes only its REMB cap.
    KeepExisting,
    /// Replaces a binding that disagrees with the pending receive identity after
    /// answer application.
    ReplaceStale,
}

/// Reconciles one receive binding and reapplies its inbound REMB cap.
///
/// Answer application can recreate `StreamRx`. Every retained or replaced
/// binding must therefore receive `max_bitrate_in` in the same pass.
pub(super) fn apply_recv_stream(
    api: &mut DirectApi<'_>,
    mid: Mid,
    rid: Option<Rid>,
    ssrc: Ssrc,
    max_bitrate_in: Bitrate,
    stale_policy: StaleSsrcPolicy,
) {
    if let Some(stream_rx) = api.stream_rx_by_mid(mid, rid) {
        let existing_ssrc = Ssrc::from(*stream_rx.ssrc());
        if stale_policy == StaleSsrcPolicy::KeepExisting || existing_ssrc == ssrc {
            stream_rx.request_remb(Str0mBitrate::bps(max_bitrate_in.as_bps()));
            return;
        }
        api.remove_stream_rx(existing_ssrc);
        debug!(
            ?mid,
            rid = ?rid,
            previous_ssrc = ?existing_ssrc,
            next_ssrc = ?ssrc,
            "replaced stale recv stream SSRC while applying answer"
        );
    }
    api.expect_stream_rx(ssrc, None, mid, rid);
    if let Some(stream_rx) = api.stream_rx_by_mid(mid, rid) {
        stream_rx.request_remb(Str0mBitrate::bps(max_bitrate_in.as_bps()));
    }
}
