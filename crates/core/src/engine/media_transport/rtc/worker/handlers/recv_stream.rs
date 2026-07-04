use str0m::{
    bwe::Bitrate as Str0mBitrate,
    change::DirectApi,
    media::{Mid, Rid},
    rtp::Ssrc,
};
use tracing::debug;

use crate::Bitrate;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum StaleSsrcPolicy {
    KeepExisting,
    ReplaceStale,
}

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
