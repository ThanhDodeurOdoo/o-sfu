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

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "recv stream helper tests should fail loudly when fixture setup breaks"
    )]

    use std::net::SocketAddr;

    use str0m::{media::MediaKind as Str0mMediaKind, rtp::Ssrc};

    use super::*;
    use crate::{
        MediaCodecFlags,
        runtime::{
            UserId,
            rtc_engine::{
                bootstrap, state::PacketLoopState, test_support::test_transport_session_key,
            },
        },
    };

    #[test]
    fn recv_stream_policy_keeps_existing_ssrc_for_publication_projection() {
        assert_eq!(
            applied_recv_stream_ssrc(StaleSsrcPolicy::KeepExisting),
            11_111
        );
    }

    #[test]
    fn recv_stream_policy_replaces_stale_ssrc_for_negotiation_refresh() {
        assert_eq!(
            applied_recv_stream_ssrc(StaleSsrcPolicy::ReplaceStale),
            22_222
        );
    }

    fn applied_recv_stream_ssrc(policy: StaleSsrcPolicy) -> u32 {
        let mut state = PacketLoopState::default();
        let session_key = test_transport_session_key(301, 0, 302, UserId::Integer(303));
        let mid = Mid::from("cam-up");
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &session_key,
            SocketAddr::from(([127, 0, 0, 1], 47_000)),
            Bitrate::from_mbps(10),
            MediaCodecFlags::default(),
        )
        .expect("test session should enter RTC state");
        let session_state = state
            .users
            .get_mut(&session_key)
            .expect("test session should exist after bootstrap");
        let mut api = session_state.rtc.direct_api();
        api.declare_media(mid, Str0mMediaKind::Video);
        api.expect_stream_rx(Ssrc::from(11_111), None, mid, None);

        apply_recv_stream(
            &mut api,
            mid,
            None,
            Ssrc::from(22_222),
            Bitrate::from_mbps(1),
            policy,
        );

        api.stream_rx_by_mid(mid, None)
            .map(|stream_rx| *stream_rx.ssrc())
            .expect("test stream should exist after applying recv stream")
    }
}
