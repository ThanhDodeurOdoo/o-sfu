use std::time::Instant;

use str0m::{
    media::{ExtensionValues, Mid, Pt, Rid},
    rtp::{RtpHeader, Ssrc},
};

use super::{ForwardedPacket, ForwardedPacketData, ForwardedRelayRtpData};
use crate::runtime::{
    media_transport::TransportSessionKey, rtc_engine::shared_payload::SharedPayload,
};

#[must_use]
pub fn sample_forwarded_packet(
    source_session_key: TransportSessionKey,
    mid: &str,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_rid(source_session_key, mid, None, payload)
}

#[must_use]
pub fn sample_forwarded_packet_with_rid(
    source_session_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        source_session_key,
        mid,
        rid,
        ExtensionValues::default(),
        payload,
    )
}

#[must_use]
pub fn sample_forwarded_packet_with_audio_activity(
    source_session_key: TransportSessionKey,
    mid: &str,
    voice_activity: Option<bool>,
    audio_level: Option<i8>,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        source_session_key,
        mid,
        None,
        ExtensionValues {
            audio_level,
            voice_activity,
            ..ExtensionValues::default()
        },
        payload,
    )
}

#[must_use]
pub fn sample_forwarded_packet_with_frame_mark(
    source_session_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    frame_mark: u32,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        source_session_key,
        mid,
        rid,
        ExtensionValues {
            frame_mark: Some(frame_mark),
            ..ExtensionValues::default()
        },
        payload,
    )
}

fn sample_forwarded_packet_with_extensions(
    source_session_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    ext_vals: ExtensionValues,
    payload: &[u8],
) -> ForwardedPacket {
    let received_at = Instant::now();
    ForwardedPacket {
        source_session_key,
        source_transport_media_id: None,
        resolved_source_rid: None,
        facts: None,
        visits_origin_sinks: true,
        received_at,
        payload: SharedPayload::from_vec(payload.to_vec()),
        data: ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
            header: RtpHeader {
                version: 2,
                has_padding: false,
                has_extension: false,
                csrc_count: 0,
                marker: false,
                payload_type: Pt::from(111),
                sequence_number: 1,
                timestamp: 1234,
                ssrc: Ssrc::from(4321),
                csrc: [0; 15],
                ext_vals: ExtensionValues {
                    rid: rid.map(Rid::from),
                    mid: Some(Mid::from(mid)),
                    ..ext_vals
                },
                header_len: 12,
            },
        }),
    }
}

#[must_use]
pub fn sample_forwarded_packet_without_mid(
    source_session_key: TransportSessionKey,
    ssrc: u32,
    payload: &[u8],
) -> ForwardedPacket {
    let received_at = Instant::now();
    ForwardedPacket {
        source_session_key,
        source_transport_media_id: None,
        resolved_source_rid: None,
        facts: None,
        visits_origin_sinks: true,
        received_at,
        payload: SharedPayload::from_vec(payload.to_vec()),
        data: ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
            header: RtpHeader {
                version: 2,
                has_padding: false,
                has_extension: false,
                csrc_count: 0,
                marker: false,
                payload_type: Pt::from(111),
                sequence_number: 1,
                timestamp: 1234,
                ssrc: Ssrc::from(ssrc),
                csrc: [0; 15],
                ext_vals: ExtensionValues::default(),
                header_len: 12,
            },
        }),
    }
}
