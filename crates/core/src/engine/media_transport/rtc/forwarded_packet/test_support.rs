use std::{sync::Arc, time::Instant};

use str0m::{
    media::{ExtensionValues, Mid, Pt, Rid},
    rtp::{RtpHeader, Ssrc},
};

use super::{ForwardedPacket, ForwardedPacketData, ForwardedPacketSource, ForwardedRelayRtpData};
use crate::engine::media_transport::TransportSessionKey;
#[cfg(test)]
use crate::engine::{
    media_transport::TransportMediaId, media_transport::rtc::slots::SessionHandle,
};

#[must_use]
pub fn sample_forwarded_packet(
    source_session_key: TransportSessionKey,
    mid: &str,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        source_session_key,
        mid,
        None,
        ExtensionValues::default(),
        payload,
    )
}

#[cfg(test)]
#[must_use]
pub(in crate::engine::media_transport::rtc) fn sample_already_relayed_packet(
    src_key: TransportSessionKey,
    src_media: TransportMediaId,
    mid: &str,
    payload: &[u8],
) -> ForwardedPacket {
    let mut packet = sample_forwarded_packet(src_key, mid, payload);
    mark_already_relayed(&mut packet, src_media);
    packet
}

#[cfg(test)]
pub(in crate::engine::media_transport::rtc) fn mark_already_relayed(
    packet: &mut ForwardedPacket,
    src_media: TransportMediaId,
) {
    packet.src_media = Some(src_media);
    packet.visits_origin_sinks = false;
}

#[cfg(test)]
#[must_use]
pub(in crate::engine::media_transport::rtc) fn sample_local_forwarded_packet(
    source_session_handle: SessionHandle,
    mid: &str,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_source(
        ForwardedPacketSource::Local(source_session_handle),
        mid,
        None,
        ExtensionValues::default(),
        payload,
    )
}

#[cfg(test)]
#[must_use]
pub fn sample_forwarded_packet_with_rid(
    src_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(src_key, mid, rid, ExtensionValues::default(), payload)
}

#[cfg(test)]
#[must_use]
pub fn sample_forwarded_packet_with_audio_activity(
    src_key: TransportSessionKey,
    mid: &str,
    voice_activity: Option<bool>,
    audio_level: Option<i8>,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        src_key,
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

#[cfg(feature = "internal-benchmarks")]
#[must_use]
pub fn sample_forwarded_packet_with_rid_and_audio_activity(
    src_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    voice_activity: Option<bool>,
    audio_level: Option<i8>,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        src_key,
        mid,
        rid,
        ExtensionValues {
            audio_level,
            voice_activity,
            ..ExtensionValues::default()
        },
        payload,
    )
}

#[cfg(test)]
#[must_use]
pub fn sample_forwarded_packet_with_frame_mark(
    src_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    frame_mark: u32,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        src_key,
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
    src_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    ext_vals: ExtensionValues,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_source(
        ForwardedPacketSource::Relayed(src_key),
        mid,
        rid,
        ext_vals,
        payload,
    )
}

fn sample_forwarded_packet_with_source(
    source: ForwardedPacketSource,
    mid: &str,
    rid: Option<&str>,
    ext_vals: ExtensionValues,
    payload: &[u8],
) -> ForwardedPacket {
    let received_at = Instant::now();
    ForwardedPacket {
        source,
        src_media: None,
        resolved_source_rid: None,
        facts: None,
        visits_origin_sinks: true,
        received_at,
        payload: Arc::from(payload),
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

#[cfg(any(test, feature = "internal-benchmarks"))]
#[must_use]
pub fn sample_forwarded_packet_without_mid(
    src_key: TransportSessionKey,
    ssrc: u32,
    payload: &[u8],
) -> ForwardedPacket {
    let received_at = Instant::now();
    ForwardedPacket {
        source: ForwardedPacketSource::Relayed(src_key),
        src_media: None,
        resolved_source_rid: None,
        facts: None,
        visits_origin_sinks: true,
        received_at,
        payload: Arc::from(payload),
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

#[cfg(feature = "internal-benchmarks")]
pub fn reset_packet_resolution(packet: &mut ForwardedPacket) {
    packet.facts = None;
    packet.src_media = None;
    packet.resolved_source_rid = None;
}
