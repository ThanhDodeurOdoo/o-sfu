//! SDP validation for the video Generic NACK and RTX repair topology.
//!
//! Generic NACK requests lost primary RTP packets through RTCP. RTX returns
//! each requested packet in a separate RTP repair stream. The SDP answer must
//! make every relationship unambiguous before `str0m` mutates session state:
//!
//! ```text
//! primary payload type p <--- a=fmtp:r apt=p --- RTX payload type r
//!          |
//!          +--- a=rtcp-fb:p nack
//!
//! primary SSRC P <--------- FID --------- repair SSRC R
//!
//! primary RID id <--- `RepairedRtpStreamId` --- repair stream
//! ```
//!
//! O-SFU accepts repair only for video with exact Generic NACK and one RTX
//! payload type whose `apt` names the primary payload type. RID-based streams
//! also require `RepairedRtpStreamId`. A sending RID-less answer section with
//! signaled SSRCs requires one complete two-SSRC FID group.
//!
//! Publisher-side loss is repaired before source lookup and fanout:
//!
//! ```text
//! Publisher                         O-SFU / str0m
//!     |                                    |
//!     | primary RTP, SSRC=P, PT=p, seq=N   X  lost
//!     | primary RTP, SSRC=P, PT=p, seq=N+1 |
//!     |----------------------------------->|  gap N detected
//!     |                                    |
//!     |<-----------------------------------|  RTCP Generic NACK
//!     |                                    |  media SSRC=P, PID=N
//!     |                                    |
//!     | RTX RTP, SSRC=R, PT=r, seq=K       |
//!     | payload=[OSN=N | media payload]    |
//!     |----------------------------------->|  authenticate and de-RTX
//!     |                                    |  normalize to SSRC=P, PT=p, seq=N
//!     |                                    |  then source lookup and fanout
//! ```
//!
//! Subscriber-side loss is repaired from the receiver-local `StreamTx` cache:
//!
//! ```text
//! Publisher             O-SFU / str0m                    Subscriber
//!     |                        |                              |
//!     | primary RTP            |                              |
//!     |----------------------->| rewrite to SSRC=C, PT=p      |
//!     |                        | seq=M and cache final write  |
//!     |                        |------------------------------X  lost
//!     |                        | primary RTP, seq=M+1         |
//!     |                        |----------------------------->|  gap M
//!     |                        |<-----------------------------|  RTCP NACK
//!     |                        | media SSRC=C, PID=M          |
//!     |                        |                              |
//!     |                        | RTX RTP, SSRC=C_rtx, PT=r    |
//!     |                        | seq=K, OSN=M, cached payload |
//!     |                        |----------------------------->|
//! ```
//!
//! Generic NACK and its `PID`/`BLP` loss set are defined by
//! [RFC 4585 section 6.2.1](https://www.rfc-editor.org/rfc/rfc4585.html#section-6.2.1).
//! The RTX packet, independent repair sequence and original sequence number
//! are defined by
//! [RFC 4588 section 4](https://www.rfc-editor.org/rfc/rfc4588.html#section-4).
//! SDP `rtcp-fb` and `apt` mappings come from
//! [RFC 4585 section 4.2](https://www.rfc-editor.org/rfc/rfc4585.html#section-4.2)
//! and
//! [RFC 4588 section 8.1](https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1).
//! FID and RID repair associations come from
//! [RFC 5576 section 7](https://www.rfc-editor.org/rfc/rfc5576.html#section-7)
//! and
//! [RFC 8851 section 4](https://www.rfc-editor.org/rfc/rfc8851.html#section-4).

use std::collections::{BTreeMap, BTreeSet};

use o_sfu_rfc::{
    rtp,
    webrtc::{self, sdp},
};
use str0m::{change::SdpOffer, format::PayloadParams};

use super::rid::send_simulcast_stream_count;
use crate::engine::media_transport::TransportAdapterError;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) struct RepairSummary(Vec<Vec<(u8, u8)>>);

impl RepairSummary {
    pub(in crate::engine::media_transport::rtc) fn from_offer(offer: &SdpOffer) -> Self {
        Self(
            offer
                .media_lines
                .iter()
                .map(|media_line| {
                    media_line
                        .rtp_params()
                        .into_iter()
                        .filter_map(|payload| {
                            payload.resend().map(|repair| (*payload.pt(), *repair))
                        })
                        .collect()
                })
                .collect(),
        )
    }

    pub(in crate::engine::media_transport::rtc) fn accepts(&self, answer: &Self) -> bool {
        // An answer can only retain repair mappings from the offer. The primary and RTX
        // payload types form one mapping.
        // https://www.rfc-editor.org/rfc/rfc3264.html#section-6.1
        // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
        self.0.len() == answer.0.len()
            && self
                .0
                .iter()
                .zip(&answer.0)
                .all(|(offered, accepted)| accepted.iter().all(|pair| offered.contains(pair)))
    }
}

pub(super) fn validate_profile_payload_types(
    payloads: &[PayloadParams],
) -> Result<(), TransportAdapterError> {
    // One bundled RTP session cannot assign one payload type to multiple formats.
    // https://www.rfc-editor.org/rfc/rfc8834.html#section-4.3
    let mut claimed = [false; rtp::RTP_PAYLOAD_TYPE_COUNT];
    for payload in payloads {
        if payload
            .resend()
            .is_some_and(|repair| !rtp::is_rtcp_mux_dynamic_payload_type(*repair))
        {
            // RTX requires a dynamically assigned payload type. RTP/RTCP mux also permits
            // unassigned values below 64 when the preferred range is exhausted.
            // https://www.rfc-editor.org/rfc/rfc4588.html#section-4
            // https://www.rfc-editor.org/rfc/rfc5761.html#section-4
            return Err(TransportAdapterError::InvalidInput);
        }
        for payload_type in [Some(payload.pt()), payload.resend()].into_iter().flatten() {
            let payload_type = usize::from(*payload_type);
            let claimed = claimed
                .get_mut(payload_type)
                .ok_or(TransportAdapterError::InvalidInput)?;
            if *claimed {
                return Err(TransportAdapterError::InvalidInput);
            }
            *claimed = true;
        }
    }
    Ok(())
}

pub fn validate_answer_sdp(answer_sdp: &str) -> Result<RepairSummary, TransportAdapterError> {
    let mut current: Option<RepairSection> = None;
    let mut sections = Vec::new();
    // SDP defaults to sendrecv when no session-level or media-level direction exists.
    // https://www.rfc-editor.org/rfc/rfc8866.html#section-6.7.2
    let mut session_remote_sends = true;
    for line in answer_sdp
        .lines()
        .map(|line| line.trim_end_matches(sdp::CR))
    {
        if let Some(media) = line.strip_prefix(sdp::MEDIA) {
            if let Some(section) = current.take() {
                sections.push(section.validate()?);
            }
            current = Some(RepairSection::new(media, session_remote_sends)?);
        } else if let Some(section) = &mut current {
            section.parse_line(line)?;
        } else if let Some(remote_sends) = remote_sends(line) {
            session_remote_sends = remote_sends;
        }
    }
    if let Some(section) = current {
        sections.push(section.validate()?);
    }
    let mut claimed_ssrcs = BTreeSet::new();
    // O-SFU uses one bundled RTP session, where one SSRC identifies one media section.
    // https://www.rfc-editor.org/rfc/rfc9143.html#section-9.2
    if sections
        .iter()
        .flat_map(|section| &section.signaled_ssrcs)
        .any(|ssrc| !claimed_ssrcs.insert(*ssrc))
    {
        return Err(TransportAdapterError::InvalidInput);
    }
    Ok(RepairSummary(
        sections
            .into_iter()
            .map(|section| section.payload_pairs)
            .collect(),
    ))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MediaKind {
    Audio,
    Video,
    Other,
}

#[derive(Clone, Copy)]
struct RtpMap {
    is_rtx: bool,
    clock_rate: u32,
}

#[derive(Default)]
struct ValidatedRepairSection {
    payload_pairs: Vec<(u8, u8)>,
    signaled_ssrcs: BTreeSet<u32>,
}

struct RepairSection {
    kind: MediaKind,
    rejected: bool,
    remote_sends: bool,
    payload_types: BTreeSet<u8>,
    rtp_maps: BTreeMap<u8, RtpMap>,
    apt: BTreeMap<u8, u8>,
    exact_nack: BTreeSet<u8>,
    fid_groups: Vec<(u32, u32)>,
    signaled_ssrcs: BTreeSet<u32>,
    extmap_ids: BTreeSet<u8>,
    rid_extension: Option<u8>,
    repaired_rid_extension: Option<u8>,
    has_rid: bool,
    send_rid_count: usize,
}

impl RepairSection {
    fn new(media: &str, remote_sends: bool) -> Result<Self, TransportAdapterError> {
        let mut fields = media.split_ascii_whitespace();
        let kind = match fields.next() {
            Some(webrtc::media_kind::AUDIO) => MediaKind::Audio,
            Some(webrtc::media_kind::VIDEO) => MediaKind::Video,
            Some(_) => MediaKind::Other,
            None => return Err(TransportAdapterError::InvalidInput),
        };
        let port = fields.next().ok_or(TransportAdapterError::InvalidInput)?;
        let _protocol = fields.next().ok_or(TransportAdapterError::InvalidInput)?;
        let mut payload_types = BTreeSet::new();
        if kind != MediaKind::Other {
            for value in fields {
                let payload_type = parse_payload_type(value)?;
                if !payload_types.insert(payload_type) {
                    return Err(TransportAdapterError::InvalidInput);
                }
            }
        }
        // Port zero rejects this offered media stream in an answer.
        // https://www.rfc-editor.org/rfc/rfc3264.html#section-6
        Ok(Self {
            kind,
            rejected: port.split(sdp::media::PORT_SEP).next() == Some(sdp::media::ZERO_PORT),
            remote_sends,
            payload_types,
            rtp_maps: BTreeMap::new(),
            apt: BTreeMap::new(),
            exact_nack: BTreeSet::new(),
            fid_groups: Vec::new(),
            signaled_ssrcs: BTreeSet::new(),
            extmap_ids: BTreeSet::new(),
            rid_extension: None,
            repaired_rid_extension: None,
            has_rid: false,
            send_rid_count: 0,
        })
    }

    fn parse_line(&mut self, line: &str) -> Result<(), TransportAdapterError> {
        if self.kind == MediaKind::Other {
            return Ok(());
        }
        if let Some(remote_sends) = remote_sends(line) {
            self.remote_sends = remote_sends;
            return Ok(());
        }
        let Some((name, value)) = line
            .strip_prefix(sdp::ATTR)
            .and_then(|attribute| attribute.split_once(sdp::ATTR_SEP))
        else {
            return Ok(());
        };
        // SDP attribute names inherit SDP's case-significant value rule.
        // https://www.rfc-editor.org/rfc/rfc8866.html#section-5
        match name {
            sdp::attribute::RTPMAP => {
                let (payload_type, rtp_map) = parse_rtpmap(value)?;
                if self.rtp_maps.insert(payload_type, rtp_map).is_some() {
                    return Err(TransportAdapterError::InvalidInput);
                }
            }
            sdp::attribute::FMTP => {
                if let Some((repair, primary)) = parse_apt(value)?
                    && self.apt.insert(repair, primary).is_some()
                {
                    return Err(TransportAdapterError::InvalidInput);
                }
            }
            sdp::attribute::RTCP_FB => {
                if let Some(payload_type) = parse_exact_nack(value)?
                    && !self.exact_nack.insert(payload_type)
                {
                    return Err(TransportAdapterError::InvalidInput);
                }
            }
            sdp::attribute::EXTMAP => self.parse_extmap(value)?,
            sdp::attribute::SSRC_GROUP => {
                if let Some(group) = parse_fid(value)? {
                    self.fid_groups.push(group);
                }
            }
            sdp::attribute::SSRC => {
                let ssrc = value
                    .split_ascii_whitespace()
                    .next()
                    .ok_or(TransportAdapterError::InvalidInput)?
                    .parse::<u32>()
                    .map_err(|_error| TransportAdapterError::InvalidInput)?;
                self.signaled_ssrcs.insert(ssrc);
            }
            sdp::attribute::RID => self.has_rid = true,
            sdp::attribute::SIMULCAST => {
                self.has_rid = true;
                // Only a `send` list creates producer RID bindings. Receive-only
                // signaling cannot disambiguate SSRCs sent by the remote endpoint.
                // https://www.rfc-editor.org/rfc/rfc8853.html#section-5.2
                self.send_rid_count = self.send_rid_count.max(
                    send_simulcast_stream_count(value)
                        .map_err(|_error| TransportAdapterError::InvalidInput)?,
                );
            }
            _ => {}
        }
        Ok(())
    }

    fn parse_extmap(&mut self, value: &str) -> Result<(), TransportAdapterError> {
        // `a=extmap` appends an optional direction to the ID and keeps IDs
        // unique within their signaling scope.
        // https://www.rfc-editor.org/rfc/rfc8285.html#section-5
        let mut fields = value.split_ascii_whitespace();
        let id = fields
            .next()
            .and_then(|value| value.split(sdp::extmap::DIR_SEP).next())
            .ok_or(TransportAdapterError::InvalidInput)?
            .parse::<u8>()
            .map_err(|_error| TransportAdapterError::InvalidInput)?;
        let uri = fields.next().ok_or(TransportAdapterError::InvalidInput)?;
        if !self.extmap_ids.insert(id) {
            return Err(TransportAdapterError::InvalidInput);
        }
        if uri.eq_ignore_ascii_case(webrtc::rtp_header_extension_uri::RTP_STREAM_ID) {
            if self.rid_extension.replace(id).is_some() {
                return Err(TransportAdapterError::InvalidInput);
            }
        } else if uri.eq_ignore_ascii_case(webrtc::rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID)
            && self.repaired_rid_extension.replace(id).is_some()
        {
            return Err(TransportAdapterError::InvalidInput);
        }
        Ok(())
    }

    fn validate(self) -> Result<ValidatedRepairSection, TransportAdapterError> {
        if self.kind == MediaKind::Other {
            return Ok(ValidatedRepairSection::default());
        }
        if self
            .rtp_maps
            .keys()
            .any(|payload_type| !self.payload_types.contains(payload_type))
            || self
                .exact_nack
                .iter()
                .any(|payload_type| !self.payload_types.contains(payload_type))
        {
            return Err(TransportAdapterError::InvalidInput);
        }

        let mut repairs_by_primary = BTreeMap::new();
        for (repair, primary) in &self.apt {
            let Some(repair_map) = self.rtp_maps.get(repair) else {
                return Err(TransportAdapterError::InvalidInput);
            };
            let Some(primary_map) = self.rtp_maps.get(primary) else {
                return Err(TransportAdapterError::InvalidInput);
            };
            // RTX identifies its primary through `apt` and uses the primary clock rate.
            // O-SFU further limits the negotiated video profile to its 90 kHz clock.
            // https://www.rfc-editor.org/rfc/rfc4588.html#section-4
            // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
            if !self.payload_types.contains(repair)
                || !self.payload_types.contains(primary)
                || repair == primary
                || !rtp::is_rtcp_mux_dynamic_payload_type(*repair)
                || !repair_map.is_rtx
                || primary_map.is_rtx
                || repair_map.clock_rate != rtp::RTP_VIDEO_CLOCK_RATE_HZ
                || repair_map.clock_rate != primary_map.clock_rate
                || repairs_by_primary.insert(*primary, *repair).is_some()
            {
                return Err(TransportAdapterError::InvalidInput);
            }
        }
        if self
            .rtp_maps
            .iter()
            .any(|(payload_type, rtp_map)| rtp_map.is_rtx != self.apt.contains_key(payload_type))
        {
            return Err(TransportAdapterError::InvalidInput);
        }

        if self.kind == MediaKind::Audio {
            // O-SFU limits this bounded repair path to video. RFC 4588 also defines
            // audio/rtx, so rejecting audio repair is an implementation policy.
            // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.2
            if !repairs_by_primary.is_empty()
                || !self.exact_nack.is_empty()
                || !self.fid_groups.is_empty()
            {
                return Err(TransportAdapterError::InvalidInput);
            }
            return Ok(ValidatedRepairSection {
                payload_pairs: Vec::new(),
                signaled_ssrcs: self.signaled_ssrcs,
            });
        }

        // O-SFU exposes Generic NACK only when its forwarding path can also negotiate RTX.
        // RFC 4585 permits Generic NACK without RTX, so this is O-SFU policy.
        // https://www.rfc-editor.org/rfc/rfc4585.html#section-4.2
        // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
        for (payload_type, rtp_map) in &self.rtp_maps {
            if rtp_map.is_rtx {
                continue;
            }
            if self.exact_nack.contains(payload_type)
                != repairs_by_primary.contains_key(payload_type)
            {
                return Err(TransportAdapterError::InvalidInput);
            }
        }
        if self.exact_nack.iter().any(|payload_type| {
            self.rtp_maps
                .get(payload_type)
                .is_none_or(|rtp_map| rtp_map.is_rtx)
        }) {
            return Err(TransportAdapterError::InvalidInput);
        }
        // RID-based redundancy must use RepairedRtpStreamId to bind each repair
        // stream to its source RID.
        // https://www.rfc-editor.org/rfc/rfc8851.html#section-4
        // https://www.rfc-editor.org/rfc/rfc8852.html#section-3
        if self.repaired_rid_extension.is_some() && self.rid_extension.is_none() {
            return Err(TransportAdapterError::InvalidInput);
        }
        if self.has_rid && !repairs_by_primary.is_empty() && self.repaired_rid_extension.is_none() {
            return Err(TransportAdapterError::InvalidInput);
        }

        self.validate_fid_topology(!repairs_by_primary.is_empty())?;
        Ok(ValidatedRepairSection {
            payload_pairs: repairs_by_primary.into_iter().collect(),
            signaled_ssrcs: self.signaled_ssrcs,
        })
    }

    fn validate_fid_topology(&self, has_repairs: bool) -> Result<(), TransportAdapterError> {
        // RFC 5576 permits multiple FID pairs. O-SFU distinguishes at most one
        // primary per accepted send RID. Raw RID or receive-only simulcast
        // attributes still use the RID-less MID fallback. Extra pairs would
        // collapse independent primaries into one producer.
        // https://www.rfc-editor.org/rfc/rfc5576.html#section-7
        let requires_complete_fid = !self.rejected
            && self.remote_sends
            && self.send_rid_count == 0
            && has_repairs
            && !self.signaled_ssrcs.is_empty();
        let mut grouped_ssrcs = BTreeSet::new();
        for &(primary, repair) in &self.fid_groups {
            if !has_repairs
                || primary == repair
                || !self.signaled_ssrcs.contains(&primary)
                || !self.signaled_ssrcs.contains(&repair)
                || !grouped_ssrcs.insert(primary)
                || !grouped_ssrcs.insert(repair)
            {
                return Err(TransportAdapterError::InvalidInput);
            }
        }
        if requires_complete_fid
            && (self.fid_groups.len() != 1 || grouped_ssrcs != self.signaled_ssrcs)
        {
            return Err(TransportAdapterError::InvalidInput);
        }
        if !self.rejected && self.remote_sends && self.fid_groups.len() > self.send_rid_count.max(1)
        {
            return Err(TransportAdapterError::InvalidInput);
        }
        Ok(())
    }
}

fn remote_sends(line: &str) -> Option<bool> {
    // In an answer, sendonly and sendrecv let the remote endpoint send this media.
    // https://www.rfc-editor.org/rfc/rfc3264.html#section-6.1
    match line.strip_prefix(sdp::ATTR)? {
        sdp::direction::SEND_ONLY | sdp::direction::SEND_RECV => Some(true),
        sdp::direction::INACTIVE | sdp::direction::RECV_ONLY => Some(false),
        _ => None,
    }
}

fn parse_payload_type(value: &str) -> Result<u8, TransportAdapterError> {
    let payload_type = value
        .parse::<u8>()
        .map_err(|_error| TransportAdapterError::InvalidInput)?;
    if !rtp::is_payload_type(payload_type) {
        return Err(TransportAdapterError::InvalidInput);
    }
    Ok(payload_type)
}

fn parse_rtpmap(value: &str) -> Result<(u8, RtpMap), TransportAdapterError> {
    let mut fields = value.split_ascii_whitespace();
    let payload_type = fields
        .next()
        .ok_or(TransportAdapterError::InvalidInput)
        .and_then(parse_payload_type)?;
    let mut encoding = fields
        .next()
        .ok_or(TransportAdapterError::InvalidInput)?
        .split(sdp::rtpmap::ENC_SEP);
    let codec = encoding.next().ok_or(TransportAdapterError::InvalidInput)?;
    let clock_rate = encoding
        .next()
        .ok_or(TransportAdapterError::InvalidInput)?
        .parse::<u32>()
        .map_err(|_error| TransportAdapterError::InvalidInput)?;
    // SDP encoding names are case-insensitive.
    // https://www.rfc-editor.org/rfc/rfc8866.html#section-6.6
    let is_rtx = codec.eq_ignore_ascii_case(rtp::codec_name::RTX);
    if fields.next().is_some() || is_rtx && encoding.next().is_some() {
        return Err(TransportAdapterError::InvalidInput);
    }
    Ok((payload_type, RtpMap { is_rtx, clock_rate }))
}

fn parse_apt(value: &str) -> Result<Option<(u8, u8)>, TransportAdapterError> {
    // SDP carries codec parameters after `a=fmtp`. RTX maps `apt` to its
    // primary PT and separates `name=value` parameters with semicolons.
    // https://www.rfc-editor.org/rfc/rfc8866.html#section-6.15
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.6
    let (payload_type, parameters) = value.split_once(char::is_whitespace).unwrap_or((value, ""));
    let payload_type = parse_payload_type(payload_type)?;
    let mut associated_payload_type = None;
    for parameter in parameters
        .split(rtp::fmtp::PARAMETER_SEPARATOR)
        .map(str::trim)
    {
        let Some((name, value)) = parameter.split_once(rtp::fmtp::NAME_VALUE_SEPARATOR) else {
            if parameter.eq_ignore_ascii_case(rtp::fmtp::RTX_ASSOCIATION) {
                return Err(TransportAdapterError::InvalidInput);
            }
            continue;
        };
        if !name.trim().eq_ignore_ascii_case(rtp::fmtp::RTX_ASSOCIATION) {
            continue;
        }
        let apt = parse_payload_type(value.trim())?;
        if associated_payload_type.replace(apt).is_some() {
            return Err(TransportAdapterError::InvalidInput);
        }
    }
    Ok(associated_payload_type.map(|primary| (payload_type, primary)))
}

fn parse_exact_nack(value: &str) -> Result<Option<u8>, TransportAdapterError> {
    let mut fields = value.split_ascii_whitespace();
    let target = fields.next().ok_or(TransportAdapterError::InvalidInput)?;
    // RTCP feedback attribute tokens are case-sensitive.
    // https://www.rfc-editor.org/rfc/rfc4585.html#section-4.2
    if fields
        .next()
        .is_none_or(|feedback| feedback != webrtc::rtcp_feedback::kind::NACK)
        || fields.next().is_some()
    {
        return Ok(None);
    }
    parse_payload_type(target).map(Some)
}

fn parse_fid(value: &str) -> Result<Option<(u32, u32)>, TransportAdapterError> {
    let mut fields = value.split_ascii_whitespace();
    if !fields
        .next()
        .is_some_and(|semantics| semantics.eq_ignore_ascii_case(sdp::ssrc_group_semantics::FID))
    {
        return Ok(None);
    }
    // O-SFU accepts the two-SSRC primary and repair form used for RTX. The general
    // `ssrc-group` grammar permits other semantics and group sizes.
    // https://www.rfc-editor.org/rfc/rfc5576.html#section-4.2
    // https://www.rfc-editor.org/rfc/rfc5576.html#section-7
    let primary = fields
        .next()
        .ok_or(TransportAdapterError::InvalidInput)?
        .parse::<u32>()
        .map_err(|_error| TransportAdapterError::InvalidInput)?;
    let repair = fields
        .next()
        .ok_or(TransportAdapterError::InvalidInput)?
        .parse::<u32>()
        .map_err(|_error| TransportAdapterError::InvalidInput)?;
    if fields.next().is_some() {
        return Err(TransportAdapterError::InvalidInput);
    }
    Ok(Some((primary, repair)))
}
