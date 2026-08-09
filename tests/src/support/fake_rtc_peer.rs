#![allow(
    dead_code,
    reason = "the str0m-backed fake RTP peer is shared across protocol integration scenarios"
)]

use std::{
    collections::BTreeMap,
    mem,
    net::SocketAddr,
    time::{Duration, Instant},
};

use o_sfu_protocol::wire::SessionDescriptionPayload;
use o_sfu_rfc::{rtp::CodecName, webrtc};
use o_sfu_router::MediaKind;
use str0m::{
    Candidate, Event, IceConnectionState, Input, Output, Rtc,
    change::SdpOffer,
    format::{Codec, PayloadParams},
    media::{KeyframeRequest, Mid, Pt, Rid},
    net::{Protocol, Receive},
    rtp::{RtpPacket, RtpWrite, Ssrc},
};
use tokio::{net::UdpSocket, time::timeout};
use tokio_util::bytes::Bytes;

use super::fake_media::{FakeClock, FakeMediaFrame, FakeMediaSource};

const RECEIVE_BUFFER_LEN: usize = 2_000;
const MAX_SOCKET_WAIT: Duration = Duration::from_millis(50);
const IO_SLICE: Duration = Duration::from_millis(20);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedRtpPacket {
    pub mid: String,
    pub payload: Bytes,
}

pub struct FakeRtcPeer {
    rtc: Rtc,
    socket: UdpSocket,
    local_addr: SocketAddr,
    send_paths: BTreeMap<MediaKind, ProtocolSendPath>,
    pending_keyframe_requests: Vec<KeyframeRequest>,
    connected: bool,
    start_wallclock: Instant,
    next_synthetic_ssrc: u32,
}

#[derive(Clone)]
struct ProtocolSendPath {
    mid: Mid,
    rids: Vec<ProtocolRid>,
}

#[derive(Clone)]
struct ProtocolRid {
    rid: Rid,
    max_bitrate: Option<u64>,
}

impl FakeRtcPeer {
    pub async fn bind(port: u16) -> Option<Self> {
        let socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], port)))
            .await
            .ok()?;
        let local_addr = socket.local_addr().ok()?;
        let mut rtc = Rtc::builder().set_rtp_mode(true).build(Instant::now());
        rtc.add_local_candidate(Candidate::host(local_addr, "udp").ok()?)?;
        Some(Self {
            rtc,
            socket,
            local_addr,
            send_paths: BTreeMap::new(),
            pending_keyframe_requests: Vec::new(),
            connected: false,
            start_wallclock: Instant::now(),
            next_synthetic_ssrc: 0x0f00_0001,
        })
    }

    pub fn answer_offer(&mut self, offer_sdp: &str) -> Option<SessionDescriptionPayload> {
        let offer = SdpOffer::from_sdp_string(offer_sdp).ok()?;
        self.send_paths = collect_protocol_send_paths(offer_sdp);
        let answer = self.rtc.sdp_api().accept_offer(offer).ok()?;
        let answer_sdp = answer_with_simulcast_send_rids(&answer.to_sdp_string(), &self.send_paths);
        self.rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
        Some(SessionDescriptionPayload {
            sdp: answer_sdp,
            upload_slots: Vec::new(),
        })
    }

    pub fn answer_offer_without_candidates(
        &mut self,
        offer_sdp: &str,
    ) -> Option<SessionDescriptionPayload> {
        let mut answer = self.answer_offer(offer_sdp)?;
        answer.sdp = answer
            .sdp
            .split_inclusive("\r\n")
            .filter(|line| {
                !line
                    .strip_prefix(webrtc::sdp::ATTRIBUTE_PREFIX)
                    .is_some_and(|attribute| {
                        attribute.starts_with(webrtc::ice::candidate_attribute::PREFIX)
                    })
            })
            .collect();
        Some(answer)
    }

    pub async fn wait_until_connected(&mut self, timeout_window: Duration) -> Option<()> {
        pump_until(
            &mut self.rtc,
            &self.socket,
            self.local_addr,
            &mut self.connected,
            &mut self.pending_keyframe_requests,
            Instant::now() + timeout_window,
            true,
        )
        .await?
        .then_some(())
    }

    pub async fn send_rtp_packets(
        &mut self,
        source: &mut FakeMediaSource,
        clock: &mut FakeClock,
        frame_count: usize,
    ) -> Option<()> {
        for _ in 0..frame_count {
            self.apply_keyframe_requests(source);
            let frame = source.next_frame(clock);
            self.write_rtp_packet(frame)?;
            pump_until(
                &mut self.rtc,
                &self.socket,
                self.local_addr,
                &mut self.connected,
                &mut self.pending_keyframe_requests,
                Instant::now() + IO_SLICE,
                false,
            )
            .await?;
        }
        Some(())
    }

    pub async fn send_rtp_packet(
        &mut self,
        source: &mut FakeMediaSource,
        clock: &mut FakeClock,
    ) -> Option<Vec<u8>> {
        self.apply_keyframe_requests(source);
        let frame = source.next_frame(clock);
        let expected_payload = frame.payload.clone();
        self.write_rtp_packet(frame)?;
        pump_until(
            &mut self.rtc,
            &self.socket,
            self.local_addr,
            &mut self.connected,
            &mut self.pending_keyframe_requests,
            Instant::now() + IO_SLICE,
            false,
        )
        .await?;
        Some(expected_payload)
    }

    pub async fn read_rtp_packet(&mut self, timeout_window: Duration) -> Option<ReceivedRtpPacket> {
        pump_until_rtp(
            &mut self.rtc,
            &self.socket,
            self.local_addr,
            &mut self.connected,
            &mut self.pending_keyframe_requests,
            Instant::now() + timeout_window,
        )
        .await
    }

    pub fn reset_rtp_ssrc(&mut self, media_kind: MediaKind, rid: Option<&str>) -> Option<()> {
        let mid = self.send_paths.get(&media_kind)?.mid;
        let ssrc = self.next_synthetic_ssrc();
        self.rtc
            .direct_api()
            .reset_stream_tx(mid, rid.map(Rid::from), ssrc, None)?;
        Some(())
    }

    fn apply_keyframe_requests(&mut self, source: &mut FakeMediaSource) {
        let Some(mid) = self
            .send_paths
            .get(&source.media_kind())
            .map(|send_path| send_path.mid)
        else {
            return;
        };
        for request in mem::take(&mut self.pending_keyframe_requests) {
            if request.mid == mid {
                source.request_keyframe(request.rid.as_deref());
            } else {
                self.pending_keyframe_requests.push(request);
            }
        }
    }

    fn write_rtp_packet(&mut self, frame: FakeMediaFrame) -> Option<()> {
        let send_path = self.send_paths.get(&frame.media_kind)?.clone();
        let payload_type = payload_type_for_codec(&self.rtc, &frame.codec)?;
        let stream_rid = frame.rid.as_deref().map(Rid::from);
        let extension_rid = frame.rid.as_deref().map(Rid::from);
        if !send_path.accepts_rid(stream_rid) {
            return None;
        }
        self.ensure_tx_stream(send_path.mid, stream_rid);
        let mut extension_values = frame.extension_values;
        extension_values.mid = Some(send_path.mid);
        extension_values.rid = extension_rid;
        self.rtc
            .direct_api()
            .stream_tx_by_mid(send_path.mid, stream_rid)?
            .write_rtp(
                RtpWrite::new(
                    payload_type,
                    u64::from(frame.sequence_number).into(),
                    frame.rtp_timestamp,
                    self.start_wallclock + frame.emitted_at,
                    frame.payload,
                )
                .marker(frame.marker)
                .ext_vals(extension_values),
            );
        Some(())
    }

    fn ensure_tx_stream(&mut self, mid: Mid, rid: Option<Rid>) {
        if self.rtc.direct_api().stream_tx_by_mid(mid, rid).is_some() {
            return;
        }
        let ssrc = self.next_synthetic_ssrc();
        self.rtc
            .direct_api()
            .declare_stream_tx(ssrc, None, mid, rid);
    }

    fn next_synthetic_ssrc(&mut self) -> Ssrc {
        let ssrc = self.next_synthetic_ssrc;
        self.next_synthetic_ssrc = self.next_synthetic_ssrc.wrapping_add(1);
        Ssrc::from(ssrc)
    }
}

impl ProtocolSendPath {
    fn accepts_rid(&self, rid: Option<Rid>) -> bool {
        rid.map_or(self.rids.is_empty(), |rid| {
            !self.rids.is_empty() && self.rids.iter().any(|candidate| candidate.rid == rid)
        })
    }
}

fn into_received_rtp_packet(rtc: &mut Rtc, packet: &RtpPacket) -> Option<ReceivedRtpPacket> {
    let mid = rtc
        .direct_api()
        .stream_rx(&packet.header.ssrc)?
        .mid()
        .to_string();
    Some(ReceivedRtpPacket {
        mid,
        payload: Bytes::copy_from_slice(packet.payload.as_ref()),
    })
}

async fn pump_until(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    local_addr: SocketAddr,
    connected: &mut bool,
    pending_keyframe_requests: &mut Vec<KeyframeRequest>,
    deadline: Instant,
    stop_on_connected: bool,
) -> Option<bool> {
    let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
    while Instant::now() < deadline {
        let now = Instant::now();
        match rtc.poll_output().ok()? {
            Output::Transmit(transmit) => {
                socket
                    .send_to(&transmit.contents, transmit.destination)
                    .await
                    .ok()?;
            }
            Output::Event(Event::Connected) => {
                *connected = true;
                if stop_on_connected {
                    return Some(true);
                }
            }
            Output::Event(Event::IceConnectionStateChange(IceConnectionState::Disconnected)) => {
                return None;
            }
            Output::Event(Event::KeyframeRequest(request)) => {
                pending_keyframe_requests.push(request);
            }
            Output::Event(_) => {}
            Output::Timeout(timeout_at) => {
                if timeout_at <= now {
                    rtc.handle_input(Input::Timeout(now)).ok()?;
                    continue;
                }

                let wait_duration = timeout_at
                    .saturating_duration_since(now)
                    .min(MAX_SOCKET_WAIT)
                    .min(deadline.saturating_duration_since(now));
                if wait_duration.is_zero() {
                    rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
                    continue;
                }

                match timeout(wait_duration, socket.recv_from(&mut receive_buffer)).await {
                    Ok(Ok((received_size, source_addr))) => {
                        if received_size == 0 {
                            continue;
                        }
                        let packet = receive_buffer.get(..received_size)?;
                        let receive = Receive {
                            proto: Protocol::Udp,
                            source: source_addr,
                            destination: local_addr,
                            contents: packet.try_into().ok()?,
                        };
                        rtc.handle_input(Input::Receive(Instant::now(), receive))
                            .ok()?;
                    }
                    Ok(Err(_error)) => return None,
                    Err(_elapsed) => {
                        rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
                    }
                }
            }
        }
    }
    Some(*connected)
}

async fn pump_until_rtp(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    local_addr: SocketAddr,
    connected: &mut bool,
    pending_keyframe_requests: &mut Vec<KeyframeRequest>,
    deadline: Instant,
) -> Option<ReceivedRtpPacket> {
    let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
    while Instant::now() < deadline {
        let now = Instant::now();
        match rtc.poll_output().ok()? {
            Output::Transmit(transmit) => {
                socket
                    .send_to(&transmit.contents, transmit.destination)
                    .await
                    .ok()?;
            }
            Output::Event(Event::RtpPacket(packet)) => {
                return into_received_rtp_packet(rtc, &packet);
            }
            Output::Event(Event::Connected) => {
                *connected = true;
            }
            Output::Event(Event::IceConnectionStateChange(IceConnectionState::Disconnected)) => {
                return None;
            }
            Output::Event(Event::KeyframeRequest(request)) => {
                pending_keyframe_requests.push(request);
            }
            Output::Event(_) => {}
            Output::Timeout(timeout_at) => {
                if timeout_at <= now {
                    rtc.handle_input(Input::Timeout(now)).ok()?;
                    continue;
                }

                let wait_duration = timeout_at
                    .saturating_duration_since(now)
                    .min(MAX_SOCKET_WAIT)
                    .min(deadline.saturating_duration_since(now));
                if wait_duration.is_zero() {
                    rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
                    continue;
                }

                match timeout(wait_duration, socket.recv_from(&mut receive_buffer)).await {
                    Ok(Ok((received_size, source_addr))) => {
                        if received_size == 0 {
                            continue;
                        }
                        let packet = receive_buffer.get(..received_size)?;
                        let receive = Receive {
                            proto: Protocol::Udp,
                            source: source_addr,
                            destination: local_addr,
                            contents: packet.try_into().ok()?,
                        };
                        rtc.handle_input(Input::Receive(Instant::now(), receive))
                            .ok()?;
                    }
                    Ok(Err(_error)) => return None,
                    Err(_elapsed) => {
                        rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
                    }
                }
            }
        }
    }
    None
}

fn collect_protocol_send_paths(offer_sdp: &str) -> BTreeMap<MediaKind, ProtocolSendPath> {
    let mut send_paths = BTreeMap::new();
    let mut current_kind: Option<MediaKind> = None;
    let mut current_mid: Option<Mid> = None;
    let mut current_rids = Vec::new();
    let mut current_direction = OfferDirection::Inactive;

    let mut flush_section = |kind: Option<MediaKind>,
                             mid: Option<Mid>,
                             rids: Vec<ProtocolRid>,
                             direction: OfferDirection| {
        let Some(kind) = kind else {
            return;
        };
        if !direction.allows_local_send() {
            return;
        }
        let Some(mid) = mid else {
            return;
        };
        send_paths.insert(kind, ProtocolSendPath { mid, rids });
    };

    for raw_line in offer_sdp.lines() {
        let line = raw_line.trim_end_matches('\r');
        if let Some(kind) = parse_offer_media_kind(line) {
            flush_section(current_kind, current_mid, current_rids, current_direction);
            current_kind = Some(kind);
            current_mid = None;
            current_rids = Vec::new();
            current_direction = OfferDirection::Inactive;
            continue;
        }
        if line.starts_with("m=") {
            flush_section(current_kind, current_mid, current_rids, current_direction);
            current_kind = None;
            current_mid = None;
            current_rids = Vec::new();
            current_direction = OfferDirection::Inactive;
            continue;
        }
        if let Some(mid) = line.strip_prefix("a=mid:") {
            current_mid = Some(Mid::from(mid));
            continue;
        }
        if let Some(rid) = parse_recv_rid(line) {
            current_rids.push(rid);
            continue;
        }
        if let Some(direction) = OfferDirection::parse(line) {
            current_direction = direction;
        }
    }
    flush_section(current_kind, current_mid, current_rids, current_direction);
    send_paths
}

fn payload_type_for_codec(rtc: &Rtc, codec_name: &CodecName) -> Option<Pt> {
    let codec = str0m_codec(codec_name)?;
    rtc.codec_config()
        .find(|params| params.spec().codec == codec)
        .map(PayloadParams::pt)
}

fn str0m_codec(codec_name: &CodecName) -> Option<Codec> {
    match codec_name {
        CodecName::Opus => Some(Codec::Opus),
        CodecName::Vp8 => Some(Codec::Vp8),
        CodecName::H264 => Some(Codec::H264),
        CodecName::Pcmu
        | CodecName::Pcma
        | CodecName::H265
        | CodecName::Vp9
        | CodecName::Av1
        | CodecName::Rtx
        | CodecName::Other(_) => None,
    }
}

fn parse_offer_media_kind(line: &str) -> Option<MediaKind> {
    if line.starts_with("m=audio ") {
        Some(MediaKind::Audio)
    } else if line.starts_with("m=video ") {
        Some(MediaKind::Video)
    } else {
        None
    }
}

fn answer_with_simulcast_send_rids(
    answer_sdp: &str,
    send_paths: &BTreeMap<MediaKind, ProtocolSendPath>,
) -> String {
    send_paths
        .values()
        .filter(|send_path| !send_path.rids.is_empty())
        .fold(answer_sdp.to_owned(), |answer_sdp, send_path| {
            answer_with_mid_send_rids(&answer_sdp, send_path)
        })
}

fn answer_with_mid_send_rids(answer_sdp: &str, send_path: &ProtocolSendPath) -> String {
    let marker = format!(
        "{}{}:{}\r\n",
        webrtc::sdp::ATTRIBUTE_PREFIX,
        webrtc::sdp::attribute::MID,
        send_path.mid
    );
    let mut replacement = marker.clone();
    for rid in &send_path.rids {
        replacement.push_str(&sdp_rid_line(rid, webrtc::sdp::rid::DIRECTION_SEND));
        replacement.push_str("\r\n");
    }
    replacement.push_str(&sdp_simulcast_line(
        webrtc::sdp::simulcast::DIRECTION_SEND,
        &send_path.rids,
    ));
    replacement.push_str("\r\n");
    answer_sdp.replacen(&marker, &replacement, 1)
}

fn sdp_rid_line(rid: &ProtocolRid, direction: &str) -> String {
    let mut line = format!(
        "{}{}:{} {}",
        webrtc::sdp::ATTRIBUTE_PREFIX,
        webrtc::sdp::attribute::RID,
        rid.rid,
        direction
    );
    if let Some(max_bitrate) = rid.max_bitrate {
        line.push(' ');
        line.push_str(webrtc::sdp::rid_restriction::MAX_BITRATE);
        line.push('=');
        line.push_str(&max_bitrate.to_string());
    }
    line
}

fn sdp_simulcast_line(direction: &str, rids: &[ProtocolRid]) -> String {
    let separator = webrtc::sdp::simulcast::STREAM_SEPARATOR.to_string();
    let rid_values = rids
        .iter()
        .map(|rid| rid.rid.to_string())
        .collect::<Vec<_>>()
        .join(&separator);
    format!(
        "{}{}:{} {}",
        webrtc::sdp::ATTRIBUTE_PREFIX,
        webrtc::sdp::attribute::SIMULCAST,
        direction,
        rid_values
    )
}

fn parse_recv_rid(line: &str) -> Option<ProtocolRid> {
    let rid_prefix = format!(
        "{}{}:",
        webrtc::sdp::ATTRIBUTE_PREFIX,
        webrtc::sdp::attribute::RID
    );
    let rest = line.trim_end_matches('\r').strip_prefix(&rid_prefix)?;
    let mut parts = rest.splitn(3, ' ');
    let rid = parts.next()?;
    if !webrtc::sdp::rid::is_id(rid) {
        return None;
    }
    let direction = parts.next()?;
    if webrtc::RtpStreamDirection::parse(direction) != Some(webrtc::RtpStreamDirection::Recv) {
        return None;
    }
    Some(ProtocolRid {
        rid: Rid::from(rid),
        max_bitrate: parts.next().and_then(parse_max_bitrate),
    })
}

fn parse_max_bitrate(restrictions: &str) -> Option<u64> {
    restrictions
        .split(';')
        .filter_map(|restriction| restriction.split_once('='))
        .find_map(|(key, value)| {
            (key.trim() == webrtc::sdp::rid_restriction::MAX_BITRATE)
                .then(|| value.trim().parse::<u64>().ok())
                .flatten()
        })
}

#[derive(Clone, Copy, Default)]
enum OfferDirection {
    SendRecv,
    SendOnly,
    RecvOnly,
    #[default]
    Inactive,
}

impl OfferDirection {
    fn parse(line: &str) -> Option<Self> {
        match line {
            "a=sendrecv" => Some(Self::SendRecv),
            "a=sendonly" => Some(Self::SendOnly),
            "a=recvonly" => Some(Self::RecvOnly),
            "a=inactive" => Some(Self::Inactive),
            _ => None,
        }
    }

    fn allows_local_send(self) -> bool {
        matches!(self, Self::RecvOnly | Self::SendRecv)
    }
}
