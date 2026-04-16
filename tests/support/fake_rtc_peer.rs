#![allow(
    dead_code,
    reason = "the str0m-backed fake RTP peer is shared across native integration scenarios"
)]

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use tokio::{net::UdpSocket, time::timeout};
use tokio_util::bytes::Bytes;

use o_sfu::{
    signaling::protocol::SessionDescriptionPayload,
    signaling::webrtc::MediaKind as SignalingMediaKind,
};
use str0m::{
    Candidate, Event, IceConnectionState, Input, Output, Rtc,
    change::SdpOffer,
    format::{Codec, PayloadParams},
    media::{Mid, Pt},
    net::{Protocol, Receive},
    rtp::{ExtensionValues, RtpPacket},
};

use super::fake_media::{FakeClock, FakeMediaFrame, FakeMediaSource};

const RECEIVE_BUFFER_LEN: usize = 2_000;
const MAX_SOCKET_WAIT: Duration = Duration::from_millis(50);
const IO_SLICE: Duration = Duration::from_millis(20);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedRtpPacket {
    pub mid: String,
    pub payload: Bytes,
}

pub struct NativeFakeRtcPeer {
    rtc: Rtc,
    socket: UdpSocket,
    local_addr: SocketAddr,
    send_paths: BTreeMap<NativeMediaKey, NativeSendPath>,
    connected: bool,
    start_wallclock: Instant,
}

#[derive(Clone, Copy)]
struct NativeSendPath {
    mid: Mid,
    payload_type: Pt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum NativeMediaKey {
    Audio,
    Video,
}

impl NativeFakeRtcPeer {
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
            connected: false,
            start_wallclock: Instant::now(),
        })
    }

    pub fn answer_offer(&mut self, offer_sdp: &str) -> Option<SessionDescriptionPayload> {
        let offer = SdpOffer::from_sdp_string(offer_sdp).ok()?;
        self.send_paths = collect_native_send_paths(offer_sdp, &self.rtc);
        let answer = self.rtc.sdp_api().accept_offer(offer).ok()?;
        self.rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
        Some(SessionDescriptionPayload {
            sdp: answer.to_sdp_string(),
        })
    }

    pub async fn wait_until_connected(&mut self, timeout_window: Duration) -> Option<()> {
        let deadline = Instant::now() + timeout_window;
        if pump_until(
            &mut self.rtc,
            &self.socket,
            self.local_addr,
            &mut self.connected,
            deadline,
            true,
        )
        .await?
        {
            Some(())
        } else {
            None
        }
    }

    pub async fn send_rtp_packets(
        &mut self,
        source: &mut FakeMediaSource,
        clock: &mut FakeClock,
        frame_count: usize,
    ) -> Option<()> {
        for _ in 0..frame_count {
            let frame = source.next_frame(clock);
            self.write_rtp_packet(source.media_kind(), frame)?;
            pump_until(
                &mut self.rtc,
                &self.socket,
                self.local_addr,
                &mut self.connected,
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
        let frame = source.next_frame(clock);
        let expected_payload = frame.payload.clone();
        self.write_rtp_packet(source.media_kind(), frame)?;
        pump_until(
            &mut self.rtc,
            &self.socket,
            self.local_addr,
            &mut self.connected,
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
            Instant::now() + timeout_window,
        )
        .await
    }

    fn write_rtp_packet(
        &mut self,
        media_kind: SignalingMediaKind,
        frame: FakeMediaFrame,
    ) -> Option<()> {
        let send_path = *self.send_paths.get(&NativeMediaKey::from(media_kind))?;
        self.rtc
            .direct_api()
            .stream_tx_by_mid(send_path.mid, None)?
            .write_rtp(
                send_path.payload_type,
                u64::from(frame.sequence_number).into(),
                frame.rtp_timestamp,
                self.start_wallclock + frame.emitted_at,
                false,
                ExtensionValues::default(),
                false,
                frame.payload,
            )
            .ok()?;
        Some(())
    }
}

fn into_received_rtp_packet(rtc: &mut Rtc, packet: RtpPacket) -> Option<ReceivedRtpPacket> {
    let mid = rtc
        .direct_api()
        .stream_rx(&packet.header.ssrc)?
        .mid()
        .to_string();
    Some(ReceivedRtpPacket {
        mid,
        payload: packet.payload.into(),
    })
}

async fn pump_until(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    local_addr: SocketAddr,
    connected: &mut bool,
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
            Output::Event(Event::IceConnectionStateChange(state)) => match state {
                IceConnectionState::Connected | IceConnectionState::Completed => {
                    *connected = true;
                    if stop_on_connected {
                        return Some(true);
                    }
                }
                IceConnectionState::Disconnected => return None,
                _ => {}
            },
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
                return into_received_rtp_packet(rtc, packet);
            }
            Output::Event(Event::Connected) => {
                *connected = true;
            }
            Output::Event(Event::IceConnectionStateChange(state)) => match state {
                IceConnectionState::Connected | IceConnectionState::Completed => {
                    *connected = true;
                }
                IceConnectionState::Disconnected => return None,
                _ => {}
            },
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

fn collect_native_send_paths(
    offer_sdp: &str,
    rtc: &Rtc,
) -> BTreeMap<NativeMediaKey, NativeSendPath> {
    let mut send_paths = BTreeMap::new();
    let mut current_kind: Option<NativeMediaKey> = None;
    let mut current_mid: Option<Mid> = None;
    let mut current_direction = NativeOfferDirection::Inactive;

    let mut flush_section =
        |kind: Option<NativeMediaKey>, mid: Option<Mid>, direction: NativeOfferDirection| {
            let Some(kind) = kind else {
                return;
            };
            if !direction.allows_local_send() {
                return;
            }
            let Some(mid) = mid else {
                return;
            };
            let Some(payload_type) = payload_type_for_media_kind(rtc, kind) else {
                return;
            };
            send_paths.insert(kind, NativeSendPath { mid, payload_type });
        };

    for raw_line in offer_sdp.lines() {
        let line = raw_line.trim_end_matches('\r');
        if let Some(kind) = parse_offer_media_kind(line) {
            flush_section(current_kind, current_mid, current_direction);
            current_kind = Some(kind);
            current_mid = None;
            current_direction = NativeOfferDirection::Inactive;
            continue;
        }
        if line.starts_with("m=") {
            flush_section(current_kind, current_mid, current_direction);
            current_kind = None;
            current_mid = None;
            current_direction = NativeOfferDirection::Inactive;
            continue;
        }
        if let Some(mid) = line.strip_prefix("a=mid:") {
            current_mid = Some(Mid::from(mid));
            continue;
        }
        if let Some(direction) = NativeOfferDirection::parse(line) {
            current_direction = direction;
        }
    }
    flush_section(current_kind, current_mid, current_direction);
    send_paths
}

fn payload_type_for_media_kind(rtc: &Rtc, media_kind: NativeMediaKey) -> Option<Pt> {
    let codec = match media_kind {
        NativeMediaKey::Audio => Codec::Opus,
        NativeMediaKey::Video => Codec::Vp8,
    };
    rtc.codec_config()
        .find(|params| params.spec().codec == codec)
        .map(PayloadParams::pt)
}

fn parse_offer_media_kind(line: &str) -> Option<NativeMediaKey> {
    if line.starts_with("m=audio ") {
        Some(NativeMediaKey::Audio)
    } else if line.starts_with("m=video ") {
        Some(NativeMediaKey::Video)
    } else {
        None
    }
}

impl From<SignalingMediaKind> for NativeMediaKey {
    fn from(value: SignalingMediaKind) -> Self {
        match value {
            SignalingMediaKind::Audio => Self::Audio,
            SignalingMediaKind::Video => Self::Video,
        }
    }
}

#[derive(Clone, Copy, Default)]
enum NativeOfferDirection {
    SendRecv,
    SendOnly,
    RecvOnly,
    #[default]
    Inactive,
}

impl NativeOfferDirection {
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
