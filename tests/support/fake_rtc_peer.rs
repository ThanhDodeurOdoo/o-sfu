#![allow(
    dead_code,
    reason = "the str0m-backed fake RTC peer is introduced incrementally as phase-8 media-path scenarios are added"
)]

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use serde_json::json;
use tokio::{net::UdpSocket, time::timeout};

use super::legacy_wire::protocol::CurrentRemoteTrackBootstrapPayload;
use o_sfu::{
    signaling::protocol::SessionDescriptionPayload,
    signaling::webrtc::{
        DtlsFingerprint, DtlsParameters, IceParameters, MediaKind as SignalingMediaKind,
        TransportBootstrap,
    },
};
use str0m::{
    Candidate, Event, IceConnectionState, IceCreds, Input, Output, Rtc, RtcConfig,
    change::SdpOffer,
    config::Fingerprint,
    format::{Codec, PayloadParams},
    media::{MediaData, MediaKind as Str0mMediaKind, Mid, Pt},
    net::{Protocol, Receive},
    rtp::{ExtensionValues, Ssrc},
};

use super::fake_media::{FakeClock, FakeMediaFrame, FakeMediaSource};

const RECEIVE_BUFFER_LEN: usize = 2_000;
const MAX_SOCKET_WAIT: Duration = Duration::from_millis(50);
const IO_SLICE: Duration = Duration::from_millis(20);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedMediaFrame {
    pub mid: String,
    pub payload: Vec<u8>,
}

pub struct FakeRtcPeer {
    rtc: Rtc,
    socket: UdpSocket,
    local_addr: SocketAddr,
    payload_type: Option<Pt>,
    mid: Option<Mid>,
    ssrc: Option<Ssrc>,
    connected: bool,
    start_wallclock: Instant,
    local_ice_parameters: IceParameters,
    local_dtls_parameters: DtlsParameters,
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

impl FakeRtcPeer {
    pub async fn connect_publisher(
        transport: &TransportBootstrap,
        source: &FakeMediaSource,
    ) -> Option<Self> {
        let remote_addr = parse_remote_addr(transport)?;
        let remote_ice_credentials = parse_remote_ice_credentials(transport)?;
        let remote_fingerprint = parse_remote_fingerprint(transport)?;
        let socket = UdpSocket::bind(("127.0.0.1", 0)).await.ok()?;
        let local_addr = socket.local_addr().ok()?;

        let local_ice_credentials = IceCreds::new();
        let mut rtc = RtcConfig::new()
            .set_local_ice_credentials(local_ice_credentials.clone())
            .build(Instant::now());
        let local_candidate = Candidate::host(local_addr, "udp").ok()?;
        rtc.add_local_candidate(local_candidate)?;
        rtc.add_remote_candidate(Candidate::host(remote_addr, "udp").ok()?);

        let mid = Mid::default();
        let ssrc = Ssrc::from(source.primary_ssrc()?);
        {
            let mut direct_api = rtc.direct_api();
            direct_api.set_ice_controlling(true);
            direct_api.set_remote_ice_credentials(remote_ice_credentials);
            direct_api.set_remote_fingerprint(remote_fingerprint);
            direct_api.declare_media(mid, signaling_to_str0m_media_kind(source.media_kind()));
            direct_api.declare_stream_tx(ssrc, None, mid, None);
            direct_api.start_dtls(true).ok()?;
        }

        let payload_type = payload_type_for_source(&rtc, source)?;
        let local_dtls_parameters = local_dtls_parameters(&mut rtc)?;
        rtc.handle_input(Input::Timeout(Instant::now())).ok()?;

        Some(Self {
            rtc,
            socket,
            local_addr,
            payload_type: Some(payload_type),
            mid: Some(mid),
            ssrc: Some(ssrc),
            connected: false,
            start_wallclock: Instant::now(),
            local_ice_parameters: local_ice_parameters(&local_ice_credentials),
            local_dtls_parameters,
        })
    }

    pub async fn connect_subscriber(transport: &TransportBootstrap) -> Option<Self> {
        let remote_addr = parse_remote_addr(transport)?;
        let remote_ice_credentials = parse_remote_ice_credentials(transport)?;
        let remote_fingerprint = parse_remote_fingerprint(transport)?;
        let socket = UdpSocket::bind(("127.0.0.1", 0)).await.ok()?;
        let local_addr = socket.local_addr().ok()?;

        let local_ice_credentials = IceCreds::new();
        let mut rtc = RtcConfig::new()
            .set_local_ice_credentials(local_ice_credentials.clone())
            .build(Instant::now());
        let local_candidate = Candidate::host(local_addr, "udp").ok()?;
        rtc.add_local_candidate(local_candidate)?;
        rtc.add_remote_candidate(Candidate::host(remote_addr, "udp").ok()?);

        {
            let mut direct_api = rtc.direct_api();
            direct_api.set_ice_controlling(true);
            direct_api.set_remote_ice_credentials(remote_ice_credentials);
            direct_api.set_remote_fingerprint(remote_fingerprint);
            direct_api.start_dtls(true).ok()?;
        }

        let local_dtls_parameters = local_dtls_parameters(&mut rtc)?;
        rtc.handle_input(Input::Timeout(Instant::now())).ok()?;

        Some(Self {
            rtc,
            socket,
            local_addr,
            payload_type: None,
            mid: None,
            ssrc: None,
            connected: false,
            start_wallclock: Instant::now(),
            local_ice_parameters: local_ice_parameters(&local_ice_credentials),
            local_dtls_parameters,
        })
    }

    #[must_use]
    pub fn local_dtls_parameters(&self) -> &DtlsParameters {
        &self.local_dtls_parameters
    }

    #[must_use]
    pub fn local_ice_parameters(&self) -> &IceParameters {
        &self.local_ice_parameters
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

    pub fn expect_remote_track(
        &mut self,
        track: &CurrentRemoteTrackBootstrapPayload,
    ) -> Option<()> {
        let mid = track
            .rtp_parameters
            .0
            .get("mid")
            .and_then(serde_json::Value::as_str)
            .map_or_else(Mid::default, Mid::from);
        let ssrc = track
            .rtp_parameters
            .0
            .get("encodings")
            .and_then(serde_json::Value::as_array)
            .and_then(|encodings| encodings.first())
            .and_then(|encoding| encoding.get("ssrc"))
            .and_then(serde_json::Value::as_u64)
            .and_then(|raw| u32::try_from(raw).ok())
            .map(Ssrc::from)?;

        let has_media = self.rtc.media(mid).is_some();
        {
            let mut direct_api = self.rtc.direct_api();
            if !has_media {
                direct_api.declare_media(mid, signaling_to_str0m_media_kind(track.media_kind));
            }
            direct_api.expect_stream_rx(ssrc, None, mid, None);
        }
        Some(())
    }

    pub async fn send_frames(
        &mut self,
        source: &mut FakeMediaSource,
        clock: &mut FakeClock,
        frame_count: usize,
    ) -> Option<()> {
        for _ in 0..frame_count {
            let frame = source.next_frame(clock);
            self.write_frame(frame)?;
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

    pub async fn send_frame(
        &mut self,
        source: &mut FakeMediaSource,
        clock: &mut FakeClock,
    ) -> Option<Vec<u8>> {
        let frame = source.next_frame(clock);
        let expected_payload = frame.payload.clone();
        self.write_frame(frame)?;
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

    pub async fn read_media_frame(
        &mut self,
        timeout_window: Duration,
    ) -> Option<ReceivedMediaFrame> {
        pump_until_media(
            &mut self.rtc,
            &self.socket,
            self.local_addr,
            &mut self.connected,
            Instant::now() + timeout_window,
        )
        .await
    }

    fn write_frame(&mut self, frame: FakeMediaFrame) -> Option<()> {
        self.rtc
            .direct_api()
            .stream_tx(&self.ssrc?)?
            .write_rtp(
                self.payload_type?,
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

impl NativeFakeRtcPeer {
    pub async fn bind(port: u16) -> Option<Self> {
        let socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], port)))
            .await
            .ok()?;
        let local_addr = socket.local_addr().ok()?;
        let mut rtc = Rtc::new(Instant::now());
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

    pub async fn send_frames(
        &mut self,
        source: &mut FakeMediaSource,
        clock: &mut FakeClock,
        frame_count: usize,
    ) -> Option<()> {
        for _ in 0..frame_count {
            let frame = source.next_frame(clock);
            self.write_frame(source.media_kind(), frame)?;
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

    pub async fn send_frame(
        &mut self,
        source: &mut FakeMediaSource,
        clock: &mut FakeClock,
    ) -> Option<Vec<u8>> {
        let frame = source.next_frame(clock);
        let expected_payload = frame.payload.clone();
        self.write_frame(source.media_kind(), frame)?;
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

    pub async fn read_media_frame(
        &mut self,
        timeout_window: Duration,
    ) -> Option<ReceivedMediaFrame> {
        pump_until_media(
            &mut self.rtc,
            &self.socket,
            self.local_addr,
            &mut self.connected,
            Instant::now() + timeout_window,
        )
        .await
    }

    fn write_frame(&mut self, media_kind: SignalingMediaKind, frame: FakeMediaFrame) -> Option<()> {
        let send_path = *self.send_paths.get(&NativeMediaKey::from(media_kind))?;
        self.rtc
            .writer(send_path.mid)?
            .write(
                send_path.payload_type,
                self.start_wallclock + frame.emitted_at,
                frame.emitted_at.into(),
                frame.payload,
            )
            .ok()?;
        Some(())
    }
}

fn media_data_into_frame(data: MediaData) -> ReceivedMediaFrame {
    ReceivedMediaFrame {
        mid: data.mid.to_string(),
        payload: data.data,
    }
}

fn payload_type_for_source(rtc: &Rtc, source: &FakeMediaSource) -> Option<Pt> {
    let codec = match source.media_kind() {
        SignalingMediaKind::Audio => Codec::Opus,
        SignalingMediaKind::Video => Codec::Vp8,
    };
    rtc.codec_config()
        .find(|params| params.spec().codec == codec)
        .map(PayloadParams::pt)
}

fn local_dtls_parameters(rtc: &mut Rtc) -> Option<DtlsParameters> {
    let rendered = rtc.direct_api().local_dtls_fingerprint().to_string();
    let (algorithm, value) = rendered.split_once(' ')?;
    Some(DtlsParameters {
        role: String::from("client"),
        fingerprints: vec![DtlsFingerprint {
            algorithm: algorithm.to_owned(),
            value: value.to_owned(),
        }],
    })
}

fn local_ice_parameters(local_ice_credentials: &IceCreds) -> IceParameters {
    IceParameters(json!({
        "usernameFragment": local_ice_credentials.ufrag,
        "password": local_ice_credentials.pass
    }))
}

fn parse_remote_addr(transport: &TransportBootstrap) -> Option<SocketAddr> {
    let candidate = transport.ice_candidates.first()?;
    let port = u16::try_from(candidate.port).ok()?;
    let ip = candidate.ip.parse().ok()?;
    Some(SocketAddr::new(ip, port))
}

fn parse_remote_ice_credentials(transport: &TransportBootstrap) -> Option<IceCreds> {
    Some(IceCreds {
        ufrag: transport
            .ice_parameters
            .0
            .get("usernameFragment")?
            .as_str()?
            .to_owned(),
        pass: transport
            .ice_parameters
            .0
            .get("password")?
            .as_str()?
            .to_owned(),
    })
}

fn parse_remote_fingerprint(transport: &TransportBootstrap) -> Option<Fingerprint> {
    let fingerprint = transport.dtls_parameters.fingerprints.first()?;
    format!("{} {}", fingerprint.algorithm, fingerprint.value)
        .parse()
        .ok()
}

fn signaling_to_str0m_media_kind(kind: SignalingMediaKind) -> Str0mMediaKind {
    match kind {
        SignalingMediaKind::Audio => Str0mMediaKind::Audio,
        SignalingMediaKind::Video => Str0mMediaKind::Video,
    }
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

async fn pump_until_media(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    local_addr: SocketAddr,
    connected: &mut bool,
    deadline: Instant,
) -> Option<ReceivedMediaFrame> {
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
            Output::Event(Event::MediaData(data)) => return Some(media_data_into_frame(data)),
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
