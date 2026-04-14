#![allow(
    dead_code,
    reason = "the str0m-backed fake RTC peer is introduced incrementally as phase-8 media-path scenarios are added"
)]

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use serde_json::json;
use tokio::{net::UdpSocket, time::timeout};

use o_sfu::{
    runtime::testing::legacy_wire::current_protocol::CurrentRemoteTrackBootstrapPayload,
    signaling::webrtc::{
        DtlsFingerprint, DtlsParameters, IceParameters, MediaKind as SignalingMediaKind,
        TransportBootstrap,
    },
};
use str0m::{
    Candidate, Event, IceConnectionState, IceCreds, Input, Output, Rtc, RtcConfig,
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
        if self.pump_until(deadline, true).await? {
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
            self.pump_until(Instant::now() + IO_SLICE, false).await?;
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
        self.pump_until(Instant::now() + IO_SLICE, false).await?;
        Some(expected_payload)
    }

    pub async fn read_media_frame(
        &mut self,
        timeout_window: Duration,
    ) -> Option<ReceivedMediaFrame> {
        self.pump_until_media(Instant::now() + timeout_window).await
    }

    async fn pump_until(&mut self, deadline: Instant, stop_on_connected: bool) -> Option<bool> {
        let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
        while Instant::now() < deadline {
            let now = Instant::now();
            match self.rtc.poll_output().ok()? {
                Output::Transmit(transmit) => {
                    self.socket
                        .send_to(&transmit.contents, transmit.destination)
                        .await
                        .ok()?;
                }
                Output::Event(Event::Connected) => {
                    self.connected = true;
                    if stop_on_connected {
                        return Some(true);
                    }
                }
                Output::Event(Event::IceConnectionStateChange(state)) => match state {
                    IceConnectionState::Connected | IceConnectionState::Completed => {
                        self.connected = true;
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
                        self.rtc.handle_input(Input::Timeout(now)).ok()?;
                        continue;
                    }

                    let wait_duration = timeout_at
                        .saturating_duration_since(now)
                        .min(MAX_SOCKET_WAIT)
                        .min(deadline.saturating_duration_since(now));
                    if wait_duration.is_zero() {
                        self.rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
                        continue;
                    }

                    match timeout(wait_duration, self.socket.recv_from(&mut receive_buffer)).await {
                        Ok(Ok((received_size, source_addr))) => {
                            if received_size == 0 {
                                continue;
                            }
                            let packet = receive_buffer.get(..received_size)?;
                            let receive = Receive {
                                proto: Protocol::Udp,
                                source: source_addr,
                                destination: self.local_addr,
                                contents: packet.try_into().ok()?,
                            };
                            self.rtc
                                .handle_input(Input::Receive(Instant::now(), receive))
                                .ok()?;
                        }
                        Ok(Err(_error)) => return None,
                        Err(_elapsed) => {
                            self.rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
                        }
                    }
                }
            }
        }
        Some(self.connected)
    }

    async fn pump_until_media(&mut self, deadline: Instant) -> Option<ReceivedMediaFrame> {
        let mut receive_buffer = [0_u8; RECEIVE_BUFFER_LEN];
        while Instant::now() < deadline {
            let now = Instant::now();
            match self.rtc.poll_output().ok()? {
                Output::Transmit(transmit) => {
                    self.socket
                        .send_to(&transmit.contents, transmit.destination)
                        .await
                        .ok()?;
                }
                Output::Event(Event::MediaData(data)) => return Some(media_data_into_frame(data)),
                Output::Event(Event::Connected) => {
                    self.connected = true;
                }
                Output::Event(Event::IceConnectionStateChange(state)) => match state {
                    IceConnectionState::Connected | IceConnectionState::Completed => {
                        self.connected = true;
                    }
                    IceConnectionState::Disconnected => return None,
                    _ => {}
                },
                Output::Event(_) => {}
                Output::Timeout(timeout_at) => {
                    if timeout_at <= now {
                        self.rtc.handle_input(Input::Timeout(now)).ok()?;
                        continue;
                    }

                    let wait_duration = timeout_at
                        .saturating_duration_since(now)
                        .min(MAX_SOCKET_WAIT)
                        .min(deadline.saturating_duration_since(now));
                    if wait_duration.is_zero() {
                        self.rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
                        continue;
                    }

                    match timeout(wait_duration, self.socket.recv_from(&mut receive_buffer)).await {
                        Ok(Ok((received_size, source_addr))) => {
                            if received_size == 0 {
                                continue;
                            }
                            let packet = receive_buffer.get(..received_size)?;
                            let receive = Receive {
                                proto: Protocol::Udp,
                                source: source_addr,
                                destination: self.local_addr,
                                contents: packet.try_into().ok()?,
                            };
                            self.rtc
                                .handle_input(Input::Receive(Instant::now(), receive))
                                .ok()?;
                        }
                        Ok(Err(_error)) => return None,
                        Err(_elapsed) => {
                            self.rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
                        }
                    }
                }
            }
        }
        None
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
