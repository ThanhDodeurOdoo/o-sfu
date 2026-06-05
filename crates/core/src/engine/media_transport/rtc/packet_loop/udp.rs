//! UDP I/O boundary for the RTC packet loop.

#[cfg(target_os = "linux")]
use std::rc::Rc;
use std::{
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket},
    sync::Arc,
    time::Instant,
};

use tokio::{net::UdpSocket as TokioUdpSocket, sync::mpsc};
#[cfg(target_os = "linux")]
use tokio_uring::net::UdpSocket as TokioUringUdpSocket;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::buffers::RECEIVE_BUFFER_LEN;
use crate::RtcUdpIoBackend;

const INGRESS_QUEUE_CAPACITY: usize = 32;
const RECEIVE_BUFFER_POOL_CAPACITY: usize = 32;

/// Shared worker UDP socket.
#[derive(Clone)]
pub(in crate::engine::media_transport::rtc) enum RtcUdpSocket {
    Tokio(Arc<TokioUdpSocket>),
    #[cfg(target_os = "linux")]
    IoUring(Rc<TokioUringUdpSocket>),
}

/// Completed UDP datagram ready for one packet-loop turn.
pub(super) struct UdpDatagram {
    pub(super) source_addr: SocketAddr,
    pub(super) candidate_addr: SocketAddr,
    pub(super) received_at: Instant,
    pub(super) packet: Vec<u8>,
}

/// Receive-side datagram pump for one worker socket.
pub(in crate::engine::media_transport::rtc) struct UdpIngress {
    rx: mpsc::Receiver<UdpDatagram>,
    recycle_tx: mpsc::Sender<Vec<u8>>,
    shutdown: CancellationToken,
    wake_addr: SocketAddr,
}

#[cfg(feature = "internal-benchmarks")]
pub(in crate::engine::media_transport::rtc) struct UdpIngressBenchHarness {
    tx: mpsc::Sender<UdpDatagram>,
    ingress: UdpIngress,
    recycle_rx: mpsc::Receiver<Vec<u8>>,
}

impl RtcUdpSocket {
    pub(in crate::engine::media_transport::rtc) fn from_std(
        socket: StdUdpSocket,
        backend: RtcUdpIoBackend,
    ) -> io::Result<Self> {
        match backend {
            RtcUdpIoBackend::Tokio => TokioUdpSocket::from_std(socket)
                .map(Arc::new)
                .map(Self::Tokio),
            RtcUdpIoBackend::IoUring => {
                #[cfg(target_os = "linux")]
                {
                    Ok(Self::IoUring(Rc::new(TokioUringUdpSocket::from_std(
                        socket,
                    ))))
                }

                #[cfg(not(target_os = "linux"))]
                {
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "io_uring RTC UDP I/O backend is only supported on Linux",
                    ))
                }
            }
        }
    }

    pub(super) async fn send_to(
        &self,
        packet: Vec<u8>,
        destination: SocketAddr,
    ) -> io::Result<usize> {
        match self {
            Self::Tokio(socket) => socket.send_to(packet.as_slice(), destination).await,
            #[cfg(target_os = "linux")]
            Self::IoUring(socket) => {
                let (result, _packet) = socket.send_to(packet, destination).await;
                result
            }
        }
    }
}

impl UdpIngress {
    pub(in crate::engine::media_transport::rtc) fn new(
        socket: RtcUdpSocket,
        bind_addr: SocketAddr,
        candidate_addr: SocketAddr,
    ) -> Self {
        let (tx, rx) = mpsc::channel(INGRESS_QUEUE_CAPACITY);
        let (recycle_tx, recycle_rx) = mpsc::channel(RECEIVE_BUFFER_POOL_CAPACITY);
        let shutdown = CancellationToken::new();
        let wake_addr = udp_wake_addr(bind_addr);
        spawn_ingress(socket, candidate_addr, tx, recycle_rx, shutdown.clone());
        Self {
            rx,
            recycle_tx,
            shutdown,
            wake_addr,
        }
    }

    pub(super) fn try_recv(&mut self) -> Option<UdpDatagram> {
        self.rx.try_recv().ok()
    }

    pub(super) async fn recv(&mut self) -> Option<UdpDatagram> {
        self.rx.recv().await
    }

    pub(super) fn recycle(&self, mut packet: Vec<u8>) {
        if packet.capacity() < RECEIVE_BUFFER_LEN {
            return;
        }
        packet.clear();
        let _ = self.recycle_tx.try_send(packet);
    }
}

#[cfg(feature = "internal-benchmarks")]
impl UdpIngressBenchHarness {
    pub(in crate::engine::media_transport::rtc) fn new(wake_addr: SocketAddr) -> Self {
        let (tx, rx) = mpsc::channel(INGRESS_QUEUE_CAPACITY);
        let (recycle_tx, recycle_rx) = mpsc::channel(RECEIVE_BUFFER_POOL_CAPACITY);
        let ingress = UdpIngress {
            rx,
            recycle_tx,
            shutdown: CancellationToken::new(),
            wake_addr,
        };
        Self {
            tx,
            ingress,
            recycle_rx,
        }
    }

    pub(in crate::engine::media_transport::rtc) fn enqueue_completed_datagram(
        &mut self,
        source_addr: SocketAddr,
        candidate_addr: SocketAddr,
        received_at: Instant,
        payload: &[u8],
    ) -> bool {
        let mut packet = receive_buffer(&mut self.recycle_rx);
        packet.extend_from_slice(payload);
        self.tx
            .try_send(UdpDatagram {
                source_addr,
                candidate_addr,
                received_at,
                packet,
            })
            .is_ok()
    }

    pub(in crate::engine::media_transport::rtc) fn ingress_mut(&mut self) -> &mut UdpIngress {
        &mut self.ingress
    }
}

impl Drop for UdpIngress {
    fn drop(&mut self) {
        self.shutdown.cancel();
        wake_udp_receiver(self.wake_addr);
    }
}

fn spawn_ingress(
    socket: RtcUdpSocket,
    candidate_addr: SocketAddr,
    tx: mpsc::Sender<UdpDatagram>,
    recycle_rx: mpsc::Receiver<Vec<u8>>,
    shutdown: CancellationToken,
) {
    match socket {
        RtcUdpSocket::Tokio(socket) => {
            tokio::spawn(run_tokio_ingress(
                socket,
                candidate_addr,
                tx,
                recycle_rx,
                shutdown,
            ));
        }
        #[cfg(target_os = "linux")]
        RtcUdpSocket::IoUring(socket) => {
            tokio_uring::spawn(run_io_uring_ingress(
                socket,
                candidate_addr,
                tx,
                recycle_rx,
                shutdown,
            ));
        }
    }
}

async fn run_tokio_ingress(
    socket: Arc<TokioUdpSocket>,
    candidate_addr: SocketAddr,
    tx: mpsc::Sender<UdpDatagram>,
    mut recycle_rx: mpsc::Receiver<Vec<u8>>,
    shutdown: CancellationToken,
) {
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        let mut packet = receive_buffer(&mut recycle_rx);
        let result = socket.recv_buf_from(&mut packet).await;
        let received_at = Instant::now();
        if shutdown.is_cancelled() {
            return;
        }
        if ingress_should_stop(result, packet, candidate_addr, received_at, &tx, &shutdown).await {
            return;
        }
    }
}

#[cfg(target_os = "linux")]
async fn run_io_uring_ingress(
    socket: Rc<TokioUringUdpSocket>,
    candidate_addr: SocketAddr,
    tx: mpsc::Sender<UdpDatagram>,
    mut recycle_rx: mpsc::Receiver<Vec<u8>>,
    shutdown: CancellationToken,
) {
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        let buffer = receive_buffer(&mut recycle_rx);
        let (result, packet) = socket.recv_from(buffer).await;
        let received_at = Instant::now();
        if shutdown.is_cancelled() {
            return;
        }
        if ingress_should_stop(result, packet, candidate_addr, received_at, &tx, &shutdown).await {
            return;
        }
    }
}

async fn ingress_should_stop(
    result: io::Result<(usize, SocketAddr)>,
    mut packet: Vec<u8>,
    candidate_addr: SocketAddr,
    received_at: Instant,
    tx: &mpsc::Sender<UdpDatagram>,
    shutdown: &CancellationToken,
) -> bool {
    match result {
        Ok((received_size, source_addr)) => {
            packet.truncate(received_size);
            let datagram = UdpDatagram {
                source_addr,
                candidate_addr,
                received_at,
                packet,
            };
            tokio::select! {
                biased;
                () = shutdown.cancelled() => true,
                send_result = tx.send(datagram) => send_result.is_err(),
            }
        }
        Err(error) => {
            warn!(?error, "rtc packet loop failed to receive datagram");
            false
        }
    }
}

fn udp_wake_addr(bind_addr: SocketAddr) -> SocketAddr {
    match bind_addr {
        SocketAddr::V4(addr) if addr.ip().is_unspecified() => {
            SocketAddr::from((Ipv4Addr::LOCALHOST, addr.port()))
        }
        SocketAddr::V6(addr) if addr.ip().is_unspecified() => {
            SocketAddr::from((Ipv6Addr::LOCALHOST, addr.port()))
        }
        addr => addr,
    }
}

fn wake_udp_receiver(addr: SocketAddr) {
    let bind_addr = match addr {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    };
    let Ok(socket) = StdUdpSocket::bind(bind_addr) else {
        return;
    };
    let _ = socket.send_to(&[], addr);
}

fn receive_buffer(recycle_rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<u8> {
    let mut buffer = recycle_rx
        .try_recv()
        .unwrap_or_else(|_| Vec::with_capacity(RECEIVE_BUFFER_LEN));
    buffer.clear();
    buffer
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        net::{SocketAddr, UdpSocket as StdUdpSocket},
        time::Duration,
    };

    use tokio::{runtime::Builder, time::timeout};

    use super::*;

    #[test]
    fn udp_ingress_delivers_received_datagrams() -> Result<(), &'static str> {
        run_udp_test(async {
            let socket = bind_std_socket()?;
            let socket_addr = socket
                .local_addr()
                .map_err(|_error| "socket should have a local addr")?;
            let socket = RtcUdpSocket::from_std(socket, RtcUdpIoBackend::Tokio)
                .map_err(|_error| "rtc UDP socket should convert")?;
            let mut ingress = UdpIngress::new(socket, socket_addr, socket_addr);
            let sender = StdUdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .map_err(|_error| "sender socket should bind")?;
            let sender_addr = sender
                .local_addr()
                .map_err(|_error| "sender should have a local addr")?;

            sender
                .send_to(b"packet", socket_addr)
                .map_err(|_error| "sender should send the datagram")?;

            let datagram = timeout(Duration::from_secs(1), ingress.recv())
                .await
                .map_err(|_error| "ingress should receive before timeout")?
                .ok_or("ingress should still be open")?;

            assert_eq!(datagram.packet.as_slice(), b"packet");
            assert_eq!(datagram.source_addr, sender_addr);
            assert_eq!(datagram.candidate_addr, socket_addr);
            assert!(datagram.received_at <= Instant::now());
            Ok(())
        })
    }

    #[test]
    fn udp_socket_sends_owned_datagram_buffers() -> Result<(), &'static str> {
        run_udp_test(async {
            let receiver = bind_std_socket()?;
            let receiver_addr = receiver
                .local_addr()
                .map_err(|_error| "receiver socket should have a local addr")?;
            let receiver = RtcUdpSocket::from_std(receiver, RtcUdpIoBackend::Tokio)
                .map_err(|_error| "receiver should convert")?;
            let mut ingress = UdpIngress::new(receiver, receiver_addr, receiver_addr);
            let sender = RtcUdpSocket::from_std(bind_std_socket()?, RtcUdpIoBackend::Tokio)
                .map_err(|_error| "rtc UDP socket should convert")?;

            let sent_len = sender
                .send_to(b"packet".to_vec(), receiver_addr)
                .await
                .map_err(|_error| "send should succeed")?;

            assert_eq!(sent_len, b"packet".len());

            let datagram = timeout(Duration::from_secs(1), ingress.recv())
                .await
                .map_err(|_error| "receiver should get the sent packet before timeout")?
                .ok_or("receiver should read the sent packet")?;

            assert_eq!(datagram.packet.as_slice(), b"packet");
            Ok(())
        })
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn io_uring_udp_ingress_delivers_received_datagrams() -> Result<(), &'static str> {
        let socket = bind_std_socket()?;
        let socket_addr = socket
            .local_addr()
            .map_err(|_error| "socket should have a local addr")?;
        let sender = StdUdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .map_err(|_error| "sender socket should bind")?;
        let sender_addr = sender
            .local_addr()
            .map_err(|_error| "sender should have a local addr")?;
        sender
            .send_to(b"packet", socket_addr)
            .map_err(|_error| "sender should send the datagram")?;

        tokio_uring::start(async {
            let socket = RtcUdpSocket::from_std(socket, RtcUdpIoBackend::IoUring)
                .map_err(|_error| "io_uring receiver should convert")?;
            let mut ingress = UdpIngress::new(socket, socket_addr, socket_addr);

            let datagram = timeout(Duration::from_secs(1), ingress.recv())
                .await
                .map_err(|_error| "io_uring receiver should receive before timeout")?
                .ok_or("io_uring receiver should read the sent packet")?;

            assert_eq!(datagram.packet.as_slice(), b"packet");
            assert_eq!(datagram.source_addr, sender_addr);
            assert_eq!(datagram.candidate_addr, socket_addr);
            assert!(datagram.received_at <= Instant::now());
            Ok(())
        })
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn io_uring_udp_socket_sends_owned_datagram_buffers() -> Result<(), &'static str> {
        let receiver = StdUdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .map_err(|_error| "receiver socket should bind")?;
        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .map_err(|_error| "receiver socket should set a read timeout")?;
        let receiver_addr = receiver
            .local_addr()
            .map_err(|_error| "receiver socket should have a local addr")?;
        let sender = bind_std_socket()?;

        let sent_len = tokio_uring::start(async {
            let sender = RtcUdpSocket::from_std(sender, RtcUdpIoBackend::IoUring)
                .map_err(|_error| "io_uring sender should convert")?;
            sender
                .send_to(b"packet".to_vec(), receiver_addr)
                .await
                .map_err(|_error| "io_uring send should succeed")
        })?;

        assert_eq!(sent_len, b"packet".len());

        let mut packet = [0; RECEIVE_BUFFER_LEN];
        let (size, _source_addr) = receiver
            .recv_from(&mut packet)
            .map_err(|_error| "receiver should get the sent packet before timeout")?;

        assert_eq!(
            packet
                .get(..size)
                .ok_or("receiver size should stay within packet buffer")?,
            b"packet"
        );
        Ok(())
    }

    fn bind_std_socket() -> Result<StdUdpSocket, &'static str> {
        let socket = StdUdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .map_err(|_error| "UDP socket should bind")?;
        socket
            .set_nonblocking(true)
            .map_err(|_error| "UDP socket should become nonblocking")?;
        Ok(socket)
    }

    fn run_udp_test(
        test: impl Future<Output = Result<(), &'static str>>,
    ) -> Result<(), &'static str> {
        Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|_error| "test runtime should build")?
            .block_on(test)
    }
}
