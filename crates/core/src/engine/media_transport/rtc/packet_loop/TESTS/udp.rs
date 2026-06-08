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

fn run_udp_test(test: impl Future<Output = Result<(), &'static str>>) -> Result<(), &'static str> {
    Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|_error| "test runtime should build")?
        .block_on(test)
}
