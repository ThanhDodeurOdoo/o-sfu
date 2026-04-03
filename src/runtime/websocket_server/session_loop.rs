use futures_util::StreamExt;
use tokio::sync::mpsc;

use super::{WsReader, close_writer};
use crate::runtime::{
    channel::SessionOutbound,
    stub_bus::{StubBusOutcome, StubBusSession, WsWriter, send_server_message_batch},
};

pub(super) async fn run(
    writer: &mut WsWriter,
    reader: &mut WsReader,
    outbound_rx: &mut mpsc::UnboundedReceiver<SessionOutbound>,
    stub_bus: &mut StubBusSession,
) {
    loop {
        tokio::select! {
            message = reader.next() => {
                match message {
                    Some(Ok(message)) => {
                        match stub_bus.handle_frame(writer, message).await {
                            StubBusOutcome::Continue => {}
                            StubBusOutcome::Break => break,
                            StubBusOutcome::Close(code) => {
                                close_writer(writer, code).await;
                                break;
                            }
                        }
                    }
                    Some(Err(_error)) => break,
                    None => break,
                }
            }
            outbound = outbound_rx.recv() => {
                match outbound {
                    Some(SessionOutbound::Message(message)) => {
                        if send_server_message_batch(writer, &message).await.is_err() {
                            break;
                        }
                    }
                    Some(SessionOutbound::Close(code)) => {
                        close_writer(writer, code).await;
                        break;
                    }
                    None => break,
                }
            }
        }
    }
}
