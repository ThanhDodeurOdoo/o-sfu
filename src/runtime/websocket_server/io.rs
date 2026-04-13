use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::SplitSink;

pub(crate) type WsWriter = SplitSink<WebSocket, Message>;
