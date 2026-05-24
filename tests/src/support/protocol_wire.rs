use futures_util::SinkExt;
use o_sfu_protocol::wire::{
    ClientEnvelope, ClientResponse, EnvelopeBatch, RequestId, ServerRequest,
};
use tokio_tungstenite::tungstenite;

use super::{TestWebSocket, fake_rtc_peer::FakeRtcPeer, read_text_message};

pub fn encode_client_batch(batch: Vec<ClientEnvelope>) -> Option<String> {
    let envelopes = batch
        .into_iter()
        .map(ClientEnvelope::into_envelope)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    serde_json::to_string(&envelopes).ok()
}

pub async fn read_protocol_batch(websocket: &mut TestWebSocket) -> Option<EnvelopeBatch> {
    serde_json::from_str(&read_text_message(websocket).await?).ok()
}

pub async fn send_server_request_response(
    websocket: &mut TestWebSocket,
    rtc_peer: &mut FakeRtcPeer,
    request_id: RequestId,
    request: ServerRequest,
) -> Option<()> {
    let response = match request {
        ServerRequest::Offer(payload) => {
            ClientResponse::Offer(rtc_peer.answer_offer(&payload.sdp)?)
        }
        ServerRequest::Renegotiate(payload) => {
            ClientResponse::Renegotiate(rtc_peer.answer_offer(&payload.sdp)?)
        }
    };
    websocket
        .send(tungstenite::Message::Text(
            encode_client_batch(vec![ClientEnvelope::Response {
                response_to: request_id,
                response,
            }])?
            .into(),
        ))
        .await
        .ok()
}
