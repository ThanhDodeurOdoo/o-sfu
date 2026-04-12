use serde::{Serialize, de::DeserializeOwned};
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};

use crate::{
    core::{NegotiationKind, ProtocolCore},
    host_bridge::{CoreSnapshot, cloned_track_binding, connection_state_tag, host_commands},
    shared::StreamType,
};

#[wasm_bindgen(js_name = ProtocolCoreWasm)]
pub struct WasmProtocolCore {
    inner: ProtocolCore,
}

#[wasm_bindgen(js_class = ProtocolCoreWasm)]
impl WasmProtocolCore {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: ProtocolCore::new(),
        }
    }

    #[wasm_bindgen(getter)]
    pub fn state(&self) -> String {
        connection_state_tag(self.inner.state()).to_owned()
    }

    #[wasm_bindgen(getter, js_name = features)]
    pub fn features_js(&self) -> Result<JsValue, JsValue> {
        to_js(self.inner.features())
    }

    #[wasm_bindgen(getter, js_name = recordingState)]
    pub fn recording_state_js(&self) -> Result<JsValue, JsValue> {
        to_js(self.inner.recording_state())
    }

    #[wasm_bindgen(js_name = snapshot)]
    pub fn snapshot_js(&self) -> Result<JsValue, JsValue> {
        to_js(&CoreSnapshot::from(&self.inner))
    }

    #[wasm_bindgen(js_name = trackBinding)]
    pub fn track_binding_js(&self, mid: String) -> Result<JsValue, JsValue> {
        to_js(&cloned_track_binding(&self.inner, &mid))
    }

    pub fn connect(
        &mut self,
        url: String,
        jwt: String,
        channel: Option<String>,
    ) -> Result<JsValue, JsValue> {
        to_js(&host_commands(self.inner.connect(url, jwt, channel)))
    }

    #[wasm_bindgen(js_name = onWsOpen)]
    pub fn on_ws_open(&mut self) -> Result<JsValue, JsValue> {
        to_js(&host_commands(self.inner.on_ws_open()))
    }

    #[wasm_bindgen(js_name = onWsMessage)]
    pub fn on_ws_message(&mut self, frame: String) -> Result<JsValue, JsValue> {
        to_js(&host_commands(self.inner.on_ws_message(&frame)))
    }

    #[wasm_bindgen(js_name = onTransportReady)]
    pub fn on_transport_ready(&mut self) -> Result<JsValue, JsValue> {
        to_js(&host_commands(self.inner.on_transport_ready()))
    }

    #[wasm_bindgen(js_name = onWsClose)]
    pub fn on_ws_close(&mut self, code: u16) -> Result<JsValue, JsValue> {
        to_js(&host_commands(self.inner.on_ws_close(code)))
    }

    #[wasm_bindgen(js_name = onTimer)]
    pub fn on_timer(&mut self, timer_id: u32) -> Result<JsValue, JsValue> {
        to_js(&host_commands(self.inner.on_timer(timer_id)))
    }

    #[wasm_bindgen(js_name = updateUpload)]
    pub fn update_upload(&mut self, stream_type: String, active: bool) -> Result<JsValue, JsValue> {
        let stream_type = parse_stream_type(&stream_type)?;
        to_js(&host_commands(
            self.inner.update_upload(stream_type, active),
        ))
    }

    #[wasm_bindgen(js_name = updateDownload)]
    pub fn update_download(
        &mut self,
        session_id: JsValue,
        states: JsValue,
    ) -> Result<JsValue, JsValue> {
        let session_id = from_js(session_id)?;
        let states = from_js(states)?;
        to_js(&host_commands(
            self.inner.update_download(session_id, states),
        ))
    }

    #[wasm_bindgen(js_name = updateInfo)]
    pub fn update_info(&mut self, info: JsValue) -> Result<JsValue, JsValue> {
        let info = from_js(info)?;
        to_js(&host_commands(self.inner.update_info(info)))
    }

    pub fn broadcast(&mut self, message: JsValue) -> Result<JsValue, JsValue> {
        let message = from_js(message)?;
        to_js(&host_commands(self.inner.broadcast(message)))
    }

    #[wasm_bindgen(js_name = startRecording)]
    pub fn start_recording(&mut self, options: Option<JsValue>) -> Result<JsValue, JsValue> {
        let options = from_optional_js(options)?;
        to_js(&host_commands(self.inner.start_recording(options)))
    }

    #[wasm_bindgen(js_name = stopRecording)]
    pub fn stop_recording(&mut self) -> Result<JsValue, JsValue> {
        to_js(&host_commands(self.inner.stop_recording()))
    }

    #[wasm_bindgen(js_name = submitNegotiationAnswer)]
    pub fn submit_negotiation_answer(
        &mut self,
        request_id: String,
        negotiation_kind: String,
        sdp: String,
    ) -> Result<JsValue, JsValue> {
        let kind = parse_negotiation_kind(&negotiation_kind)?;
        to_js(&host_commands(self.inner.submit_negotiation_answer(
            &crate::signaling::RequestId::new(request_id),
            kind,
            sdp,
        )))
    }

    pub fn disconnect(&mut self) -> Result<JsValue, JsValue> {
        to_js(&host_commands(self.inner.disconnect()))
    }
}

fn parse_stream_type(value: &str) -> Result<StreamType, JsValue> {
    match value {
        "audio" => Ok(StreamType::Audio),
        "camera" => Ok(StreamType::Camera),
        "screen" => Ok(StreamType::Screen),
        _ => Err(js_error(format!("invalid stream type: {value}"))),
    }
}

fn parse_negotiation_kind(value: &str) -> Result<NegotiationKind, JsValue> {
    match value {
        "offer" => Ok(NegotiationKind::Offer),
        "renegotiate" => Ok(NegotiationKind::Renegotiate),
        _ => Err(js_error(format!("invalid negotiation kind: {value}"))),
    }
}

fn to_js<T: Serialize>(value: &T) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(value).map_err(|error| js_error(error.to_string()))
}

fn from_js<T: DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value).map_err(|error| js_error(error.to_string()))
}

fn from_optional_js<T>(value: Option<JsValue>) -> Result<T, JsValue>
where
    T: Default + DeserializeOwned,
{
    let Some(value) = value else {
        return Ok(T::default());
    };
    if value.is_null() || value.is_undefined() {
        Ok(T::default())
    } else {
        from_js(value)
    }
}

fn js_error(message: impl Into<String>) -> JsValue {
    JsValue::from_str(&message.into())
}
