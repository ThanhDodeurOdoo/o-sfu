use serde::{Serialize, de::DeserializeOwned};
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};

use crate::{
    core::{NegotiationKind, ProtocolCore},
    host_bridge::{CoreSnapshot, connection_state_tag, project_commands, project_request_result},
    shared::StreamType,
};

/// wasm-bindgen facade for the browser [`ProtocolCore`] contract
///
/// each method returns projected host commands as plain JS objects so the TS
/// runtime can validate and execute side effects outside [`ProtocolCore`]
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

    pub fn connect(
        &mut self,
        url: String,
        jwt: String,
        room: Option<String>,
    ) -> Result<JsValue, JsValue> {
        to_js(&project_commands(self.inner.connect(url, jwt, room)))
    }

    #[wasm_bindgen(js_name = onWsOpen)]
    pub fn on_ws_open(&mut self) -> Result<JsValue, JsValue> {
        to_js(&project_commands(self.inner.on_ws_open()))
    }

    #[wasm_bindgen(js_name = onWsMessage)]
    pub fn on_ws_message(&mut self, frame: String) -> Result<JsValue, JsValue> {
        to_js(&project_commands(self.inner.on_ws_message(&frame)))
    }

    #[wasm_bindgen(js_name = onTransportReady)]
    pub fn on_transport_ready(&mut self) -> Result<JsValue, JsValue> {
        to_js(&project_commands(self.inner.on_transport_ready()))
    }

    #[wasm_bindgen(js_name = onWsClose)]
    pub fn on_ws_close(&mut self, code: u16) -> Result<JsValue, JsValue> {
        to_js(&project_commands(self.inner.on_ws_close(code)))
    }

    #[wasm_bindgen(js_name = onTimer)]
    pub fn on_timer(&mut self, timer_id: u32) -> Result<JsValue, JsValue> {
        to_js(&project_commands(self.inner.on_timer(timer_id)))
    }

    pub fn publish(&mut self, stream_type: &str, active: bool) -> Result<JsValue, JsValue> {
        let stream_type: StreamType = from_js(JsValue::from_str(stream_type))?;
        to_js(&project_commands(self.inner.publish(stream_type, active)))
    }

    pub fn subscribe(&mut self, user_id: JsValue, states: JsValue) -> Result<JsValue, JsValue> {
        let user_id = from_js(user_id)?;
        let states = from_js(states)?;
        to_js(&project_commands(self.inner.subscribe(user_id, states)))
    }

    #[wasm_bindgen(js_name = updateInfo)]
    pub fn update_info(&mut self, info: JsValue) -> Result<JsValue, JsValue> {
        let info = from_js(info)?;
        to_js(&project_commands(self.inner.update_info(info)))
    }

    pub fn broadcast(&mut self, message: JsValue) -> Result<JsValue, JsValue> {
        let message = from_js(message)?;
        to_js(&project_commands(self.inner.broadcast(message)))
    }

    #[wasm_bindgen(js_name = startRecording)]
    pub fn start_recording(&mut self, options: Option<JsValue>) -> Result<JsValue, JsValue> {
        let options = from_optional_js(options)?;
        to_js(&project_request_result(self.inner.start_recording(options)))
    }

    #[wasm_bindgen(js_name = stopRecording)]
    pub fn stop_recording(&mut self) -> Result<JsValue, JsValue> {
        to_js(&project_request_result(self.inner.stop_recording()))
    }

    #[wasm_bindgen(js_name = submitNegotiationAnswer)]
    pub fn submit_negotiation_answer(
        &mut self,
        request_id: String,
        negotiation_kind: &str,
        sdp: String,
    ) -> Result<JsValue, JsValue> {
        let kind: NegotiationKind = from_js(JsValue::from_str(negotiation_kind))?;
        to_js(&project_commands(self.inner.submit_negotiation_answer(
            &crate::signaling::RequestId::new(request_id),
            kind,
            sdp,
        )))
    }

    pub fn disconnect(&mut self) -> Result<JsValue, JsValue> {
        to_js(&project_commands(self.inner.disconnect()))
    }
}

fn to_js<T: Serialize>(value: &T) -> Result<JsValue, JsValue> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true))
        .map_err(|error| js_error(error.to_string()))
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
