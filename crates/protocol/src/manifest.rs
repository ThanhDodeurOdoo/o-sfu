use std::{
    collections::BTreeMap,
    error::Error,
    fs, io,
    path::{Path, PathBuf},
};

use o_sfu_model::{StopCode, StreamType};
use o_sfu_rfc::webrtc::MediaKind;
use serde::Serialize;

use crate::{
    host_bridge::{HOST_COMMAND_KINDS, HostNegotiationKind, HostPendingRequestKind},
    signaling::{
        ClientMessage, ClientRequest, ClientResponse, EnvelopeKind, EnvelopeSpec, ServerMessage,
        ServerRequest, ServerResponse, UploadLayerPolicyRole, WebSocketCloseCode,
    },
};

type ManifestResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProtocolManifest {
    stream_types: Vec<String>,
    upload_kinds: Vec<String>,
    source_encoding_policy_roles: Vec<String>,
    recording_stop_codes: Vec<String>,
    negotiation_kind: BTreeMap<&'static str, String>,
    pending_request_kind: BTreeMap<&'static str, String>,
    command_kind: BTreeMap<&'static str, &'static str>,
    ws_close_code: BTreeMap<&'static str, u16>,
    envelopes: EnvelopeGroups,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvelopeGroups {
    client_message: Vec<EnvelopeManifest>,
    client_request: Vec<EnvelopeManifest>,
    client_response: Vec<EnvelopeManifest>,
    server_message: Vec<EnvelopeManifest>,
    server_request: Vec<EnvelopeManifest>,
    server_response: Vec<EnvelopeManifest>,
}

#[derive(Debug, Serialize)]
struct EnvelopeManifest {
    kind: &'static str,
    tag: &'static str,
}

/// Return the ignored client protocol manifest path used by the repository npm scripts.
///
/// The path is derived from the protocol crate location so callers can run the
/// exporter from the workspace root or from `crates/client`.
#[must_use]
pub fn default_output_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let _ = path.pop();
    path.push("client/src/generated/protocol_manifest.json");
    path
}

/// Write the Rust-owned protocol manifest to `path`.
///
/// # Errors
///
/// Returns an error when the output directory cannot be created, when the file
/// cannot be written, or when a Rust serialization surface cannot be projected
/// into the manifest.
pub fn write_manifest(path: &Path) -> ManifestResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, manifest_json()?)?;
    Ok(())
}

fn manifest_json() -> ManifestResult<String> {
    let mut json = serde_json::to_string_pretty(&manifest()?)?;
    json.push('\n');
    Ok(json)
}

fn manifest() -> ManifestResult<ProtocolManifest> {
    Ok(ProtocolManifest {
        stream_types: string_values(&[StreamType::Audio, StreamType::Camera, StreamType::Screen])?,
        upload_kinds: string_values(&[MediaKind::Audio, MediaKind::Video])?,
        source_encoding_policy_roles: string_values(&[
            UploadLayerPolicyRole::Featured,
            UploadLayerPolicyRole::Thumbnail,
            UploadLayerPolicyRole::DegradedThumbnail,
        ])?,
        recording_stop_codes: string_values(&[
            StopCode::UserRequest,
            StopCode::ChannelClosed,
            StopCode::RecordingTimeout,
            StopCode::RecordingFailed,
            StopCode::DiskSpaceExhausted,
        ])?,
        negotiation_kind: string_object(&[
            ("OFFER", HostNegotiationKind::Offer),
            ("RENEGOTIATE", HostNegotiationKind::Renegotiate),
        ])?,
        pending_request_kind: string_object(&[
            ("START_RECORDING", HostPendingRequestKind::StartRecording),
            ("STOP_RECORDING", HostPendingRequestKind::StopRecording),
        ])?,
        command_kind: string_entries(HOST_COMMAND_KINDS),
        ws_close_code: number_entries(&[
            ("CLEAN", WebSocketCloseCode::Clean.into()),
            ("LEAVING", WebSocketCloseCode::Leaving.into()),
            ("PROTOCOL_ERROR", WebSocketCloseCode::ProtocolError.into()),
            ("ERROR", WebSocketCloseCode::Error.into()),
            ("AUTH_FAILED", WebSocketCloseCode::AuthFailed.into()),
            ("AUTH_TIMEOUT", WebSocketCloseCode::AuthTimeout.into()),
            ("KICKED", WebSocketCloseCode::Kicked.into()),
            ("CHANNEL_FULL", WebSocketCloseCode::RoomFull.into()),
        ]),
        envelopes: EnvelopeGroups {
            client_message: envelopes(ClientMessage::specs()),
            client_request: envelopes(ClientRequest::specs()),
            client_response: envelopes(ClientResponse::specs()),
            server_message: envelopes(ServerMessage::specs()),
            server_request: envelopes(ServerRequest::specs()),
            server_response: envelopes(ServerResponse::specs()),
        },
    })
}

fn envelopes(specs: impl IntoIterator<Item = EnvelopeSpec>) -> Vec<EnvelopeManifest> {
    specs
        .into_iter()
        .map(|spec| EnvelopeManifest {
            kind: envelope_kind(spec.kind()),
            tag: spec.tag(),
        })
        .collect()
}

fn envelope_kind(kind: EnvelopeKind) -> &'static str {
    match kind {
        EnvelopeKind::Message => "message",
        EnvelopeKind::Request => "request",
        EnvelopeKind::Response => "response",
    }
}

fn string_values<T: Serialize>(values: &[T]) -> ManifestResult<Vec<String>> {
    values
        .iter()
        .map(|value| json_string(value, "manifest string value"))
        .collect()
}

fn string_object<T: Serialize>(
    entries: &[(&'static str, T)],
) -> ManifestResult<BTreeMap<&'static str, String>> {
    entries
        .iter()
        .map(|(key, value)| json_string(value, key).map(|value| (*key, value)))
        .collect()
}

fn string_entries(
    entries: &[(&'static str, &'static str)],
) -> BTreeMap<&'static str, &'static str> {
    entries.iter().copied().collect()
}

fn number_entries(entries: &[(&'static str, u16)]) -> BTreeMap<&'static str, u16> {
    entries.iter().copied().collect()
}

fn json_string<T: Serialize + ?Sized>(value: &T, context: &str) -> ManifestResult<String> {
    match serde_json::to_value(value)? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(invalid_data(context, "did not serialize as a string")),
    }
}

fn invalid_data(context: &str, detail: &str) -> Box<dyn Error> {
    io::Error::new(io::ErrorKind::InvalidData, format!("{context} {detail}")).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_contains_envelope_catalog() -> ManifestResult<()> {
        let manifest = manifest()?;
        assert_eq!(
            manifest
                .envelopes
                .client_message
                .iter()
                .map(|envelope| envelope.tag)
                .collect::<Vec<_>>(),
            [
                "auth",
                "publish",
                "unpublish",
                "subscribe",
                "info",
                "broadcast"
            ]
        );
        assert_eq!(
            manifest
                .envelopes
                .server_response
                .iter()
                .map(|envelope| envelope.tag)
                .collect::<Vec<_>>(),
            ["startrecording", "stoprecording"]
        );
        Ok(())
    }

    #[test]
    fn manifest_contains_runtime_constants() -> ManifestResult<()> {
        let manifest = manifest()?;
        assert_eq!(
            manifest.command_kind.get("APPLY_NEGOTIATION"),
            Some(&"applyNegotiation")
        );
        assert_eq!(
            manifest.pending_request_kind.get("START_RECORDING"),
            Some(&"startRecording".to_owned())
        );
        assert_eq!(manifest.ws_close_code.get("AUTH_FAILED"), Some(&4106));
        Ok(())
    }
}
