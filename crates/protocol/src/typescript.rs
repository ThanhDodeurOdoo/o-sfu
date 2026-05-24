use std::{
    error::Error,
    fmt::{self, Write as _},
    fs, io,
    path::{Path, PathBuf},
};

use o_sfu_model::{
    AvailableFeatures, DownloadStates, PeerSnapshot, RecordingOptions, RecordingState,
    RecordingStateUpdate, StopCode, StreamType, UserId, UserInfo, VideoLayoutIntent,
};
use o_sfu_rfc::webrtc::MediaKind;
use serde::Serialize;
use ts_rs::{Config, TS};

use crate::{
    host_bridge::{HOST_COMMAND_KINDS, HostNegotiationKind, HostPendingRequestKind},
    signaling::{
        AuthPayload, CLIENT_MESSAGE_ENVELOPES, CLIENT_REQUEST_ENVELOPES, CLIENT_RESPONSE_ENVELOPES,
        ClientBroadcastPayload, EnvelopeKind, EnvelopeSpec, NegotiationUploadEncoding,
        NegotiationUploadSlot, PeerInfoPayload, PeerLeftPayload, RecordingActionResult,
        SERVER_MESSAGE_ENVELOPES, SERVER_REQUEST_ENVELOPES, SERVER_RESPONSE_ENVELOPES,
        ServerBroadcastPayload, SessionDescriptionPayload, SourceDescriptor,
        SourceEncodingDescriptor, StreamIntentPayload, SubscribePayload, TrackBinding,
        UploadLayerPolicyRole, WIRE_TAGS, WebSocketCloseCode, WelcomePayload,
    },
};

type ExportResult<T> = Result<T, Box<dyn Error>>;

/// Return the ignored client contract path used by the repository npm scripts.
///
/// The path is derived from the protocol crate location so callers can run the
/// exporter from the workspace root or from `crates/client`.
#[must_use]
pub fn default_output_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let _ = path.pop();
    path.push("client/src/generated/protocol_contract.ts");
    path
}

/// Write the generated TypeScript protocol contract to `path`.
///
/// # Errors
///
/// Returns an error when the output directory cannot be created, when the file
/// cannot be written, or when a Rust serialization surface cannot be projected
/// into the literal catalog expected by the TypeScript contract.
pub fn write_contract(path: &Path) -> ExportResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contract()?)?;
    Ok(())
}

fn contract() -> ExportResult<String> {
    let mut output = String::new();
    push_types(&mut output);
    push_constants(&mut output)?;
    push_envelopes(&mut output)?;
    Ok(output)
}

fn push_types(output: &mut String) {
    let config = Config::default().with_large_int("number");
    push_decl::<UserId>(output, &config);
    push_decl::<AvailableFeatures>(output, &config);
    push_decl::<RecordingState>(output, &config);
    push_decl::<StopCode>(output, &config);
    push_decl::<RecordingStateUpdate>(output, &config);
    push_decl::<UserInfo>(output, &config);
    push_decl::<PeerSnapshot>(output, &config);
    push_decl::<DownloadStates>(output, &config);
    push_decl::<VideoLayoutIntent>(output, &config);
    push_decl::<StreamType>(output, &config);
    push_decl::<RecordingOptions>(output, &config);
    push_decl::<MediaKind>(output, &config);
    push_decl::<AuthPayload>(output, &config);
    push_decl::<WelcomePayload>(output, &config);
    push_decl::<SessionDescriptionPayload>(output, &config);
    push_decl::<NegotiationUploadSlot>(output, &config);
    push_decl::<NegotiationUploadEncoding>(output, &config);
    push_decl::<UploadLayerPolicyRole>(output, &config);
    push_decl::<StreamIntentPayload>(output, &config);
    push_decl::<SubscribePayload>(output, &config);
    push_decl::<TrackBinding>(output, &config);
    push_decl::<SourceDescriptor>(output, &config);
    push_decl::<SourceEncodingDescriptor>(output, &config);
    push_decl::<PeerInfoPayload>(output, &config);
    push_decl::<PeerLeftPayload>(output, &config);
    push_decl::<ClientBroadcastPayload>(output, &config);
    push_decl::<ServerBroadcastPayload>(output, &config);
    push_decl::<RecordingActionResult>(output, &config);
    output.push_str("export type RequestId = string;\n");
    output.push_str("export type RecordingChangePayload = RecordingStateUpdate;\n\n");
}

fn push_decl<T: TS>(output: &mut String, config: &Config) {
    output.push_str("export ");
    output.push_str(&T::decl(config));
    output.push_str("\n\n");
}

fn push_constants(output: &mut String) -> ExportResult<()> {
    push_serialized_string_array(
        output,
        "STREAM_TYPES",
        &[StreamType::Audio, StreamType::Camera, StreamType::Screen],
    )?;
    push_serialized_string_array(
        output,
        "UPLOAD_KINDS",
        &[MediaKind::Audio, MediaKind::Video],
    )?;
    push_serialized_string_array(
        output,
        "SOURCE_ENCODING_POLICY_ROLES",
        &[
            UploadLayerPolicyRole::Featured,
            UploadLayerPolicyRole::Thumbnail,
            UploadLayerPolicyRole::DegradedThumbnail,
        ],
    )?;
    push_serialized_string_array(
        output,
        "RECORDING_STOP_CODES",
        &[
            StopCode::UserRequest,
            StopCode::ChannelClosed,
            StopCode::RecordingTimeout,
            StopCode::RecordingFailed,
            StopCode::DiskSpaceExhausted,
        ],
    )?;
    push_serialized_string_object(
        output,
        "NEGOTIATION_KIND",
        &[
            ("OFFER", HostNegotiationKind::Offer),
            ("RENEGOTIATE", HostNegotiationKind::Renegotiate),
        ],
    )?;
    push_serialized_string_object(
        output,
        "PENDING_REQUEST_KIND",
        &[
            ("START_RECORDING", HostPendingRequestKind::StartRecording),
            ("STOP_RECORDING", HostPendingRequestKind::StopRecording),
        ],
    )?;
    push_string_object(output, "COMMAND_KIND", HOST_COMMAND_KINDS)?;
    push_number_object(
        output,
        "WS_CLOSE_CODE",
        &[
            ("CLEAN", WebSocketCloseCode::Clean.into()),
            ("LEAVING", WebSocketCloseCode::Leaving.into()),
            ("PROTOCOL_ERROR", WebSocketCloseCode::ProtocolError.into()),
            ("ERROR", WebSocketCloseCode::Error.into()),
            ("AUTH_FAILED", WebSocketCloseCode::AuthFailed.into()),
            ("AUTH_TIMEOUT", WebSocketCloseCode::AuthTimeout.into()),
            ("KICKED", WebSocketCloseCode::Kicked.into()),
            ("CHANNEL_FULL", WebSocketCloseCode::RoomFull.into()),
        ],
    )?;
    push_string_object(output, "WIRE_TAG", WIRE_TAGS)?;
    Ok(())
}

fn push_envelopes(output: &mut String) -> ExportResult<()> {
    push_envelope_base(output);
    push_envelope_union(output, "ClientMessageEnvelope", CLIENT_MESSAGE_ENVELOPES)?;
    push_envelope_union(output, "ClientRequestEnvelope", CLIENT_REQUEST_ENVELOPES)?;
    push_envelope_union(output, "ClientResponseEnvelope", CLIENT_RESPONSE_ENVELOPES)?;
    push_named_union(
        output,
        "ClientOutboundEnvelope",
        [
            "ClientMessageEnvelope",
            "ClientRequestEnvelope",
            "ClientResponseEnvelope",
        ],
    )?;
    push_envelope_union(output, "ServerMessageEnvelope", SERVER_MESSAGE_ENVELOPES)?;
    push_envelope_union(output, "ServerRequestEnvelope", SERVER_REQUEST_ENVELOPES)?;
    push_envelope_union(output, "ServerResponseEnvelope", SERVER_RESPONSE_ENVELOPES)?;
    push_named_union(
        output,
        "ServerOutboundEnvelope",
        [
            "ServerMessageEnvelope",
            "ServerRequestEnvelope",
            "ServerResponseEnvelope",
        ],
    )?;
    push_named_union(
        output,
        "Envelope",
        [
            "ClientOutboundEnvelope",
            "ServerOutboundEnvelope",
            "RequestEnvelope<string, unknown>",
            "ResponseEnvelope<string, unknown>",
        ],
    )?;
    output.push_str("export type EnvelopeBatch = Envelope[];\n");
    Ok(())
}

fn push_envelope_base(output: &mut String) {
    output.push_str(
        "export interface MessageEnvelope<TType extends string, TPayload> {
    t: TType;
    p: TPayload;
}

export interface RequestEnvelope<TType extends string, TPayload = undefined> {
    t: TType;
    q: RequestId;
    p?: TPayload;
}

export interface ResponseEnvelope<TType extends string, TPayload = undefined> {
    t: TType;
    r: RequestId;
    p?: TPayload;
}

",
    );
}

fn push_envelope_union(
    output: &mut String,
    name: &str,
    specs: &[EnvelopeSpec],
) -> ExportResult<()> {
    writeln!(output, "export type {name} =")?;
    for spec in specs {
        writeln!(output, "    | {}", envelope_variant(*spec)?)?;
    }
    output.push_str(";\n\n");
    Ok(())
}

fn envelope_variant(spec: EnvelopeSpec) -> ExportResult<String> {
    let envelope = match spec.kind {
        EnvelopeKind::Message => "MessageEnvelope",
        EnvelopeKind::Request => "RequestEnvelope",
        EnvelopeKind::Response => "ResponseEnvelope",
    };
    let tag = ts_string(spec.tag)?;
    let payload = match spec.payload {
        Some(payload) => format!(", {payload}"),
        None if spec.kind == EnvelopeKind::Message => {
            return Err(invalid_data(spec.tag, "message envelope has no payload"));
        }
        None => String::new(),
    };
    Ok(format!("{envelope}<{tag}{payload}>"))
}

fn push_named_union<I, S>(output: &mut String, name: &str, variants: I) -> fmt::Result
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    writeln!(output, "export type {name} =")?;
    for variant in variants {
        writeln!(output, "    | {}", variant.as_ref())?;
    }
    output.push_str(";\n\n");
    Ok(())
}

fn push_serialized_string_array<T: Serialize>(
    output: &mut String,
    name: &str,
    values: &[T],
) -> ExportResult<()> {
    write!(output, "export const {name} = [")?;
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&ts_string(&json_string(value, name)?)?);
    }
    output.push_str("] as const;\n\n");
    Ok(())
}

fn push_string_object(
    output: &mut String,
    name: &str,
    entries: &[(&str, &str)],
) -> ExportResult<()> {
    writeln!(output, "export const {name} = {{")?;
    for (key, value) in entries {
        writeln!(output, "    {key}: {},", ts_string(value)?)?;
    }
    output.push_str("} as const;\n\n");
    Ok(())
}

fn push_serialized_string_object<T: Serialize>(
    output: &mut String,
    name: &str,
    entries: &[(&str, T)],
) -> ExportResult<()> {
    writeln!(output, "export const {name} = {{")?;
    for (key, value) in entries {
        writeln!(
            output,
            "    {key}: {},",
            ts_string(&json_string(value, key)?)?
        )?;
    }
    output.push_str("} as const;\n\n");
    Ok(())
}

fn push_number_object(output: &mut String, name: &str, entries: &[(&str, u16)]) -> fmt::Result {
    writeln!(output, "export const {name} = {{")?;
    for (key, value) in entries {
        writeln!(output, "    {key}: {value},")?;
    }
    output.push_str("} as const;\n\n");
    Ok(())
}

fn json_string<T: Serialize + ?Sized>(value: &T, context: &str) -> ExportResult<String> {
    match serde_json::to_value(value)? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(invalid_data(context, "did not serialize as a string")),
    }
}

fn invalid_data(context: &str, detail: &str) -> Box<dyn Error> {
    io::Error::new(io::ErrorKind::InvalidData, format!("{context} {detail}")).into()
}

fn ts_string(value: &str) -> Result<String, serde_json::Error> {
    serde_json::to_string(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogs_use_rust_contracts() -> ExportResult<()> {
        assert_eq!(wire_tag("AUTH"), Some("auth"));
        assert_eq!(wire_tag("START_RECORDING"), Some("startrecording"));
        assert_eq!(wire_tag("RECORDING_CHANGE"), Some("recordingchange"));
        let contract = contract()?;
        assert!(contract.contains(r#"MessageEnvelope<"auth", AuthPayload>"#));
        assert!(contract.contains(r#"RequestEnvelope<"stoprecording">"#));
        assert!(contract.contains(r#"ResponseEnvelope<"startrecording", RecordingActionResult>"#));

        assert_eq!(command_kind("APPLY_NEGOTIATION"), Some("applyNegotiation"));
        assert_eq!(
            command_kind("REPLACE_SOURCE_DESCRIPTORS"),
            Some("replaceSourceDescriptors")
        );
        Ok(())
    }

    fn wire_tag(key: &str) -> Option<&'static str> {
        WIRE_TAGS
            .iter()
            .find(|(entry_key, _value)| *entry_key == key)
            .map(|(_key, value)| *value)
    }

    fn command_kind(key: &str) -> Option<&'static str> {
        HOST_COMMAND_KINDS
            .iter()
            .find(|(entry_key, _value)| *entry_key == key)
            .map(|(_key, value)| *value)
    }
}
