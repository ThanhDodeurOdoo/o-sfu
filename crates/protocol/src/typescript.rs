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
    bundle_api::{BundleBroadcastUpdate, BundleDisconnectUpdate},
    host_bridge::{
        HOST_COMMAND_KINDS, HostCommand, HostConnectionState, HostNegotiationKind,
        HostPendingRequestKind, HostUpdate,
    },
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
    push_runtime_schemas(&mut output);
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
    push_decl::<BundleBroadcastUpdate>(output, &config);
    push_decl::<BundleDisconnectUpdate>(output, &config);
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
    push_decl::<HostConnectionState>(output, &config);
    push_decl::<HostNegotiationKind>(output, &config);
    push_decl::<HostPendingRequestKind>(output, &config);
    push_decl::<HostUpdate>(output, &config);
    push_decl::<HostCommand>(output, &config);
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

fn push_runtime_schemas(output: &mut String) {
    push_runtime_schema_types(output);
    push_runtime_schema_values(output);
}

fn push_runtime_schema_types(output: &mut String) {
    output.push_str(
        r#"export type HostCommandKind = HostCommand["kind"];

export type ProtocolValidationSchema =
    | "boolean"
    | "browserCloseCode"
    | "finiteNumber"
    | "nonNegativeInteger"
    | "positiveNumber"
    | "sessionId"
    | "string"
    | "temporalLayerId"
    | "unknown"
    | { kind: "array"; items: ProtocolValidationSchema }
    | { kind: "enum"; values: readonly string[]; message?: string }
    | { kind: "literal"; value: string }
    | { kind: "object"; fields: Record<string, ProtocolValidationSchema> }
    | { kind: "optional"; value: ProtocolValidationSchema }
    | { kind: "record"; values: ProtocolValidationSchema }
    | { kind: "taggedUnion"; tag: string; variants: Record<string, ProtocolValidationSchema> };

const objectSchema = (fields: Record<string, ProtocolValidationSchema>): ProtocolValidationSchema => ({ kind: "object", fields });
const optionalSchema = (value: ProtocolValidationSchema): ProtocolValidationSchema => ({ kind: "optional", value });
const arraySchema = (items: ProtocolValidationSchema): ProtocolValidationSchema => ({ kind: "array", items });
const enumSchema = (values: readonly string[], message?: string): ProtocolValidationSchema =>
    message === undefined ? { kind: "enum", values } : { kind: "enum", values, message };
const literalSchema = (value: string): ProtocolValidationSchema => ({ kind: "literal", value });
const recordSchema = (values: ProtocolValidationSchema): ProtocolValidationSchema => ({ kind: "record", values });
const taggedUnionSchema = (tag: string, variants: Record<string, ProtocolValidationSchema>): ProtocolValidationSchema =>
    ({ kind: "taggedUnion", tag, variants });

"#,
    );
}

fn push_runtime_schema_values(output: &mut String) {
    output.push_str(
        r#"const streamTypeSchema = enumSchema(STREAM_TYPES);
const negotiationKindSchema = enumSchema(Object.values(NEGOTIATION_KIND));
const pendingRequestKindSchema = enumSchema(Object.values(PENDING_REQUEST_KIND));
const recordingStopCodeSchema = enumSchema(RECORDING_STOP_CODES);
const policyRoleSchema = enumSchema(SOURCE_ENCODING_POLICY_ROLES, "must be a supported upload layer policy role");
const connectionStateSchema = enumSchema(["disconnected", "connecting", "authenticated", "connected", "recovering", "closed"]);
const userInfoSchema = objectSchema({
    isTalking: optionalSchema("boolean"), isFeatured: optionalSchema("boolean"), isCameraOn: optionalSchema("boolean"),
    isScreenSharingOn: optionalSchema("boolean"), isSelfMuted: optionalSchema("boolean"), isDeaf: optionalSchema("boolean"),
    isRaisingHand: optionalSchema("boolean"),
});
const recordingStateSchema = objectSchema({
    recording: optionalSchema("boolean"), audio: optionalSchema("boolean"), video: optionalSchema("boolean"),
    transcription: optionalSchema("boolean"),
});
const recordingStateUpdateSchema = objectSchema({ state: recordingStateSchema, stopCode: optionalSchema(recordingStopCodeSchema) });
const sourceEncodingSchema = objectSchema({
    encodingId: "string", rid: optionalSchema("string"), maxBitrate: optionalSchema("nonNegativeInteger"),
    resolutionScale: optionalSchema("positiveNumber"), maxFramerate: optionalSchema("nonNegativeInteger"),
    policyRole: optionalSchema(policyRoleSchema), maxTemporalLayerId: optionalSchema("temporalLayerId"),
});
const sourceDescriptorSchema = objectSchema({
    sourceId: "string", sessionId: "sessionId", type: streamTypeSchema, active: "boolean",
    mid: optionalSchema("string"), encodings: arraySchema(sourceEncodingSchema),
});
const trackBindingSchema = objectSchema({
    mid: "string", sessionId: "sessionId", type: streamTypeSchema, active: "boolean",
    source: optionalSchema(sourceDescriptorSchema),
});
const uploadEncodingSchema = objectSchema({
    rid: "string", maxBitrate: optionalSchema("nonNegativeInteger"), resolutionScale: optionalSchema("finiteNumber"),
    maxFramerate: optionalSchema("nonNegativeInteger"),
});
const uploadSlotSchema = objectSchema({
    mid: "string", kind: enumSchema(UPLOAD_KINDS), codecs: optionalSchema(arraySchema("string")),
    simulcastEncodings: optionalSchema(arraySchema(uploadEncodingSchema)),
});

export const RUNTIME_SCHEMAS = {
    availableFeatures: objectSchema({ rtc: "boolean", transcription: "boolean", audioRecording: "boolean", videoRecording: "boolean" }),
    recordingState: recordingStateSchema,
    trackBinding: trackBindingSchema,
} as const satisfies Record<string, ProtocolValidationSchema>;

export const HOST_UPDATE_SCHEMA = taggedUnionSchema("name", {
    disconnect: objectSchema({ name: literalSchema("disconnect"), payload: objectSchema({ sessionId: "sessionId" }) }),
    info_change: objectSchema({ name: literalSchema("info_change"), payload: recordSchema(userInfoSchema) }),
    broadcast: objectSchema({ name: literalSchema("broadcast"), payload: objectSchema({ senderId: "sessionId", message: "unknown" }) }),
    channel_info_change: objectSchema({ name: literalSchema("channel_info_change"), payload: recordingStateUpdateSchema }),
});

export const HOST_COMMAND_SCHEMAS = {
    [COMMAND_KIND.CONNECT]: objectSchema({ kind: literalSchema(COMMAND_KIND.CONNECT), url: "string" }),
    [COMMAND_KIND.SEND_WEB_SOCKET]: objectSchema({ kind: literalSchema(COMMAND_KIND.SEND_WEB_SOCKET), frame: "string" }),
    [COMMAND_KIND.CLOSE_WEB_SOCKET]: objectSchema({ kind: literalSchema(COMMAND_KIND.CLOSE_WEB_SOCKET), code: "browserCloseCode" }),
    [COMMAND_KIND.APPLY_NEGOTIATION]: objectSchema({
        kind: literalSchema(COMMAND_KIND.APPLY_NEGOTIATION), requestId: "string", negotiationKind: negotiationKindSchema,
        sdp: "string", uploadSlots: arraySchema(uploadSlotSchema),
    }),
    [COMMAND_KIND.CREATE_PEER_CONNECTION]: objectSchema({ kind: literalSchema(COMMAND_KIND.CREATE_PEER_CONNECTION) }),
    [COMMAND_KIND.CLOSE_PEER_CONNECTION]: objectSchema({ kind: literalSchema(COMMAND_KIND.CLOSE_PEER_CONNECTION) }),
    [COMMAND_KIND.ATTACH_TRACK]: objectSchema({ kind: literalSchema(COMMAND_KIND.ATTACH_TRACK), mid: "string", streamType: streamTypeSchema }),
    [COMMAND_KIND.DETACH_TRACK]: objectSchema({ kind: literalSchema(COMMAND_KIND.DETACH_TRACK), streamType: streamTypeSchema }),
    [COMMAND_KIND.REPLACE_TRACK_BINDINGS]: objectSchema({ kind: literalSchema(COMMAND_KIND.REPLACE_TRACK_BINDINGS), bindings: arraySchema(trackBindingSchema) }),
    [COMMAND_KIND.REPLACE_SOURCE_DESCRIPTORS]: objectSchema({ kind: literalSchema(COMMAND_KIND.REPLACE_SOURCE_DESCRIPTORS), sources: arraySchema(sourceDescriptorSchema) }),
    [COMMAND_KIND.REMOVE_SESSION_TRACKS]: objectSchema({ kind: literalSchema(COMMAND_KIND.REMOVE_SESSION_TRACKS), sessionId: "sessionId" }),
    [COMMAND_KIND.EMIT_STATE_CHANGE]: objectSchema({ kind: literalSchema(COMMAND_KIND.EMIT_STATE_CHANGE), state: connectionStateSchema, cause: optionalSchema("string") }),
    [COMMAND_KIND.EMIT_UPDATE]: objectSchema({ kind: literalSchema(COMMAND_KIND.EMIT_UPDATE), update: HOST_UPDATE_SCHEMA }),
    [COMMAND_KIND.REGISTER_PENDING_REQUEST]: objectSchema({
        kind: literalSchema(COMMAND_KIND.REGISTER_PENDING_REQUEST), requestId: "string", requestKind: pendingRequestKindSchema,
    }),
    [COMMAND_KIND.RESOLVE_PENDING_REQUEST]: objectSchema({ kind: literalSchema(COMMAND_KIND.RESOLVE_PENDING_REQUEST), requestId: "string", ok: "boolean" }),
    [COMMAND_KIND.SCHEDULE_TIMER]: objectSchema({ kind: literalSchema(COMMAND_KIND.SCHEDULE_TIMER), id: "nonNegativeInteger", ms: "nonNegativeInteger" }),
    [COMMAND_KIND.CANCEL_TIMER]: objectSchema({ kind: literalSchema(COMMAND_KIND.CANCEL_TIMER), id: "nonNegativeInteger" }),
} as const satisfies Record<HostCommandKind, ProtocolValidationSchema>;

"#,
    );
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
        assert!(contract.contains(r#"export type HostCommandKind = HostCommand["kind"];"#));
        assert!(contract.contains(r#""browserCloseCode""#));
        assert!(contract.contains("HOST_COMMAND_SCHEMAS"));

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
