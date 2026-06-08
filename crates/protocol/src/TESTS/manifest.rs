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
