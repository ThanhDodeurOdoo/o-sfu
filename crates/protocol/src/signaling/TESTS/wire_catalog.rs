use super::*;

#[test]
fn envelope_specs_match_decode_catalogs() {
    assert_specs_match_entries(ClientMessage::specs(), ClientMessage::ENTRIES);
    assert_specs_match_entries(ClientRequest::specs(), ClientRequest::ENTRIES);
    assert_specs_match_entries(ServerRequest::specs(), ServerRequest::ENTRIES);
    assert_specs_match_entries(ClientResponse::specs(), ClientResponse::ENTRIES);
    assert_specs_match_entries(ServerMessage::specs(), ServerMessage::ENTRIES);
    assert_specs_match_entries(ServerResponse::specs(), ServerResponse::ENTRIES);
}

fn assert_specs_match_entries<T>(
    specs: impl IntoIterator<Item = EnvelopeSpec>,
    entries: &[EnvelopeEntry<T>],
) {
    for spec in specs {
        assert_eq!(
            entry_for_tag(entries, spec.tag()).map(|entry| entry.kind),
            Some(spec.kind())
        );
    }
}
