use super::fixtures::*;

#[test]
fn shared_payload_clones_for_non_final_destination() {
    let mut payload = SharedPayload::from_vec(vec![1, 2, 3, 4]);

    let write_payload = payload.take_write_payload(false);

    assert_eq!(write_payload, vec![1, 2, 3, 4]);
    assert_eq!(payload.as_slice(), [1, 2, 3, 4]);
}

#[test]
fn shared_payload_moves_for_final_destination() {
    let mut payload = SharedPayload::from_vec(vec![5, 6, 7, 8]);

    let write_payload = payload.take_write_payload(true);

    assert_eq!(write_payload, vec![5, 6, 7, 8]);
    assert!(payload.as_slice().is_empty());
}

#[test]
fn shared_payload_reports_length_through_the_helper() {
    let payload = SharedPayload::from_vec(vec![9, 10, 11]);

    assert_eq!(payload.len(), 3);
    assert_eq!(payload.as_slice(), [9, 10, 11]);
}
