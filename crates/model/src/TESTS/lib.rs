use super::UserId;

#[test]
fn user_id_normalization_keeps_numeric_runtime_identity_canonical() {
    assert_eq!(
        UserId::String("42".to_owned()).normalized_for_runtime(),
        UserId::Integer(42)
    );
    assert_eq!(
        UserId::Integer(42).normalized_for_runtime(),
        UserId::Integer(42)
    );
}

#[test]
fn user_id_normalization_preserves_arbitrary_string_ids() {
    assert_eq!(
        UserId::String("guest-42".to_owned()).normalized_for_runtime(),
        UserId::String("guest-42".to_owned())
    );
}
