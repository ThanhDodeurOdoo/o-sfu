use super::{PreAuthWebSocketAdmission, PreAuthWebSocketAdmissionRejection};

fn available_global_permits(admission: &PreAuthWebSocketAdmission) -> usize {
    admission.global.available_permits()
}

fn tracked_origin_count(admission: &PreAuthWebSocketAdmission) -> usize {
    super::lock_origins(&admission.origins).len()
}

#[test]
fn pre_auth_admission_releases_dropped_permits() {
    let admission = PreAuthWebSocketAdmission::new(1, 1);
    let permit = admission.try_acquire("198.51.100.24");
    assert!(permit.is_ok());
    assert_eq!(available_global_permits(&admission), 0);
    assert!(matches!(
        admission.try_acquire("198.51.100.24"),
        Err(PreAuthWebSocketAdmissionRejection::Global)
    ));

    drop(permit);

    assert_eq!(available_global_permits(&admission), 1);
    assert_eq!(tracked_origin_count(&admission), 0);
    assert!(admission.try_acquire("198.51.100.24").is_ok());
}

#[test]
fn pre_auth_admission_enforces_per_origin_capacity() {
    let admission = PreAuthWebSocketAdmission::new(2, 1);
    let first = match admission.try_acquire("198.51.100.24") {
        Ok(permit) => permit,
        Err(error) => {
            assert_eq!(Some(error), None, "first origin permit should be available");
            return;
        }
    };
    assert!(matches!(
        admission.try_acquire("198.51.100.24"),
        Err(PreAuthWebSocketAdmissionRejection::Origin)
    ));
    assert!(admission.try_acquire("203.0.113.4").is_ok());
    assert_eq!(first.origin.as_ref(), "198.51.100.24");
    drop(first);
}
