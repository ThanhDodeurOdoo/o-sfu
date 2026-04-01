pub(super) fn count_present<T, const N: usize>(values: &[Option<T>; N]) -> usize {
    let mut count = 0;
    for value in values {
        if value.is_some() {
            count += 1;
        }
    }
    count
}
