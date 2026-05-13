use core::hint::cold_path;

#[allow(
    clippy::inline_always,
    reason = "the branch hint must inline so cold_path marks the caller branch"
)]
#[inline(always)]
pub(in crate::runtime) const fn unlikely(condition: bool) -> bool {
    if condition {
        cold_path();
    }
    condition
}
