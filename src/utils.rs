use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[must_use]
pub(crate) fn rfc3339_now() -> String {
    match OffsetDateTime::now_utc().format(&Rfc3339) {
        Ok(timestamp) => timestamp,
        Err(_error) => String::from("1970-01-01T00:00:00Z"),
    }
}
