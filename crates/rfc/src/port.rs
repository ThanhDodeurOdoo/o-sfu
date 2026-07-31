//! Transport protocol port ranges from RFC 6335.

/// First port in the dynamic port range from RFC 6335 section 6.
pub const DYNAMIC_RANGE_START: u16 = 49_152;

/// Last port in the dynamic port range from RFC 6335 section 6.
pub const DYNAMIC_RANGE_END: u16 = u16::MAX;
