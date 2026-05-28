//! command-line entry point for the browser protocol manifest exporter
//!
//! this binary is the npm-facing boundary used by `protocol:manifest`
//! it keeps path selection and process failure reporting outside the exporter library,
//! while [`o_sfu_protocol::manifest`] owns the Rust-authored manifest data
//!
//! when an argument is provided, it is treated as the output file path
//! without an argument, the exporter writes to the ignored client generated manifest
//! path used by the repository build scripts

use std::{env, error::Error, path::PathBuf};

use o_sfu_protocol::manifest::{default_output_path, write_manifest};

/// generate the protocol manifest for the client build
///
/// # Errors
///
/// returns an error when the selected output path cannot be created or written,
/// or when the Rust protocol catalog cannot be projected into the manifest
fn main() -> Result<(), Box<dyn Error>> {
    let path = env::args_os()
        .nth(1)
        .map_or_else(default_output_path, PathBuf::from);
    write_manifest(&path)?;
    Ok(())
}
