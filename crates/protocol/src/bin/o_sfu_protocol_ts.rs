//! command-line entry point for the browser protocol contract exporter
//!
//! this binary is the npm-facing boundary used by `protocol:generate`
//! it keeps path selection and process failure reporting outside the exporter library,
//! while [`o_sfu_protocol::typescript`] owns the schema, literal catalogs and
//! file writing contract
//!
//! when an argument is provided, it is treated as the output file path
//! without an argument, the exporter writes to the ignored client generated contract
//! path used by the repository build scripts

use std::{env, error::Error, path::PathBuf};

use o_sfu_protocol::typescript::{default_output_path, write_contract};

/// generate the TypeScript protocol contract for the client build
///
/// # Errors
///
/// returns an error when the selected output path cannot be created or written,
/// or when the Rust protocol catalog cannot be projected into the TypeScript
/// contract
fn main() -> Result<(), Box<dyn Error>> {
    let path = env::args_os()
        .nth(1)
        .map_or_else(default_output_path, PathBuf::from);
    write_contract(&path)?;
    Ok(())
}
