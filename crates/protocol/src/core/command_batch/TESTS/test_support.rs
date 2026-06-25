use super::{Command, CommandBatch};

/// build a validated batch from manually assembled test commands
///
/// # Errors
///
/// returns a validation message when the command order violates the
/// production command-batch invariants
pub fn command_batch(commands: Vec<Command>) -> Result<CommandBatch, String> {
    CommandBatch::validate_commands(&commands).map_err(|error| error.to_string())?;
    Ok(CommandBatch { commands })
}
