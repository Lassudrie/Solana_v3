use std::{env, error::Error, path::PathBuf, process};

use signerd::{SecureSignerService, SecureSignerServiceConfig};
use thiserror::Error;

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let config = config_from_args()?;
    let service = SecureSignerService::bind(config)?;
    service.serve_forever()?;
    Ok(())
}

fn config_from_args() -> Result<SecureSignerServiceConfig, CliError> {
    let mut args = env::args().skip(1);
    let mut socket_path = None;
    let mut keypair_path = None;

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--socket" => socket_path = args.next().map(PathBuf::from),
            "--keypair-path" => keypair_path = args.next().map(PathBuf::from),
            other => {
                return Err(CliError::UnknownArgument(other.to_string()));
            }
        }
    }

    Ok(SecureSignerServiceConfig {
        socket_path: socket_path.ok_or(CliError::MissingSocketPath)?,
        keypair_path: keypair_path.ok_or(CliError::MissingKeypairPath)?,
    })
}

#[derive(Debug, Error)]
enum CliError {
    #[error("missing --socket <path>")]
    MissingSocketPath,
    #[error("missing --keypair-path <path>")]
    MissingKeypairPath,
    #[error("unknown argument {0}")]
    UnknownArgument(String),
}
