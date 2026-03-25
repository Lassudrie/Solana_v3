use std::{env, error::Error, process};

use bot::{BotConfig, BotDaemon, ConfigError, DaemonExit};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let config_path = config_path_from_args()?;
    let config = BotConfig::from_path(&config_path)?;
    let mut daemon = BotDaemon::from_config(config)?;
    match daemon.run()? {
        DaemonExit::SourceExhausted => Ok(()),
    }
}

fn config_path_from_args() -> Result<String, ConfigError> {
    let mut args = env::args().skip(1);
    while let Some(argument) = args.next() {
        if argument == "--config" {
            if let Some(path) = args.next() {
                return Ok(path);
            }
        }
    }

    if let Ok(path) = env::var("BOT_CONFIG") {
        if !path.is_empty() {
            return Ok(path);
        }
    }

    Err(ConfigError::MissingPath)
}
