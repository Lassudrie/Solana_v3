use std::{env, error::Error, path::PathBuf, process};

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

fn config_path_from_args() -> Result<PathBuf, ConfigError> {
    config_path_from_parts(env::args().skip(1), env::var("BOT_CONFIG").ok())
}

fn config_path_from_parts<I>(
    args: I,
    bot_config_env: Option<String>,
) -> Result<PathBuf, ConfigError>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if argument == "--config" {
            if let Some(path) = args.next() {
                return Ok(PathBuf::from(path));
            }
        }
    }

    if let Some(path) = bot_config_env {
        if !path.is_empty() {
            return Ok(PathBuf::from(path));
        }
    }

    Ok(default_config_path())
}

fn default_config_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("sol_usdc_routes_amm_fast.toml")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{config_path_from_parts, default_config_path};

    #[test]
    fn config_flag_overrides_env_and_default() {
        let path = config_path_from_parts(
            vec!["--config".into(), "/tmp/custom.toml".into()],
            Some("/tmp/env.toml".into()),
        )
        .unwrap();

        assert_eq!(path, Path::new("/tmp/custom.toml"));
    }

    #[test]
    fn bot_config_env_overrides_default() {
        let path =
            config_path_from_parts(Vec::<String>::new(), Some("/tmp/env.toml".into())).unwrap();

        assert_eq!(path, Path::new("/tmp/env.toml"));
    }

    #[test]
    fn falls_back_to_amm_fast_manifest_by_default() {
        let path = config_path_from_parts(Vec::<String>::new(), None).unwrap();

        assert_eq!(path, default_config_path());
        assert!(path.ends_with("sol_usdc_routes_amm_fast.toml"));
    }

    #[test]
    fn empty_bot_config_env_still_falls_back_to_default_manifest() {
        let path = config_path_from_parts(Vec::<String>::new(), Some(String::new())).unwrap();

        assert_eq!(path, default_config_path());
    }
}
