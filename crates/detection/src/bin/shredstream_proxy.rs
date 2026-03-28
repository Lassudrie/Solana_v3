use std::{env, error::Error, net::SocketAddr, process};

use detection::{ShredstreamProxyConfig, ShredstreamProxyService, serve_shredstream_proxy};
use thiserror::Error;

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let config = config_from_args()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let listen_address = config.listen_address;
        let service = ShredstreamProxyService::new(&config);

        eprintln!("shredstream proxy scaffold listening on {listen_address}");
        eprintln!("upstream entry producer is not wired yet");

        serve_shredstream_proxy(config, service, shutdown_signal()).await?;
        Ok::<(), Box<dyn Error>>(())
    })
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn config_from_args() -> Result<ShredstreamProxyConfig, CliError> {
    config_from_parts(
        env::args().skip(1),
        env::var("SHREDSTREAM_PROXY_LISTEN_ADDRESS").ok(),
        env::var("SHREDSTREAM_PROXY_BUFFER_CAPACITY").ok(),
        env::var("SHREDSTREAM_PROXY_HEARTBEAT_TTL_MS").ok(),
    )
}

fn config_from_parts<I>(
    args: I,
    env_listen_address: Option<String>,
    env_buffer_capacity: Option<String>,
    env_heartbeat_ttl_ms: Option<String>,
) -> Result<ShredstreamProxyConfig, CliError>
where
    I: IntoIterator<Item = String>,
{
    let mut config = ShredstreamProxyConfig::default();

    if let Some(value) = non_empty(env_listen_address) {
        config.listen_address = parse_socket_addr("--listen-address", &value)?;
    }
    if let Some(value) = non_empty(env_buffer_capacity) {
        config.buffer_capacity = parse_usize_flag("--buffer-capacity", &value)?;
    }
    if let Some(value) = non_empty(env_heartbeat_ttl_ms) {
        config.heartbeat_ttl_ms = parse_u32_flag("--heartbeat-ttl-ms", &value)?;
    }

    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--listen-address" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--listen-address"))?;
                config.listen_address = parse_socket_addr("--listen-address", &value)?;
            }
            "--buffer-capacity" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--buffer-capacity"))?;
                config.buffer_capacity = parse_usize_flag("--buffer-capacity", &value)?;
            }
            "--heartbeat-ttl-ms" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--heartbeat-ttl-ms"))?;
                config.heartbeat_ttl_ms = parse_u32_flag("--heartbeat-ttl-ms", &value)?;
            }
            other => return Err(CliError::UnknownArgument(other.to_string())),
        }
    }

    if config.buffer_capacity == 0 {
        return Err(CliError::InvalidValue {
            flag: "--buffer-capacity",
            value: "0".into(),
            detail: "must be positive",
        });
    }
    if config.heartbeat_ttl_ms == 0 {
        return Err(CliError::InvalidValue {
            flag: "--heartbeat-ttl-ms",
            value: "0".into(),
            detail: "must be positive",
        });
    }

    Ok(config)
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

fn parse_socket_addr(flag: &'static str, value: &str) -> Result<SocketAddr, CliError> {
    value.parse().map_err(|_| CliError::InvalidValue {
        flag,
        value: value.to_string(),
        detail: "must be a valid socket address",
    })
}

fn parse_usize_flag(flag: &'static str, value: &str) -> Result<usize, CliError> {
    value.parse().map_err(|_| CliError::InvalidValue {
        flag,
        value: value.to_string(),
        detail: "must be a positive integer",
    })
}

fn parse_u32_flag(flag: &'static str, value: &str) -> Result<u32, CliError> {
    value.parse().map_err(|_| CliError::InvalidValue {
        flag,
        value: value.to_string(),
        detail: "must be a positive integer",
    })
}

#[derive(Debug, Error)]
enum CliError {
    #[error("missing value for {0}")]
    MissingValue(&'static str),
    #[error("invalid value for {flag}: {value} ({detail})")]
    InvalidValue {
        flag: &'static str,
        value: String,
        detail: &'static str,
    },
    #[error("unknown argument {0}")]
    UnknownArgument(String),
}

#[cfg(test)]
mod tests {
    use super::config_from_parts;

    #[test]
    fn uses_default_proxy_config() {
        let config = config_from_parts(Vec::<String>::new(), None, None, None).unwrap();

        assert_eq!(config.listen_address.to_string(), "127.0.0.1:50051");
        assert_eq!(config.buffer_capacity, 4_096);
        assert_eq!(config.heartbeat_ttl_ms, 1_000);
    }

    #[test]
    fn env_overrides_defaults() {
        let config = config_from_parts(
            Vec::<String>::new(),
            Some("127.0.0.1:60000".into()),
            Some("128".into()),
            Some("2500".into()),
        )
        .unwrap();

        assert_eq!(config.listen_address.to_string(), "127.0.0.1:60000");
        assert_eq!(config.buffer_capacity, 128);
        assert_eq!(config.heartbeat_ttl_ms, 2_500);
    }

    #[test]
    fn cli_overrides_env() {
        let config = config_from_parts(
            vec![
                "--listen-address".into(),
                "127.0.0.1:61000".into(),
                "--buffer-capacity".into(),
                "256".into(),
            ],
            Some("127.0.0.1:60000".into()),
            Some("128".into()),
            Some("2500".into()),
        )
        .unwrap();

        assert_eq!(config.listen_address.to_string(), "127.0.0.1:61000");
        assert_eq!(config.buffer_capacity, 256);
        assert_eq!(config.heartbeat_ttl_ms, 2_500);
    }

    #[test]
    fn rejects_zero_buffer_capacity() {
        let error = config_from_parts(
            vec!["--buffer-capacity".into(), "0".into()],
            None,
            None,
            None,
        )
        .expect_err("zero buffer capacity should fail");

        assert!(error.to_string().contains("--buffer-capacity"));
    }
}
