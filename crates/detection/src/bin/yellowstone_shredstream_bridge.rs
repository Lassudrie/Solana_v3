use std::{
    collections::HashMap,
    env,
    error::Error,
    io,
    net::SocketAddr,
    process,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use detection::{
    ShredstreamProxyConfig, ShredstreamProxyHandle, ShredstreamProxyService,
    serve_shredstream_proxy,
};
use futures::{SinkExt, StreamExt};
use solana_entry_legacy::entry::Entry as LegacyEntry;
use solana_message_legacy::{
    MessageHeader as LegacyMessageHeader, VersionedMessage as LegacyVersionedMessage,
    compiled_instruction::CompiledInstruction as LegacyCompiledInstruction, v0,
};
use solana_pubkey_legacy::Pubkey as LegacyPubkey;
use solana_signature_legacy::Signature as LegacySignature;
use solana_transaction_legacy::versioned::VersionedTransaction as LegacyVersionedTransaction;
use tokio::sync::watch;
use tokio::time::{MissedTickBehavior, interval};
use yellowstone_grpc_client::GeyserGrpcBuilder;
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    subscribe_update::UpdateOneof,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let config = config_from_args()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_async(config))?;
    Ok(())
}

async fn run_async(config: BridgeConfig) -> io::Result<()> {
    let service = ShredstreamProxyService::new(&config.proxy);
    let handle = service.handle();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut server_shutdown = shutdown_rx.clone();
    let server_config = config.proxy.clone();
    let mut server_task = tokio::spawn(async move {
        serve_shredstream_proxy(server_config, service, async move {
            wait_for_shutdown(&mut server_shutdown).await;
        })
        .await
        .map_err(io::Error::other)
    });

    let mut bridge_shutdown = shutdown_rx.clone();
    let mut bridge_task =
        tokio::spawn(async move { run_bridge(config, handle, &mut bridge_shutdown).await });

    tokio::select! {
        _ = shutdown_signal() => {
            let _ = shutdown_tx.send(true);
            let _ = server_task.await;
            let _ = bridge_task.await;
            Ok(())
        }
        result = &mut server_task => {
            let _ = shutdown_tx.send(true);
            let _ = bridge_task.await;
            join_result(result)
        }
        result = &mut bridge_task => {
            let _ = shutdown_tx.send(true);
            let _ = server_task.await;
            join_result(result)
        }
    }
}

fn join_result(result: Result<io::Result<()>, tokio::task::JoinError>) -> io::Result<()> {
    match result {
        Ok(inner) => inner,
        Err(error) => Err(io::Error::other(error)),
    }
}

async fn run_bridge(
    config: BridgeConfig,
    handle: ShredstreamProxyHandle,
    shutdown: &mut watch::Receiver<bool>,
) -> io::Result<()> {
    let connect_timeout = Duration::from_millis(config.connect_timeout_ms.max(1));
    let base_backoff = Duration::from_millis(config.reconnect_backoff_ms.max(1));
    let max_backoff = Duration::from_millis(config.max_reconnect_backoff_ms.max(1));
    let batch_flush = Duration::from_millis(config.batch_flush_ms.max(1));
    let mut backoff = base_backoff;

    loop {
        if *shutdown.borrow() {
            return Ok(());
        }

        let connect_result = connect_client(&config, connect_timeout).await;
        let mut client = match connect_result {
            Ok(client) => client,
            Err(error) => {
                eprintln!(
                    "yellowstone bridge: connect to {} failed: {error}",
                    config.yellowstone_endpoint
                );
                wait_backoff(shutdown, backoff).await?;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        let request = subscription_request(config.include_votes);
        let subscribe_result = client.subscribe_with_request(Some(request)).await;
        let (mut subscribe_tx, mut stream) = match subscribe_result {
            Ok(result) => result,
            Err(error) => {
                eprintln!("yellowstone bridge: subscribe failed: {error}");
                wait_backoff(shutdown, backoff).await?;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        eprintln!(
            "yellowstone bridge: streaming {} -> {}",
            config.yellowstone_endpoint, config.proxy.listen_address
        );
        backoff = base_backoff;

        let mut flush_timer = interval(batch_flush);
        flush_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut pending = PendingBatch::default();

        loop {
            tokio::select! {
                _ = wait_for_shutdown(shutdown) => {
                    flush_pending_batch(&handle, &mut pending)?;
                    return Ok(());
                }
                _ = flush_timer.tick() => {
                    flush_pending_batch(&handle, &mut pending)?;
                }
                message = stream.next() => {
                    let Some(message) = message else {
                        eprintln!("yellowstone bridge: upstream stream closed");
                        break;
                    };
                    let message = match message {
                        Ok(message) => message,
                        Err(error) => {
                            eprintln!("yellowstone bridge: upstream stream error: {error}");
                            break;
                        }
                    };

                    match message.update_oneof {
                        Some(UpdateOneof::Transaction(update)) => {
                            let Some(transaction) = update.transaction else {
                                continue;
                            };
                            let transaction = match convert_transaction(transaction.transaction) {
                                Ok(transaction) => transaction,
                                Err(error) => {
                                    eprintln!("yellowstone bridge: skipping invalid transaction: {error}");
                                    continue;
                                }
                            };
                            let created_at =
                                timestamp_to_system_time(message.created_at).unwrap_or_else(SystemTime::now);
                            queue_transaction(
                                &handle,
                                &mut pending,
                                update.slot,
                                created_at,
                                transaction,
                                config.batch_max_transactions,
                            )?;
                        }
                        Some(UpdateOneof::Ping(_)) => {
                            if let Err(error) = subscribe_tx
                                .send(SubscribeRequest {
                                    ping: Some(SubscribeRequestPing { id: 1 }),
                                    ..Default::default()
                                })
                                .await
                            {
                                eprintln!("yellowstone bridge: failed to answer upstream ping: {error}");
                                break;
                            }
                        }
                        Some(UpdateOneof::Pong(_)) => {}
                        Some(_) => {}
                        None => {
                            eprintln!("yellowstone bridge: empty upstream update");
                            break;
                        }
                    }
                }
            }
        }

        flush_pending_batch(&handle, &mut pending)?;
        wait_backoff(shutdown, backoff).await?;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn connect_client(
    config: &BridgeConfig,
    connect_timeout: Duration,
) -> Result<
    yellowstone_grpc_client::GeyserGrpcClient<impl yellowstone_grpc_client::Interceptor + Clone>,
    yellowstone_grpc_client::GeyserGrpcBuilderError,
> {
    let mut builder = GeyserGrpcBuilder::from_shared(config.yellowstone_endpoint.clone())?
        .connect_timeout(connect_timeout);
    if let Some(x_token) = config.x_token.clone() {
        builder = builder.x_token(Some(x_token))?;
    }
    builder.connect().await
}

fn subscription_request(include_votes: bool) -> SubscribeRequest {
    let mut transactions = HashMap::new();
    transactions.insert(
        "bridge".to_string(),
        SubscribeRequestFilterTransactions {
            vote: if include_votes { None } else { Some(false) },
            failed: Some(false),
            signature: None,
            account_include: Vec::new(),
            account_exclude: Vec::new(),
            account_required: Vec::new(),
        },
    );
    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions,
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

fn queue_transaction(
    handle: &ShredstreamProxyHandle,
    pending: &mut PendingBatch,
    slot: u64,
    created_at: SystemTime,
    transaction: LegacyVersionedTransaction,
    batch_max_transactions: usize,
) -> io::Result<()> {
    if pending
        .slot
        .is_some_and(|pending_slot| pending_slot != slot)
    {
        flush_pending_batch(handle, pending)?;
    }

    if pending.slot.is_none() {
        pending.slot = Some(slot);
        pending.created_at = Some(created_at);
    } else if pending
        .created_at
        .is_none_or(|existing| created_at < existing)
    {
        pending.created_at = Some(created_at);
    }
    pending.transactions.push(transaction);

    if pending.transactions.len() >= batch_max_transactions.max(1) {
        flush_pending_batch(handle, pending)?;
    }

    Ok(())
}

fn flush_pending_batch(
    handle: &ShredstreamProxyHandle,
    pending: &mut PendingBatch,
) -> io::Result<()> {
    if pending.transactions.is_empty() {
        pending.slot = None;
        pending.created_at = None;
        return Ok(());
    }

    let slot = pending.slot.take().unwrap_or_default();
    let created_at = pending.created_at.take().unwrap_or_else(SystemTime::now);
    let entry = LegacyEntry {
        num_hashes: 1,
        hash: Default::default(),
        transactions: std::mem::take(&mut pending.transactions),
    };
    let payload = bincode::serialize(&vec![entry]).map_err(io::Error::other)?;
    let _ = handle.publish_entry_at(slot, payload, created_at);
    Ok(())
}

fn convert_transaction(
    transaction: Option<yellowstone_grpc_proto::prelude::Transaction>,
) -> Result<LegacyVersionedTransaction, &'static str> {
    let transaction = transaction.ok_or("missing transaction payload")?;
    let signatures = transaction
        .signatures
        .into_iter()
        .map(|signature| {
            LegacySignature::try_from(signature.as_slice()).map_err(|_| "invalid signature")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let message = convert_message(transaction.message.ok_or("missing transaction message")?)?;

    Ok(LegacyVersionedTransaction {
        signatures,
        message,
    })
}

fn convert_message(
    message: yellowstone_grpc_proto::prelude::Message,
) -> Result<LegacyVersionedMessage, &'static str> {
    let header = convert_message_header(message.header.ok_or("missing message header")?)?;
    let account_keys = convert_pubkeys(message.account_keys)?;
    let instructions = convert_instructions(message.instructions)?;

    if message.versioned {
        let address_table_lookups = message
            .address_table_lookups
            .into_iter()
            .map(convert_lookup_table)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LegacyVersionedMessage::V0(v0::Message {
            header,
            account_keys,
            recent_blockhash: Default::default(),
            instructions,
            address_table_lookups,
        }))
    } else {
        Ok(LegacyVersionedMessage::Legacy(
            solana_message_legacy::legacy::Message {
                header,
                account_keys,
                recent_blockhash: Default::default(),
                instructions,
            },
        ))
    }
}

fn convert_message_header(
    header: yellowstone_grpc_proto::prelude::MessageHeader,
) -> Result<LegacyMessageHeader, &'static str> {
    Ok(LegacyMessageHeader {
        num_required_signatures: header
            .num_required_signatures
            .try_into()
            .map_err(|_| "invalid num_required_signatures")?,
        num_readonly_signed_accounts: header
            .num_readonly_signed_accounts
            .try_into()
            .map_err(|_| "invalid num_readonly_signed_accounts")?,
        num_readonly_unsigned_accounts: header
            .num_readonly_unsigned_accounts
            .try_into()
            .map_err(|_| "invalid num_readonly_unsigned_accounts")?,
    })
}

fn convert_lookup_table(
    lookup: yellowstone_grpc_proto::prelude::MessageAddressTableLookup,
) -> Result<v0::MessageAddressTableLookup, &'static str> {
    Ok(v0::MessageAddressTableLookup {
        account_key: LegacyPubkey::try_from(lookup.account_key.as_slice())
            .map_err(|_| "invalid address lookup table key")?,
        writable_indexes: lookup.writable_indexes,
        readonly_indexes: lookup.readonly_indexes,
    })
}

fn convert_pubkeys(pubkeys: Vec<Vec<u8>>) -> Result<Vec<LegacyPubkey>, &'static str> {
    pubkeys
        .into_iter()
        .map(|pubkey| LegacyPubkey::try_from(pubkey.as_slice()).map_err(|_| "invalid pubkey"))
        .collect()
}

fn convert_instructions(
    instructions: Vec<yellowstone_grpc_proto::prelude::CompiledInstruction>,
) -> Result<Vec<LegacyCompiledInstruction>, &'static str> {
    instructions
        .into_iter()
        .map(|instruction| {
            Ok(LegacyCompiledInstruction {
                program_id_index: instruction
                    .program_id_index
                    .try_into()
                    .map_err(|_| "invalid program_id_index")?,
                accounts: instruction.accounts,
                data: instruction.data,
            })
        })
        .collect()
}

fn timestamp_to_system_time(
    timestamp: Option<yellowstone_grpc_proto::prost_types::Timestamp>,
) -> Option<SystemTime> {
    let timestamp = timestamp?;
    let seconds = u64::try_from(timestamp.seconds).ok()?;
    let nanos = u32::try_from(timestamp.nanos).ok()?;
    UNIX_EPOCH
        .checked_add(Duration::from_secs(seconds))?
        .checked_add(Duration::from_nanos(u64::from(nanos)))
}

async fn wait_backoff(shutdown: &mut watch::Receiver<bool>, duration: Duration) -> io::Result<()> {
    tokio::select! {
        _ = tokio::time::sleep(duration) => Ok(()),
        _ = wait_for_shutdown(shutdown) => Ok(()),
    }
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate =
            signal(SignalKind::terminate()).expect("install SIGTERM signal handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BridgeConfig {
    yellowstone_endpoint: String,
    x_token: Option<String>,
    connect_timeout_ms: u64,
    reconnect_backoff_ms: u64,
    max_reconnect_backoff_ms: u64,
    batch_max_transactions: usize,
    batch_flush_ms: u64,
    include_votes: bool,
    proxy: ShredstreamProxyConfig,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            yellowstone_endpoint: "http://127.0.0.1:10000".into(),
            x_token: None,
            connect_timeout_ms: 1_000,
            reconnect_backoff_ms: 100,
            max_reconnect_backoff_ms: 2_000,
            batch_max_transactions: 128,
            batch_flush_ms: 5,
            include_votes: false,
            proxy: ShredstreamProxyConfig {
                listen_address: "127.0.0.1:50051"
                    .parse()
                    .expect("default bridge listen address"),
                buffer_capacity: 32_768,
                heartbeat_ttl_ms: 1_000,
            },
        }
    }
}

#[derive(Debug, Default)]
struct PendingBatch {
    slot: Option<u64>,
    created_at: Option<SystemTime>,
    transactions: Vec<LegacyVersionedTransaction>,
}

fn config_from_args() -> Result<BridgeConfig, CliError> {
    config_from_parts(
        env::args().skip(1),
        env::var("YELLOWSTONE_GRPC_ENDPOINT").ok(),
        env::var("YELLOWSTONE_X_TOKEN").ok(),
        env::var("YELLOWSTONE_CONNECT_TIMEOUT_MS").ok(),
        env::var("YELLOWSTONE_RECONNECT_BACKOFF_MS").ok(),
        env::var("YELLOWSTONE_MAX_RECONNECT_BACKOFF_MS").ok(),
        env::var("YELLOWSTONE_BATCH_MAX_TRANSACTIONS").ok(),
        env::var("YELLOWSTONE_BATCH_FLUSH_MS").ok(),
        env::var("YELLOWSTONE_INCLUDE_VOTES").ok(),
        env::var("SHREDSTREAM_PROXY_LISTEN_ADDRESS").ok(),
        env::var("SHREDSTREAM_PROXY_BUFFER_CAPACITY").ok(),
        env::var("SHREDSTREAM_PROXY_HEARTBEAT_TTL_MS").ok(),
    )
}

#[allow(clippy::too_many_arguments)]
fn config_from_parts<I>(
    args: I,
    env_yellowstone_endpoint: Option<String>,
    env_x_token: Option<String>,
    env_connect_timeout_ms: Option<String>,
    env_reconnect_backoff_ms: Option<String>,
    env_max_reconnect_backoff_ms: Option<String>,
    env_batch_max_transactions: Option<String>,
    env_batch_flush_ms: Option<String>,
    env_include_votes: Option<String>,
    env_listen_address: Option<String>,
    env_buffer_capacity: Option<String>,
    env_heartbeat_ttl_ms: Option<String>,
) -> Result<BridgeConfig, CliError>
where
    I: IntoIterator<Item = String>,
{
    let mut config = BridgeConfig::default();

    if let Some(value) = non_empty(env_yellowstone_endpoint) {
        config.yellowstone_endpoint = normalize_endpoint(&value);
    }
    if let Some(value) = non_empty(env_x_token) {
        config.x_token = Some(value);
    }
    if let Some(value) = non_empty(env_connect_timeout_ms) {
        config.connect_timeout_ms = parse_u64_flag("--connect-timeout-ms", &value)?;
    }
    if let Some(value) = non_empty(env_reconnect_backoff_ms) {
        config.reconnect_backoff_ms = parse_u64_flag("--reconnect-backoff-ms", &value)?;
    }
    if let Some(value) = non_empty(env_max_reconnect_backoff_ms) {
        config.max_reconnect_backoff_ms = parse_u64_flag("--max-reconnect-backoff-ms", &value)?;
    }
    if let Some(value) = non_empty(env_batch_max_transactions) {
        config.batch_max_transactions = parse_usize_flag("--batch-max-transactions", &value)?;
    }
    if let Some(value) = non_empty(env_batch_flush_ms) {
        config.batch_flush_ms = parse_u64_flag("--batch-flush-ms", &value)?;
    }
    if let Some(value) = non_empty(env_include_votes) {
        config.include_votes = parse_bool_flag("--include-votes", &value)?;
    }
    if let Some(value) = non_empty(env_listen_address) {
        config.proxy.listen_address = parse_socket_addr("--listen-address", &value)?;
    }
    if let Some(value) = non_empty(env_buffer_capacity) {
        config.proxy.buffer_capacity = parse_usize_flag("--buffer-capacity", &value)?;
    }
    if let Some(value) = non_empty(env_heartbeat_ttl_ms) {
        config.proxy.heartbeat_ttl_ms = parse_u32_flag("--heartbeat-ttl-ms", &value)?;
    }

    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--yellowstone-endpoint" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--yellowstone-endpoint"))?;
                config.yellowstone_endpoint = normalize_endpoint(&value);
            }
            "--x-token" => {
                let value = args.next().ok_or(CliError::MissingValue("--x-token"))?;
                config.x_token = non_empty(Some(value));
            }
            "--connect-timeout-ms" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--connect-timeout-ms"))?;
                config.connect_timeout_ms = parse_u64_flag("--connect-timeout-ms", &value)?;
            }
            "--reconnect-backoff-ms" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--reconnect-backoff-ms"))?;
                config.reconnect_backoff_ms = parse_u64_flag("--reconnect-backoff-ms", &value)?;
            }
            "--max-reconnect-backoff-ms" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--max-reconnect-backoff-ms"))?;
                config.max_reconnect_backoff_ms =
                    parse_u64_flag("--max-reconnect-backoff-ms", &value)?;
            }
            "--batch-max-transactions" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--batch-max-transactions"))?;
                config.batch_max_transactions =
                    parse_usize_flag("--batch-max-transactions", &value)?;
            }
            "--batch-flush-ms" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--batch-flush-ms"))?;
                config.batch_flush_ms = parse_u64_flag("--batch-flush-ms", &value)?;
            }
            "--include-votes" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--include-votes"))?;
                config.include_votes = parse_bool_flag("--include-votes", &value)?;
            }
            "--listen-address" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--listen-address"))?;
                config.proxy.listen_address = parse_socket_addr("--listen-address", &value)?;
            }
            "--buffer-capacity" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--buffer-capacity"))?;
                config.proxy.buffer_capacity = parse_usize_flag("--buffer-capacity", &value)?;
            }
            "--heartbeat-ttl-ms" => {
                let value = args
                    .next()
                    .ok_or(CliError::MissingValue("--heartbeat-ttl-ms"))?;
                config.proxy.heartbeat_ttl_ms = parse_u32_flag("--heartbeat-ttl-ms", &value)?;
            }
            other => return Err(CliError::UnknownArgument(other.to_string())),
        }
    }

    if config.proxy.buffer_capacity == 0 {
        return Err(CliError::InvalidValue {
            flag: "--buffer-capacity",
            value: "0".into(),
            detail: "must be positive",
        });
    }
    if config.proxy.heartbeat_ttl_ms == 0 {
        return Err(CliError::InvalidValue {
            flag: "--heartbeat-ttl-ms",
            value: "0".into(),
            detail: "must be positive",
        });
    }
    if config.batch_max_transactions == 0 {
        return Err(CliError::InvalidValue {
            flag: "--batch-max-transactions",
            value: "0".into(),
            detail: "must be positive",
        });
    }
    if config.batch_flush_ms == 0 {
        return Err(CliError::InvalidValue {
            flag: "--batch-flush-ms",
            value: "0".into(),
            detail: "must be positive",
        });
    }
    if config.max_reconnect_backoff_ms < config.reconnect_backoff_ms {
        return Err(CliError::InvalidValue {
            flag: "--max-reconnect-backoff-ms",
            value: config.max_reconnect_backoff_ms.to_string(),
            detail: "must be >= reconnect_backoff_ms",
        });
    }

    Ok(config)
}

fn normalize_endpoint(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
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

fn parse_u64_flag(flag: &'static str, value: &str) -> Result<u64, CliError> {
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

fn parse_bool_flag(flag: &'static str, value: &str) -> Result<bool, CliError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(CliError::InvalidValue {
            flag,
            value: value.to_string(),
            detail: "must be one of true/false/1/0/yes/no/on/off",
        }),
    }
}

#[derive(Debug, thiserror::Error)]
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
    use super::*;

    #[test]
    fn uses_default_bridge_config() {
        let config = config_from_parts(
            Vec::<String>::new(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(config.yellowstone_endpoint, "http://127.0.0.1:10000");
        assert_eq!(config.proxy.listen_address.to_string(), "127.0.0.1:50051");
        assert_eq!(config.proxy.buffer_capacity, 32_768);
        assert_eq!(config.batch_max_transactions, 128);
        assert!(!config.include_votes);
    }

    #[test]
    fn normalizes_endpoints_without_scheme() {
        let config = config_from_parts(
            vec!["--yellowstone-endpoint".into(), "127.0.0.1:1234".into()],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(config.yellowstone_endpoint, "http://127.0.0.1:1234");
    }

    #[test]
    fn rejects_zero_batch_size() {
        let error = config_from_parts(
            vec!["--batch-max-transactions".into(), "0".into()],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect_err("zero batch size should fail");

        assert!(error.to_string().contains("--batch-max-transactions"));
    }
}
