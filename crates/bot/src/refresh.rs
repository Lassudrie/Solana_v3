use base64::Engine;
use std::{
    collections::HashMap,
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};

use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::json;
use state::types::LookupTableSnapshot;

use crate::account_batcher::{
    GetMultipleAccountsBatcher, LookupTableCacheHandle, RpcAccountValue, RpcContext,
    decode_lookup_table,
};
use crate::config::AsyncRefreshConfig;
use crate::rpc::{RpcError, RpcRateLimitBackoff, rpc_call};

const MOCK_SCHEME: &str = "mock://";
const SPL_TOKEN_ACCOUNT_LEN: usize = 165;
const SPL_TOKEN_ACCOUNT_AMOUNT_OFFSET: usize = 64;
const SPL_TOKEN_ACCOUNT_STATE_OFFSET: usize = 108;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockhashRefresh {
    pub blockhash: String,
    pub slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotRefresh {
    pub slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupTablesRefresh {
    pub tables: Vec<LookupTableSnapshot>,
    pub slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletRefresh {
    pub balance_lamports: u64,
    pub ready: bool,
    pub slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTokenBalancesRefresh {
    pub balances: HashMap<String, u64>,
    pub slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshFailure {
    pub worker: &'static str,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AsyncRefreshSnapshot {
    pub blockhash: Option<BlockhashRefresh>,
    pub slot: Option<SlotRefresh>,
    pub lookup_tables: Option<LookupTablesRefresh>,
    pub wallet: Option<WalletRefresh>,
    pub source_token_balances: Option<SourceTokenBalancesRefresh>,
    pub failures: Vec<RefreshFailure>,
}

enum RefreshUpdate {
    Blockhash(BlockhashRefresh),
    Slot(SlotRefresh),
    LookupTables(LookupTablesRefresh),
    Wallet(WalletRefresh),
    SourceTokenBalances(SourceTokenBalancesRefresh),
    Failure(RefreshFailure),
}

pub struct AsyncStateRefresher {
    receiver: Receiver<RefreshUpdate>,
    lookup_table_cache: LookupTableCacheHandle,
}

impl AsyncStateRefresher {
    pub(crate) fn new(
        config: &AsyncRefreshConfig,
        rpc_http_endpoint: &str,
        wallet_pubkey: &str,
        lookup_table_keys: &[String],
        source_token_accounts: &[String],
        account_batcher: GetMultipleAccountsBatcher,
    ) -> Self {
        if !config.enabled
            || rpc_http_endpoint.is_empty()
            || rpc_http_endpoint.starts_with(MOCK_SCHEME)
        {
            return Self::disabled();
        }

        let (sender, receiver) = mpsc::channel();
        let lookup_table_cache = LookupTableCacheHandle::default();

        if config.blockhash_refresh_millis > 0 {
            spawn_refresher(
                sender.clone(),
                Duration::from_millis(config.blockhash_refresh_millis),
                "blockhash",
                RpcRefreshClient::new(rpc_http_endpoint),
                |client| client.latest_blockhash_update(),
            );
        }

        if config.slot_refresh_millis > 0 {
            spawn_refresher(
                sender.clone(),
                Duration::from_millis(config.slot_refresh_millis),
                "slot",
                RpcRefreshClient::new(rpc_http_endpoint),
                |client| client.latest_slot_update(),
            );
        }

        if config.alt_refresh_millis > 0 && !lookup_table_keys.is_empty() {
            let lookup_table_keys = lookup_table_keys.to_vec();
            let account_batcher = account_batcher.clone();
            let lookup_table_cache = lookup_table_cache.clone();
            spawn_refresher(
                sender.clone(),
                Duration::from_millis(config.alt_refresh_millis),
                "lookup_tables",
                RpcRefreshClient::new(rpc_http_endpoint),
                move |_client| {
                    lookup_tables_update(&account_batcher, &lookup_table_cache, &lookup_table_keys)
                },
            );
        }

        if config.wallet_refresh_millis > 0 && !wallet_pubkey.is_empty() {
            let wallet_pubkey = wallet_pubkey.to_owned();
            spawn_refresher(
                sender.clone(),
                Duration::from_millis(config.wallet_refresh_millis),
                "wallet",
                RpcRefreshClient::new(rpc_http_endpoint),
                move |client| client.wallet_balance_update(&wallet_pubkey),
            );
        }

        if config.wallet_refresh_millis > 0 && !source_token_accounts.is_empty() {
            let source_token_accounts = source_token_accounts.to_vec();
            let account_batcher = account_batcher.clone();
            spawn_refresher(
                sender,
                Duration::from_millis(config.wallet_refresh_millis),
                "source_token_balances",
                RpcRefreshClient::new(rpc_http_endpoint),
                move |_client| {
                    source_token_balances_update(&account_batcher, &source_token_accounts)
                },
            );
        }

        Self {
            receiver,
            lookup_table_cache,
        }
    }

    pub(crate) fn disabled() -> Self {
        let (_sender, receiver) = mpsc::channel();
        Self {
            receiver,
            lookup_table_cache: LookupTableCacheHandle::default(),
        }
    }

    pub(crate) fn drain_snapshot(&self) -> AsyncRefreshSnapshot {
        let mut snapshot = AsyncRefreshSnapshot::default();
        while let Ok(update) = self.receiver.try_recv() {
            match update {
                RefreshUpdate::Blockhash(blockhash) => snapshot.blockhash = Some(blockhash),
                RefreshUpdate::Slot(slot) => snapshot.slot = Some(slot),
                RefreshUpdate::LookupTables(lookup_tables) => {
                    snapshot.lookup_tables = Some(lookup_tables);
                }
                RefreshUpdate::Wallet(wallet) => snapshot.wallet = Some(wallet),
                RefreshUpdate::SourceTokenBalances(source_token_balances) => {
                    snapshot.source_token_balances = Some(source_token_balances);
                }
                RefreshUpdate::Failure(failure) => snapshot.failures.push(failure),
            }
        }
        snapshot
    }

    pub(crate) fn lookup_table_cache_handle(&self) -> LookupTableCacheHandle {
        self.lookup_table_cache.clone()
    }
}

#[derive(Clone)]
struct RpcRefreshClient {
    endpoint: String,
    http: Client,
}

impl RpcRefreshClient {
    fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.into(),
            http: Client::builder()
                .connect_timeout(Duration::from_millis(300))
                .timeout(Duration::from_millis(1_000))
                .build()
                .expect("async refresh HTTP client should build"),
        }
    }

    fn latest_blockhash_update(&self) -> Result<RefreshUpdate, String> {
        let body = self.rpc::<LatestBlockhashResult>(
            "getLatestBlockhash",
            json!([{
                "commitment": "processed"
            }]),
        )?;
        Ok(RefreshUpdate::Blockhash(BlockhashRefresh {
            blockhash: body.value.blockhash,
            slot: body.context.slot,
        }))
    }

    fn latest_slot_update(&self) -> Result<RefreshUpdate, String> {
        let body = self.rpc::<u64>(
            "getSlot",
            json!([{
                "commitment": "processed"
            }]),
        )?;
        Ok(RefreshUpdate::Slot(SlotRefresh { slot: body }))
    }

    fn wallet_balance_update(&self, wallet_pubkey: &str) -> Result<RefreshUpdate, String> {
        let body = self.rpc::<BalanceResult>(
            "getBalance",
            json!([
                wallet_pubkey,
                { "commitment": "processed" }
            ]),
        )?;
        Ok(RefreshUpdate::Wallet(WalletRefresh {
            balance_lamports: body.value,
            ready: true,
            slot: body.context.slot,
        }))
    }

    fn rpc<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, String> {
        rpc_call(&self.http, &self.endpoint, method, params).map_err(|error| error.to_string())
    }
}

fn lookup_tables_update(
    account_batcher: &GetMultipleAccountsBatcher,
    lookup_table_cache: &LookupTableCacheHandle,
    lookup_table_keys: &[String],
) -> Result<RefreshUpdate, String> {
    let fetched = account_batcher
        .fetch(lookup_table_keys)
        .map_err(|error| error.to_string())?;
    let slot = fetched.slot;
    let tables = lookup_table_keys
        .iter()
        .zip(fetched.ordered_values(lookup_table_keys))
        .filter_map(|(account_key, account)| decode_lookup_table(account_key, account, slot))
        .collect::<Vec<_>>();
    lookup_table_cache.merge(slot, tables.clone());
    Ok(RefreshUpdate::LookupTables(LookupTablesRefresh {
        tables,
        slot,
    }))
}

fn source_token_balances_update(
    account_batcher: &GetMultipleAccountsBatcher,
    account_keys: &[String],
) -> Result<RefreshUpdate, String> {
    let fetched = account_batcher
        .fetch(account_keys)
        .map_err(|error| error.to_string())?;
    let mut balances = HashMap::with_capacity(account_keys.len());
    for account_key in account_keys {
        let Some(account) = fetched.values_by_key.get(account_key).cloned().flatten() else {
            continue;
        };
        let amount = parse_spl_token_amount(&account)?;
        balances.insert(account_key.clone(), amount);
    }
    Ok(RefreshUpdate::SourceTokenBalances(
        SourceTokenBalancesRefresh {
            balances,
            slot: fetched.slot,
        },
    ))
}

fn parse_spl_token_amount(account: &RpcAccountValue) -> Result<u64, String> {
    if account.data.1 != "base64" {
        return Err(format!("unsupported account encoding {}", account.data.1));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(account.data.0.as_bytes())
        .map_err(|error| error.to_string())?;
    if bytes.len() < SPL_TOKEN_ACCOUNT_LEN {
        return Err(format!(
            "account data too short: expected at least {SPL_TOKEN_ACCOUNT_LEN} bytes, got {}",
            bytes.len()
        ));
    }
    if bytes[SPL_TOKEN_ACCOUNT_STATE_OFFSET] != 1 {
        return Err(format!(
            "token account is not initialized (state={})",
            bytes[SPL_TOKEN_ACCOUNT_STATE_OFFSET]
        ));
    }
    let mut amount_bytes = [0u8; 8];
    amount_bytes.copy_from_slice(
        &bytes[SPL_TOKEN_ACCOUNT_AMOUNT_OFFSET..SPL_TOKEN_ACCOUNT_AMOUNT_OFFSET + 8],
    );
    Ok(u64::from_le_bytes(amount_bytes))
}

fn spawn_refresher<F>(
    sender: mpsc::Sender<RefreshUpdate>,
    interval: Duration,
    worker: &'static str,
    client: RpcRefreshClient,
    mut fetch: F,
) where
    F: FnMut(&RpcRefreshClient) -> Result<RefreshUpdate, String> + Send + 'static,
{
    thread::spawn(move || {
        let mut backoff = RpcRateLimitBackoff::default();
        loop {
            match fetch(&client) {
                Ok(update) => {
                    backoff.on_success();
                    if sender.send(update).is_err() {
                        break;
                    }
                }
                Err(detail) => {
                    eprintln!("async refresh {worker} failed: {detail}");
                    let backoff_delay = if detail.contains("rate limited") {
                        backoff.on_error(&RpcError::RateLimited {
                            method: worker.into(),
                            detail: detail.clone(),
                        })
                    } else {
                        Duration::ZERO
                    };
                    if sender
                        .send(RefreshUpdate::Failure(RefreshFailure { worker, detail }))
                        .is_err()
                    {
                        break;
                    }
                    if !backoff_delay.is_zero() {
                        thread::sleep(backoff_delay);
                    }
                }
            }
            thread::sleep(interval);
        }
    });
}

#[derive(Debug, Deserialize)]
struct LatestBlockhashResult {
    context: RpcContext,
    value: LatestBlockhashValue,
}

#[derive(Debug, Deserialize)]
struct LatestBlockhashValue {
    blockhash: String,
}

#[derive(Debug, Deserialize)]
struct BalanceResult {
    context: RpcContext,
    value: u64,
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::{
        AsyncRefreshSnapshot, AsyncStateRefresher, LookupTableCacheHandle, RefreshFailure,
        RefreshUpdate, SlotRefresh,
    };

    #[test]
    fn drain_snapshot_keeps_latest_slot_refresh() {
        let (sender, receiver) = mpsc::channel();
        let refresher = AsyncStateRefresher {
            receiver,
            lookup_table_cache: LookupTableCacheHandle::default(),
        };

        sender
            .send(RefreshUpdate::Slot(SlotRefresh { slot: 41 }))
            .unwrap();
        sender
            .send(RefreshUpdate::Slot(SlotRefresh { slot: 42 }))
            .unwrap();

        assert_eq!(
            refresher.drain_snapshot(),
            AsyncRefreshSnapshot {
                blockhash: None,
                slot: Some(SlotRefresh { slot: 42 }),
                lookup_tables: None,
                wallet: None,
                source_token_balances: None,
                failures: Vec::new(),
            }
        );
    }

    #[test]
    fn drain_snapshot_collects_refresh_failures() {
        let (sender, receiver) = mpsc::channel();
        let refresher = AsyncStateRefresher {
            receiver,
            lookup_table_cache: LookupTableCacheHandle::default(),
        };

        sender
            .send(RefreshUpdate::Failure(RefreshFailure {
                worker: "slot",
                detail: "getSlot request failed".into(),
            }))
            .unwrap();

        assert_eq!(
            refresher.drain_snapshot().failures,
            vec![RefreshFailure {
                worker: "slot",
                detail: "getSlot request failed".into(),
            }]
        );
    }
}
