use std::{
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::json;
use solana_address_lookup_table_interface::{program as alt_program, state::AddressLookupTable};
use state::types::LookupTableSnapshot;

use crate::config::AsyncRefreshConfig;

const JSON_RPC_VERSION: &str = "2.0";
const MOCK_SCHEME: &str = "mock://";

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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AsyncRefreshSnapshot {
    pub blockhash: Option<BlockhashRefresh>,
    pub slot: Option<SlotRefresh>,
    pub lookup_tables: Option<LookupTablesRefresh>,
    pub wallet: Option<WalletRefresh>,
}

enum RefreshUpdate {
    Blockhash(BlockhashRefresh),
    Slot(SlotRefresh),
    LookupTables(LookupTablesRefresh),
    Wallet(WalletRefresh),
}

pub struct AsyncStateRefresher {
    receiver: Receiver<RefreshUpdate>,
}

impl AsyncStateRefresher {
    pub fn new(
        config: &AsyncRefreshConfig,
        rpc_http_endpoint: &str,
        wallet_pubkey: &str,
        lookup_table_keys: &[String],
    ) -> Self {
        if !config.enabled
            || rpc_http_endpoint.is_empty()
            || rpc_http_endpoint.starts_with(MOCK_SCHEME)
        {
            return Self::disabled();
        }

        let (sender, receiver) = mpsc::channel();

        if config.blockhash_refresh_millis > 0 {
            spawn_refresher(
                sender.clone(),
                Duration::from_millis(config.blockhash_refresh_millis),
                RpcRefreshClient::new(rpc_http_endpoint),
                |client| client.latest_blockhash_update(),
            );
        }

        if config.slot_refresh_millis > 0 {
            spawn_refresher(
                sender.clone(),
                Duration::from_millis(config.slot_refresh_millis),
                RpcRefreshClient::new(rpc_http_endpoint),
                |client| client.latest_slot_update(),
            );
        }

        if config.alt_refresh_millis > 0 && !lookup_table_keys.is_empty() {
            let lookup_table_keys = lookup_table_keys.to_vec();
            spawn_refresher(
                sender.clone(),
                Duration::from_millis(config.alt_refresh_millis),
                RpcRefreshClient::new(rpc_http_endpoint),
                move |client| client.lookup_tables_update(&lookup_table_keys),
            );
        }

        if config.wallet_refresh_millis > 0 && !wallet_pubkey.is_empty() {
            let wallet_pubkey = wallet_pubkey.to_owned();
            spawn_refresher(
                sender,
                Duration::from_millis(config.wallet_refresh_millis),
                RpcRefreshClient::new(rpc_http_endpoint),
                move |client| client.wallet_balance_update(&wallet_pubkey),
            );
        }

        Self { receiver }
    }

    pub fn disabled() -> Self {
        let (_sender, receiver) = mpsc::channel();
        Self { receiver }
    }

    pub fn drain_snapshot(&self) -> AsyncRefreshSnapshot {
        let mut snapshot = AsyncRefreshSnapshot::default();
        while let Ok(update) = self.receiver.try_recv() {
            match update {
                RefreshUpdate::Blockhash(blockhash) => snapshot.blockhash = Some(blockhash),
                RefreshUpdate::Slot(slot) => snapshot.slot = Some(slot),
                RefreshUpdate::LookupTables(lookup_tables) => {
                    snapshot.lookup_tables = Some(lookup_tables);
                }
                RefreshUpdate::Wallet(wallet) => snapshot.wallet = Some(wallet),
            }
        }
        snapshot
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

    fn latest_blockhash_update(&self) -> Option<RefreshUpdate> {
        let body = self.rpc::<LatestBlockhashEnvelope>(
            "getLatestBlockhash",
            json!([{
                "commitment": "processed"
            }]),
        )?;
        Some(RefreshUpdate::Blockhash(BlockhashRefresh {
            blockhash: body.result.value.blockhash,
            slot: body.result.context.slot,
        }))
    }

    fn latest_slot_update(&self) -> Option<RefreshUpdate> {
        let body = self.rpc::<SlotEnvelope>(
            "getSlot",
            json!([{
                "commitment": "processed"
            }]),
        )?;
        Some(RefreshUpdate::Slot(SlotRefresh { slot: body.result }))
    }

    fn lookup_tables_update(&self, lookup_table_keys: &[String]) -> Option<RefreshUpdate> {
        let body = self.rpc::<MultipleAccountsEnvelope>(
            "getMultipleAccounts",
            json!([
                lookup_table_keys,
                {
                    "commitment": "processed",
                    "encoding": "base64"
                }
            ]),
        )?;
        let slot = body.result.context.slot;
        let tables = lookup_table_keys
            .iter()
            .zip(body.result.value)
            .filter_map(|(account_key, account)| decode_lookup_table(account_key, account, slot))
            .collect();
        Some(RefreshUpdate::LookupTables(LookupTablesRefresh {
            tables,
            slot,
        }))
    }

    fn wallet_balance_update(&self, wallet_pubkey: &str) -> Option<RefreshUpdate> {
        let body = self.rpc::<BalanceEnvelope>(
            "getBalance",
            json!([
                wallet_pubkey,
                { "commitment": "processed" }
            ]),
        )?;
        Some(RefreshUpdate::Wallet(WalletRefresh {
            balance_lamports: body.result.value,
            ready: true,
            slot: body.result.context.slot,
        }))
    }

    fn rpc<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Option<T> {
        self.http
            .post(&self.endpoint)
            .json(&json!({
                "jsonrpc": JSON_RPC_VERSION,
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .ok()?
            .json::<T>()
            .ok()
    }
}

fn spawn_refresher<F>(
    sender: mpsc::Sender<RefreshUpdate>,
    interval: Duration,
    client: RpcRefreshClient,
    mut fetch: F,
) where
    F: FnMut(&RpcRefreshClient) -> Option<RefreshUpdate> + Send + 'static,
{
    thread::spawn(move || {
        loop {
            if let Some(update) = fetch(&client) {
                let _ = sender.send(update);
            }
            thread::sleep(interval);
        }
    });
}

#[derive(Debug, Deserialize)]
struct LatestBlockhashEnvelope {
    result: LatestBlockhashResult,
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
struct SlotEnvelope {
    result: u64,
}

#[derive(Debug, Deserialize)]
struct MultipleAccountsEnvelope {
    result: MultipleAccountsResult,
}

#[derive(Debug, Deserialize)]
struct MultipleAccountsResult {
    context: RpcContext,
    value: Vec<Option<RpcAccountValue>>,
}

#[derive(Debug, Deserialize)]
struct RpcAccountValue {
    data: (String, String),
    owner: String,
}

#[derive(Debug, Deserialize)]
struct BalanceEnvelope {
    result: BalanceResult,
}

#[derive(Debug, Deserialize)]
struct BalanceResult {
    context: RpcContext,
    value: u64,
}

#[derive(Debug, Deserialize)]
struct RpcContext {
    slot: u64,
}

fn decode_lookup_table(
    account_key: &str,
    account: Option<RpcAccountValue>,
    fetched_slot: u64,
) -> Option<LookupTableSnapshot> {
    let account = account?;
    if account.owner != alt_program::id().to_string() {
        return None;
    }
    if account.data.1 != "base64" {
        return None;
    }

    let raw = BASE64_STANDARD.decode(account.data.0).ok()?;
    let table = AddressLookupTable::deserialize(&raw).ok()?;
    Some(LookupTableSnapshot {
        account_key: account_key.to_string(),
        addresses: table.addresses.iter().map(ToString::to_string).collect(),
        last_extended_slot: table.meta.last_extended_slot,
        fetched_slot,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::{AsyncRefreshSnapshot, AsyncStateRefresher, RefreshUpdate, SlotRefresh};

    #[test]
    fn drain_snapshot_keeps_latest_slot_refresh() {
        let (sender, receiver) = mpsc::channel();
        let refresher = AsyncStateRefresher { receiver };

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
            }
        );
    }
}
