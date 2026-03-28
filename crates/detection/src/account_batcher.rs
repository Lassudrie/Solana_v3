use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, RwLock,
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    thread,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use domain::LookupTableSnapshot;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::json;
use solana_address_lookup_table_interface::{program as alt_program, state::AddressLookupTable};

use crate::rpc::{RpcError, rpc_call};

const MOCK_SCHEME: &str = "mock://";
const DEFAULT_GET_MULTIPLE_ACCOUNTS_BATCH_WINDOW: Duration = Duration::from_millis(20);
const MAX_GET_MULTIPLE_ACCOUNTS_KEYS: usize = 100;
const DEFAULT_GET_MULTIPLE_ACCOUNTS_PARALLEL_RPC_REQUESTS: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RpcContext {
    pub slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RpcAccountValue {
    pub data: (String, String),
    pub owner: String,
    #[serde(default)]
    pub lamports: u64,
}

#[derive(Debug, Deserialize)]
struct MultipleAccountsResult {
    context: RpcContext,
    value: Vec<Option<RpcAccountValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedAccounts {
    pub slot: u64,
    pub values_by_key: HashMap<String, Option<RpcAccountValue>>,
}

impl FetchedAccounts {
    pub fn ordered_values(&self, account_keys: &[String]) -> Vec<Option<RpcAccountValue>> {
        account_keys
            .iter()
            .map(|key| self.values_by_key.get(key).cloned().unwrap_or(None))
            .collect()
    }

    pub fn present_accounts(&self) -> HashMap<String, RpcAccountValue> {
        self.values_by_key
            .iter()
            .filter_map(|(key, value)| value.clone().map(|account| (key.clone(), account)))
            .collect()
    }

    fn subset_for(&self, account_keys: &[String]) -> Self {
        let values_by_key = account_keys
            .iter()
            .map(|key| {
                (
                    key.clone(),
                    self.values_by_key.get(key).cloned().unwrap_or(None),
                )
            })
            .collect();
        Self {
            slot: self.slot,
            values_by_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LookupTableCacheSnapshot {
    pub revision: u64,
    pub tables_vec: Vec<LookupTableSnapshot>,
    pub tables_by_key: HashMap<String, LookupTableSnapshot>,
}

#[derive(Debug, Clone)]
pub struct LookupTableCacheHandle {
    snapshot: Arc<RwLock<Arc<LookupTableCacheSnapshot>>>,
}

impl Default for LookupTableCacheHandle {
    fn default() -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(Arc::new(LookupTableCacheSnapshot::default()))),
        }
    }
}

impl LookupTableCacheHandle {
    pub fn snapshot(&self) -> Arc<LookupTableCacheSnapshot> {
        self.snapshot
            .read()
            .ok()
            .map(|guard| Arc::clone(&guard))
            .unwrap_or_else(|| Arc::new(LookupTableCacheSnapshot::default()))
    }

    pub fn replace(&self, revision: u64, tables: Vec<LookupTableSnapshot>) {
        let tables_by_key = tables
            .iter()
            .cloned()
            .map(|table| (table.account_key.clone(), table))
            .collect::<HashMap<_, _>>();
        let next = Arc::new(LookupTableCacheSnapshot {
            revision,
            tables_vec: tables,
            tables_by_key,
        });
        if let Ok(mut guard) = self.snapshot.write() {
            *guard = next;
        }
    }

    pub fn merge(&self, revision: u64, tables: Vec<LookupTableSnapshot>) {
        let current = self.snapshot();
        let mut tables_by_key = current.tables_by_key.clone();
        for table in tables {
            let should_replace = tables_by_key
                .get(&table.account_key)
                .map(|existing| {
                    (table.fetched_slot, table.last_extended_slot)
                        >= (existing.fetched_slot, existing.last_extended_slot)
                })
                .unwrap_or(true);
            if should_replace {
                tables_by_key.insert(table.account_key.clone(), table);
            }
        }
        let mut tables_vec = tables_by_key.values().cloned().collect::<Vec<_>>();
        tables_vec.sort_by(|left, right| left.account_key.cmp(&right.account_key));
        let next = Arc::new(LookupTableCacheSnapshot {
            revision: revision.max(current.revision),
            tables_vec,
            tables_by_key,
        });
        if let Ok(mut guard) = self.snapshot.write() {
            *guard = next;
        }
    }
}

#[derive(Clone)]
pub struct GetMultipleAccountsBatcher {
    sender: Sender<BatchRequest>,
}

impl GetMultipleAccountsBatcher {
    pub fn new(endpoint: &str) -> Self {
        Self::new_with_window_and_parallel_rpc_requests(
            endpoint,
            DEFAULT_GET_MULTIPLE_ACCOUNTS_BATCH_WINDOW,
            DEFAULT_GET_MULTIPLE_ACCOUNTS_PARALLEL_RPC_REQUESTS,
        )
    }

    pub fn new_with_window(endpoint: &str, batch_window: Duration) -> Self {
        Self::new_with_window_and_parallel_rpc_requests(
            endpoint,
            batch_window,
            DEFAULT_GET_MULTIPLE_ACCOUNTS_PARALLEL_RPC_REQUESTS,
        )
    }

    pub fn new_with_window_and_parallel_rpc_requests(
        endpoint: &str,
        batch_window: Duration,
        parallel_rpc_requests: usize,
    ) -> Self {
        let (sender, receiver) = mpsc::channel();
        let endpoint = endpoint.to_owned();
        let parallel_rpc_requests = parallel_rpc_requests.max(1);
        thread::spawn(move || run_batcher(endpoint, receiver, batch_window, parallel_rpc_requests));
        Self { sender }
    }

    pub fn fetch(&self, account_keys: &[String]) -> Result<FetchedAccounts, RpcError> {
        self.fetch_with_min_context_slot(account_keys, None)
    }

    pub fn fetch_with_min_context_slot(
        &self,
        account_keys: &[String],
        min_context_slot: Option<u64>,
    ) -> Result<FetchedAccounts, RpcError> {
        if account_keys.is_empty() {
            return Err(RpcError::RequestFailed {
                method: "getMultipleAccounts".into(),
                detail: "empty account set".into(),
            });
        }
        let (response_tx, response_rx) = mpsc::channel();
        self.sender
            .send(BatchRequest {
                account_keys: account_keys.to_vec(),
                min_context_slot,
                response_tx,
            })
            .map_err(|_| RpcError::RequestFailed {
                method: "getMultipleAccounts".into(),
                detail: "batch worker unavailable".into(),
            })?;
        response_rx.recv().map_err(|_| RpcError::RequestFailed {
            method: "getMultipleAccounts".into(),
            detail: "batch worker dropped response".into(),
        })?
    }
}

pub fn decode_lookup_table(
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

struct BatchRequest {
    account_keys: Vec<String>,
    min_context_slot: Option<u64>,
    response_tx: Sender<Result<FetchedAccounts, RpcError>>,
}

fn run_batcher(
    endpoint: String,
    receiver: Receiver<BatchRequest>,
    batch_window: Duration,
    parallel_rpc_requests: usize,
) {
    let http = Client::builder()
        .connect_timeout(Duration::from_millis(300))
        .timeout(Duration::from_millis(1_000))
        .build()
        .expect("getMultipleAccounts batch HTTP client should build");

    loop {
        let first_request = match receiver.recv() {
            Ok(request) => request,
            Err(_) => break,
        };
        let mut pending = vec![first_request];
        let deadline = Instant::now() + batch_window;
        let mut disconnected = false;

        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match receiver.recv_timeout(deadline.saturating_duration_since(now)) {
                Ok(request) => pending.push(request),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        let mut groups = BTreeMap::<Option<u64>, Vec<BatchRequest>>::new();
        for request in pending {
            groups
                .entry(request.min_context_slot)
                .or_default()
                .push(request);
        }

        for group in groups.into_values() {
            let result = fetch_batch(&http, &endpoint, &group, parallel_rpc_requests);
            for request in group {
                let response = result
                    .as_ref()
                    .map(|fetched| fetched.subset_for(&request.account_keys))
                    .map_err(Clone::clone);
                let _ = request.response_tx.send(response);
            }
        }

        if disconnected {
            break;
        }
    }
}

fn fetch_batch(
    http: &Client,
    endpoint: &str,
    requests: &[BatchRequest],
    parallel_rpc_requests: usize,
) -> Result<FetchedAccounts, RpcError> {
    if endpoint.is_empty() || endpoint.starts_with(MOCK_SCHEME) {
        return Err(RpcError::RequestFailed {
            method: "getMultipleAccounts".into(),
            detail: "mock endpoint".into(),
        });
    }

    let account_keys = requests
        .iter()
        .flat_map(|request| request.account_keys.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let min_context_slot = requests
        .iter()
        .filter_map(|request| request.min_context_slot)
        .max();
    if account_keys.is_empty() {
        return Err(RpcError::RequestFailed {
            method: "getMultipleAccounts".into(),
            detail: "empty account set".into(),
        });
    }

    let mut slot = 0u64;
    let mut values_by_key = HashMap::with_capacity(account_keys.len());
    let parallel_rpc_requests = parallel_rpc_requests.max(1);
    let chunks = account_keys
        .chunks(MAX_GET_MULTIPLE_ACCOUNTS_KEYS)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>();

    for chunk_group in chunks.chunks(parallel_rpc_requests) {
        let results = thread::scope(|scope| {
            let mut handles = Vec::with_capacity(chunk_group.len());
            for chunk in chunk_group {
                let http = http.clone();
                let endpoint = endpoint.to_string();
                let chunk = chunk.clone();
                handles.push(scope.spawn(move || {
                    fetch_accounts_chunk(&http, &endpoint, chunk, min_context_slot)
                }));
            }

            let mut results = Vec::with_capacity(handles.len());
            for handle in handles {
                let result = handle.join().map_err(|_| RpcError::RequestFailed {
                    method: "getMultipleAccounts".into(),
                    detail: "parallel getMultipleAccounts worker panicked".into(),
                })??;
                results.push(result);
            }
            Ok::<Vec<_>, RpcError>(results)
        })?;

        for (chunk_keys, response) in results {
            slot = slot.max(response.context.slot);
            for (key, value) in chunk_keys.into_iter().zip(response.value) {
                values_by_key.insert(key, value);
            }
        }
    }

    Ok(FetchedAccounts {
        slot,
        values_by_key,
    })
}

fn fetch_accounts_chunk(
    http: &Client,
    endpoint: &str,
    chunk: Vec<String>,
    min_context_slot: Option<u64>,
) -> Result<(Vec<String>, MultipleAccountsResult), RpcError> {
    let response = rpc_call::<MultipleAccountsResult>(
        http,
        endpoint,
        "getMultipleAccounts",
        json!([
            &chunk,
            {
                "commitment": "processed",
                "encoding": "base64",
                "minContextSlot": min_context_slot
            }
        ]),
    )?;
    Ok((chunk, response))
}

#[cfg(test)]
mod tests {
    use super::{GetMultipleAccountsBatcher, LookupTableCacheHandle, RpcError};
    use domain::LookupTableSnapshot;
    use serde_json::{Value, json};
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{
            Arc, Barrier, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::Duration,
    };

    #[test]
    fn batches_overlapping_requests_into_one_rpc_call() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let requested_keys = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
        let endpoint = spawn_mock_rpc_server({
            let call_count = Arc::clone(&call_count);
            let requested_keys = Arc::clone(&requested_keys);
            move |body| {
                call_count.fetch_add(1, Ordering::SeqCst);
                let payload: Value = serde_json::from_str(body).expect("json-rpc body");
                let keys = payload["params"][0]
                    .as_array()
                    .expect("keys array")
                    .iter()
                    .map(|value| value.as_str().expect("key").to_string())
                    .collect::<Vec<_>>();
                requested_keys
                    .lock()
                    .expect("requested keys")
                    .push(keys.clone());
                multiple_accounts_response(42, &keys)
            }
        });
        let batcher = GetMultipleAccountsBatcher::new(&endpoint);
        let barrier = Arc::new(Barrier::new(3));

        let first = {
            let batcher = batcher.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                batcher.fetch(&["acct-a".into(), "acct-b".into()])
            })
        };
        let second = {
            let batcher = batcher.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                thread::sleep(Duration::from_millis(5));
                batcher.fetch(&["acct-b".into(), "acct-c".into()])
            })
        };
        barrier.wait();

        let first = first.join().expect("first fetch").expect("first result");
        let second = second.join().expect("second fetch").expect("second result");

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            requested_keys.lock().expect("requested keys")[0],
            vec![
                "acct-a".to_string(),
                "acct-b".to_string(),
                "acct-c".to_string()
            ]
        );
        assert_eq!(first.values_by_key.len(), 2);
        assert_eq!(second.values_by_key.len(), 2);
    }

    #[test]
    fn splits_large_requests_into_rpc_chunks() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let chunk_sizes = Arc::new(Mutex::new(Vec::<usize>::new()));
        let endpoint = spawn_mock_rpc_server({
            let call_count = Arc::clone(&call_count);
            let chunk_sizes = Arc::clone(&chunk_sizes);
            move |body| {
                call_count.fetch_add(1, Ordering::SeqCst);
                let payload: Value = serde_json::from_str(body).expect("json-rpc body");
                let keys = payload["params"][0]
                    .as_array()
                    .expect("keys array")
                    .iter()
                    .map(|value| value.as_str().expect("key").to_string())
                    .collect::<Vec<_>>();
                chunk_sizes.lock().expect("chunk sizes").push(keys.len());
                multiple_accounts_response(7, &keys)
            }
        });
        let batcher = GetMultipleAccountsBatcher::new(&endpoint);
        let keys = (0..101)
            .map(|index| format!("acct-{index:03}"))
            .collect::<Vec<_>>();

        let result = batcher.fetch(&keys).expect("batched fetch");

        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        assert_eq!(
            chunk_sizes.lock().expect("chunk sizes").as_slice(),
            &[100, 1]
        );
        assert_eq!(result.values_by_key.len(), 101);
    }

    #[test]
    fn large_requests_can_fetch_rpc_chunks_in_parallel() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let current_in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_mock_rpc_server({
            let call_count = Arc::clone(&call_count);
            let current_in_flight = Arc::clone(&current_in_flight);
            let max_in_flight = Arc::clone(&max_in_flight);
            move |body| {
                call_count.fetch_add(1, Ordering::SeqCst);
                let in_flight = current_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                let mut observed = max_in_flight.load(Ordering::SeqCst);
                while in_flight > observed
                    && max_in_flight
                        .compare_exchange(observed, in_flight, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                {
                    observed = max_in_flight.load(Ordering::SeqCst);
                }
                thread::sleep(Duration::from_millis(50));
                current_in_flight.fetch_sub(1, Ordering::SeqCst);

                let payload: Value = serde_json::from_str(body).expect("json-rpc body");
                let keys = payload["params"][0]
                    .as_array()
                    .expect("keys array")
                    .iter()
                    .map(|value| value.as_str().expect("key").to_string())
                    .collect::<Vec<_>>();
                multiple_accounts_response(9, &keys)
            }
        });
        let batcher = GetMultipleAccountsBatcher::new_with_window_and_parallel_rpc_requests(
            &endpoint,
            Duration::from_millis(1),
            2,
        );
        let keys = (0..200)
            .map(|index| format!("acct-{index:03}"))
            .collect::<Vec<_>>();

        let result = batcher.fetch(&keys).expect("parallel batched fetch");

        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        assert_eq!(result.values_by_key.len(), 200);
        assert!(
            max_in_flight.load(Ordering::SeqCst) >= 2,
            "expected overlapping getMultipleAccounts chunk requests"
        );
    }

    #[test]
    fn fans_out_rate_limit_errors_to_all_waiters() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_mock_rpc_server({
            let call_count = Arc::clone(&call_count);
            move |_body| {
                call_count.fetch_add(1, Ordering::SeqCst);
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32429,"message":"rate limited"}}"#
                    .to_string()
            }
        });
        let batcher = GetMultipleAccountsBatcher::new(&endpoint);
        let barrier = Arc::new(Barrier::new(3));

        let first = {
            let batcher = batcher.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                batcher.fetch(&["acct-a".into()])
            })
        };
        let second = {
            let batcher = batcher.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                batcher.fetch(&["acct-b".into()])
            })
        };
        barrier.wait();

        let first = first.join().expect("first join").expect_err("first error");
        let second = second
            .join()
            .expect("second join")
            .expect_err("second error");

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert!(matches!(first, RpcError::RateLimited { .. }));
        assert!(matches!(second, RpcError::RateLimited { .. }));
    }

    #[test]
    fn lookup_table_cache_replaces_snapshot_atomically() {
        let cache = LookupTableCacheHandle::default();
        cache.replace(
            11,
            vec![LookupTableSnapshot {
                account_key: "table-a".into(),
                addresses: vec!["addr-1".into(), "addr-2".into()],
                last_extended_slot: 9,
                fetched_slot: 11,
            }],
        );

        let snapshot = cache.snapshot();

        assert_eq!(snapshot.revision, 11);
        assert_eq!(snapshot.tables_vec.len(), 1);
        assert_eq!(
            snapshot.tables_by_key.get("table-a"),
            Some(&LookupTableSnapshot {
                account_key: "table-a".into(),
                addresses: vec!["addr-1".into(), "addr-2".into()],
                last_extended_slot: 9,
                fetched_slot: 11,
            })
        );
    }

    #[test]
    fn separates_requests_with_distinct_min_context_slots() {
        let seen_min_context_slot = Arc::new(Mutex::new(Vec::<Option<u64>>::new()));
        let endpoint = spawn_mock_rpc_server({
            let seen_min_context_slot = Arc::clone(&seen_min_context_slot);
            move |body| {
                let payload: Value = serde_json::from_str(body).expect("json-rpc body");
                let min_context_slot = payload["params"][1]["minContextSlot"].as_u64();
                seen_min_context_slot
                    .lock()
                    .expect("min context slots")
                    .push(min_context_slot);
                let keys = payload["params"][0]
                    .as_array()
                    .expect("keys array")
                    .iter()
                    .map(|value| value.as_str().expect("key").to_string())
                    .collect::<Vec<_>>();
                multiple_accounts_response(42, &keys)
            }
        });
        let batcher = GetMultipleAccountsBatcher::new(&endpoint);
        let barrier = Arc::new(Barrier::new(3));

        let first = {
            let batcher = batcher.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                batcher.fetch_with_min_context_slot(&["acct-a".into()], Some(40))
            })
        };
        let second = {
            let batcher = batcher.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                thread::sleep(Duration::from_millis(5));
                batcher.fetch_with_min_context_slot(&["acct-b".into()], Some(45))
            })
        };
        barrier.wait();

        first.join().expect("first join").expect("first result");
        second.join().expect("second join").expect("second result");

        assert_eq!(
            seen_min_context_slot
                .lock()
                .expect("min context slots")
                .as_slice(),
            &[Some(40), Some(45)]
        );
    }

    #[test]
    fn lookup_table_cache_merge_preserves_existing_entries() {
        let cache = LookupTableCacheHandle::default();
        cache.replace(
            11,
            vec![LookupTableSnapshot {
                account_key: "table-a".into(),
                addresses: vec!["addr-1".into(), "addr-2".into()],
                last_extended_slot: 9,
                fetched_slot: 11,
            }],
        );
        cache.merge(
            12,
            vec![LookupTableSnapshot {
                account_key: "table-b".into(),
                addresses: vec!["addr-3".into()],
                last_extended_slot: 12,
                fetched_slot: 12,
            }],
        );

        let snapshot = cache.snapshot();

        assert_eq!(snapshot.revision, 12);
        assert_eq!(snapshot.tables_vec.len(), 2);
        assert!(snapshot.tables_by_key.contains_key("table-a"));
        assert!(snapshot.tables_by_key.contains_key("table-b"));
    }

    fn multiple_accounts_response(slot: u64, keys: &[String]) -> String {
        let values = keys
            .iter()
            .map(|key| {
                json!({
                    "lamports": key.len() as u64,
                    "owner": "11111111111111111111111111111111",
                    "data": ["", "base64"],
                    "executable": false,
                    "rentEpoch": 0,
                    "space": 0
                })
            })
            .collect::<Vec<_>>();
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "context": { "slot": slot },
                "value": values
            }
        })
        .to_string()
    }

    fn spawn_mock_rpc_server<F>(handler: F) -> String
    where
        F: Fn(&str) -> String + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock rpc server");
        let address = listener.local_addr().expect("mock rpc address");
        let handler = Arc::new(handler);

        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else {
                    continue;
                };
                let handler = Arc::clone(&handler);
                thread::spawn(move || {
                    let mut stream = stream;
                    let body = read_http_body(&mut stream);
                    let response_body = (handler.as_ref())(&body);
                    write_http_response(&mut stream, &response_body);
                });
            }
        });

        format!("http://{address}")
    }

    fn read_http_body(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        let mut header_end = None;
        let mut content_length = 0usize;

        loop {
            let bytes_read = stream.read(&mut buffer).expect("read mock request");
            if bytes_read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..bytes_read]);

            if header_end.is_none() {
                header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4);
                if let Some(end) = header_end {
                    let headers = String::from_utf8_lossy(&request[..end]);
                    content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.split_once(':').and_then(|(name, value)| {
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().ok())
                                    .flatten()
                            })
                        })
                        .unwrap_or(0);
                }
            }

            if let Some(end) = header_end {
                if request.len() >= end + content_length {
                    let body = &request[end..end + content_length];
                    return String::from_utf8_lossy(body).into_owned();
                }
            }
        }

        String::new()
    }

    fn write_http_response(stream: &mut TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write mock response");
    }
}
