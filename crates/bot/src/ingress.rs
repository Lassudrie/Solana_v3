use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::SystemTime;

use domain::{EventLane, MarketEvent, NormalizedEvent, lane_for_payload};

use crate::config::{RuntimeParallelismConfig, StateUpdateModeConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IngressStats {
    pub coalesced_updates: u64,
    pub state_dirty_mailboxes: usize,
    pub trigger_queue_depth: usize,
}

#[derive(Debug)]
pub(crate) struct IngressRouter {
    state_update_mode: StateUpdateModeConfig,
    account_to_pool: HashMap<String, String>,
    state_shards: Vec<StateShard>,
    trigger_queue: VecDeque<TriggerQueueEntry>,
    broadcast_queue: VecDeque<NormalizedEvent>,
    next_state_shard: usize,
}

impl IngressRouter {
    pub(crate) fn new(
        parallelism: &RuntimeParallelismConfig,
        account_to_pool: HashMap<String, String>,
    ) -> Self {
        let shard_count = parallelism.state_shard_count.max(1);
        Self {
            state_update_mode: parallelism.state_update_mode,
            account_to_pool,
            state_shards: (0..shard_count).map(|_| StateShard::default()).collect(),
            trigger_queue: VecDeque::with_capacity(parallelism.trigger_queue_capacity.max(64)),
            broadcast_queue: VecDeque::new(),
            next_state_shard: 0,
        }
    }

    pub(crate) fn route(&mut self, mut event: NormalizedEvent) -> IngressStats {
        let now = SystemTime::now();
        event.latency.lane = lane_for_payload(&event.payload);
        event.latency.router_received_at = now;
        event.latency.trigger_blocked_at = None;

        let mut coalesced_updates = 0u64;
        match event.latency.lane {
            EventLane::StateOnly => {
                event.latency.state_published_at = Some(now);
                let (state_key, pool_key) = self.state_key(&event);
                let shard_index = self.shard_index(&pool_key);
                if self.state_shards[shard_index].insert(
                    state_key,
                    StateMailboxEntry { pool_key, event },
                    self.state_update_mode,
                ) {
                    coalesced_updates = 1;
                }
            }
            EventLane::Trigger => {
                self.trigger_queue.push_back(TriggerQueueEntry {
                    event,
                    first_blocked_at: None,
                });
            }
            EventLane::Broadcast => {
                self.broadcast_queue.push_back(event);
            }
        }

        IngressStats {
            coalesced_updates,
            state_dirty_mailboxes: self.state_dirty_mailboxes(),
            trigger_queue_depth: self.trigger_queue.len(),
        }
    }

    pub(crate) fn pop_next(&mut self) -> Option<NormalizedEvent> {
        if let Some(mut trigger) = self.trigger_queue.pop_front() {
            if let Some(event) = self.pop_blocking_state_for_trigger(&mut trigger) {
                self.trigger_queue.push_front(trigger);
                return Some(event);
            }
            if let Some(blocked_at) = trigger.first_blocked_at {
                trigger.event.latency.trigger_blocked_at = Some(blocked_at);
            }
            return Some(trigger.event);
        }

        // Favor state updates over broadcast traffic so warmup and hot-path freshness
        // do not stall behind slot/heartbeat chatter under bursty ingress.
        if let Some(event) = self.pop_next_state() {
            return Some(event);
        }

        self.broadcast_queue.pop_front()
    }

    pub(crate) fn has_pending(&self) -> bool {
        !self.trigger_queue.is_empty()
            || !self.broadcast_queue.is_empty()
            || self.state_dirty_mailboxes() > 0
    }

    pub(crate) fn state_dirty_mailboxes(&self) -> usize {
        self.state_shards.iter().map(StateShard::len).sum()
    }

    pub(crate) fn trigger_queue_depth(&self) -> usize {
        self.trigger_queue.len()
    }

    fn pop_blocking_state_for_trigger(
        &mut self,
        trigger: &mut TriggerQueueEntry,
    ) -> Option<NormalizedEvent> {
        let pool_id = trigger_pool_id(&trigger.event)?;
        let mut best: Option<(usize, String, u64)> = None;

        for (shard_index, shard) in self.state_shards.iter().enumerate() {
            for (key, entry) in &shard.mailboxes {
                if entry.pool_key != pool_id
                    || entry.event.source.sequence >= trigger.event.source.sequence
                {
                    continue;
                }
                let candidate = (shard_index, key.clone(), entry.event.source.sequence);
                if best.as_ref().is_none_or(|best| candidate.2 < best.2) {
                    best = Some(candidate);
                }
            }
        }

        let (shard_index, key, _) = best?;
        if trigger.first_blocked_at.is_none() {
            trigger.first_blocked_at = Some(SystemTime::now());
        }
        self.state_shards[shard_index]
            .take(&key)
            .map(|entry| entry.event)
    }

    fn pop_next_state(&mut self) -> Option<NormalizedEvent> {
        if self.state_shards.is_empty() {
            return None;
        }

        for _ in 0..self.state_shards.len() {
            let shard_index = self.next_state_shard % self.state_shards.len();
            self.next_state_shard = (shard_index + 1) % self.state_shards.len();
            if let Some(entry) = self.state_shards[shard_index].pop_next() {
                return Some(entry.event);
            }
        }

        None
    }

    fn state_key(&self, event: &NormalizedEvent) -> (String, String) {
        match &event.payload {
            MarketEvent::AccountUpdate(update) => {
                let pool_key = self
                    .account_to_pool
                    .get(&update.pubkey)
                    .cloned()
                    .unwrap_or_else(|| format!("account:{}", update.pubkey));
                (format!("state:{pool_key}:account"), pool_key)
            }
            MarketEvent::PoolSnapshotUpdate(update) => {
                let pool_key = update.pool_id.clone();
                (format!("state:{pool_key}:snapshot"), pool_key)
            }
            MarketEvent::PoolQuoteModelUpdate(update) => {
                let pool_key = update.pool_id.clone();
                (format!("state:{pool_key}:quote_model"), pool_key)
            }
            MarketEvent::PoolInvalidation(update) => {
                let pool_key = update.pool_id.clone();
                (format!("state:{pool_key}:invalidation"), pool_key)
            }
            other => (
                format!("state:{}", fallback_key(other)),
                fallback_key(other),
            ),
        }
    }

    fn shard_index(&self, key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.state_shards.len().max(1)
    }
}

#[cfg(test)]
mod tests {
    use domain::{
        EventSourceKind, Heartbeat, MarketEvent, NormalizedEvent, PoolQuoteModelUpdate,
        PoolSnapshotUpdate, SnapshotConfidence, types::PoolVenue,
    };

    use crate::config::{RuntimeParallelismConfig, StateUpdateModeConfig};

    use super::IngressRouter;

    fn snapshot_event(sequence: u64) -> NormalizedEvent {
        NormalizedEvent::pool_snapshot_update(
            EventSourceKind::ShredStream,
            sequence,
            100,
            PoolSnapshotUpdate {
                pool_id: "pool-a".into(),
                price_bps: 10_000,
                fee_bps: 30,
                reserve_depth: 1_000,
                reserve_a: Some(500),
                reserve_b: Some(500),
                active_liquidity: Some(1_000),
                sqrt_price_x64: Some(1u128 << 64),
                venue: PoolVenue::OrcaWhirlpool,
                confidence: SnapshotConfidence::Executable,
                repair_pending: Some(false),
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                tick_spacing: 64,
                current_tick_index: Some(0),
                slot: 100,
                write_version: 1,
            },
        )
    }

    fn quote_model_event(sequence: u64) -> NormalizedEvent {
        NormalizedEvent::pool_quote_model_update(
            EventSourceKind::ShredStream,
            sequence,
            100,
            PoolQuoteModelUpdate {
                pool_id: "pool-a".into(),
                liquidity: 1_000,
                sqrt_price_x64: 1u128 << 64,
                current_tick_index: 0,
                tick_spacing: 64,
                required_a_to_b: true,
                required_b_to_a: true,
                a_to_b: None,
                b_to_a: None,
                slot: 100,
                write_version: 1,
            },
        )
    }

    fn broadcast_event(sequence: u64) -> NormalizedEvent {
        NormalizedEvent::with_payload(
            EventSourceKind::ShredStream,
            sequence,
            100,
            MarketEvent::Heartbeat(Heartbeat {
                slot: 100,
                note: "test",
            }),
        )
    }

    #[test]
    fn latest_only_keeps_snapshot_and_quote_model_for_same_pool() {
        let mut parallelism = RuntimeParallelismConfig::default();
        parallelism.state_shard_count = 1;
        parallelism.state_update_mode = StateUpdateModeConfig::LatestOnly;
        let mut router = IngressRouter::new(&parallelism, Default::default());

        let snapshot_stats = router.route(snapshot_event(1));
        let quote_stats = router.route(quote_model_event(2));

        assert_eq!(snapshot_stats.coalesced_updates, 0);
        assert_eq!(quote_stats.coalesced_updates, 0);
        assert_eq!(router.state_dirty_mailboxes(), 2);

        let first = router.pop_next().expect("snapshot event");
        let second = router.pop_next().expect("quote model event");

        assert!(matches!(
            first.payload,
            domain::MarketEvent::PoolSnapshotUpdate(_)
        ));
        assert!(matches!(
            second.payload,
            domain::MarketEvent::PoolQuoteModelUpdate(_)
        ));
    }

    #[test]
    fn state_updates_take_priority_over_broadcast_events() {
        let mut router =
            IngressRouter::new(&RuntimeParallelismConfig::default(), Default::default());

        router.route(broadcast_event(1));
        router.route(snapshot_event(2));

        let first = router.pop_next().expect("state event");
        let second = router.pop_next().expect("broadcast event");

        assert!(matches!(
            first.payload,
            domain::MarketEvent::PoolSnapshotUpdate(_)
        ));
        assert!(matches!(second.payload, domain::MarketEvent::Heartbeat(_)));
    }
}

#[derive(Debug, Default)]
struct StateShard {
    order: VecDeque<String>,
    mailboxes: HashMap<String, StateMailboxEntry>,
}

impl StateShard {
    fn insert(
        &mut self,
        key: String,
        entry: StateMailboxEntry,
        state_update_mode: StateUpdateModeConfig,
    ) -> bool {
        match self.mailboxes.get_mut(&key) {
            Some(existing) => {
                if state_update_mode == StateUpdateModeConfig::LatestOnly
                    && existing.event.source.sequence <= entry.event.source.sequence
                {
                    *existing = entry;
                    return true;
                }
                false
            }
            None => {
                self.order.push_back(key.clone());
                self.mailboxes.insert(key, entry);
                false
            }
        }
    }

    fn pop_next(&mut self) -> Option<StateMailboxEntry> {
        while let Some(key) = self.order.pop_front() {
            if let Some(entry) = self.mailboxes.remove(&key) {
                return Some(entry);
            }
        }
        None
    }

    fn take(&mut self, key: &str) -> Option<StateMailboxEntry> {
        self.mailboxes.remove(key)
    }

    fn len(&self) -> usize {
        self.mailboxes.len()
    }
}

#[derive(Debug)]
struct StateMailboxEntry {
    pool_key: String,
    event: NormalizedEvent,
}

#[derive(Debug)]
struct TriggerQueueEntry {
    event: NormalizedEvent,
    first_blocked_at: Option<SystemTime>,
}

fn trigger_pool_id(event: &NormalizedEvent) -> Option<&str> {
    match &event.payload {
        MarketEvent::DestabilizingTransaction(trigger) => Some(trigger.pool_id.as_str()),
        _ => None,
    }
}

fn fallback_key(event: &MarketEvent) -> String {
    match event {
        MarketEvent::SlotBoundary(boundary) => format!("slot:{}", boundary.slot),
        MarketEvent::Heartbeat(heartbeat) => format!("heartbeat:{}", heartbeat.slot),
        MarketEvent::DestabilizingTransaction(trigger) => format!("trigger:{}", trigger.pool_id),
        MarketEvent::AccountUpdate(update) => format!("account:{}", update.pubkey),
        MarketEvent::PoolSnapshotUpdate(update) => format!("pool:{}", update.pool_id),
        MarketEvent::PoolQuoteModelUpdate(update) => format!("pool:{}", update.pool_id),
        MarketEvent::PoolInvalidation(update) => format!("pool:{}", update.pool_id),
    }
}
