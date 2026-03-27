pub mod account_store;
pub mod decoder;
pub mod dependency_graph;
pub mod executable_pool_state;
pub mod pool_snapshots;
pub mod quote_models;
pub mod types;
pub mod warmup;

use std::collections::HashSet;
use std::time::SystemTime;

use domain::{
    AccountUpdate, DirectionalPoolQuoteModelUpdate, MarketEvent, NormalizedEvent, PoolInvalidation,
    PoolQuoteModelUpdate, PoolSnapshotUpdate, SnapshotConfidence,
};
use thiserror::Error;

use crate::{
    account_store::AccountStore,
    decoder::DecoderRegistry,
    dependency_graph::DependencyGraph,
    pool_snapshots::PoolSnapshotStore,
    quote_models::ConcentratedQuoteModelStore,
    types::{
        AccountKey, AccountRecord, AccountUpdateStatus, LiquidityModel, PoolConfidence, PoolId,
        StateApplyOutcome, WarmupStatus,
    },
    warmup::WarmupManager,
};

#[derive(Debug, Error)]
pub enum StateError {
    #[error("decoder {decoder_key} not registered")]
    UnknownDecoder { decoder_key: String },
}

#[derive(Debug)]
pub struct StatePlane {
    account_store: AccountStore,
    decoder_registry: DecoderRegistry,
    dependency_graph: DependencyGraph,
    pool_snapshots: PoolSnapshotStore,
    concentrated_quote_models: ConcentratedQuoteModelStore,
    warmup: WarmupManager,
    latest_slot: u64,
    max_slot_lag: u64,
}

impl StatePlane {
    pub fn new(max_slot_lag: u64) -> Self {
        Self {
            account_store: AccountStore::default(),
            decoder_registry: DecoderRegistry::default(),
            dependency_graph: DependencyGraph::default(),
            pool_snapshots: PoolSnapshotStore::default(),
            concentrated_quote_models: ConcentratedQuoteModelStore::default(),
            warmup: WarmupManager::default(),
            latest_slot: 0,
            max_slot_lag,
        }
    }

    pub fn decoder_registry_mut(&mut self) -> &mut DecoderRegistry {
        &mut self.decoder_registry
    }

    pub fn register_route(&mut self, route_id: crate::types::RouteId, required_pools: Vec<PoolId>) {
        let mut deduped_pools = Vec::with_capacity(required_pools.len());
        let mut seen = HashSet::with_capacity(required_pools.len());
        for pool_id in required_pools {
            if seen.insert(pool_id.clone()) {
                deduped_pools.push(pool_id);
            }
        }

        for pool_id in &deduped_pools {
            self.dependency_graph
                .register_pool_route(pool_id.clone(), route_id.clone());
        }
        self.warmup.register_route(route_id, deduped_pools);
    }

    pub fn register_account_dependency(
        &mut self,
        account: AccountKey,
        pool_id: PoolId,
        decoder_key: impl Into<String>,
    ) {
        self.dependency_graph
            .register_account_pool(account, pool_id, decoder_key);
    }

    pub fn latest_slot(&self) -> u64 {
        self.latest_slot
    }

    pub fn set_latest_slot(&mut self, slot: u64) {
        self.latest_slot = self.latest_slot.max(slot);
    }

    pub fn route_warmup_status(&self, route_id: &crate::types::RouteId) -> WarmupStatus {
        self.warmup.status(route_id)
    }

    pub fn route_count(&self) -> usize {
        self.warmup.route_count()
    }

    pub fn ready_route_count(&self) -> usize {
        self.warmup.ready_route_count()
    }

    pub fn tradable_route_count(&self) -> usize {
        self.warmup
            .route_ids()
            .into_iter()
            .filter(|route_id| self.route_is_tradable(route_id))
            .count()
    }

    pub fn route_pool_ids(&self, route_id: &crate::types::RouteId) -> &[PoolId] {
        self.dependency_graph.route_pools(route_id)
    }

    pub fn pool_snapshot(&self, pool_id: &PoolId) -> Option<&crate::types::PoolSnapshot> {
        self.pool_snapshots.get(pool_id)
    }

    pub fn pool_snapshots_for(&self, pool_ids: &[PoolId]) -> Vec<crate::types::PoolSnapshot> {
        pool_ids
            .iter()
            .filter_map(|pool_id| self.pool_snapshots.get(pool_id).cloned())
            .collect()
    }

    pub fn concentrated_quote_model(
        &self,
        pool_id: &PoolId,
    ) -> Option<&crate::quote_models::ConcentratedQuoteModel> {
        self.concentrated_quote_models.get(pool_id)
    }

    pub fn route_state(&self, route_id: &crate::types::RouteId) -> crate::types::RouteState {
        self.warmup.route_state(route_id)
    }

    pub fn pool_has_executable_quote_model(&self, pool_id: &PoolId) -> bool {
        let Some(snapshot) = self.pool_snapshots.get(pool_id) else {
            return false;
        };
        match snapshot.liquidity_model {
            LiquidityModel::ConstantProduct => true,
            LiquidityModel::ConcentratedLiquidity => self
                .concentrated_quote_models
                .get(pool_id)
                .map(|model| model.has_required_directions())
                .unwrap_or(false),
            LiquidityModel::Unknown => false,
        }
    }

    fn route_is_tradable(&self, route_id: &crate::types::RouteId) -> bool {
        if self.route_warmup_status(route_id) != WarmupStatus::Ready {
            return false;
        }

        let pool_ids = self.dependency_graph.route_pools(route_id);
        !pool_ids.is_empty()
            && pool_ids.iter().all(|pool_id| {
                self.pool_snapshots
                    .get(pool_id)
                    .map(|snapshot| {
                        snapshot.is_executable()
                            && self.pool_has_executable_quote_model(pool_id)
                            && !snapshot.freshness.is_stale
                    })
                    .unwrap_or(false)
            })
    }

    pub fn apply_event(
        &mut self,
        event: &NormalizedEvent,
    ) -> Result<Option<StateApplyOutcome>, StateError> {
        match &event.payload {
            MarketEvent::AccountUpdate(update) => self.apply_account_update(update),
            MarketEvent::PoolSnapshotUpdate(update) => self.apply_pool_snapshot_update(update),
            MarketEvent::PoolQuoteModelUpdate(update) => self.apply_pool_quote_model_update(update),
            MarketEvent::PoolInvalidation(invalidation) => {
                Ok(Some(self.apply_pool_invalidation(invalidation)))
            }
            MarketEvent::SlotBoundary(slot_boundary) => {
                self.latest_slot = self.latest_slot.max(slot_boundary.slot);
                self.pool_snapshots
                    .refresh_freshness(self.latest_slot, self.max_slot_lag);
                Ok(Some(StateApplyOutcome {
                    update_status: AccountUpdateStatus::Applied,
                    impacted_pools: Vec::new(),
                    impacted_routes: Vec::new(),
                    refreshed_snapshots: 0,
                    latest_slot: self.latest_slot,
                }))
            }
            MarketEvent::Heartbeat(heartbeat) => {
                self.latest_slot = self.latest_slot.max(heartbeat.slot);
                Ok(None)
            }
        }
    }

    fn apply_account_update(
        &mut self,
        update: &AccountUpdate,
    ) -> Result<Option<StateApplyOutcome>, StateError> {
        self.latest_slot = self.latest_slot.max(update.slot);
        let record = AccountRecord {
            key: AccountKey(update.pubkey.clone()),
            owner: update.owner.clone(),
            lamports: update.lamports,
            data: update.data.clone(),
            slot: update.slot,
            write_version: update.write_version,
            observed_at: SystemTime::now(),
        };
        let update_status = self.account_store.upsert(record.clone());
        if update_status == AccountUpdateStatus::StaleRejected {
            return Ok(Some(StateApplyOutcome {
                update_status,
                impacted_pools: Vec::new(),
                impacted_routes: Vec::new(),
                refreshed_snapshots: 0,
                latest_slot: self.latest_slot,
            }));
        }

        let dependencies = self
            .dependency_graph
            .account_dependencies(&record.key)
            .to_vec();
        let mut impacted_pools = Vec::with_capacity(dependencies.len());
        for dependency in dependencies {
            let decoder = self
                .decoder_registry
                .get(&dependency.decoder_key)
                .ok_or_else(|| StateError::UnknownDecoder {
                    decoder_key: dependency.decoder_key.clone(),
                })?;
            if let crate::types::DecodedAccount::PoolState(mut snapshot) =
                decoder.decode(&dependency.pool_id, &record, self.latest_slot)
            {
                snapshot.freshness = crate::types::FreshnessState::at(
                    self.latest_slot,
                    snapshot.last_update_slot,
                    self.max_slot_lag,
                );
                self.pool_snapshots
                    .upsert_with_version(snapshot, update.write_version);
                impacted_pools.push(dependency.pool_id);
            }
        }

        Ok(Some(self.state_outcome(update_status, impacted_pools)))
    }

    fn apply_pool_snapshot_update(
        &mut self,
        update: &PoolSnapshotUpdate,
    ) -> Result<Option<StateApplyOutcome>, StateError> {
        self.latest_slot = self.latest_slot.max(update.slot);
        let pool_id = crate::types::PoolId(update.pool_id.clone());
        let snapshot = crate::types::PoolSnapshot {
            pool_id: pool_id.clone(),
            price_bps: update.price_bps,
            fee_bps: update.fee_bps,
            reserve_depth: update.reserve_depth,
            reserve_a: update.reserve_a,
            reserve_b: update.reserve_b,
            active_liquidity: update.active_liquidity.unwrap_or(update.reserve_depth),
            sqrt_price_x64: update.sqrt_price_x64,
            venue: None,
            confidence: match update.confidence {
                SnapshotConfidence::Decoded => PoolConfidence::Decoded,
                SnapshotConfidence::Verified => PoolConfidence::Verified,
                SnapshotConfidence::Executable => PoolConfidence::Executable,
            },
            repair_pending: update.repair_pending.unwrap_or(false),
            liquidity_model: LiquidityModel::from_market_hints(
                update.tick_spacing,
                update.current_tick_index,
                update.sqrt_price_x64,
            ),
            slippage_factor_bps: crate::types::PoolSnapshot::default_slippage_factor_bps(
                LiquidityModel::from_market_hints(
                    update.tick_spacing,
                    update.current_tick_index,
                    update.sqrt_price_x64,
                ),
                update.tick_spacing,
            ),
            token_mint_a: update.token_mint_a.clone(),
            token_mint_b: update.token_mint_b.clone(),
            tick_spacing: update.tick_spacing,
            current_tick_index: update.current_tick_index,
            last_update_slot: update.slot,
            derived_at: SystemTime::now(),
            freshness: crate::types::FreshnessState::at(
                self.latest_slot,
                update.slot,
                self.max_slot_lag,
            ),
        };
        let update_status = if self
            .pool_snapshots
            .upsert_with_version(snapshot, update.write_version)
        {
            AccountUpdateStatus::Applied
        } else {
            AccountUpdateStatus::StaleRejected
        };
        let impacted_pools = if update_status == AccountUpdateStatus::Applied {
            vec![pool_id]
        } else {
            Vec::new()
        };
        Ok(Some(self.state_outcome(update_status, impacted_pools)))
    }

    fn apply_pool_quote_model_update(
        &mut self,
        update: &PoolQuoteModelUpdate,
    ) -> Result<Option<StateApplyOutcome>, StateError> {
        self.latest_slot = self.latest_slot.max(update.slot);
        let pool_id = crate::types::PoolId(update.pool_id.clone());
        let to_directional = |direction: &DirectionalPoolQuoteModelUpdate| {
            crate::quote_models::DirectionalConcentratedQuoteModel {
                loaded_tick_arrays: direction.loaded_tick_arrays,
                expected_tick_arrays: direction.expected_tick_arrays,
                complete: direction.complete,
                windows: direction
                    .windows
                    .iter()
                    .map(|window| crate::quote_models::TickArrayWindow {
                        start_tick_index: window.start_tick_index,
                        end_tick_index: window.end_tick_index,
                        initialized_tick_count: window.initialized_tick_count,
                    })
                    .collect(),
                initialized_ticks: direction
                    .initialized_ticks
                    .iter()
                    .map(|tick| crate::quote_models::InitializedTick {
                        tick_index: tick.tick_index,
                        liquidity_net: tick.liquidity_net,
                        liquidity_gross: tick.liquidity_gross,
                    })
                    .collect(),
            }
        };
        let model = crate::quote_models::ConcentratedQuoteModel {
            pool_id: pool_id.clone(),
            liquidity: update.liquidity,
            sqrt_price_x64: update.sqrt_price_x64,
            current_tick_index: update.current_tick_index,
            tick_spacing: update.tick_spacing,
            required_a_to_b: update.required_a_to_b,
            required_b_to_a: update.required_b_to_a,
            a_to_b: update.a_to_b.as_ref().map(to_directional),
            b_to_a: update.b_to_a.as_ref().map(to_directional),
            last_update_slot: update.slot,
            write_version: update.write_version,
        };
        let update_status = if self.concentrated_quote_models.upsert(model) {
            AccountUpdateStatus::Applied
        } else {
            AccountUpdateStatus::StaleRejected
        };
        let impacted_pools = if update_status == AccountUpdateStatus::Applied {
            vec![pool_id]
        } else {
            Vec::new()
        };
        Ok(Some(self.state_outcome(update_status, impacted_pools)))
    }

    fn apply_pool_invalidation(&mut self, invalidation: &PoolInvalidation) -> StateApplyOutcome {
        let pool_id = crate::types::PoolId(invalidation.pool_id.clone());
        let impacted_pools = vec![pool_id.clone()];
        self.pool_snapshots
            .invalidate(&pool_id, self.latest_slot, self.max_slot_lag);
        self.state_outcome(AccountUpdateStatus::Applied, impacted_pools)
    }

    fn state_outcome(
        &mut self,
        update_status: AccountUpdateStatus,
        impacted_pools: Vec<crate::types::PoolId>,
    ) -> StateApplyOutcome {
        let impacted_routes = self
            .dependency_graph
            .impacted_routes_for_pools(&impacted_pools);
        for route_id in &impacted_routes {
            self.warmup
                .refresh_route(route_id, &self.pool_snapshots, self.latest_slot);
        }

        StateApplyOutcome {
            update_status,
            refreshed_snapshots: impacted_pools.len(),
            impacted_pools,
            impacted_routes,
            latest_slot: self.latest_slot,
        }
    }
}

#[cfg(test)]
mod tests {
    use domain::{
        AccountUpdate, EventSourceKind, NormalizedEvent, PoolInvalidation, PoolSnapshotUpdate,
        SnapshotConfidence,
    };

    use super::StatePlane;
    use crate::{
        decoder::PoolPriceAccountDecoder,
        types::{AccountKey, PoolId, RouteId, WarmupStatus},
    };

    fn encode_pool(price_bps: u64, fee_bps: u16, reserve_depth: u64) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&price_bps.to_le_bytes());
        data.extend_from_slice(&fee_bps.to_le_bytes());
        data.extend_from_slice(&reserve_depth.to_le_bytes());
        data
    }

    #[test]
    fn route_becomes_ready_after_all_required_pools_arrive() {
        let route_id = RouteId("route-a".into());
        let mut plane = StatePlane::new(2);
        plane
            .decoder_registry_mut()
            .register(PoolPriceAccountDecoder);
        plane.register_route(
            route_id.clone(),
            vec![PoolId("pool-a".into()), PoolId("pool-b".into())],
        );
        plane.register_account_dependency(
            AccountKey("acct-a".into()),
            PoolId("pool-a".into()),
            "pool-price-v1",
        );
        plane.register_account_dependency(
            AccountKey("acct-b".into()),
            PoolId("pool-b".into()),
            "pool-price-v1",
        );

        let first = NormalizedEvent::account_update(
            EventSourceKind::Synthetic,
            1,
            10,
            AccountUpdate {
                pubkey: "acct-a".into(),
                owner: "pool".into(),
                lamports: 0,
                data: encode_pool(10_100, 4, 100_000),
                slot: 10,
                write_version: 1,
            },
        );
        let second = NormalizedEvent::account_update(
            EventSourceKind::Synthetic,
            2,
            11,
            AccountUpdate {
                pubkey: "acct-b".into(),
                owner: "pool".into(),
                lamports: 0,
                data: encode_pool(10_050, 4, 100_000),
                slot: 11,
                write_version: 1,
            },
        );

        plane.apply_event(&first).unwrap();
        assert_eq!(plane.route_warmup_status(&route_id), WarmupStatus::Warming);
        let outcome = plane.apply_event(&second).unwrap().unwrap();
        assert_eq!(plane.route_warmup_status(&route_id), WarmupStatus::Ready);
        assert_eq!(outcome.impacted_routes, vec![route_id]);
    }

    #[test]
    fn pool_snapshot_update_warms_route_without_account_decoder_path() {
        let route_id = RouteId("route-live".into());
        let pool_id = PoolId("pool-live".into());
        let mut plane = StatePlane::new(2);
        plane.register_route(route_id.clone(), vec![pool_id.clone()]);

        let event = NormalizedEvent::pool_snapshot_update(
            EventSourceKind::ShredStream,
            1,
            55,
            PoolSnapshotUpdate {
                pool_id: pool_id.0.clone(),
                price_bps: 10_250,
                fee_bps: 4,
                reserve_depth: 77_000,
                reserve_a: None,
                reserve_b: None,
                active_liquidity: Some(77_000),
                sqrt_price_x64: None,
                confidence: SnapshotConfidence::Decoded,
                repair_pending: None,
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                tick_spacing: 4,
                current_tick_index: Some(12),
                slot: 55,
                write_version: 1,
            },
        );

        let outcome = plane.apply_event(&event).unwrap().unwrap();
        assert_eq!(plane.route_warmup_status(&route_id), WarmupStatus::Ready);
        assert_eq!(outcome.impacted_pools, vec![pool_id.clone()]);
        assert_eq!(outcome.latest_slot, 55);
        assert_eq!(plane.pool_snapshot(&pool_id).unwrap().price_bps, 10_250);
    }

    #[test]
    fn conversion_pool_updates_mark_route_impacted() {
        let route_id = RouteId("route-usdt-usdc".into());
        let pool_a = PoolId("pool-usdt-usdc-a".into());
        let pool_b = PoolId("pool-usdt-usdc-b".into());
        let conversion_pool = PoolId("pool-sol-usdc".into());
        let mut plane = StatePlane::new(2);
        plane.register_route(
            route_id.clone(),
            vec![pool_a, pool_b, conversion_pool.clone()],
        );

        let outcome = plane
            .apply_event(&NormalizedEvent::pool_snapshot_update(
                EventSourceKind::ShredStream,
                1,
                55,
                PoolSnapshotUpdate {
                    pool_id: conversion_pool.0.clone(),
                    price_bps: 1_500_000,
                    fee_bps: 4,
                    reserve_depth: 77_000,
                    reserve_a: Some(77_000),
                    reserve_b: Some(77_000),
                    active_liquidity: Some(77_000),
                    sqrt_price_x64: None,
                    confidence: SnapshotConfidence::Decoded,
                    repair_pending: Some(false),
                    token_mint_a: "So11111111111111111111111111111111111111112".into(),
                    token_mint_b: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
                    tick_spacing: 0,
                    current_tick_index: None,
                    slot: 55,
                    write_version: 1,
                },
            ))
            .unwrap()
            .unwrap();

        assert_eq!(outcome.impacted_pools, vec![conversion_pool]);
        assert_eq!(outcome.impacted_routes, vec![route_id]);
    }

    #[test]
    fn pool_invalidation_marks_snapshot_stale_immediately() {
        let pool_id = PoolId("pool-stale".into());
        let mut plane = StatePlane::new(2);
        let seed = NormalizedEvent::pool_snapshot_update(
            EventSourceKind::ShredStream,
            1,
            88,
            PoolSnapshotUpdate {
                pool_id: pool_id.0.clone(),
                price_bps: 10_000,
                fee_bps: 4,
                reserve_depth: 1_000,
                reserve_a: None,
                reserve_b: None,
                active_liquidity: Some(1_000),
                sqrt_price_x64: None,
                confidence: SnapshotConfidence::Decoded,
                repair_pending: None,
                token_mint_a: "mint-a".into(),
                token_mint_b: "mint-b".into(),
                tick_spacing: 4,
                current_tick_index: Some(1),
                slot: 88,
                write_version: 1,
            },
        );
        plane.apply_event(&seed).unwrap();

        let invalidation = NormalizedEvent::pool_invalidation(
            EventSourceKind::ShredStream,
            2,
            88,
            PoolInvalidation {
                pool_id: pool_id.0.clone(),
            },
        );
        plane.apply_event(&invalidation).unwrap();

        assert!(plane.pool_snapshot(&pool_id).unwrap().freshness.is_stale);
    }

    #[test]
    fn tradable_route_count_excludes_exact_but_stale_routes() {
        let route_id = RouteId("route-stale".into());
        let pool_a = PoolId("pool-a".into());
        let pool_b = PoolId("pool-b".into());
        let mut plane = StatePlane::new(2);
        plane.register_route(route_id, vec![pool_a.clone(), pool_b.clone()]);

        for (sequence, pool_id) in [(1, &pool_a), (2, &pool_b)] {
            plane
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::ShredStream,
                    sequence,
                    10,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.0.clone(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 1_000,
                        reserve_a: Some(1_000),
                        reserve_b: Some(1_000),
                        active_liquidity: Some(1_000),
                        sqrt_price_x64: None,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "mint-a".into(),
                        token_mint_b: "mint-b".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot: 10,
                        write_version: 1,
                    },
                ))
                .unwrap();
        }

        assert_eq!(plane.tradable_route_count(), 1);

        plane
            .apply_event(&NormalizedEvent::with_payload(
                EventSourceKind::ShredStream,
                3,
                13,
                domain::MarketEvent::SlotBoundary(domain::SlotBoundary {
                    slot: 13,
                    leader: None,
                }),
            ))
            .unwrap();

        assert_eq!(plane.tradable_route_count(), 0);
    }

    #[test]
    fn tradable_route_count_excludes_executable_clmm_without_quote_model() {
        let route_id = RouteId("route-clmm".into());
        let pool_a = PoolId("pool-a".into());
        let pool_b = PoolId("pool-b".into());
        let mut plane = StatePlane::new(2);
        plane.register_route(route_id, vec![pool_a.clone(), pool_b.clone()]);

        plane
            .apply_event(&NormalizedEvent::pool_snapshot_update(
                EventSourceKind::ShredStream,
                1,
                10,
                PoolSnapshotUpdate {
                    pool_id: pool_a.0.clone(),
                    price_bps: 10_000,
                    fee_bps: 4,
                    reserve_depth: 1_000,
                    reserve_a: None,
                    reserve_b: None,
                    active_liquidity: Some(1_000),
                    sqrt_price_x64: Some(1u128 << 64),
                    confidence: SnapshotConfidence::Executable,
                    repair_pending: Some(false),
                    token_mint_a: "mint-a".into(),
                    token_mint_b: "mint-b".into(),
                    tick_spacing: 4,
                    current_tick_index: Some(12),
                    slot: 10,
                    write_version: 1,
                },
            ))
            .unwrap();
        plane
            .apply_event(&NormalizedEvent::pool_snapshot_update(
                EventSourceKind::ShredStream,
                2,
                10,
                PoolSnapshotUpdate {
                    pool_id: pool_b.0.clone(),
                    price_bps: 10_000,
                    fee_bps: 4,
                    reserve_depth: 1_000,
                    reserve_a: Some(1_000),
                    reserve_b: Some(1_000),
                    active_liquidity: Some(1_000),
                    sqrt_price_x64: None,
                    confidence: SnapshotConfidence::Executable,
                    repair_pending: Some(false),
                    token_mint_a: "mint-a".into(),
                    token_mint_b: "mint-b".into(),
                    tick_spacing: 0,
                    current_tick_index: None,
                    slot: 10,
                    write_version: 1,
                },
            ))
            .unwrap();

        assert_eq!(plane.tradable_route_count(), 0);
    }
}
