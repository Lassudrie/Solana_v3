use crate::{
    RouteExecutionSizingState,
    guards::GuardrailSet,
    opportunity::{
        CandidateSelectionSource, OpportunityCandidate, OpportunityDecision, SelectionOutcome,
    },
    quote::{
        LocalTwoLegQuoteEngine, PoolPricingView, QuoteEngine, QuoteError,
        QuoteExecutionAdjustments, quote_atoms_to_sol_lamports, route_uses_sol_quote_conversion,
        sol_lamports_to_quote_atoms,
    },
    reasons::RejectionReason,
    route_registry::{
        JitoTipMode, JitoTipPolicy, RouteDefinition, RouteLegSequence, RouteRegistry, SizingMode,
        StrategySizingConfig, SwapSide,
    },
};
use domain::{ExecutionSnapshot, PoolSnapshot, RouteId};
use state::StatePlane;
use std::collections::{BTreeSet, HashMap};

const DEFAULT_SIZE_LADDER: [u64; 6] =
    [250_000, 500_000, 1_000_000, 2_000_000, 3_000_000, 5_000_000];
const MAX_RESERVE_USAGE_BPS: u64 = 2_000;

#[derive(Debug)]
pub struct OpportunitySelector<E = LocalTwoLegQuoteEngine> {
    quote_engine: E,
    guards: GuardrailSet,
    jito_tip_policy: JitoTipPolicy,
}

#[derive(Debug, Clone)]
struct RouteSelection {
    live_candidate: OpportunityCandidate,
    shadow_candidate: Option<OpportunityCandidate>,
}

#[derive(Debug, Default)]
struct EvaluationBatch {
    decisions: Vec<OpportunityDecision>,
    live_candidates: Vec<OpportunityCandidate>,
    shadow_candidates: Vec<OpportunityCandidate>,
}

impl<E> OpportunitySelector<E>
where
    E: QuoteEngine,
{
    pub fn new(quote_engine: E, guards: GuardrailSet) -> Self {
        Self::with_jito_tip_policy(
            quote_engine,
            guards,
            JitoTipPolicy {
                mode: JitoTipMode::Fixed,
                share_bps_of_expected_net_profit: 0,
                min_lamports: 0,
                max_lamports: 0,
            },
        )
    }

    pub fn with_jito_tip_policy(
        quote_engine: E,
        guards: GuardrailSet,
        jito_tip_policy: JitoTipPolicy,
    ) -> Self {
        Self {
            quote_engine,
            guards,
            jito_tip_policy,
        }
    }

    pub(crate) fn evaluate(
        &self,
        registry: &RouteRegistry,
        state: &StatePlane,
        execution_state: &ExecutionSnapshot,
        impacted_routes: &[RouteId],
        inflight_submissions: usize,
        sizing_config: &StrategySizingConfig,
        route_execution_buffers: &HashMap<RouteId, u16>,
        route_execution_sizing: &HashMap<RouteId, RouteExecutionSizingState>,
        _route_eval_worker_count: usize,
    ) -> SelectionOutcome {
        if impacted_routes.is_empty() {
            return SelectionOutcome {
                decisions: vec![OpportunityDecision::Rejected {
                    route_id: RouteId("none".into()),
                    route_kind: None,
                    leg_count: 0,
                    reason: RejectionReason::NoImpactedRoutes,
                }],
                best_candidate: None,
                shadow_candidate: None,
            };
        }

        let batches = vec![self.evaluate_chunk(
            registry,
            state,
            execution_state,
            impacted_routes,
            inflight_submissions,
            sizing_config,
            route_execution_buffers,
            route_execution_sizing,
        )];

        let mut decisions = Vec::with_capacity(impacted_routes.len());
        let mut live_candidates = Vec::new();
        let mut shadow_candidates = Vec::new();
        for batch in batches {
            decisions.extend(batch.decisions);
            live_candidates.extend(batch.live_candidates);
            shadow_candidates.extend(batch.shadow_candidates);
        }

        let mut best_candidate = None;
        for candidate in live_candidates {
            select_best_candidate(&mut best_candidate, candidate);
        }
        let mut shadow_candidate = None;
        for candidate in shadow_candidates {
            select_best_candidate(&mut shadow_candidate, candidate);
        }

        SelectionOutcome {
            decisions,
            best_candidate,
            shadow_candidate,
        }
    }

    fn evaluate_chunk(
        &self,
        registry: &RouteRegistry,
        state: &StatePlane,
        execution_state: &ExecutionSnapshot,
        impacted_routes: &[RouteId],
        inflight_submissions: usize,
        sizing_config: &StrategySizingConfig,
        route_execution_buffers: &HashMap<RouteId, u16>,
        route_execution_sizing: &HashMap<RouteId, RouteExecutionSizingState>,
    ) -> EvaluationBatch {
        let mut batch = EvaluationBatch {
            decisions: Vec::with_capacity(impacted_routes.len()),
            ..EvaluationBatch::default()
        };

        for route_id in impacted_routes {
            let Some(route) = registry.get(route_id) else {
                batch.decisions.push(OpportunityDecision::Rejected {
                    route_id: route_id.clone(),
                    route_kind: None,
                    leg_count: 0,
                    reason: RejectionReason::RouteNotRegistered,
                });
                continue;
            };

            match self.evaluate_route(
                route,
                state,
                execution_state,
                inflight_submissions,
                sizing_config,
                route_execution_buffers.get(route_id).copied(),
                route_execution_sizing.get(route_id).copied(),
            ) {
                Ok(selection) => {
                    batch.live_candidates.push(selection.live_candidate.clone());
                    if let Some(candidate) = selection.shadow_candidate.clone() {
                        batch.shadow_candidates.push(candidate);
                    }
                    batch
                        .decisions
                        .push(OpportunityDecision::Accepted(selection.live_candidate));
                }
                Err(reason) => batch.decisions.push(OpportunityDecision::Rejected {
                    route_id: route_id.clone(),
                    route_kind: Some(route.kind),
                    leg_count: route.leg_count(),
                    reason,
                }),
            }
        }

        batch
    }

    fn evaluate_route(
        &self,
        route: &RouteDefinition,
        state: &StatePlane,
        execution_state: &ExecutionSnapshot,
        inflight_submissions: usize,
        sizing_config: &StrategySizingConfig,
        active_execution_buffer_bps: Option<u16>,
        route_execution_sizing: Option<RouteExecutionSizingState>,
    ) -> Result<RouteSelection, RejectionReason> {
        self.guards
            .evaluate_route_readiness(&route.route_id, state)?;

        let leg_snapshots = route
            .legs
            .iter()
            .map(|leg| {
                state
                    .pool_snapshot(&leg.pool_id)
                    .ok_or_else(|| RejectionReason::MissingSnapshot {
                        pool_id: leg.pool_id.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let sol_quote_conversion_snapshot = route
            .sol_quote_conversion_pool_id
            .as_ref()
            .map(|pool_id| {
                state
                    .pool_snapshot(pool_id)
                    .ok_or_else(|| RejectionReason::MissingSnapshot {
                        pool_id: pool_id.clone(),
                    })
            })
            .transpose()?;
        let used_sol_quote_conversion_snapshot =
            route_uses_sol_quote_conversion(route, route.estimated_execution_cost_lamports)
                .then_some(sol_quote_conversion_snapshot)
                .flatten();

        let mut snapshots = leg_snapshots.clone();
        if let Some(snapshot) = sol_quote_conversion_snapshot {
            snapshots.push(snapshot);
        }
        self.guards.evaluate_snapshots(state, &snapshots)?;
        let mut execution_snapshots = leg_snapshots.clone();
        if let Some(snapshot) = used_sol_quote_conversion_snapshot {
            execution_snapshots.push(snapshot);
        }
        reject_stale_for_execution(route, execution_state.head_slot, &execution_snapshots)?;

        let pricing_views = route
            .legs
            .iter()
            .zip(leg_snapshots.iter())
            .map(|(leg, snapshot)| PoolPricingView {
                snapshot,
                concentrated: state.concentrated_quote_model(&leg.pool_id),
            })
            .collect::<Vec<_>>();

        let route_sizing_state = route_execution_sizing.unwrap_or(RouteExecutionSizingState {
            landing_rate_bps: route.sizing.base_landing_rate_bps,
            expected_shortfall_bps: route.sizing.base_expected_shortfall_bps,
            max_trade_size: route.max_trade_size,
        });
        let source_input_balance = source_input_balance(route, execution_state)?;
        let effective_max_trade_size = effective_max_trade_size(
            route,
            route_sizing_state.max_trade_size,
            source_input_balance,
        );
        if sizing_config.fixed_trade_size {
            return self.evaluate_route_fixed_size(
                route,
                state,
                execution_state,
                inflight_submissions,
                sizing_config,
                active_execution_buffer_bps,
                &leg_snapshots,
                &pricing_views,
                sol_quote_conversion_snapshot,
                used_sol_quote_conversion_snapshot,
                source_input_balance,
                effective_max_trade_size,
                route_sizing_state,
            );
        }

        let trade_sizes = {
            let effective_min_trade_size =
                effective_min_trade_size(route, sol_quote_conversion_snapshot)
                    .map_err(|detail| RejectionReason::SizingFloorNotConvertible { detail })?;
            if effective_min_trade_size > effective_max_trade_size.max(1) {
                if let (Some(account), Some(current)) =
                    (route.input_source_account.as_ref(), source_input_balance)
                {
                    return Err(RejectionReason::SourceBalanceTooLow {
                        account: account.clone(),
                        current,
                        minimum: effective_min_trade_size,
                    });
                }
                return Err(RejectionReason::TradeSizeBelowSizingFloor {
                    maximum: effective_max_trade_size.max(1),
                    minimum: effective_min_trade_size,
                });
            }
            trade_sizes(route, effective_min_trade_size, effective_max_trade_size)
        };

        let mut best_legacy_candidate: Option<OpportunityCandidate> = None;
        let mut best_ev_candidate: Option<OpportunityCandidate> = None;
        let mut last_rejection = None;
        let adjustments = route_quote_adjustments(route, active_execution_buffer_bps);
        for trade_size in trade_sizes {
            let quote = match self.quote_engine.quote(
                route,
                &pricing_views,
                state.latest_slot(),
                trade_size,
                &adjustments,
            ) {
                Ok(quote) => quote,
                Err(error) => {
                    last_rejection = Some(RejectionReason::QuoteFailed {
                        detail: error.to_string(),
                    });
                    continue;
                }
            };
            let network_quote = quote
                .clone()
                .with_estimated_execution_cost(
                    route,
                    sol_quote_conversion_snapshot,
                    route.estimated_execution_cost_lamports,
                )
                .map_err(|error| RejectionReason::ExecutionCostNotConvertible {
                    detail: error.to_string(),
                })?;

            let reserve_usage_bps =
                reserve_usage_bps(route, &leg_snapshots, &network_quote).unwrap_or_default();
            let p_land_bps = effective_landing_rate_bps(
                sizing_config,
                execution_state,
                inflight_submissions,
                route_sizing_state.landing_rate_bps,
                &network_quote,
            );
            let expected_shortfall_quote_atoms = expected_shortfall_quote_atoms(
                route,
                &network_quote,
                route_sizing_state.expected_shortfall_bps,
                active_execution_buffer_bps.unwrap_or_default(),
            );
            let execution_risk_penalty_quote_atoms = reserve_usage_penalty_quote_atoms(
                sizing_config,
                reserve_usage_bps,
                network_quote.expected_gross_profit_quote_atoms,
            );
            let pre_tip_expected_value_quote_atoms = expected_value_quote_atoms(
                p_land_bps,
                network_quote.expected_gross_profit_quote_atoms,
                network_quote.estimated_execution_cost_quote_atoms,
                expected_shortfall_quote_atoms,
                execution_risk_penalty_quote_atoms,
            );
            let quote = apply_jito_tip_policy(
                route,
                quote,
                sol_quote_conversion_snapshot,
                self.jito_tip_policy,
                tip_basis_quote_atoms(
                    self.jito_tip_policy.mode,
                    network_quote.expected_net_profit_quote_atoms,
                    pre_tip_expected_value_quote_atoms,
                ),
            )
            .map_err(|error| RejectionReason::ExecutionCostNotConvertible {
                detail: error.to_string(),
            })?;

            match self.guards.evaluate_quote(
                route,
                &quote.quote,
                execution_state,
                inflight_submissions,
            ) {
                Ok(()) => {
                    let expected_value_quote_atoms = expected_value_quote_atoms(
                        p_land_bps,
                        quote.quote.expected_gross_profit_quote_atoms,
                        quote.quote.estimated_execution_cost_quote_atoms,
                        expected_shortfall_quote_atoms,
                        execution_risk_penalty_quote_atoms,
                    );
                    let shared_fields = CandidateSharedFields {
                        route,
                        quote: &quote.quote,
                        leg_snapshot_slots: leg_snapshot_slots(&leg_snapshots),
                        sol_quote_conversion_snapshot_slot: used_sol_quote_conversion_snapshot
                            .map(|snapshot| snapshot.last_update_slot),
                        active_execution_buffer_bps,
                        source_input_balance,
                        minimum_acceptable_output: self.guards.minimum_acceptable_output(
                            route,
                            quote.quote.input_amount,
                            quote.quote.estimated_execution_cost_quote_atoms,
                        ),
                        p_land_bps,
                        expected_shortfall_quote_atoms,
                        expected_value_quote_atoms,
                        estimated_network_fee_lamports: quote.estimated_network_fee_lamports,
                        estimated_network_fee_quote_atoms: quote.estimated_network_fee_quote_atoms,
                        jito_tip_lamports: quote.jito_tip_lamports,
                        jito_tip_quote_atoms: quote.jito_tip_quote_atoms,
                    };
                    let legacy_candidate = build_candidate(
                        shared_fields.clone(),
                        CandidateSelectionSource::Legacy,
                        quote.quote.expected_net_profit_quote_atoms,
                    );
                    select_best_candidate(&mut best_legacy_candidate, legacy_candidate);

                    let minimum_profit = self
                        .guards
                        .minimum_profit_quote_atoms_for_route(route, quote.quote.input_amount);
                    if expected_value_quote_atoms >= minimum_profit {
                        let ev_candidate = build_candidate(
                            shared_fields.clone(),
                            CandidateSelectionSource::Ev,
                            expected_value_quote_atoms,
                        );
                        select_best_candidate(&mut best_ev_candidate, ev_candidate);
                    } else {
                        last_rejection = Some(RejectionReason::ProfitBelowThreshold {
                            expected: expected_value_quote_atoms,
                            minimum: minimum_profit,
                        });
                    }
                }
                Err(reason) => last_rejection = Some(reason),
            }
        }

        let live_candidate = match route.sizing.mode {
            SizingMode::Legacy | SizingMode::EvShadow => best_legacy_candidate.clone(),
            SizingMode::EvLive => best_ev_candidate.clone(),
        };
        let shadow_candidate = match route.sizing.mode {
            SizingMode::Legacy => None,
            SizingMode::EvShadow => best_ev_candidate,
            SizingMode::EvLive => best_legacy_candidate,
        };

        live_candidate
            .map(|live_candidate| RouteSelection {
                live_candidate,
                shadow_candidate,
            })
            .ok_or_else(|| {
                last_rejection.unwrap_or(RejectionReason::RouteFilteredOut {
                    route_id: route.route_id.clone(),
                })
            })
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_route_fixed_size(
        &self,
        route: &RouteDefinition,
        state: &StatePlane,
        execution_state: &ExecutionSnapshot,
        inflight_submissions: usize,
        sizing_config: &StrategySizingConfig,
        active_execution_buffer_bps: Option<u16>,
        leg_snapshots: &[&PoolSnapshot],
        pricing_views: &[PoolPricingView<'_>],
        sol_quote_conversion_snapshot: Option<&PoolSnapshot>,
        used_sol_quote_conversion_snapshot: Option<&PoolSnapshot>,
        source_input_balance: Option<u64>,
        effective_max_trade_size: u64,
        route_sizing_state: RouteExecutionSizingState,
    ) -> Result<RouteSelection, RejectionReason> {
        let effective_min_trade_size =
            effective_min_trade_size(route, sol_quote_conversion_snapshot)
                .map_err(|detail| RejectionReason::SizingFloorNotConvertible { detail })?;
        if effective_min_trade_size > effective_max_trade_size.max(1) {
            if let (Some(account), Some(current)) =
                (route.input_source_account.as_ref(), source_input_balance)
            {
                return Err(RejectionReason::SourceBalanceTooLow {
                    account: account.clone(),
                    current,
                    minimum: effective_min_trade_size,
                });
            }
            return Err(RejectionReason::TradeSizeBelowSizingFloor {
                maximum: effective_max_trade_size.max(1),
                minimum: effective_min_trade_size,
            });
        }
        let trade_sizes = fixed_trade_sizes(
            route,
            sol_quote_conversion_snapshot,
            effective_max_trade_size,
        )
        .map_err(|detail| RejectionReason::SizingFloorNotConvertible { detail })?;
        let adjustments = route_quote_adjustments(route, active_execution_buffer_bps);
        let mut last_rejection = None;

        for trade_size in trade_sizes {
            let quote = match self.quote_engine.quote(
                route,
                pricing_views,
                state.latest_slot(),
                trade_size,
                &adjustments,
            ) {
                Ok(quote) => quote,
                Err(QuoteError::ConcentratedWindowExceeded) => {
                    last_rejection = Some(RejectionReason::QuoteFailed {
                        detail: QuoteError::ConcentratedWindowExceeded.to_string(),
                    });
                    continue;
                }
                Err(error) => {
                    last_rejection = Some(RejectionReason::QuoteFailed {
                        detail: error.to_string(),
                    });
                    continue;
                }
            };
            let network_quote = quote
                .clone()
                .with_estimated_execution_cost(
                    route,
                    sol_quote_conversion_snapshot,
                    route.estimated_execution_cost_lamports,
                )
                .map_err(|error| RejectionReason::ExecutionCostNotConvertible {
                    detail: error.to_string(),
                })?;
            let reserve_usage_bps =
                reserve_usage_bps(route, leg_snapshots, &network_quote).unwrap_or_default();
            let p_land_bps = effective_landing_rate_bps(
                sizing_config,
                execution_state,
                inflight_submissions,
                route_sizing_state.landing_rate_bps,
                &network_quote,
            );
            let expected_shortfall_quote_atoms = expected_shortfall_quote_atoms(
                route,
                &network_quote,
                route_sizing_state.expected_shortfall_bps,
                active_execution_buffer_bps.unwrap_or_default(),
            );
            let execution_risk_penalty_quote_atoms = reserve_usage_penalty_quote_atoms(
                sizing_config,
                reserve_usage_bps,
                network_quote.expected_gross_profit_quote_atoms,
            );
            let pre_tip_expected_value_quote_atoms = expected_value_quote_atoms(
                p_land_bps,
                network_quote.expected_gross_profit_quote_atoms,
                network_quote.estimated_execution_cost_quote_atoms,
                expected_shortfall_quote_atoms,
                execution_risk_penalty_quote_atoms,
            );
            let quote = apply_jito_tip_policy(
                route,
                quote,
                sol_quote_conversion_snapshot,
                self.jito_tip_policy,
                tip_basis_quote_atoms(
                    self.jito_tip_policy.mode,
                    network_quote.expected_net_profit_quote_atoms,
                    pre_tip_expected_value_quote_atoms,
                ),
            )
            .map_err(|error| RejectionReason::ExecutionCostNotConvertible {
                detail: error.to_string(),
            })?;

            match self.guards.evaluate_quote(
                route,
                &quote.quote,
                execution_state,
                inflight_submissions,
            ) {
                Ok(()) => {}
                Err(reason) => {
                    last_rejection = Some(reason);
                    continue;
                }
            }

            let expected_value_quote_atoms = expected_value_quote_atoms(
                p_land_bps,
                quote.quote.expected_gross_profit_quote_atoms,
                quote.quote.estimated_execution_cost_quote_atoms,
                expected_shortfall_quote_atoms,
                execution_risk_penalty_quote_atoms,
            );
            let shared_fields = CandidateSharedFields {
                route,
                quote: &quote.quote,
                leg_snapshot_slots: leg_snapshot_slots(leg_snapshots),
                sol_quote_conversion_snapshot_slot: used_sol_quote_conversion_snapshot
                    .map(|snapshot| snapshot.last_update_slot),
                active_execution_buffer_bps,
                source_input_balance,
                minimum_acceptable_output: self.guards.minimum_acceptable_output(
                    route,
                    quote.quote.input_amount,
                    quote.quote.estimated_execution_cost_quote_atoms,
                ),
                p_land_bps,
                expected_shortfall_quote_atoms,
                expected_value_quote_atoms,
                estimated_network_fee_lamports: quote.estimated_network_fee_lamports,
                estimated_network_fee_quote_atoms: quote.estimated_network_fee_quote_atoms,
                jito_tip_lamports: quote.jito_tip_lamports,
                jito_tip_quote_atoms: quote.jito_tip_quote_atoms,
            };
            let live_candidate = match route.sizing.mode {
                SizingMode::Legacy | SizingMode::EvShadow => build_candidate(
                    shared_fields.clone(),
                    CandidateSelectionSource::Legacy,
                    quote.quote.expected_net_profit_quote_atoms,
                ),
                SizingMode::EvLive => {
                    let minimum_profit = self
                        .guards
                        .minimum_profit_quote_atoms_for_route(route, quote.quote.input_amount);
                    if expected_value_quote_atoms < minimum_profit {
                        last_rejection = Some(RejectionReason::ProfitBelowThreshold {
                            expected: expected_value_quote_atoms,
                            minimum: minimum_profit,
                        });
                        continue;
                    }
                    build_candidate(
                        shared_fields.clone(),
                        CandidateSelectionSource::Ev,
                        expected_value_quote_atoms,
                    )
                }
            };
            let shadow_candidate = match route.sizing.mode {
                SizingMode::Legacy => None,
                SizingMode::EvShadow => {
                    let minimum_profit = self
                        .guards
                        .minimum_profit_quote_atoms_for_route(route, quote.quote.input_amount);
                    if expected_value_quote_atoms >= minimum_profit {
                        Some(build_candidate(
                            shared_fields.clone(),
                            CandidateSelectionSource::Ev,
                            expected_value_quote_atoms,
                        ))
                    } else {
                        None
                    }
                }
                SizingMode::EvLive => Some(build_candidate(
                    shared_fields,
                    CandidateSelectionSource::Legacy,
                    quote.quote.expected_net_profit_quote_atoms,
                )),
            };
            return Ok(RouteSelection {
                live_candidate,
                shadow_candidate,
            });
        }

        Err(last_rejection.unwrap_or(RejectionReason::RouteFilteredOut {
            route_id: route.route_id.clone(),
        }))
    }
}

fn route_quote_adjustments(
    route: &RouteDefinition,
    active_execution_buffer_bps: Option<u16>,
) -> QuoteExecutionAdjustments {
    let mut adjustments = QuoteExecutionAdjustments::zero(route.kind);
    let Some(_) = route.execution_protection.as_ref() else {
        return adjustments;
    };
    let buffer_bps = active_execution_buffer_bps.unwrap_or(0);
    for (index, leg) in route.legs.iter().enumerate() {
        if leg.side == SwapSide::BuyBase {
            adjustments.extra_leg_slippage_bps.as_mut_slice()[index] = buffer_bps;
        }
    }
    adjustments
}

fn trade_sizes(
    route: &RouteDefinition,
    min_trade_size: u64,
    effective_max_trade_size: u64,
) -> Vec<u64> {
    let max_trade_size = route
        .max_trade_size
        .max(1)
        .min(effective_max_trade_size.max(1));
    let default_trade_size = route
        .default_trade_size
        .clamp(min_trade_size, max_trade_size);

    let mut candidates = BTreeSet::from([min_trade_size, default_trade_size, max_trade_size]);
    if !route.size_ladder.is_empty() {
        candidates.extend(
            route
                .size_ladder
                .iter()
                .copied()
                .filter(|size| *size >= min_trade_size && *size <= max_trade_size),
        );
    } else {
        candidates.extend(
            DEFAULT_SIZE_LADDER
                .iter()
                .copied()
                .filter(|size| *size >= min_trade_size && *size <= max_trade_size),
        );
    }

    prioritize_trade_sizes(candidates.into_iter().collect(), default_trade_size)
}

fn effective_min_trade_size(
    route: &RouteDefinition,
    sol_quote_conversion_snapshot: Option<&PoolSnapshot>,
) -> Result<u64, String> {
    let route_min_trade_size = route
        .min_trade_size
        .max(1)
        .min(route.default_trade_size.max(1))
        .min(route.max_trade_size.max(1));
    let sizing_floor = sol_lamports_to_quote_atoms(
        route,
        sol_quote_conversion_snapshot,
        route.sizing.min_trade_floor_sol_lamports,
    )
    .map_err(|error| error.to_string())?;
    Ok(route_min_trade_size.max(sizing_floor))
}

fn fixed_trade_size(
    route: &RouteDefinition,
    sol_quote_conversion_snapshot: Option<&PoolSnapshot>,
    effective_max_trade_size: u64,
) -> Result<u64, String> {
    let route_min_trade_size = route.min_trade_size.max(1);
    let route_max_trade_size = route
        .max_trade_size
        .max(1)
        .min(effective_max_trade_size.max(1));
    if route_min_trade_size > route_max_trade_size {
        return Err("route max trade size below minimum trade size".to_string());
    }
    let sizing_floor = sol_lamports_to_quote_atoms(
        route,
        sol_quote_conversion_snapshot,
        route.sizing.min_trade_floor_sol_lamports,
    )
    .map_err(|error| error.to_string())?;
    Ok(route_min_trade_size
        .max(sizing_floor)
        .min(route_max_trade_size))
}

fn fixed_trade_sizes(
    route: &RouteDefinition,
    sol_quote_conversion_snapshot: Option<&PoolSnapshot>,
    effective_max_trade_size: u64,
) -> Result<Vec<u64>, String> {
    let target = fixed_trade_size(
        route,
        sol_quote_conversion_snapshot,
        effective_max_trade_size,
    )?;
    let route_min_trade_size = route.min_trade_size.max(1);
    let route_max_trade_size = route
        .max_trade_size
        .max(1)
        .min(effective_max_trade_size.max(1));
    let mut candidates = BTreeSet::from([target, route_min_trade_size]);
    candidates.extend(
        route
            .size_ladder
            .iter()
            .copied()
            .filter(|size| *size >= route_min_trade_size && *size <= route_max_trade_size),
    );
    let mut ordered = vec![target];
    let mut smaller = Vec::new();
    let mut larger = Vec::new();
    for size in candidates {
        if size < target {
            smaller.push(size);
        } else if size > target {
            larger.push(size);
        }
    }
    smaller.sort_unstable_by(|left, right| right.cmp(left));
    ordered.extend(smaller);
    larger.sort_unstable();
    ordered.extend(larger);
    Ok(ordered)
}

fn prioritize_trade_sizes(mut sizes: Vec<u64>, default_trade_size: u64) -> Vec<u64> {
    sizes.sort_unstable();
    sizes.dedup();

    let mut ordered = Vec::with_capacity(sizes.len());
    if sizes.contains(&default_trade_size) {
        ordered.push(default_trade_size);
    }
    for size in sizes
        .iter()
        .copied()
        .filter(|size| *size < default_trade_size)
        .rev()
    {
        ordered.push(size);
    }
    for size in sizes
        .iter()
        .copied()
        .filter(|size| *size > default_trade_size)
    {
        ordered.push(size);
    }
    ordered
}

fn source_input_balance(
    route: &RouteDefinition,
    execution_state: &ExecutionSnapshot,
) -> Result<Option<u64>, RejectionReason> {
    let Some(account) = route.input_source_account.as_ref() else {
        return Ok(None);
    };
    let Some(balance) = execution_state.source_token_balances.get(account).copied() else {
        return Err(RejectionReason::SourceBalanceUnavailable {
            account: account.clone(),
        });
    };
    Ok(Some(balance))
}

fn effective_max_trade_size(
    route: &RouteDefinition,
    route_sizing_max_trade_size: u64,
    source_input_balance: Option<u64>,
) -> u64 {
    let mut maximum = route.max_trade_size.min(route_sizing_max_trade_size);
    if let Some(balance) = source_input_balance {
        maximum = maximum.min(balance);
    }
    maximum
}

fn reserve_usage_bps(
    route: &RouteDefinition,
    snapshots: &[&PoolSnapshot],
    quote: &crate::quote::RouteQuote,
) -> Option<u64> {
    route
        .legs
        .iter()
        .zip(snapshots.iter())
        .enumerate()
        .filter_map(|(index, (leg, snapshot))| {
            let (reserve_in, _) =
                snapshot.constant_product_reserves_for(&leg.input_mint, &leg.output_mint)?;
            let input_amount = quote.leg_quotes[index].input_amount;
            Some(
                ((u128::from(input_amount) * 10_000u128)
                    .saturating_add(u128::from(reserve_in).saturating_sub(1))
                    / u128::from(reserve_in)) as u64,
            )
        })
        .max()
}

fn effective_landing_rate_bps(
    sizing_config: &StrategySizingConfig,
    execution_state: &ExecutionSnapshot,
    inflight_submissions: usize,
    route_landing_rate_bps: u16,
    quote: &crate::quote::RouteQuote,
) -> u16 {
    let inflight_penalty_bps = u16::try_from(inflight_submissions)
        .unwrap_or(u16::MAX)
        .saturating_mul(sizing_config.inflight_penalty_bps_per_submission)
        .min(sizing_config.max_inflight_penalty_bps);
    let blockhash_penalty_bps = execution_state
        .blockhash_slot_lag()
        .unwrap_or_default()
        .min(u64::from(u16::MAX)) as u16;
    let blockhash_penalty_bps = blockhash_penalty_bps
        .saturating_mul(sizing_config.blockhash_penalty_bps_per_slot)
        .min(sizing_config.max_blockhash_penalty_bps);
    let quote_age_slots = execution_state.head_slot.saturating_sub(quote.quoted_slot);
    let quote_age_penalty_bps = quote_age_slots.min(u64::from(u16::MAX)) as u16;
    let quote_age_penalty_bps = quote_age_penalty_bps
        .saturating_mul(sizing_config.quote_age_penalty_bps_per_slot)
        .min(sizing_config.max_quote_age_penalty_bps);
    let max_ticks_crossed = quote
        .leg_quotes
        .iter()
        .map(|leg| leg.ticks_crossed)
        .max()
        .unwrap_or_default()
        .min(u32::from(u16::MAX)) as u16;
    let tick_cross_penalty_bps = max_ticks_crossed
        .saturating_mul(sizing_config.tick_cross_penalty_bps_per_tick)
        .min(sizing_config.max_tick_cross_penalty_bps);
    route_landing_rate_bps
        .saturating_sub(inflight_penalty_bps)
        .saturating_sub(blockhash_penalty_bps)
        .saturating_sub(quote_age_penalty_bps)
        .saturating_sub(tick_cross_penalty_bps)
}

fn expected_shortfall_quote_atoms(
    route: &RouteDefinition,
    quote: &crate::quote::RouteQuote,
    route_expected_shortfall_bps: u16,
    active_execution_buffer_bps: u16,
) -> u64 {
    let shortfall_bps = route_expected_shortfall_bps
        .max(active_execution_buffer_bps)
        .saturating_mul(route.kind.expected_shortfall_multiplier_bps())
        .div_ceil(10_000);
    ((u128::from(quote.net_output_amount) * u128::from(shortfall_bps)) / 10_000u128) as u64
}

fn reserve_usage_penalty_quote_atoms(
    sizing_config: &StrategySizingConfig,
    reserve_usage_bps: u64,
    expected_gross_profit_quote_atoms: i64,
) -> u64 {
    if expected_gross_profit_quote_atoms <= 0 {
        return 0;
    }
    let penalty_bps = ((reserve_usage_bps
        .saturating_mul(u64::from(sizing_config.max_reserve_usage_penalty_bps)))
        / MAX_RESERVE_USAGE_BPS)
        .min(u64::from(sizing_config.max_reserve_usage_penalty_bps));
    ((expected_gross_profit_quote_atoms as u128) * u128::from(penalty_bps) / 10_000u128) as u64
}

fn expected_value_quote_atoms(
    p_land_bps: u16,
    expected_gross_profit_quote_atoms: i64,
    estimated_execution_cost_quote_atoms: u64,
    expected_shortfall_quote_atoms: u64,
    execution_risk_penalty_quote_atoms: u64,
) -> i64 {
    let value_before_penalty = i128::from(expected_gross_profit_quote_atoms)
        .saturating_sub(i128::from(estimated_execution_cost_quote_atoms))
        .saturating_sub(i128::from(expected_shortfall_quote_atoms));
    let weighted = value_before_penalty
        .saturating_mul(i128::from(p_land_bps))
        .saturating_div(10_000);
    (weighted - i128::from(execution_risk_penalty_quote_atoms))
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

struct TipAdjustedQuote {
    quote: crate::quote::RouteQuote,
    estimated_network_fee_lamports: u64,
    estimated_network_fee_quote_atoms: u64,
    jito_tip_lamports: u64,
    jito_tip_quote_atoms: u64,
}

#[derive(Clone)]
struct CandidateSharedFields<'a> {
    route: &'a RouteDefinition,
    quote: &'a crate::quote::RouteQuote,
    leg_snapshot_slots: RouteLegSequence<u64>,
    sol_quote_conversion_snapshot_slot: Option<u64>,
    active_execution_buffer_bps: Option<u16>,
    source_input_balance: Option<u64>,
    minimum_acceptable_output: u64,
    p_land_bps: u16,
    expected_shortfall_quote_atoms: u64,
    expected_value_quote_atoms: i64,
    estimated_network_fee_lamports: u64,
    estimated_network_fee_quote_atoms: u64,
    jito_tip_lamports: u64,
    jito_tip_quote_atoms: u64,
}

fn build_candidate(
    shared: CandidateSharedFields<'_>,
    selected_by: CandidateSelectionSource,
    ranking_score_quote_atoms: i64,
) -> OpportunityCandidate {
    OpportunityCandidate {
        route_id: shared.route.route_id.clone(),
        route_kind: shared.route.kind,
        quoted_slot: shared.quote.quoted_slot,
        leg_snapshot_slots: shared.leg_snapshot_slots,
        sol_quote_conversion_snapshot_slot: shared.sol_quote_conversion_snapshot_slot,
        trade_size: shared.quote.input_amount,
        selected_by,
        ranking_score_quote_atoms,
        expected_value_quote_atoms: shared.expected_value_quote_atoms,
        p_land_bps: shared.p_land_bps,
        expected_shortfall_quote_atoms: shared.expected_shortfall_quote_atoms,
        active_execution_buffer_bps: shared
            .route
            .execution_protection
            .as_ref()
            .map(|_| shared.active_execution_buffer_bps.unwrap_or(0)),
        source_input_balance: shared.source_input_balance,
        expected_net_output: shared.quote.net_output_amount,
        minimum_acceptable_output: shared.minimum_acceptable_output,
        expected_gross_profit_quote_atoms: shared.quote.expected_gross_profit_quote_atoms,
        estimated_network_fee_lamports: shared.estimated_network_fee_lamports,
        estimated_network_fee_quote_atoms: shared.estimated_network_fee_quote_atoms,
        jito_tip_lamports: shared.jito_tip_lamports,
        jito_tip_quote_atoms: shared.jito_tip_quote_atoms,
        estimated_execution_cost_lamports: shared.quote.estimated_execution_cost_lamports,
        estimated_execution_cost_quote_atoms: shared.quote.estimated_execution_cost_quote_atoms,
        expected_net_profit_quote_atoms: shared.quote.expected_net_profit_quote_atoms,
        intermediate_output_amounts: shared
            .quote
            .leg_quotes
            .as_slice()
            .iter()
            .take(shared.quote.leg_quotes.len().saturating_sub(1))
            .map(|leg| leg.output_amount)
            .collect(),
        leg_quotes: shared.quote.leg_quotes.clone(),
    }
}

fn apply_jito_tip_policy(
    route: &RouteDefinition,
    quote: crate::quote::RouteQuote,
    sol_quote_conversion_snapshot: Option<&PoolSnapshot>,
    jito_tip_policy: JitoTipPolicy,
    tip_basis_quote_atoms: i64,
) -> Result<TipAdjustedQuote, QuoteError> {
    let network_quote = quote.clone().with_estimated_execution_cost(
        route,
        sol_quote_conversion_snapshot,
        route.estimated_execution_cost_lamports,
    )?;
    let jito_tip_lamports = compute_jito_tip_lamports(
        route,
        sol_quote_conversion_snapshot,
        &network_quote,
        jito_tip_policy,
        tip_basis_quote_atoms,
    )?;
    let quote = quote.with_estimated_execution_cost(
        route,
        sol_quote_conversion_snapshot,
        route
            .estimated_execution_cost_lamports
            .saturating_add(jito_tip_lamports),
    )?;
    Ok(TipAdjustedQuote {
        estimated_network_fee_lamports: network_quote.estimated_execution_cost_lamports,
        estimated_network_fee_quote_atoms: network_quote.estimated_execution_cost_quote_atoms,
        jito_tip_lamports,
        jito_tip_quote_atoms: quote
            .estimated_execution_cost_quote_atoms
            .saturating_sub(network_quote.estimated_execution_cost_quote_atoms),
        quote,
    })
}

fn compute_jito_tip_lamports(
    route: &RouteDefinition,
    sol_quote_conversion_snapshot: Option<&PoolSnapshot>,
    _network_quote: &crate::quote::RouteQuote,
    jito_tip_policy: JitoTipPolicy,
    tip_basis_quote_atoms: i64,
) -> Result<u64, QuoteError> {
    match jito_tip_policy.mode {
        JitoTipMode::Fixed => Ok(route.default_jito_tip_lamports),
        JitoTipMode::PnlRatio | JitoTipMode::RiskAdjustedPnlRatio => {
            let tip_basis_quote_atoms = u64::try_from(tip_basis_quote_atoms.max(0)).unwrap_or(0);
            let raw_tip_quote_atoms = ((u128::from(tip_basis_quote_atoms)
                * u128::from(jito_tip_policy.share_bps_of_expected_net_profit))
                / 10_000u128) as u64;
            let raw_tip_lamports = quote_atoms_to_sol_lamports(
                route,
                sol_quote_conversion_snapshot,
                raw_tip_quote_atoms,
            )?;
            let min_lamports = jito_tip_policy.min_lamports;
            let max_lamports = jito_tip_policy.max_lamports.max(min_lamports);
            Ok(raw_tip_lamports.clamp(min_lamports, max_lamports))
        }
    }
}

fn tip_basis_quote_atoms(
    mode: JitoTipMode,
    expected_net_profit_quote_atoms: i64,
    expected_value_quote_atoms: i64,
) -> i64 {
    match mode {
        JitoTipMode::Fixed => 0,
        JitoTipMode::PnlRatio => expected_net_profit_quote_atoms,
        JitoTipMode::RiskAdjustedPnlRatio => expected_value_quote_atoms,
    }
}

fn leg_snapshot_slots(snapshots: &[&PoolSnapshot]) -> RouteLegSequence<u64> {
    RouteLegSequence::from_vec(
        snapshots
            .iter()
            .map(|snapshot| snapshot.last_update_slot)
            .collect(),
    )
    .expect("route snapshots must have 2 or 3 legs")
}

fn select_best_candidate(best: &mut Option<OpportunityCandidate>, candidate: OpportunityCandidate) {
    match best {
        Some(current)
            if current.ranking_score_quote_atoms > candidate.ranking_score_quote_atoms => {}
        Some(current)
            if current.ranking_score_quote_atoms == candidate.ranking_score_quote_atoms
                && current.trade_size <= candidate.trade_size => {}
        _ => *best = Some(candidate),
    }
}

fn reject_stale_for_execution(
    route: &RouteDefinition,
    head_slot: u64,
    snapshots: &[&PoolSnapshot],
) -> Result<(), RejectionReason> {
    let maximum = route.max_quote_slot_lag;
    let Some((pool_id, slot_lag)) = snapshots
        .iter()
        .copied()
        .map(|snapshot| {
            (
                snapshot.pool_id.clone(),
                head_slot.saturating_sub(snapshot.last_update_slot),
            )
        })
        .max_by_key(|(_, slot_lag)| *slot_lag)
    else {
        return Ok(());
    };

    if slot_lag > maximum {
        return Err(RejectionReason::QuoteStaleForExecution {
            pool_id,
            slot_lag,
            maximum,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

    use super::{OpportunitySelector, effective_min_trade_size, fixed_trade_size, trade_sizes};
    use crate::{
        guards::{GuardrailConfig, GuardrailSet},
        quote::{
            LegQuote, PoolPricingView, QuoteEngine, QuoteError, QuoteExecutionAdjustments,
            RouteQuote,
        },
        reasons::RejectionReason,
        route_registry::{
            ExecutionProtectionPolicy, JitoTipMode, JitoTipPolicy, RouteDefinition, RouteKind,
            RouteLeg, RouteLegSequence, RouteSizingPolicy, SizingMode, StrategySizingConfig,
            SwapSide,
        },
    };
    use domain::{
        EventSourceKind, ExecutionStateSnapshot, NormalizedEvent, PoolId, PoolSnapshotUpdate,
        PoolVenue, RouteId, SnapshotConfidence,
    };
    use state::StatePlane;

    #[derive(Debug, Default)]
    struct MockQuoteEngine;

    fn sizing_config() -> StrategySizingConfig {
        StrategySizingConfig {
            mode: SizingMode::Legacy,
            fixed_trade_size: false,
            min_trade_floor_sol_lamports: 0,
            base_landing_rate_bps: 8_500,
            ewma_alpha_bps: 2_000,
            base_expected_shortfall_bps: 75,
            max_expected_shortfall_bps: 500,
            too_little_output_shortfall_step_bps: 75,
            inflight_penalty_bps_per_submission: 25,
            max_inflight_penalty_bps: 1_500,
            blockhash_penalty_bps_per_slot: 10,
            max_blockhash_penalty_bps: 1_000,
            quote_age_penalty_bps_per_slot: 15,
            max_quote_age_penalty_bps: 750,
            tick_cross_penalty_bps_per_tick: 20,
            max_tick_cross_penalty_bps: 1_000,
            max_reserve_usage_penalty_bps: 1_250,
        }
    }

    impl QuoteEngine for MockQuoteEngine {
        fn quote<'a>(
            &self,
            route: &RouteDefinition,
            _snapshots: &[PoolPricingView<'a>],
            quoted_slot: u64,
            input_amount: u64,
            adjustments: &QuoteExecutionAdjustments,
        ) -> Result<RouteQuote, QuoteError> {
            let adjustment_penalty = i64::from(adjustments.extra_leg_slippage_bps[0])
                * i64::try_from(input_amount / 250_000).unwrap_or(0)
                / 5;
            let expected_net_profit_quote_atoms = if route.execution_protection.is_some() {
                match input_amount {
                    1_000_000 => 60 - adjustment_penalty,
                    500_000 => 55 - adjustment_penalty,
                    250_000 => 40 - adjustment_penalty,
                    _ => -100,
                }
            } else {
                match input_amount {
                    1_000_000 => -50,
                    500_000 => 25,
                    250_000 => 25,
                    _ => -100,
                }
            };
            let net_output_amount = if expected_net_profit_quote_atoms >= 0 {
                input_amount.saturating_add(expected_net_profit_quote_atoms as u64)
            } else {
                input_amount.saturating_sub((-expected_net_profit_quote_atoms) as u64)
            };
            let mut leg_quotes = Vec::with_capacity(route.legs.len());
            let mut next_leg_input = input_amount;
            for (index, leg) in route.legs.iter().enumerate() {
                let output_amount = if index + 1 == route.legs.len() {
                    net_output_amount
                } else {
                    next_leg_input
                };
                leg_quotes.push(LegQuote {
                    venue: leg.venue.clone(),
                    pool_id: leg.pool_id.clone(),
                    side: leg.side,
                    input_amount: next_leg_input,
                    output_amount,
                    fee_paid: 0,
                    current_tick_index: None,
                    ticks_crossed: 0,
                });
                next_leg_input = output_amount;
            }
            Ok(RouteQuote {
                quoted_slot,
                input_amount,
                gross_output_amount: net_output_amount,
                net_output_amount,
                expected_gross_profit_quote_atoms: expected_net_profit_quote_atoms,
                estimated_execution_cost_lamports: 0,
                estimated_execution_cost_quote_atoms: 0,
                expected_net_profit_quote_atoms,
                leg_quotes: RouteLegSequence::from_vec(leg_quotes)
                    .expect("test route should have a valid leg count"),
            })
        }
    }

    #[derive(Debug)]
    struct CountingQuoteEngine {
        calls: Arc<AtomicUsize>,
    }

    impl QuoteEngine for CountingQuoteEngine {
        fn quote<'a>(
            &self,
            route: &RouteDefinition,
            snapshots: &[PoolPricingView<'a>],
            quoted_slot: u64,
            input_amount: u64,
            adjustments: &QuoteExecutionAdjustments,
        ) -> Result<RouteQuote, QuoteError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            MockQuoteEngine.quote(route, snapshots, quoted_slot, input_amount, adjustments)
        }
    }

    #[derive(Debug)]
    struct FailingDefaultQuoteEngine;

    impl QuoteEngine for FailingDefaultQuoteEngine {
        fn quote<'a>(
            &self,
            route: &RouteDefinition,
            _snapshots: &[PoolPricingView<'a>],
            quoted_slot: u64,
            input_amount: u64,
            _adjustments: &QuoteExecutionAdjustments,
        ) -> Result<RouteQuote, QuoteError> {
            match input_amount {
                1_000_000 | 500_000 => Err(QuoteError::ConcentratedWindowExceeded),
                250_000 => {
                    let mut leg_quotes = Vec::with_capacity(route.legs.len());
                    let mut next_leg_input = input_amount;
                    for (index, leg) in route.legs.iter().enumerate() {
                        let output_amount = if index + 1 == route.legs.len() {
                            250_025
                        } else {
                            next_leg_input
                        };
                        leg_quotes.push(LegQuote {
                            venue: leg.venue.clone(),
                            pool_id: leg.pool_id.clone(),
                            side: leg.side,
                            input_amount: next_leg_input,
                            output_amount,
                            fee_paid: 0,
                            current_tick_index: None,
                            ticks_crossed: 0,
                        });
                        next_leg_input = output_amount;
                    }
                    Ok(RouteQuote {
                        quoted_slot,
                        input_amount,
                        gross_output_amount: 250_025,
                        net_output_amount: 250_025,
                        expected_gross_profit_quote_atoms: 25,
                        estimated_execution_cost_lamports: 0,
                        estimated_execution_cost_quote_atoms: 0,
                        expected_net_profit_quote_atoms: 25,
                        leg_quotes: RouteLegSequence::from_vec(leg_quotes)
                            .expect("test route should have a valid leg count"),
                    })
                }
                _ => Err(QuoteError::ConcentratedWindowExceeded),
            }
        }
    }

    fn route_definition() -> RouteDefinition {
        RouteDefinition {
            kind: RouteKind::TwoLeg,
            route_id: RouteId("route-a".into()),
            input_mint: "USDC".into(),
            output_mint: "USDC".into(),
            input_source_account: None,
            base_mint: Some("SOL".into()),
            quote_mint: Some("USDC".into()),
            min_profit_quote_atoms: None,
            sol_quote_conversion_pool_id: Some(PoolId("pool-a".into())),
            legs: [
                RouteLeg {
                    venue: "venue-a".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    input_mint: "USDC".into(),
                    output_mint: "SOL".into(),
                    fee_bps: None,
                },
                RouteLeg {
                    venue: "venue-b".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    input_mint: "SOL".into(),
                    output_mint: "USDC".into(),
                    fee_bps: None,
                },
            ]
            .into(),
            max_quote_slot_lag: 32,
            min_trade_size: 250_000,
            default_trade_size: 1_000_000,
            max_trade_size: 5_000_000,
            size_ladder: Vec::new(),
            default_jito_tip_lamports: 0,
            estimated_execution_cost_lamports: 0,
            sizing: RouteSizingPolicy {
                mode: SizingMode::Legacy,
                min_trade_floor_sol_lamports: 0,
                base_landing_rate_bps: 8_500,
                base_expected_shortfall_bps: 75,
                max_expected_shortfall_bps: 500,
            },
            execution_protection: None,
        }
    }

    fn triangular_route_definition() -> RouteDefinition {
        RouteDefinition {
            kind: RouteKind::Triangular,
            route_id: RouteId("route-tri".into()),
            input_mint: "USDC".into(),
            output_mint: "USDC".into(),
            input_source_account: None,
            base_mint: Some("SOL".into()),
            quote_mint: Some("USDC".into()),
            min_profit_quote_atoms: None,
            sol_quote_conversion_pool_id: Some(PoolId("pool-a".into())),
            legs: [
                RouteLeg {
                    venue: "venue-a".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    input_mint: "USDC".into(),
                    output_mint: "SOL".into(),
                    fee_bps: None,
                },
                RouteLeg {
                    venue: "venue-b".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    input_mint: "SOL".into(),
                    output_mint: "USDT".into(),
                    fee_bps: None,
                },
                RouteLeg {
                    venue: "venue-c".into(),
                    pool_id: PoolId("pool-c".into()),
                    side: SwapSide::SellBase,
                    input_mint: "USDT".into(),
                    output_mint: "USDC".into(),
                    fee_bps: None,
                },
            ]
            .into(),
            max_quote_slot_lag: 32,
            min_trade_size: 250_000,
            default_trade_size: 1_000_000,
            max_trade_size: 5_000_000,
            size_ladder: Vec::new(),
            default_jito_tip_lamports: 0,
            estimated_execution_cost_lamports: 0,
            sizing: RouteSizingPolicy {
                mode: SizingMode::Legacy,
                min_trade_floor_sol_lamports: 0,
                base_landing_rate_bps: 8_500,
                base_expected_shortfall_bps: 75,
                max_expected_shortfall_bps: 500,
            },
            execution_protection: None,
        }
    }

    fn trade_sizes_for_test(route: &RouteDefinition) -> Vec<u64> {
        trade_sizes(
            route,
            effective_min_trade_size(route, None).expect("sizing floor"),
            route.max_trade_size,
        )
    }

    #[test]
    fn default_ladder_evaluates_default_then_smaller_then_larger_sizes() {
        let sizes = trade_sizes_for_test(&route_definition());
        assert_eq!(
            sizes,
            vec![1_000_000, 500_000, 250_000, 2_000_000, 3_000_000, 5_000_000]
        );
    }

    #[test]
    fn explicit_ladder_respects_min_trade_size_and_centered_order() {
        let mut route = route_definition();
        route.size_ladder = vec![100_000, 250_000, 500_000, 2_000_000, 3_000_000];

        let sizes = trade_sizes_for_test(&route);
        assert_eq!(
            sizes,
            vec![1_000_000, 500_000, 250_000, 2_000_000, 3_000_000, 5_000_000]
        );
    }

    #[test]
    fn fixed_trade_size_uses_floor_but_caps_at_route_maximum() {
        let mut route = route_definition();
        route.input_mint = SOL_MINT.into();
        route.output_mint = SOL_MINT.into();
        route.quote_mint = Some(SOL_MINT.into());
        route.sol_quote_conversion_pool_id = None;
        route.sizing.min_trade_floor_sol_lamports = 10_000_000;
        assert_eq!(
            fixed_trade_size(&route, None, route.max_trade_size).expect("fixed size"),
            5_000_000
        );
    }

    #[test]
    fn selector_uses_fixed_trade_size_when_enabled() {
        let mut route = route_definition();
        route.input_mint = SOL_MINT.into();
        route.output_mint = SOL_MINT.into();
        route.quote_mint = Some(SOL_MINT.into());
        route.sol_quote_conversion_pool_id = None;
        route.min_trade_size = 250_000;
        route.default_trade_size = 1_000_000;
        route.max_trade_size = 5_000_000;
        route.sizing.min_trade_floor_sol_lamports = 500_000;
        let mut sizing = sizing_config();
        sizing.fixed_trade_size = true;
        let execution_state = ExecutionStateSnapshot {
            head_slot: 10,
            rpc_slot: Some(10),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(10),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::new(
            MockQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: -10_000,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(2);

        for pool_id in ["pool-a", "pool-b"] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    10,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "SOL".into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot: 10,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }

        let candidate = selector
            .evaluate_route(&route, &state, &execution_state, 0, &sizing, None, None)
            .expect("fixed trade size should be evaluated");
        assert_eq!(candidate.live_candidate.trade_size, 500_000);
    }

    #[test]
    fn selector_applies_pnl_ratio_tip_to_selected_candidate() {
        let mut route = route_definition();
        route.base_mint = Some(SOL_MINT.into());
        route.legs.as_mut_slice()[0].output_mint = SOL_MINT.into();
        route.legs.as_mut_slice()[1].input_mint = SOL_MINT.into();
        let execution_state = ExecutionStateSnapshot {
            head_slot: 10,
            rpc_slot: Some(10),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(10),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::with_jito_tip_policy(
            MockQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
            JitoTipPolicy {
                mode: JitoTipMode::PnlRatio,
                share_bps_of_expected_net_profit: 2_000,
                min_lamports: 1,
                max_lamports: 10,
            },
        );
        let mut state = StatePlane::new(2);

        for pool_id in ["pool-a", "pool-b"] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    10,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: SOL_MINT.into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot: 10,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }

        let candidate = selector
            .evaluate_route(
                &route,
                &state,
                &execution_state,
                0,
                &sizing_config(),
                None,
                None,
            )
            .expect("candidate should remain profitable with pnl-ratio tip");
        assert_eq!(candidate.live_candidate.trade_size, 250_000);
        assert_eq!(candidate.live_candidate.estimated_network_fee_lamports, 0);
        assert_eq!(candidate.live_candidate.jito_tip_lamports, 5);
        assert_eq!(candidate.live_candidate.jito_tip_quote_atoms, 5);
        assert_eq!(candidate.live_candidate.expected_net_profit_quote_atoms, 20);
    }

    #[test]
    fn selector_rejects_route_when_minimum_tip_makes_trade_unprofitable() {
        let mut route = route_definition();
        route.base_mint = Some(SOL_MINT.into());
        route.legs.as_mut_slice()[0].output_mint = SOL_MINT.into();
        route.legs.as_mut_slice()[1].input_mint = SOL_MINT.into();
        let execution_state = ExecutionStateSnapshot {
            head_slot: 10,
            rpc_slot: Some(10),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(10),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::with_jito_tip_policy(
            MockQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
            JitoTipPolicy {
                mode: JitoTipMode::PnlRatio,
                share_bps_of_expected_net_profit: 0,
                min_lamports: 30,
                max_lamports: 30,
            },
        );
        let mut state = StatePlane::new(2);

        for pool_id in ["pool-a", "pool-b"] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    10,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: SOL_MINT.into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot: 10,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }

        let rejection = selector
            .evaluate_route(
                &route,
                &state,
                &execution_state,
                0,
                &sizing_config(),
                None,
                None,
            )
            .expect_err("minimum tip should reject all profitable sizes");
        assert!(matches!(
            rejection,
            RejectionReason::ProfitBelowThreshold { minimum: 10, .. }
        ));
    }

    #[test]
    fn selector_fixed_trade_size_falls_back_when_target_exceeds_concentrated_window() {
        let route = route_definition();
        let execution_state = ExecutionStateSnapshot {
            head_slot: 10,
            rpc_slot: Some(10),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(10),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::new(
            FailingDefaultQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(2);

        for pool_id in ["pool-a", "pool-b"] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    10,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "SOL".into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot: 10,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }

        let mut sizing = sizing_config();
        sizing.fixed_trade_size = true;

        let candidate = selector
            .evaluate_route(&route, &state, &execution_state, 0, &sizing, None, None)
            .expect("smaller fixed trade size should be selected after window failures");

        assert_eq!(candidate.live_candidate.trade_size, 250_000);
        assert_eq!(candidate.live_candidate.expected_net_profit_quote_atoms, 25);
    }

    #[test]
    fn selector_picks_smaller_trade_size_when_default_is_unprofitable() {
        let route = route_definition();
        let execution_state = ExecutionStateSnapshot {
            head_slot: 10,
            rpc_slot: Some(10),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(10),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::new(
            MockQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(2);

        for pool_id in ["pool-a", "pool-b"] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    10,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "SOL".into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot: 10,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }

        let candidate = selector
            .evaluate_route(
                &route,
                &state,
                &execution_state,
                0,
                &sizing_config(),
                None,
                None,
            )
            .expect("smaller profitable trade size should be selected");

        assert_eq!(candidate.live_candidate.trade_size, 250_000);
        assert_eq!(candidate.live_candidate.expected_net_profit_quote_atoms, 25);
        assert_eq!(candidate.live_candidate.leg_snapshot_slots, [10, 10].into());
    }

    #[test]
    fn selector_fixed_trade_size_picks_smaller_trade_when_target_is_unprofitable() {
        let mut route = route_definition();
        route.input_mint = SOL_MINT.into();
        route.output_mint = SOL_MINT.into();
        route.quote_mint = Some(SOL_MINT.into());
        route.sol_quote_conversion_pool_id = None;
        route.sizing.min_trade_floor_sol_lamports = 1_000_000;
        let execution_state = ExecutionStateSnapshot {
            head_slot: 10,
            rpc_slot: Some(10),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(10),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::new(
            MockQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(2);

        for pool_id in ["pool-a", "pool-b"] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    10,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "SOL".into(),
                        token_mint_b: "SOL".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot: 10,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }

        let mut sizing = sizing_config();
        sizing.fixed_trade_size = true;

        let candidate = selector
            .evaluate_route(&route, &state, &execution_state, 0, &sizing, None, None)
            .expect("smaller profitable fixed trade size should be selected");

        assert_eq!(candidate.live_candidate.trade_size, 250_000);
        assert_eq!(candidate.live_candidate.expected_net_profit_quote_atoms, 25);
    }

    #[test]
    fn selector_keeps_trying_smaller_trade_sizes_after_quote_failures() {
        let route = route_definition();
        let execution_state = ExecutionStateSnapshot {
            head_slot: 10,
            rpc_slot: Some(10),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(10),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::new(
            FailingDefaultQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(2);

        for pool_id in ["pool-a", "pool-b"] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    10,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "SOL".into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot: 10,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }

        let candidate = selector
            .evaluate_route(
                &route,
                &state,
                &execution_state,
                0,
                &sizing_config(),
                None,
                None,
            )
            .expect("smaller trade size should still be selected after quote failures");

        assert_eq!(candidate.live_candidate.trade_size, 250_000);
        assert_eq!(candidate.live_candidate.expected_net_profit_quote_atoms, 25);
    }

    #[test]
    fn protected_route_buffer_can_push_selection_to_smaller_size() {
        let mut route = route_definition();
        route.execution_protection = Some(ExecutionProtectionPolicy {
            tight_max_quote_slot_lag: 4,
            base_extra_buy_leg_slippage_bps: 50,
            failure_step_bps: 25,
            max_extra_buy_leg_slippage_bps: 150,
            recovery_success_count: 3,
        });
        let execution_state = ExecutionStateSnapshot {
            head_slot: 10,
            rpc_slot: Some(10),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(10),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::new(
            MockQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(2);

        for pool_id in ["pool-a", "pool-b"] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    10,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "SOL".into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot: 10,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }

        let candidate = selector
            .evaluate_route(
                &route,
                &state,
                &execution_state,
                0,
                &sizing_config(),
                Some(50),
                None,
            )
            .expect("protected route should remain tradable");

        assert_eq!(candidate.live_candidate.trade_size, 500_000);
        assert_eq!(
            candidate.live_candidate.active_execution_buffer_bps,
            Some(50)
        );
    }

    #[test]
    fn selector_carries_leg_snapshot_slots_even_when_head_slot_is_newer() {
        let route = route_definition();
        let execution_state = ExecutionStateSnapshot {
            head_slot: 12,
            rpc_slot: Some(12),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(12),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::new(
            MockQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(4);

        for (pool_id, slot) in [("pool-a", 8), ("pool-b", 11)] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    slot,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "SOL".into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }
        state.set_latest_slot(12);

        let candidate = selector
            .evaluate_route(
                &route,
                &state,
                &execution_state,
                0,
                &sizing_config(),
                None,
                None,
            )
            .expect("candidate should be selected");

        assert_eq!(candidate.live_candidate.quoted_slot, 12);
        assert_eq!(candidate.live_candidate.leg_snapshot_slots, [8, 11].into());
        assert_eq!(candidate.live_candidate.oldest_leg_snapshot_slot(), 8);
    }

    #[test]
    fn selector_carries_conversion_snapshot_slot_when_fee_conversion_uses_third_pool() {
        let mut route = route_definition();
        route.base_mint = Some(SOL_MINT.into());
        route.sol_quote_conversion_pool_id = Some(PoolId("pool-c".into()));
        route.estimated_execution_cost_lamports = 1;
        let execution_state = ExecutionStateSnapshot {
            head_slot: 12,
            rpc_slot: Some(12),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(12),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let selector = OpportunitySelector::new(
            MockQuoteEngine,
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(4);

        for (pool_id, slot) in [("pool-a", 8), ("pool-b", 11), ("pool-c", 7)] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    slot,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: SOL_MINT.into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }
        state.set_latest_slot(12);

        let candidate = selector
            .evaluate_route(
                &route,
                &state,
                &execution_state,
                0,
                &sizing_config(),
                None,
                None,
            )
            .expect("candidate should be selected");

        assert_eq!(candidate.live_candidate.quoted_slot, 7);
        assert_eq!(candidate.live_candidate.leg_snapshot_slots, [8, 11].into());
        assert_eq!(
            candidate.live_candidate.sol_quote_conversion_snapshot_slot,
            Some(7)
        );
        assert_eq!(candidate.live_candidate.oldest_relevant_snapshot_slot(), 7);
    }

    #[test]
    fn selector_rejects_quote_stale_for_execution_before_quoting() {
        let mut route = route_definition();
        route.max_quote_slot_lag = 32;
        let execution_state = ExecutionStateSnapshot {
            head_slot: 43,
            rpc_slot: Some(43),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(43),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let selector = OpportunitySelector::new(
            CountingQuoteEngine {
                calls: Arc::clone(&calls),
            },
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(2_048);

        for (pool_id, slot) in [("pool-a", 10), ("pool-b", 11)] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    slot,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: "SOL".into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }
        state.set_latest_slot(43);

        let rejected = selector
            .evaluate_route(
                &route,
                &state,
                &execution_state,
                0,
                &sizing_config(),
                None,
                None,
            )
            .expect_err("stale-for-execution route should be rejected before quoting");

        assert_eq!(
            rejected,
            RejectionReason::QuoteStaleForExecution {
                pool_id: PoolId("pool-a".into()),
                slot_lag: 33,
                maximum: 32,
            }
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn selector_rejects_stale_conversion_snapshot_for_execution_before_quoting() {
        let mut route = route_definition();
        route.base_mint = Some(SOL_MINT.into());
        route.max_quote_slot_lag = 32;
        route.sol_quote_conversion_pool_id = Some(PoolId("pool-c".into()));
        route.estimated_execution_cost_lamports = 1;
        let execution_state = ExecutionStateSnapshot {
            head_slot: 43,
            rpc_slot: Some(43),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(43),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let selector = OpportunitySelector::new(
            CountingQuoteEngine {
                calls: Arc::clone(&calls),
            },
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(2_048);

        for (pool_id, slot) in [("pool-a", 43), ("pool-b", 42), ("pool-c", 10)] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    slot,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: SOL_MINT.into(),
                        token_mint_b: "USDC".into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }
        state.set_latest_slot(43);

        let rejected = selector
            .evaluate_route(
                &route,
                &state,
                &execution_state,
                0,
                &sizing_config(),
                None,
                None,
            )
            .expect_err("stale conversion pool should be rejected before quoting");

        assert_eq!(
            rejected,
            RejectionReason::QuoteStaleForExecution {
                pool_id: PoolId("pool-c".into()),
                slot_lag: 33,
                maximum: 32,
            }
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn selector_rejects_triangular_route_when_oldest_leg_snapshot_is_stale() {
        let route = triangular_route_definition();
        let execution_state = ExecutionStateSnapshot {
            head_slot: 43,
            rpc_slot: Some(43),
            latest_blockhash: Some("blockhash-1".into()),
            blockhash_slot: Some(43),
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 1_000_000,
            source_token_balances: std::collections::HashMap::new(),
            wallet_ready: true,
            kill_switch_enabled: false,
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let selector = OpportunitySelector::new(
            CountingQuoteEngine {
                calls: Arc::clone(&calls),
            },
            GuardrailSet::new(GuardrailConfig {
                min_profit_quote_atoms: 10,
                require_route_warm: false,
                ..GuardrailConfig::default()
            }),
        );
        let mut state = StatePlane::new(2_048);

        for (pool_id, slot, mint_a, mint_b) in [
            ("pool-a", 10, "SOL", "USDC"),
            ("pool-b", 41, "SOL", "USDT"),
            ("pool-c", 42, "USDT", "USDC"),
        ] {
            state
                .apply_event(&NormalizedEvent::pool_snapshot_update(
                    EventSourceKind::Synthetic,
                    1,
                    slot,
                    PoolSnapshotUpdate {
                        pool_id: pool_id.into(),
                        price_bps: 10_000,
                        fee_bps: 4,
                        reserve_depth: 10_000_000,
                        reserve_a: Some(10_000_000),
                        reserve_b: Some(10_000_000),
                        active_liquidity: Some(10_000_000),
                        sqrt_price_x64: None,
                        venue: PoolVenue::OrcaSimplePool,
                        confidence: SnapshotConfidence::Executable,
                        repair_pending: Some(false),
                        token_mint_a: mint_a.into(),
                        token_mint_b: mint_b.into(),
                        tick_spacing: 0,
                        current_tick_index: None,
                        slot,
                        write_version: 1,
                    },
                ))
                .expect("snapshot update should apply");
        }
        state.set_latest_slot(43);

        let rejected = selector
            .evaluate_route(
                &route,
                &state,
                &execution_state,
                0,
                &sizing_config(),
                None,
                None,
            )
            .expect_err("oldest triangular leg should drive stale rejection");

        assert_eq!(
            rejected,
            RejectionReason::QuoteStaleForExecution {
                pool_id: PoolId("pool-a".into()),
                slot_lag: 33,
                maximum: 32,
            }
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
