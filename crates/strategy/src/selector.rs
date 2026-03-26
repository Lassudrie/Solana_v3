use crate::{
    guards::GuardrailSet,
    opportunity::{OpportunityCandidate, OpportunityDecision, SelectionOutcome},
    quote::{LocalTwoLegQuoteEngine, QuoteEngine},
    reasons::RejectionReason,
    route_registry::{RouteDefinition, RouteRegistry, SwapSide},
};
use state::{StatePlane, types::RouteId};

const DEFAULT_SIZE_LADDER: [u64; 6] = [250_000, 500_000, 1_000_000, 2_000_000, 3_000_000, 5_000_000];
const MAX_RESERVE_USAGE_BPS: u64 = 2_000;

#[derive(Debug)]
pub struct OpportunitySelector<E = LocalTwoLegQuoteEngine> {
    quote_engine: E,
    guards: GuardrailSet,
}

impl<E> OpportunitySelector<E>
where
    E: QuoteEngine,
{
    pub fn new(quote_engine: E, guards: GuardrailSet) -> Self {
        Self {
            quote_engine,
            guards,
        }
    }

    pub fn evaluate(
        &self,
        registry: &RouteRegistry,
        state: &StatePlane,
        impacted_routes: &[RouteId],
        inflight_submissions: usize,
    ) -> SelectionOutcome {
        if impacted_routes.is_empty() {
            return SelectionOutcome {
                decisions: vec![OpportunityDecision::Rejected {
                    route_id: RouteId("none".into()),
                    reason: RejectionReason::NoImpactedRoutes,
                }],
                best_candidate: None,
            };
        }

        let execution_state = state.execution_state();
        let mut decisions = Vec::with_capacity(impacted_routes.len());
        let mut accepted = Vec::new();

        for route_id in impacted_routes {
            let Some(route) = registry.get(route_id) else {
                decisions.push(OpportunityDecision::Rejected {
                    route_id: route_id.clone(),
                    reason: RejectionReason::RouteNotRegistered,
                });
                continue;
            };

            match self.evaluate_route(route, state, &execution_state, inflight_submissions) {
                Ok(candidate) => {
                    accepted.push(candidate.clone());
                    decisions.push(OpportunityDecision::Accepted(candidate));
                }
                Err(reason) => decisions.push(OpportunityDecision::Rejected {
                    route_id: route_id.clone(),
                    reason,
                }),
            }
        }

        let best_candidate = accepted
            .into_iter()
            .max_by_key(|candidate| candidate.expected_net_profit);

        SelectionOutcome {
            decisions,
            best_candidate,
        }
    }

    fn evaluate_route(
        &self,
        route: &RouteDefinition,
        state: &StatePlane,
        execution_state: &state::types::ExecutionStateSnapshot,
        inflight_submissions: usize,
    ) -> Result<OpportunityCandidate, RejectionReason> {
        self.guards
            .evaluate_route_readiness(&route.route_id, state)?;

        let first = state.pool_snapshot(&route.legs[0].pool_id).ok_or_else(|| {
            RejectionReason::MissingSnapshot {
                pool_id: route.legs[0].pool_id.clone(),
            }
        })?;
        let second = state.pool_snapshot(&route.legs[1].pool_id).ok_or_else(|| {
            RejectionReason::MissingSnapshot {
                pool_id: route.legs[1].pool_id.clone(),
            }
        })?;

        self.guards.evaluate_snapshots([first, second])?;

        let mut best_candidate: Option<OpportunityCandidate> = None;
        let mut last_rejection = None;
        let mut previous_net_profit: Option<i64> = None;
        let mut consecutive_declines = 0usize;
        for trade_size in trade_sizes(route) {
            let quote = self
                .quote_engine
                .quote(route, [first, second], state.latest_slot(), trade_size)
                .map(|quote| {
                    quote.with_estimated_execution_cost(route.estimated_execution_cost_lamports)
                })
                .map_err(|error| RejectionReason::QuoteFailed {
                    detail: error.to_string(),
                })?;

            if let Some(reason) = reserve_usage_rejection(route, [first, second], &quote) {
                last_rejection = Some(reason);
                break;
            }

            if let Some(previous) = previous_net_profit {
                if quote.expected_net_profit < previous {
                    consecutive_declines = consecutive_declines.saturating_add(1);
                } else {
                    consecutive_declines = 0;
                }
            }
            previous_net_profit = Some(quote.expected_net_profit);

            match self
                .guards
                .evaluate_quote(route, &quote, execution_state, inflight_submissions)
            {
                Ok(()) => {
                    let candidate = OpportunityCandidate {
                        route_id: route.route_id.clone(),
                        quoted_slot: quote.quoted_slot,
                        trade_size: quote.input_amount,
                        expected_net_output: quote.net_output_amount,
                        expected_gross_profit: quote.expected_gross_profit,
                        estimated_execution_cost_lamports: quote.estimated_execution_cost_lamports,
                        expected_net_profit: quote.expected_net_profit,
                        leg_quotes: quote.leg_quotes,
                    };
                    match &best_candidate {
                        Some(best) if best.expected_net_profit >= candidate.expected_net_profit => {
                        }
                        _ => best_candidate = Some(candidate),
                    }
                }
                Err(reason) => last_rejection = Some(reason),
            }

            if consecutive_declines >= 2 {
                break;
            }
        }

        best_candidate.ok_or_else(|| {
            last_rejection.unwrap_or(RejectionReason::RouteFilteredOut {
                route_id: route.route_id.clone(),
            })
        })
    }
}

fn trade_sizes(route: &RouteDefinition) -> Vec<u64> {
    if !route.size_ladder.is_empty() {
        let mut sizes = vec![route.default_trade_size];
        sizes.extend(
            route
            .size_ladder
            .iter()
            .copied()
            .filter(|size| *size > 0)
            .filter(|size| *size >= route.default_trade_size && *size <= route.max_trade_size)
        );
        return dedup_sizes(sizes);
    }

    if route.default_trade_size >= route.max_trade_size {
        return vec![route.default_trade_size];
    }

    let mut sizes = vec![route.default_trade_size];
    for size in DEFAULT_SIZE_LADDER {
        if size >= route.default_trade_size && size <= route.max_trade_size {
            sizes.push(size);
        }
    }
    sizes.push(route.max_trade_size);
    dedup_sizes(sizes)
}

fn dedup_sizes(mut sizes: Vec<u64>) -> Vec<u64> {
    let mut deduped = Vec::with_capacity(sizes.len());
    for size in sizes.drain(..) {
        if deduped.last().copied() != Some(size) && !deduped.contains(&size) {
            deduped.push(size);
        }
    }
    deduped
}

fn reserve_usage_rejection(
    route: &RouteDefinition,
    snapshots: [&state::types::PoolSnapshot; 2],
    quote: &crate::quote::RouteQuote,
) -> Option<RejectionReason> {
    let base_mint = route.base_mint.as_deref()?;
    let quote_mint = route.quote_mint.as_deref()?;

    for (index, (leg, snapshot)) in route.legs.iter().zip(snapshots.iter()).enumerate() {
        let (input_mint, output_mint) = match leg.side {
            SwapSide::BuyBase => (quote_mint, base_mint),
            SwapSide::SellBase => (base_mint, quote_mint),
        };
        let (reserve_in, _) = snapshot.constant_product_reserves_for(input_mint, output_mint)?;
        let input_amount = quote.leg_quotes[index].input_amount;
        let usage_bps = ((u128::from(input_amount) * 10_000u128)
            .saturating_add(u128::from(reserve_in).saturating_sub(1))
            / u128::from(reserve_in)) as u64;
        if usage_bps > MAX_RESERVE_USAGE_BPS {
            return Some(RejectionReason::ReserveUsageTooHigh {
                pool_id: snapshot.pool_id.clone(),
                usage_bps,
                maximum_bps: MAX_RESERVE_USAGE_BPS,
            });
        }
    }

    None
}
