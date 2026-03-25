use crate::{
    guards::GuardrailSet,
    opportunity::{OpportunityCandidate, OpportunityDecision, SelectionOutcome},
    quote::{LocalTwoLegQuoteEngine, QuoteEngine},
    reasons::RejectionReason,
    route_registry::{RouteDefinition, RouteRegistry},
};
use state::{StatePlane, types::RouteId};

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
        let quote = self
            .quote_engine
            .quote(route, [first, second], state.latest_slot())
            .map_err(|error| RejectionReason::QuoteFailed {
                detail: error.to_string(),
            })?;
        self.guards
            .evaluate_quote(route, &quote, execution_state, inflight_submissions)?;

        Ok(OpportunityCandidate {
            route_id: route.route_id.clone(),
            quoted_slot: quote.quoted_slot,
            trade_size: quote.input_amount,
            expected_net_output: quote.net_output_amount,
            expected_net_profit: quote.expected_net_profit,
            leg_quotes: quote.leg_quotes,
        })
    }
}
