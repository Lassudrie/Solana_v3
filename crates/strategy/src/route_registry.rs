use std::{
    collections::{HashMap, HashSet},
    ops::Index,
    slice,
};

use domain::{PoolId, RouteId};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SizingMode {
    #[default]
    Legacy,
    EvShadow,
    EvLive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrategySizingConfig {
    pub mode: SizingMode,
    pub fixed_trade_size: bool,
    pub min_trade_floor_sol_lamports: u64,
    pub base_landing_rate_bps: u16,
    pub ewma_alpha_bps: u16,
    pub base_expected_shortfall_bps: u16,
    pub max_expected_shortfall_bps: u16,
    pub too_little_output_shortfall_step_bps: u16,
    pub inflight_penalty_bps_per_submission: u16,
    pub max_inflight_penalty_bps: u16,
    pub blockhash_penalty_bps_per_slot: u16,
    pub max_blockhash_penalty_bps: u16,
    pub quote_age_penalty_bps_per_slot: u16,
    pub max_quote_age_penalty_bps: u16,
    pub tick_cross_penalty_bps_per_tick: u16,
    pub max_tick_cross_penalty_bps: u16,
    pub max_reserve_usage_penalty_bps: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JitoTipMode {
    Fixed,
    #[default]
    PnlRatio,
    RiskAdjustedPnlRatio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JitoTipPolicy {
    pub mode: JitoTipMode,
    pub share_bps_of_expected_net_profit: u16,
    pub min_lamports: u64,
    pub max_lamports: u64,
}

impl Default for JitoTipPolicy {
    fn default() -> Self {
        Self {
            mode: JitoTipMode::PnlRatio,
            share_bps_of_expected_net_profit: 1_000,
            min_lamports: 5_000,
            max_lamports: 50_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteSizingPolicy {
    pub mode: SizingMode,
    pub min_trade_floor_sol_lamports: u64,
    pub base_landing_rate_bps: u16,
    pub base_expected_shortfall_bps: u16,
    pub max_expected_shortfall_bps: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapSide {
    BuyBase,
    SellBase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RouteKind {
    TwoLeg,
    Triangular,
}

impl RouteKind {
    pub const fn expected_leg_count(self) -> usize {
        match self {
            Self::TwoLeg => 2,
            Self::Triangular => 3,
        }
    }

    pub const fn minimum_profit_multiplier_bps(self) -> u16 {
        match self {
            Self::TwoLeg => 10_000,
            // Three hops carry more spread erosion and stale-state risk.
            Self::Triangular => 15_000,
        }
    }

    pub const fn expected_shortfall_multiplier_bps(self) -> u16 {
        match self {
            Self::TwoLeg => 10_000,
            Self::Triangular => 15_000,
        }
    }

    pub const fn minimum_compute_unit_limit(self) -> u32 {
        match self {
            Self::TwoLeg => 250_000,
            Self::Triangular => 420_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteLeg {
    pub venue: String,
    pub pool_id: PoolId,
    pub side: SwapSide,
    pub input_mint: String,
    pub output_mint: String,
    pub fee_bps: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteLegSequence<T> {
    Two([T; 2]),
    Three([T; 3]),
}

impl<T> RouteLegSequence<T> {
    pub fn from_vec(values: Vec<T>) -> Result<Self, RouteValidationError> {
        let len = values.len();
        match len {
            2 => {
                let mut iter = values.into_iter();
                Ok(Self::Two([
                    iter.next().expect("len=2"),
                    iter.next().expect("len=2"),
                ]))
            }
            3 => {
                let mut iter = values.into_iter();
                Ok(Self::Three([
                    iter.next().expect("len=3"),
                    iter.next().expect("len=3"),
                    iter.next().expect("len=3"),
                ]))
            }
            _ => Err(RouteValidationError::InvalidLegCount {
                expected: "2 or 3",
                actual: len,
            }),
        }
    }

    pub const fn len(&self) -> usize {
        match self {
            Self::Two(_) => 2,
            Self::Three(_) => 3,
        }
    }

    pub const fn kind(&self) -> RouteKind {
        match self {
            Self::Two(_) => RouteKind::TwoLeg,
            Self::Three(_) => RouteKind::Triangular,
        }
    }

    pub fn as_slice(&self) -> &[T] {
        match self {
            Self::Two(values) => values,
            Self::Three(values) => values,
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        match self {
            Self::Two(values) => values,
            Self::Three(values) => values,
        }
    }

    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.as_slice().iter()
    }
}

impl<T> From<[T; 2]> for RouteLegSequence<T> {
    fn from(value: [T; 2]) -> Self {
        Self::Two(value)
    }
}

impl<T> From<[T; 3]> for RouteLegSequence<T> {
    fn from(value: [T; 3]) -> Self {
        Self::Three(value)
    }
}

impl<T> Index<usize> for RouteLegSequence<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.as_slice()[index]
    }
}

impl<'a, T> IntoIterator for &'a RouteLegSequence<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionProtectionPolicy {
    pub tight_max_quote_slot_lag: u64,
    pub base_extra_buy_leg_slippage_bps: u16,
    pub failure_step_bps: u16,
    pub max_extra_buy_leg_slippage_bps: u16,
    pub recovery_success_count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDefinition {
    pub route_id: RouteId,
    pub kind: RouteKind,
    pub input_mint: String,
    pub output_mint: String,
    pub input_source_account: Option<String>,
    pub base_mint: Option<String>,
    pub quote_mint: Option<String>,
    pub sol_quote_conversion_pool_id: Option<PoolId>,
    pub min_profit_quote_atoms: Option<i64>,
    pub legs: RouteLegSequence<RouteLeg>,
    pub max_quote_slot_lag: u64,
    pub min_trade_size: u64,
    pub default_trade_size: u64,
    pub max_trade_size: u64,
    pub size_ladder: Vec<u64>,
    pub default_jito_tip_lamports: u64,
    pub estimated_execution_cost_lamports: u64,
    pub sizing: RouteSizingPolicy,
    pub execution_protection: Option<ExecutionProtectionPolicy>,
}

impl RouteDefinition {
    pub fn validate(&self) -> Result<(), RouteValidationError> {
        if self.legs.kind() != self.kind {
            return Err(RouteValidationError::RouteKindLegCountMismatch {
                route_kind: self.kind,
                leg_count: self.legs.len(),
            });
        }
        if self.input_mint != self.output_mint {
            return Err(RouteValidationError::RouteNotClosed {
                route_id: self.route_id.clone(),
                entry_mint: self.input_mint.clone(),
                exit_mint: self.output_mint.clone(),
            });
        }
        if self
            .input_source_account
            .as_deref()
            .is_some_and(str::is_empty)
        {
            return Err(RouteValidationError::EmptyInputSourceAccount);
        }
        if let Some(quote_mint) = self.quote_mint.as_deref()
            && quote_mint != self.input_mint
        {
            return Err(RouteValidationError::QuoteMintMismatch {
                quote_mint: quote_mint.to_string(),
                route_mint: self.input_mint.clone(),
            });
        }

        let legs = self.legs.as_slice();
        let mut seen_pools = HashSet::with_capacity(legs.len());
        for (index, leg) in legs.iter().enumerate() {
            if leg.input_mint.is_empty() || leg.output_mint.is_empty() {
                return Err(RouteValidationError::EmptyLegMint { leg_index: index });
            }
            if leg.input_mint == leg.output_mint {
                return Err(RouteValidationError::DegenerateLeg {
                    leg_index: index,
                    mint: leg.input_mint.clone(),
                });
            }
            if !seen_pools.insert(leg.pool_id.clone()) {
                return Err(RouteValidationError::DuplicateLegPool {
                    pool_id: leg.pool_id.clone(),
                });
            }
        }

        if legs.first().map(|leg| leg.input_mint.as_str()) != Some(self.input_mint.as_str()) {
            return Err(RouteValidationError::FirstLegInputMismatch {
                expected: self.input_mint.clone(),
                actual: legs
                    .first()
                    .map(|leg| leg.input_mint.clone())
                    .unwrap_or_default(),
            });
        }
        if legs.last().map(|leg| leg.output_mint.as_str()) != Some(self.output_mint.as_str()) {
            return Err(RouteValidationError::FinalLegOutputMismatch {
                expected: self.output_mint.clone(),
                actual: legs
                    .last()
                    .map(|leg| leg.output_mint.clone())
                    .unwrap_or_default(),
            });
        }

        for (index, pair) in legs.windows(2).enumerate() {
            let left = &pair[0];
            let right = &pair[1];
            if left.output_mint != right.input_mint {
                return Err(RouteValidationError::LegChainBroken {
                    left_leg_index: index,
                    left_output_mint: left.output_mint.clone(),
                    right_leg_index: index + 1,
                    right_input_mint: right.input_mint.clone(),
                });
            }
        }

        Ok(())
    }

    pub const fn leg_count(&self) -> usize {
        self.kind.expected_leg_count()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RouteValidationError {
    #[error("invalid leg count: expected {expected}, got {actual}")]
    InvalidLegCount {
        expected: &'static str,
        actual: usize,
    },
    #[error(
        "route kind {route_kind:?} expects {} legs but got {leg_count}",
        route_kind.expected_leg_count()
    )]
    RouteKindLegCountMismatch {
        route_kind: RouteKind,
        leg_count: usize,
    },
    #[error(
        "route {route_id:?} is not a closed cycle: entry mint {entry_mint} != exit mint {exit_mint}"
    )]
    RouteNotClosed {
        route_id: RouteId,
        entry_mint: String,
        exit_mint: String,
    },
    #[error("route quote mint {quote_mint} must match route profit mint {route_mint}")]
    QuoteMintMismatch {
        quote_mint: String,
        route_mint: String,
    },
    #[error("route input_source_account must not be empty when provided")]
    EmptyInputSourceAccount,
    #[error("leg {leg_index} is missing an explicit input/output mint")]
    EmptyLegMint { leg_index: usize },
    #[error("leg {leg_index} is degenerate on mint {mint}")]
    DegenerateLeg { leg_index: usize, mint: String },
    #[error("duplicate pool {pool_id:?} across route legs")]
    DuplicateLegPool { pool_id: PoolId },
    #[error("first leg input mint {actual} does not match route input mint {expected}")]
    FirstLegInputMismatch { expected: String, actual: String },
    #[error("final leg output mint {actual} does not match route output mint {expected}")]
    FinalLegOutputMismatch { expected: String, actual: String },
    #[error(
        "legs do not chain: leg {left_leg_index} outputs {left_output_mint} but leg {right_leg_index} expects {right_input_mint}"
    )]
    LegChainBroken {
        left_leg_index: usize,
        left_output_mint: String,
        right_leg_index: usize,
        right_input_mint: String,
    },
}

#[derive(Debug, Default)]
pub struct RouteRegistry {
    routes: HashMap<RouteId, RouteDefinition>,
}

impl RouteRegistry {
    pub fn register(&mut self, route: RouteDefinition) {
        debug_assert!(
            route.validate().is_ok(),
            "registered route definitions must satisfy route invariants"
        );
        self.routes.insert(route.route_id.clone(), route);
    }

    pub fn get(&self, route_id: &RouteId) -> Option<&RouteDefinition> {
        self.routes.get(route_id)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RouteDefinition, RouteKind, RouteLeg, RouteSizingPolicy, RouteValidationError, SizingMode,
        SwapSide,
    };
    use domain::{PoolId, RouteId};

    fn triangular_route() -> RouteDefinition {
        RouteDefinition {
            route_id: RouteId("tri-sol-usdc-usdt".into()),
            kind: RouteKind::Triangular,
            input_mint: "USDC".into(),
            output_mint: "USDC".into(),
            input_source_account: None,
            base_mint: Some("SOL".into()),
            quote_mint: Some("USDC".into()),
            sol_quote_conversion_pool_id: Some(PoolId("pool-sol-usdc".into())),
            min_profit_quote_atoms: Some(50),
            legs: [
                RouteLeg {
                    venue: "orca_whirlpool".into(),
                    pool_id: PoolId("pool-sol-usdc".into()),
                    side: SwapSide::BuyBase,
                    input_mint: "USDC".into(),
                    output_mint: "SOL".into(),
                    fee_bps: Some(4),
                },
                RouteLeg {
                    venue: "raydium_clmm".into(),
                    pool_id: PoolId("pool-sol-usdt".into()),
                    side: SwapSide::SellBase,
                    input_mint: "SOL".into(),
                    output_mint: "USDT".into(),
                    fee_bps: Some(4),
                },
                RouteLeg {
                    venue: "orca_whirlpool".into(),
                    pool_id: PoolId("pool-usdt-usdc".into()),
                    side: SwapSide::SellBase,
                    input_mint: "USDT".into(),
                    output_mint: "USDC".into(),
                    fee_bps: Some(1),
                },
            ]
            .into(),
            max_quote_slot_lag: 8,
            min_trade_size: 1_000,
            default_trade_size: 5_000,
            max_trade_size: 10_000,
            size_ladder: vec![2_500, 5_000],
            default_jito_tip_lamports: 0,
            estimated_execution_cost_lamports: 5_000,
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

    #[test]
    fn validates_closed_triangular_route() {
        triangular_route()
            .validate()
            .expect("triangular cycle should satisfy route invariants");
    }

    #[test]
    fn rejects_triangular_route_that_does_not_return_to_entry_asset() {
        let mut route = triangular_route();
        route.legs.as_mut_slice()[2].output_mint = "SOL".into();

        assert_eq!(
            route.validate(),
            Err(RouteValidationError::FinalLegOutputMismatch {
                expected: "USDC".into(),
                actual: "SOL".into(),
            })
        );
    }
}
