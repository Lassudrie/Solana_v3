use crate::route_registry::SwapSide;
use thiserror::Error;

use crate::route_registry::RouteDefinition;
use state::types::PoolSnapshot;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegQuote {
    pub venue: String,
    pub pool_id: state::types::PoolId,
    pub side: SwapSide,
    pub input_amount: u64,
    pub output_amount: u64,
    pub fee_paid: u64,
    pub current_tick_index: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteQuote {
    pub quoted_slot: u64,
    pub input_amount: u64,
    pub gross_output_amount: u64,
    pub net_output_amount: u64,
    pub expected_gross_profit_quote_atoms: i64,
    pub estimated_execution_cost_lamports: u64,
    pub estimated_execution_cost_quote_atoms: u64,
    pub expected_net_profit_quote_atoms: i64,
    pub leg_quotes: [LegQuote; 2],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QuoteExecutionAdjustments {
    pub extra_leg_slippage_bps: [u16; 2],
}

impl RouteQuote {
    pub fn with_estimated_execution_cost(
        mut self,
        route: &RouteDefinition,
        sol_quote_conversion_snapshot: Option<&PoolSnapshot>,
        estimated_execution_cost_lamports: u64,
    ) -> Result<Self, QuoteError> {
        let estimated_execution_cost_quote_atoms = execution_cost_quote_atoms(
            route,
            sol_quote_conversion_snapshot,
            estimated_execution_cost_lamports,
        )?;
        self.estimated_execution_cost_lamports = estimated_execution_cost_lamports;
        self.estimated_execution_cost_quote_atoms = estimated_execution_cost_quote_atoms;
        self.expected_net_profit_quote_atoms = clamp_i128(
            self.expected_gross_profit_quote_atoms as i128
                - estimated_execution_cost_quote_atoms as i128,
        );
        Ok(self)
    }
}

#[derive(Debug, Error)]
pub enum QuoteError {
    #[error("expected exactly 2 snapshots for the 2-leg template")]
    InvalidRouteShape,
    #[error("arithmetic overflow during quote")]
    ArithmeticOverflow,
    #[error("invalid snapshot price")]
    InvalidSnapshotPrice,
    #[error("invalid snapshot liquidity")]
    InvalidSnapshotLiquidity,
    #[error("unable to convert execution cost into quote atoms")]
    ExecutionCostNotConvertible,
}

pub trait QuoteEngine: Send + Sync {
    fn quote(
        &self,
        route: &RouteDefinition,
        snapshots: [&PoolSnapshot; 2],
        quoted_slot: u64,
        input_amount: u64,
        adjustments: &QuoteExecutionAdjustments,
    ) -> Result<RouteQuote, QuoteError>;
}

#[derive(Debug, Default)]
pub struct LocalTwoLegQuoteEngine;

impl QuoteEngine for LocalTwoLegQuoteEngine {
    fn quote(
        &self,
        route: &RouteDefinition,
        snapshots: [&PoolSnapshot; 2],
        quoted_slot: u64,
        input_amount: u64,
        adjustments: &QuoteExecutionAdjustments,
    ) -> Result<RouteQuote, QuoteError> {
        let first = apply_additional_leg_slippage(
            apply_route_price(route, &route.legs[0], input_amount, snapshots[0])?,
            adjustments.extra_leg_slippage_bps[0],
        )?;
        let second = apply_additional_leg_slippage(
            apply_route_price(route, &route.legs[1], first.net_output, snapshots[1])?,
            adjustments.extra_leg_slippage_bps[1],
        )?;
        let expected_gross_profit = second
            .net_output
            .checked_sub(input_amount)
            .map(|profit| profit as i64)
            .unwrap_or_else(|| -((input_amount - second.net_output) as i64));

        Ok(RouteQuote {
            quoted_slot,
            input_amount,
            gross_output_amount: second.gross_output,
            net_output_amount: second.net_output,
            expected_gross_profit_quote_atoms: expected_gross_profit,
            estimated_execution_cost_lamports: 0,
            estimated_execution_cost_quote_atoms: 0,
            expected_net_profit_quote_atoms: expected_gross_profit,
            leg_quotes: [
                LegQuote {
                    venue: route.legs[0].venue.clone(),
                    pool_id: route.legs[0].pool_id.clone(),
                    side: route.legs[0].side,
                    input_amount,
                    output_amount: first.net_output,
                    fee_paid: first.fee_paid,
                    current_tick_index: snapshots[0].current_tick_index,
                },
                LegQuote {
                    venue: route.legs[1].venue.clone(),
                    pool_id: route.legs[1].pool_id.clone(),
                    side: route.legs[1].side,
                    input_amount: first.net_output,
                    output_amount: second.net_output,
                    fee_paid: second.fee_paid,
                    current_tick_index: snapshots[1].current_tick_index,
                },
            ],
        })
    }
}

fn execution_cost_quote_atoms(
    route: &RouteDefinition,
    sol_quote_conversion_snapshot: Option<&PoolSnapshot>,
    estimated_execution_cost_lamports: u64,
) -> Result<u64, QuoteError> {
    if estimated_execution_cost_lamports == 0 {
        return Ok(0);
    }

    let Some(quote_mint) = route.quote_mint.as_deref() else {
        return Err(QuoteError::ExecutionCostNotConvertible);
    };
    if route.input_mint != route.output_mint || route.input_mint != quote_mint {
        return Err(QuoteError::ExecutionCostNotConvertible);
    }
    if quote_mint == SOL_MINT {
        return Ok(estimated_execution_cost_lamports);
    }

    let Some(snapshot) = sol_quote_conversion_snapshot else {
        return Err(QuoteError::ExecutionCostNotConvertible);
    };
    if snapshot.price_bps == 0 {
        return Err(QuoteError::ExecutionCostNotConvertible);
    }
    if snapshot.token_mint_a == SOL_MINT && snapshot.token_mint_b == quote_mint {
        return mul_div_u64(
            estimated_execution_cost_lamports,
            snapshot.price_bps,
            10_000,
        );
    }
    if snapshot.token_mint_a == quote_mint && snapshot.token_mint_b == SOL_MINT {
        return mul_div_u64(
            estimated_execution_cost_lamports,
            10_000,
            snapshot.price_bps,
        );
    }

    Err(QuoteError::ExecutionCostNotConvertible)
}

struct PricedAmount {
    gross_output: u64,
    net_output: u64,
    fee_paid: u64,
}

fn apply_additional_leg_slippage(
    priced: PricedAmount,
    extra_slippage_bps: u16,
) -> Result<PricedAmount, QuoteError> {
    if extra_slippage_bps == 0 {
        return Ok(priced);
    }

    let extra_paid: u64 = ((priced.gross_output as u128) * u128::from(extra_slippage_bps)
        / 10_000u128)
        .try_into()
        .map_err(|_| QuoteError::ArithmeticOverflow)?;
    let net_output = priced
        .net_output
        .checked_sub(extra_paid)
        .ok_or(QuoteError::ArithmeticOverflow)?;

    Ok(PricedAmount {
        net_output,
        ..priced
    })
}

fn apply_price(
    input_amount: u64,
    price_bps: u64,
    fee_bps: u16,
    snapshot: &PoolSnapshot,
) -> Result<PricedAmount, QuoteError> {
    let gross_output: u64 = ((input_amount as u128) * (price_bps as u128) / 10_000u128)
        .try_into()
        .map_err(|_| QuoteError::ArithmeticOverflow)?;
    let fee_paid: u64 = ((gross_output as u128) * (fee_bps as u128) / 10_000u128)
        .try_into()
        .map_err(|_| QuoteError::ArithmeticOverflow)?;
    let slippage_bps = snapshot.estimated_slippage_bps(input_amount);
    let slippage_paid: u64 = ((gross_output as u128) * (slippage_bps as u128) / 10_000u128)
        .try_into()
        .map_err(|_| QuoteError::ArithmeticOverflow)?;
    let net_output = gross_output
        .checked_sub(fee_paid)
        .and_then(|value| value.checked_sub(slippage_paid))
        .ok_or(QuoteError::ArithmeticOverflow)?;
    Ok(PricedAmount {
        gross_output,
        net_output,
        fee_paid,
    })
}

fn constant_product_output(
    input_amount: u64,
    reserve_in: u64,
    reserve_out: u64,
    fee_bps: u16,
) -> Result<u64, QuoteError> {
    if input_amount == 0 || reserve_in == 0 || reserve_out == 0 {
        return Err(QuoteError::InvalidSnapshotLiquidity);
    }

    let denominator = 10_000u128;
    let effective_in = u128::from(input_amount)
        .checked_mul(denominator.saturating_sub(u128::from(fee_bps)))
        .ok_or(QuoteError::ArithmeticOverflow)?
        / denominator;
    if effective_in == 0 {
        return Err(QuoteError::InvalidSnapshotLiquidity);
    }

    let numerator = effective_in
        .checked_mul(u128::from(reserve_out))
        .ok_or(QuoteError::ArithmeticOverflow)?;
    let output = numerator
        .checked_div(u128::from(reserve_in).saturating_add(effective_in))
        .ok_or(QuoteError::ArithmeticOverflow)?;
    output
        .try_into()
        .map_err(|_| QuoteError::ArithmeticOverflow)
}

fn apply_constant_product_price(
    input_amount: u64,
    reserve_in: u64,
    reserve_out: u64,
    fee_bps: u16,
) -> Result<PricedAmount, QuoteError> {
    let gross_output = constant_product_output(input_amount, reserve_in, reserve_out, 0)?;
    let net_output = constant_product_output(input_amount, reserve_in, reserve_out, fee_bps)?;
    Ok(PricedAmount {
        gross_output,
        net_output,
        fee_paid: gross_output.saturating_sub(net_output),
    })
}

fn clamp_i128(value: i128) -> i64 {
    value.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

fn mul_div_u64(value: u64, numerator: u64, denominator: u64) -> Result<u64, QuoteError> {
    if denominator == 0 {
        return Err(QuoteError::ExecutionCostNotConvertible);
    }

    (u128::from(value) * u128::from(numerator) / u128::from(denominator))
        .try_into()
        .map_err(|_| QuoteError::ArithmeticOverflow)
}

fn apply_route_price(
    route: &RouteDefinition,
    leg: &crate::route_registry::RouteLeg,
    input_amount: u64,
    snapshot: &PoolSnapshot,
) -> Result<PricedAmount, QuoteError> {
    let fee_bps = leg.fee_bps.unwrap_or(snapshot.fee_bps);
    let Some(base_mint) = route.base_mint.as_deref() else {
        return apply_price(input_amount, snapshot.price_bps, fee_bps, snapshot);
    };
    let Some(quote_mint) = route.quote_mint.as_deref() else {
        return apply_price(input_amount, snapshot.price_bps, fee_bps, snapshot);
    };
    if snapshot.token_mint_a.is_empty() || snapshot.token_mint_b.is_empty() {
        return apply_price(input_amount, snapshot.price_bps, fee_bps, snapshot);
    }

    let (input_mint, output_mint) = match leg.side {
        SwapSide::BuyBase => (quote_mint, base_mint),
        SwapSide::SellBase => (base_mint, quote_mint),
    };

    if snapshot.liquidity_model == state::types::LiquidityModel::ConstantProduct {
        if let Some((reserve_in, reserve_out)) =
            snapshot.constant_product_reserves_for(input_mint, output_mint)
        {
            return apply_constant_product_price(input_amount, reserve_in, reserve_out, fee_bps);
        }
    }

    let price_bps = snapshot.price_bps;
    if price_bps == 0 {
        return Err(QuoteError::InvalidSnapshotPrice);
    }

    let gross_output: u64 =
        if input_mint == snapshot.token_mint_a && output_mint == snapshot.token_mint_b {
            ((input_amount as u128) * (price_bps as u128) / 10_000u128)
                .try_into()
                .map_err(|_| QuoteError::ArithmeticOverflow)?
        } else if input_mint == snapshot.token_mint_b && output_mint == snapshot.token_mint_a {
            ((input_amount as u128) * 10_000u128 / (price_bps as u128))
                .try_into()
                .map_err(|_| QuoteError::ArithmeticOverflow)?
        } else {
            return Err(QuoteError::InvalidSnapshotPrice);
        };
    let fee_paid: u64 = ((gross_output as u128) * (fee_bps as u128) / 10_000u128)
        .try_into()
        .map_err(|_| QuoteError::ArithmeticOverflow)?;
    let slippage_bps = snapshot.estimated_slippage_bps(input_amount);
    let slippage_paid: u64 = ((gross_output as u128) * (slippage_bps as u128) / 10_000u128)
        .try_into()
        .map_err(|_| QuoteError::ArithmeticOverflow)?;
    let net_output = gross_output
        .checked_sub(fee_paid)
        .and_then(|value| value.checked_sub(slippage_paid))
        .ok_or(QuoteError::ArithmeticOverflow)?;
    Ok(PricedAmount {
        gross_output,
        net_output,
        fee_paid,
    })
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use state::types::{
        FreshnessState, LiquidityModel, PoolConfidence, PoolId, PoolSnapshot, RouteId,
    };

    use crate::{
        quote::{QuoteError, RouteQuote, SOL_MINT},
        route_registry::{RouteDefinition, RouteLeg, SwapSide},
    };

    fn route_definition(
        base_mint: &str,
        quote_mint: &str,
        conversion_pool: Option<&str>,
    ) -> RouteDefinition {
        RouteDefinition {
            route_id: RouteId("route-a".into()),
            input_mint: quote_mint.into(),
            output_mint: quote_mint.into(),
            base_mint: Some(base_mint.into()),
            quote_mint: Some(quote_mint.into()),
            sol_quote_conversion_pool_id: conversion_pool.map(|pool_id| PoolId(pool_id.into())),
            legs: [
                RouteLeg {
                    venue: "venue-a".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    fee_bps: None,
                },
                RouteLeg {
                    venue: "venue-b".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    fee_bps: None,
                },
            ],
            min_trade_size: 1,
            default_trade_size: 1,
            max_trade_size: 10,
            size_ladder: Vec::new(),
            estimated_execution_cost_lamports: 0,
            execution_protection: None,
        }
    }

    fn quote() -> RouteQuote {
        RouteQuote {
            quoted_slot: 1,
            input_amount: 1_000,
            gross_output_amount: 1_100,
            net_output_amount: 1_080,
            expected_gross_profit_quote_atoms: 80,
            estimated_execution_cost_lamports: 0,
            estimated_execution_cost_quote_atoms: 0,
            expected_net_profit_quote_atoms: 80,
            leg_quotes: [
                crate::quote::LegQuote {
                    venue: "venue-a".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    input_amount: 1_000,
                    output_amount: 1_050,
                    fee_paid: 0,
                    current_tick_index: None,
                },
                crate::quote::LegQuote {
                    venue: "venue-b".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    input_amount: 1_050,
                    output_amount: 1_080,
                    fee_paid: 0,
                    current_tick_index: None,
                },
            ],
        }
    }

    fn snapshot(
        pool_id: &str,
        token_mint_a: &str,
        token_mint_b: &str,
        price_bps: u64,
    ) -> PoolSnapshot {
        PoolSnapshot {
            pool_id: PoolId(pool_id.into()),
            price_bps,
            fee_bps: 0,
            reserve_depth: 1_000_000,
            reserve_a: Some(1_000_000),
            reserve_b: Some(1_000_000),
            active_liquidity: 1_000_000,
            sqrt_price_x64: None,
            venue: None,
            confidence: PoolConfidence::Executable,
            repair_pending: false,
            liquidity_model: LiquidityModel::ConstantProduct,
            slippage_factor_bps: 10_000,
            token_mint_a: token_mint_a.into(),
            token_mint_b: token_mint_b.into(),
            tick_spacing: 0,
            current_tick_index: None,
            last_update_slot: 1,
            derived_at: SystemTime::now(),
            freshness: FreshnessState::at(1, 1, 1),
        }
    }

    #[test]
    fn execution_cost_in_sol_quote_atoms_is_identity_for_sol_routes() {
        let route = route_definition("JTO", SOL_MINT, None);
        let quote = quote()
            .with_estimated_execution_cost(&route, None, 5_000)
            .expect("sol quote cost should be an identity conversion");

        assert_eq!(quote.estimated_execution_cost_quote_atoms, 5_000);
        assert_eq!(quote.expected_net_profit_quote_atoms, -4_920);
    }

    #[test]
    fn execution_cost_uses_explicit_sol_quote_snapshot() {
        let route = route_definition("USDT", "USDC", Some("pool-sol-usdc"));
        let sol_quote_snapshot = snapshot("pool-sol-usdc", SOL_MINT, "USDC", 1_500_000);
        let route_leg_snapshot = snapshot("pool-usdt-usdc", "USDT", "USDC", 9_900);
        let quote = quote()
            .with_estimated_execution_cost(&route, Some(&sol_quote_snapshot), 5_000)
            .expect("sol/usdc conversion should succeed");

        assert_eq!(quote.estimated_execution_cost_quote_atoms, 750_000);
        assert_ne!(
            quote.estimated_execution_cost_quote_atoms, 4_950,
            "must not reuse the base/quote route price"
        );
        assert_eq!(route_leg_snapshot.price_bps, 9_900);
    }

    #[test]
    fn execution_cost_supports_inverted_sol_quote_snapshot_orientation() {
        let route = route_definition("USDT", "USDC", Some("pool-sol-usdc"));
        let sol_quote_snapshot = snapshot("pool-sol-usdc", "USDC", SOL_MINT, 50);
        let quote = quote()
            .with_estimated_execution_cost(&route, Some(&sol_quote_snapshot), 5_000)
            .expect("inverted usdc/sol conversion should succeed");

        assert_eq!(quote.estimated_execution_cost_quote_atoms, 1_000_000);
    }

    #[test]
    fn execution_cost_rejects_missing_or_invalid_sol_quote_snapshot() {
        let route = route_definition("USDT", "USDC", Some("pool-sol-usdc"));
        let wrong_pair_snapshot = snapshot("pool-usdt-usdc", "USDT", "USDC", 10_000);

        let missing = quote().with_estimated_execution_cost(&route, None, 5_000);
        assert!(matches!(
            missing,
            Err(QuoteError::ExecutionCostNotConvertible)
        ));

        let invalid =
            quote().with_estimated_execution_cost(&route, Some(&wrong_pair_snapshot), 5_000);
        assert!(matches!(
            invalid,
            Err(QuoteError::ExecutionCostNotConvertible)
        ));
    }
}
