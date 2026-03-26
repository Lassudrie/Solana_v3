use crate::route_registry::SwapSide;
use thiserror::Error;

use crate::route_registry::RouteDefinition;
use state::types::PoolSnapshot;

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
        snapshots: [&PoolSnapshot; 2],
        estimated_execution_cost_lamports: u64,
    ) -> Result<Self, QuoteError> {
        let estimated_execution_cost_quote_atoms =
            execution_cost_quote_atoms(route, snapshots, estimated_execution_cost_lamports)?;
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
    snapshots: [&PoolSnapshot; 2],
    estimated_execution_cost_lamports: u64,
) -> Result<u64, QuoteError> {
    if estimated_execution_cost_lamports == 0 {
        return Ok(0);
    }

    let Some(base_mint) = route.base_mint.as_deref() else {
        return Err(QuoteError::ExecutionCostNotConvertible);
    };
    let Some(quote_mint) = route.quote_mint.as_deref() else {
        return Err(QuoteError::ExecutionCostNotConvertible);
    };
    if route.input_mint != route.output_mint || route.input_mint != quote_mint {
        return Err(QuoteError::ExecutionCostNotConvertible);
    }

    for snapshot in snapshots {
        if snapshot.price_bps == 0 {
            continue;
        }
        if snapshot.token_mint_a == base_mint && snapshot.token_mint_b == quote_mint {
            return mul_div_u64(
                estimated_execution_cost_lamports,
                snapshot.price_bps,
                10_000,
            );
        }
        if snapshot.token_mint_a == quote_mint && snapshot.token_mint_b == base_mint {
            return mul_div_u64(
                estimated_execution_cost_lamports,
                10_000,
                snapshot.price_bps,
            );
        }
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
