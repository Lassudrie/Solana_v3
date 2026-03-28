use crate::route_registry::SwapSide;
use domain::{
    LiquidityModel, PoolId, PoolSnapshot, PoolVenue,
    quote_models::{ConcentratedQuoteModel, DirectionalConcentratedQuoteModel},
};
use primitive_types::U256;
use thiserror::Error;

use crate::route_registry::RouteDefinition;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegQuote {
    pub venue: String,
    pub pool_id: PoolId,
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

#[derive(Debug, Clone, Copy)]
pub struct PoolPricingView<'a> {
    pub snapshot: &'a PoolSnapshot,
    pub concentrated: Option<&'a ConcentratedQuoteModel>,
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
        if route_uses_sol_quote_conversion(route, estimated_execution_cost_lamports) {
            let snapshot =
                sol_quote_conversion_snapshot.ok_or(QuoteError::ExecutionCostNotConvertible)?;
            self.quoted_slot = self.quoted_slot.min(snapshot.last_update_slot);
        }
        self.estimated_execution_cost_lamports = estimated_execution_cost_lamports;
        self.estimated_execution_cost_quote_atoms = estimated_execution_cost_quote_atoms;
        self.expected_net_profit_quote_atoms = clamp_i128(
            self.expected_gross_profit_quote_atoms as i128
                - estimated_execution_cost_quote_atoms as i128,
        );
        Ok(self)
    }
}

pub fn sol_lamports_to_quote_atoms(
    route: &RouteDefinition,
    sol_quote_conversion_snapshot: Option<&PoolSnapshot>,
    lamports: u64,
) -> Result<u64, QuoteError> {
    execution_cost_quote_atoms(route, sol_quote_conversion_snapshot, lamports)
}

#[derive(Debug, Error)]
pub enum QuoteError {
    #[error("expected exactly 2 snapshots for the 2-leg template")]
    InvalidRouteShape,
    #[error("arithmetic overflow during quote")]
    ArithmeticOverflow,
    #[error("arithmetic overflow during quote ({0})")]
    ArithmeticOverflowAt(&'static str),
    #[error("invalid snapshot price")]
    InvalidSnapshotPrice,
    #[error("invalid snapshot liquidity")]
    InvalidSnapshotLiquidity,
    #[error("pool quote model is not executable")]
    QuoteModelNotExecutable,
    #[error("trade exceeds loaded concentrated-liquidity window")]
    ConcentratedWindowExceeded,
    #[error("unable to convert execution cost into quote atoms")]
    ExecutionCostNotConvertible,
}

impl QuoteError {
    fn is_arithmetic_overflow(&self) -> bool {
        matches!(
            self,
            Self::ArithmeticOverflow | Self::ArithmeticOverflowAt(_)
        )
    }
}

fn overflow_at(context: &'static str) -> QuoteError {
    QuoteError::ArithmeticOverflowAt(context)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcentratedSwapState {
    pub sqrt_price_x64: u128,
    pub liquidity: u128,
    pub current_tick_index: i32,
    pub input_amount: u64,
    pub output_amount: u64,
}

pub trait QuoteEngine: Send + Sync {
    fn quote<'a>(
        &self,
        route: &RouteDefinition,
        views: [PoolPricingView<'a>; 2],
        quoted_slot: u64,
        input_amount: u64,
        adjustments: &QuoteExecutionAdjustments,
    ) -> Result<RouteQuote, QuoteError>;
}

#[derive(Debug, Default)]
pub struct LocalTwoLegQuoteEngine;

impl QuoteEngine for LocalTwoLegQuoteEngine {
    fn quote<'a>(
        &self,
        route: &RouteDefinition,
        views: [PoolPricingView<'a>; 2],
        quoted_slot: u64,
        input_amount: u64,
        adjustments: &QuoteExecutionAdjustments,
    ) -> Result<RouteQuote, QuoteError> {
        let first = apply_additional_leg_slippage(
            apply_route_price(route, &route.legs[0], input_amount, views[0])?,
            adjustments.extra_leg_slippage_bps[0],
        )?;
        let second = apply_additional_leg_slippage(
            apply_route_price(route, &route.legs[1], first.net_output, views[1])?,
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
                    current_tick_index: views[0].snapshot.current_tick_index,
                },
                LegQuote {
                    venue: route.legs[1].venue.clone(),
                    pool_id: route.legs[1].pool_id.clone(),
                    side: route.legs[1].side,
                    input_amount: first.net_output,
                    output_amount: second.net_output,
                    fee_paid: second.fee_paid,
                    current_tick_index: views[1].snapshot.current_tick_index,
                },
            ],
        })
    }
}

pub(crate) fn route_uses_sol_quote_conversion(
    route: &RouteDefinition,
    estimated_execution_cost_lamports: u64,
) -> bool {
    estimated_execution_cost_lamports > 0
        && route.input_mint == route.output_mint
        && route.quote_mint.as_deref() == Some(route.input_mint.as_str())
        && route.quote_mint.as_deref() != Some(SOL_MINT)
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
    view: PoolPricingView<'_>,
) -> Result<PricedAmount, QuoteError> {
    let snapshot = view.snapshot;
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

    if snapshot.liquidity_model == LiquidityModel::ConcentratedLiquidity {
        return apply_concentrated_price(
            snapshot,
            view.concentrated,
            input_mint,
            output_mint,
            input_amount,
            fee_bps,
        );
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

fn apply_concentrated_price(
    snapshot: &PoolSnapshot,
    model: Option<&ConcentratedQuoteModel>,
    input_mint: &str,
    output_mint: &str,
    input_amount: u64,
    fee_bps: u16,
) -> Result<PricedAmount, QuoteError> {
    let Some(model) = model else {
        return Err(QuoteError::QuoteModelNotExecutable);
    };
    if input_amount == 0 {
        return Err(QuoteError::InvalidSnapshotLiquidity);
    }

    let a_to_b = if input_mint == snapshot.token_mint_a && output_mint == snapshot.token_mint_b {
        true
    } else if input_mint == snapshot.token_mint_b && output_mint == snapshot.token_mint_a {
        false
    } else {
        return Err(QuoteError::InvalidSnapshotPrice);
    };
    let Some(direction) = model.direction(a_to_b) else {
        return Err(QuoteError::QuoteModelNotExecutable);
    };
    if !direction.is_executable_at(model.current_tick_index)
        || model.tick_spacing == 0
        || model.liquidity == 0
    {
        return Err(QuoteError::QuoteModelNotExecutable);
    }

    let net_output = simulate_concentrated_exact_input(
        snapshot,
        model,
        direction,
        a_to_b,
        input_amount,
        fee_bps,
    )?;
    let gross_output = if fee_bps == 0 {
        net_output
    } else {
        simulate_concentrated_exact_input(snapshot, model, direction, a_to_b, input_amount, 0)?
    };
    Ok(PricedAmount {
        gross_output,
        net_output,
        fee_paid: gross_output.saturating_sub(net_output),
    })
}

pub fn apply_concentrated_exact_input_update(
    venue: PoolVenue,
    model: &ConcentratedQuoteModel,
    direction: &DirectionalConcentratedQuoteModel,
    a_to_b: bool,
    input_amount: u64,
    fee_bps: u16,
) -> Result<ConcentratedSwapState, QuoteError> {
    if !direction.is_executable_at(model.current_tick_index)
        || model.tick_spacing == 0
        || model.liquidity == 0
    {
        return Err(QuoteError::QuoteModelNotExecutable);
    }
    if input_amount == 0 {
        return Ok(ConcentratedSwapState {
            sqrt_price_x64: model.sqrt_price_x64,
            liquidity: model.liquidity,
            current_tick_index: model.current_tick_index,
            input_amount: 0,
            output_amount: 0,
        });
    }

    let mut amount_remaining = input_amount;
    let mut amount_out_total = 0u64;
    let mut sqrt_price = model.sqrt_price_x64;
    let mut liquidity = model.liquidity;
    let mut current_tick = model.current_tick_index;
    let mut iterations = 0usize;
    let lower_bound_tick = direction
        .windows
        .iter()
        .map(|window| window.start_tick_index)
        .min()
        .ok_or(QuoteError::QuoteModelNotExecutable)?;
    let upper_bound_tick = direction
        .windows
        .iter()
        .map(|window| window.end_tick_index)
        .max()
        .ok_or(QuoteError::QuoteModelNotExecutable)?;

    while amount_remaining > 0 {
        iterations = iterations.saturating_add(1);
        if iterations > 4_096 {
            eprintln!(
                "concentrated quote iteration limit exceeded: pool_id={}, venue={:?}, a_to_b={}, input_amount={}, amount_remaining={}, current_tick={}, sqrt_price_x64={}, liquidity={}",
                model.pool_id.0,
                Some(venue),
                a_to_b,
                input_amount,
                amount_remaining,
                current_tick,
                sqrt_price,
                liquidity
            );
            return Err(overflow_at("concentrated_exact_in.iteration_limit"));
        }

        let next_tick = next_initialized_tick(direction, current_tick, a_to_b);
        let target_tick = if let Some(tick) = next_tick {
            tick
        } else if a_to_b {
            lower_bound_tick
        } else {
            upper_bound_tick
        };
        let target_price = tick_sqrt_price(Some(venue), target_tick)?;
        if let Some(tick) = next_tick {
            if sqrt_price == target_price {
                liquidity = apply_tick_cross(liquidity, direction, tick, a_to_b)?;
                current_tick = if a_to_b { tick.saturating_sub(1) } else { tick };
                continue;
            }
        } else if sqrt_price == target_price {
            return Err(QuoteError::ConcentratedWindowExceeded);
        }
        let step = compute_concentrated_swap_step(
            sqrt_price,
            target_price,
            liquidity,
            amount_remaining,
            fee_bps,
            a_to_b,
        )
        .map_err(|error| {
            if error.is_arithmetic_overflow() {
                eprintln!(
                    "concentrated quote step overflow: pool_id={}, venue={:?}, a_to_b={}, input_amount={}, amount_remaining={}, current_tick={}, next_tick={:?}, target_tick={}, sqrt_price_x64={}, target_price_x64={}, liquidity={}",
                    model.pool_id.0,
                    Some(venue),
                    a_to_b,
                    input_amount,
                    amount_remaining,
                    current_tick,
                    next_tick,
                    target_tick,
                    sqrt_price,
                    target_price,
                    liquidity
                );
            }
            error
        })?;
        if step.amount_in == 0 && step.fee_amount == 0 && step.amount_out == 0 {
            return Err(QuoteError::InvalidSnapshotLiquidity);
        }

        amount_remaining = amount_remaining
            .checked_sub(step.amount_in.saturating_add(step.fee_amount))
            .ok_or_else(|| overflow_at("concentrated_exact_in.amount_remaining_sub"))?;
        amount_out_total = amount_out_total
            .checked_add(step.amount_out)
            .ok_or_else(|| overflow_at("concentrated_exact_in.amount_out_accumulate"))?;
        sqrt_price = step.sqrt_price_next_x64;

        if let Some(tick) = next_tick {
            if step.sqrt_price_next_x64 == target_price {
                liquidity = apply_tick_cross(liquidity, direction, tick, a_to_b).map_err(|error| {
                    if error.is_arithmetic_overflow() {
                        eprintln!(
                            "concentrated quote cross overflow: pool_id={}, venue={:?}, a_to_b={}, tick={}, current_tick={}, liquidity={}",
                            model.pool_id.0,
                            Some(venue),
                            a_to_b,
                            tick,
                            current_tick,
                            liquidity
                        );
                    }
                    error
                })?;
                current_tick = if a_to_b { tick.saturating_sub(1) } else { tick };
                continue;
            }
        }

        current_tick = tick_from_sqrt_price(Some(venue), sqrt_price).map_err(|error| {
            if error.is_arithmetic_overflow() {
                eprintln!(
                    "concentrated quote tick overflow: pool_id={}, venue={:?}, a_to_b={}, sqrt_price_x64={}, liquidity={}, amount_remaining={}",
                    model.pool_id.0,
                    Some(venue),
                    a_to_b,
                    sqrt_price,
                    liquidity,
                    amount_remaining
                );
            }
            error
        })?;
        if amount_remaining > 0 {
            if a_to_b && current_tick < lower_bound_tick {
                return Err(QuoteError::ConcentratedWindowExceeded);
            }
            if !a_to_b && current_tick > upper_bound_tick {
                return Err(QuoteError::ConcentratedWindowExceeded);
            }
        }
        if next_tick.is_none() && amount_remaining > 0 {
            return Err(QuoteError::ConcentratedWindowExceeded);
        }
    }

    Ok(ConcentratedSwapState {
        sqrt_price_x64: sqrt_price,
        liquidity,
        current_tick_index: current_tick,
        input_amount,
        output_amount: amount_out_total,
    })
}

pub fn apply_concentrated_exact_output_update(
    venue: PoolVenue,
    model: &ConcentratedQuoteModel,
    direction: &DirectionalConcentratedQuoteModel,
    a_to_b: bool,
    output_amount: u64,
    fee_bps: u16,
) -> Result<ConcentratedSwapState, QuoteError> {
    if !direction.is_executable_at(model.current_tick_index)
        || model.tick_spacing == 0
        || model.liquidity == 0
    {
        return Err(QuoteError::QuoteModelNotExecutable);
    }
    if output_amount == 0 {
        return Ok(ConcentratedSwapState {
            sqrt_price_x64: model.sqrt_price_x64,
            liquidity: model.liquidity,
            current_tick_index: model.current_tick_index,
            input_amount: 0,
            output_amount: 0,
        });
    }

    let mut amount_remaining = output_amount;
    let mut amount_in_total = 0u64;
    let mut sqrt_price = model.sqrt_price_x64;
    let mut liquidity = model.liquidity;
    let mut current_tick = model.current_tick_index;
    let mut iterations = 0usize;
    let lower_bound_tick = direction
        .windows
        .iter()
        .map(|window| window.start_tick_index)
        .min()
        .ok_or(QuoteError::QuoteModelNotExecutable)?;
    let upper_bound_tick = direction
        .windows
        .iter()
        .map(|window| window.end_tick_index)
        .max()
        .ok_or(QuoteError::QuoteModelNotExecutable)?;

    while amount_remaining > 0 {
        iterations = iterations.saturating_add(1);
        if iterations > 4_096 {
            eprintln!(
                "concentrated exact-out iteration limit exceeded: pool_id={}, venue={:?}, a_to_b={}, output_amount={}, amount_remaining={}, current_tick={}, sqrt_price_x64={}, liquidity={}",
                model.pool_id.0,
                Some(venue),
                a_to_b,
                output_amount,
                amount_remaining,
                current_tick,
                sqrt_price,
                liquidity
            );
            return Err(overflow_at("concentrated_exact_out.iteration_limit"));
        }

        let next_tick = next_initialized_tick(direction, current_tick, a_to_b);
        let target_tick = if let Some(tick) = next_tick {
            tick
        } else if a_to_b {
            lower_bound_tick
        } else {
            upper_bound_tick
        };
        let target_price = tick_sqrt_price(Some(venue), target_tick)?;
        if let Some(tick) = next_tick {
            if sqrt_price == target_price {
                liquidity = apply_tick_cross(liquidity, direction, tick, a_to_b)?;
                current_tick = if a_to_b { tick.saturating_sub(1) } else { tick };
                continue;
            }
        } else if sqrt_price == target_price {
            return Err(QuoteError::ConcentratedWindowExceeded);
        }

        let step = compute_concentrated_swap_step_exact_output(
            sqrt_price,
            target_price,
            liquidity,
            amount_remaining,
            fee_bps,
            a_to_b,
        )
        .map_err(|error| {
            if error.is_arithmetic_overflow() {
                eprintln!(
                    "concentrated exact-out step overflow: pool_id={}, venue={:?}, a_to_b={}, output_amount={}, amount_remaining={}, current_tick={}, next_tick={:?}, target_tick={}, sqrt_price_x64={}, target_price_x64={}, liquidity={}",
                    model.pool_id.0,
                    Some(venue),
                    a_to_b,
                    output_amount,
                    amount_remaining,
                    current_tick,
                    next_tick,
                    target_tick,
                    sqrt_price,
                    target_price,
                    liquidity
                );
            }
            error
        })?;
        if step.amount_in == 0 && step.fee_amount == 0 && step.amount_out == 0 {
            return Err(QuoteError::InvalidSnapshotLiquidity);
        }

        amount_remaining = amount_remaining
            .checked_sub(step.amount_out)
            .ok_or_else(|| overflow_at("concentrated_exact_out.amount_remaining_sub"))?;
        amount_in_total = amount_in_total
            .checked_add(step.amount_in.saturating_add(step.fee_amount))
            .ok_or_else(|| overflow_at("concentrated_exact_out.amount_in_accumulate"))?;
        sqrt_price = step.sqrt_price_next_x64;

        if let Some(tick) = next_tick {
            if step.sqrt_price_next_x64 == target_price {
                liquidity = apply_tick_cross(liquidity, direction, tick, a_to_b).map_err(|error| {
                    if error.is_arithmetic_overflow() {
                        eprintln!(
                            "concentrated exact-out cross overflow: pool_id={}, venue={:?}, a_to_b={}, tick={}, current_tick={}, liquidity={}",
                            model.pool_id.0,
                            Some(venue),
                            a_to_b,
                            tick,
                            current_tick,
                            liquidity
                        );
                    }
                    error
                })?;
                current_tick = if a_to_b { tick.saturating_sub(1) } else { tick };
                continue;
            }
        }

        current_tick = tick_from_sqrt_price(Some(venue), sqrt_price).map_err(|error| {
            if error.is_arithmetic_overflow() {
                eprintln!(
                    "concentrated exact-out tick overflow: pool_id={}, venue={:?}, a_to_b={}, sqrt_price_x64={}, liquidity={}, amount_remaining={}",
                    model.pool_id.0,
                    Some(venue),
                    a_to_b,
                    sqrt_price,
                    liquidity,
                    amount_remaining
                );
            }
            error
        })?;
        if amount_remaining > 0 {
            if a_to_b && current_tick < lower_bound_tick {
                return Err(QuoteError::ConcentratedWindowExceeded);
            }
            if !a_to_b && current_tick > upper_bound_tick {
                return Err(QuoteError::ConcentratedWindowExceeded);
            }
        }
        if next_tick.is_none() && amount_remaining > 0 {
            return Err(QuoteError::ConcentratedWindowExceeded);
        }
    }

    Ok(ConcentratedSwapState {
        sqrt_price_x64: sqrt_price,
        liquidity,
        current_tick_index: current_tick,
        input_amount: amount_in_total,
        output_amount,
    })
}

fn simulate_concentrated_exact_input(
    snapshot: &PoolSnapshot,
    model: &ConcentratedQuoteModel,
    direction: &DirectionalConcentratedQuoteModel,
    a_to_b: bool,
    input_amount: u64,
    fee_bps: u16,
) -> Result<u64, QuoteError> {
    apply_concentrated_exact_input_update(
        snapshot.venue.ok_or(QuoteError::InvalidSnapshotPrice)?,
        model,
        direction,
        a_to_b,
        input_amount,
        fee_bps,
    )
    .map(|state| state.output_amount)
}

#[derive(Debug, Clone, Copy)]
struct ConcentratedSwapStep {
    sqrt_price_next_x64: u128,
    amount_in: u64,
    amount_out: u64,
    fee_amount: u64,
}

fn compute_concentrated_swap_step(
    sqrt_price_current_x64: u128,
    sqrt_price_target_x64: u128,
    liquidity: u128,
    amount_remaining: u64,
    fee_bps: u16,
    a_to_b: bool,
) -> Result<ConcentratedSwapStep, QuoteError> {
    let fee_denominator = 10_000u64;
    if fee_bps >= fee_denominator as u16 {
        return Err(QuoteError::InvalidSnapshotLiquidity);
    }

    let amount_remaining_less_fee = mul_div_floor_u64(
        amount_remaining,
        fee_denominator.saturating_sub(u64::from(fee_bps)),
        fee_denominator,
    )?;
    let amount_in_max = amount_in_between_prices_u256(
        sqrt_price_current_x64,
        sqrt_price_target_x64,
        liquidity,
        a_to_b,
    )?;
    let sqrt_price_next_x64 = if U256::from(amount_remaining_less_fee) >= amount_in_max {
        sqrt_price_target_x64
    } else {
        next_sqrt_price_from_input(
            sqrt_price_current_x64,
            liquidity,
            amount_remaining_less_fee,
            a_to_b,
        )?
    };

    let amount_in = amount_in_between_prices(
        sqrt_price_current_x64,
        sqrt_price_next_x64,
        liquidity,
        a_to_b,
    )?;
    let amount_out = amount_out_between_prices(
        sqrt_price_current_x64,
        sqrt_price_next_x64,
        liquidity,
        a_to_b,
    )?;
    let fee_amount = if sqrt_price_next_x64 != sqrt_price_target_x64 {
        amount_remaining
            .checked_sub(amount_in)
            .ok_or(QuoteError::ArithmeticOverflow)?
    } else {
        mul_div_ceil_u64(
            amount_in,
            u64::from(fee_bps),
            fee_denominator.saturating_sub(u64::from(fee_bps)),
        )?
    };

    Ok(ConcentratedSwapStep {
        sqrt_price_next_x64,
        amount_in,
        amount_out,
        fee_amount,
    })
}

fn compute_concentrated_swap_step_exact_output(
    sqrt_price_current_x64: u128,
    sqrt_price_target_x64: u128,
    liquidity: u128,
    amount_remaining: u64,
    fee_bps: u16,
    a_to_b: bool,
) -> Result<ConcentratedSwapStep, QuoteError> {
    let fee_denominator = 10_000u64;
    if fee_bps >= fee_denominator as u16 {
        return Err(QuoteError::InvalidSnapshotLiquidity);
    }

    let amount_out_max = amount_out_between_prices(
        sqrt_price_current_x64,
        sqrt_price_target_x64,
        liquidity,
        a_to_b,
    )?;
    let sqrt_price_next_x64 = if amount_remaining >= amount_out_max {
        sqrt_price_target_x64
    } else {
        next_sqrt_price_from_output(sqrt_price_current_x64, liquidity, amount_remaining, a_to_b)?
    };
    let amount_out = amount_out_between_prices(
        sqrt_price_current_x64,
        sqrt_price_next_x64,
        liquidity,
        a_to_b,
    )?;
    let amount_in = amount_in_between_prices(
        sqrt_price_current_x64,
        sqrt_price_next_x64,
        liquidity,
        a_to_b,
    )?;
    let fee_amount = mul_div_ceil_u64(
        amount_in,
        u64::from(fee_bps),
        fee_denominator.saturating_sub(u64::from(fee_bps)),
    )?;

    Ok(ConcentratedSwapStep {
        sqrt_price_next_x64,
        amount_in,
        amount_out,
        fee_amount,
    })
}

fn amount_in_between_prices(
    sqrt_price_current_x64: u128,
    sqrt_price_target_x64: u128,
    liquidity: u128,
    a_to_b: bool,
) -> Result<u64, QuoteError> {
    u256_to_u64(amount_in_between_prices_u256(
        sqrt_price_current_x64,
        sqrt_price_target_x64,
        liquidity,
        a_to_b,
    )?)
}

fn amount_in_between_prices_u256(
    sqrt_price_current_x64: u128,
    sqrt_price_target_x64: u128,
    liquidity: u128,
    a_to_b: bool,
) -> Result<U256, QuoteError> {
    if a_to_b {
        delta_amount_0_u256(
            sqrt_price_target_x64,
            sqrt_price_current_x64,
            liquidity,
            true,
        )
    } else {
        delta_amount_1_u256(
            sqrt_price_current_x64,
            sqrt_price_target_x64,
            liquidity,
            true,
        )
    }
}

fn amount_out_between_prices(
    sqrt_price_current_x64: u128,
    sqrt_price_target_x64: u128,
    liquidity: u128,
    a_to_b: bool,
) -> Result<u64, QuoteError> {
    if a_to_b {
        delta_amount_1(
            sqrt_price_target_x64,
            sqrt_price_current_x64,
            liquidity,
            false,
        )
    } else {
        delta_amount_0(
            sqrt_price_current_x64,
            sqrt_price_target_x64,
            liquidity,
            false,
        )
    }
}

fn next_sqrt_price_from_input(
    sqrt_price_x64: u128,
    liquidity: u128,
    amount_in: u64,
    a_to_b: bool,
) -> Result<u128, QuoteError> {
    if a_to_b {
        next_sqrt_price_from_amount_0_rounding_up(sqrt_price_x64, liquidity, amount_in, true)
    } else {
        next_sqrt_price_from_amount_1_rounding_down(sqrt_price_x64, liquidity, amount_in, true)
    }
}

fn next_sqrt_price_from_output(
    sqrt_price_x64: u128,
    liquidity: u128,
    amount_out: u64,
    a_to_b: bool,
) -> Result<u128, QuoteError> {
    if a_to_b {
        next_sqrt_price_from_amount_1_rounding_down(sqrt_price_x64, liquidity, amount_out, false)
    } else {
        next_sqrt_price_from_amount_0_rounding_up(sqrt_price_x64, liquidity, amount_out, false)
    }
}

fn next_sqrt_price_from_amount_0_rounding_up(
    sqrt_price_x64: u128,
    liquidity: u128,
    amount: u64,
    add: bool,
) -> Result<u128, QuoteError> {
    if amount == 0 {
        return Ok(sqrt_price_x64);
    }
    let numerator_1 = U256::from(liquidity) << 64;
    if add {
        let product = U256::from(amount).saturating_mul(U256::from(sqrt_price_x64));
        let denominator = numerator_1.checked_add(product).ok_or_else(|| {
            overflow_at("next_sqrt_price_from_amount_0_rounding_up.add_denominator")
        })?;
        u256_to_u128_ctx(
            mul_div_ceil_u256(numerator_1, U256::from(sqrt_price_x64), denominator)?,
            "next_sqrt_price_from_amount_0_rounding_up.add_to_u128",
        )
    } else {
        let product = U256::from(amount).saturating_mul(U256::from(sqrt_price_x64));
        let denominator = numerator_1.checked_sub(product).ok_or_else(|| {
            overflow_at("next_sqrt_price_from_amount_0_rounding_up.sub_denominator")
        })?;
        u256_to_u128_ctx(
            mul_div_ceil_u256(numerator_1, U256::from(sqrt_price_x64), denominator)?,
            "next_sqrt_price_from_amount_0_rounding_up.sub_to_u128",
        )
    }
}

fn next_sqrt_price_from_amount_1_rounding_down(
    sqrt_price_x64: u128,
    liquidity: u128,
    amount: u64,
    add: bool,
) -> Result<u128, QuoteError> {
    let quotient = if add {
        (U256::from(amount) << 64)
            .checked_div(U256::from(liquidity))
            .ok_or_else(|| {
                overflow_at("next_sqrt_price_from_amount_1_rounding_down.add_quotient_div")
            })?
    } else {
        div_rounding_up_u256_ctx(
            U256::from(amount) << 64,
            U256::from(liquidity),
            "next_sqrt_price_from_amount_1_rounding_down.sub_quotient_div",
        )?
    };
    let next = if add {
        U256::from(sqrt_price_x64)
            .checked_add(quotient)
            .ok_or_else(|| overflow_at("next_sqrt_price_from_amount_1_rounding_down.add_next"))?
    } else {
        U256::from(sqrt_price_x64)
            .checked_sub(quotient)
            .ok_or_else(|| overflow_at("next_sqrt_price_from_amount_1_rounding_down.sub_next"))?
    };
    u256_to_u128_ctx(
        next,
        "next_sqrt_price_from_amount_1_rounding_down.next_to_u128",
    )
}

fn increasing_price_order(sqrt_price_0: u128, sqrt_price_1: u128) -> (u128, u128) {
    if sqrt_price_0 > sqrt_price_1 {
        (sqrt_price_1, sqrt_price_0)
    } else {
        (sqrt_price_0, sqrt_price_1)
    }
}

fn delta_amount_0(
    sqrt_ratio_a_x64: u128,
    sqrt_ratio_b_x64: u128,
    liquidity: u128,
    round_up: bool,
) -> Result<u64, QuoteError> {
    u256_to_u64(delta_amount_0_u256(
        sqrt_ratio_a_x64,
        sqrt_ratio_b_x64,
        liquidity,
        round_up,
    )?)
}

fn delta_amount_1(
    sqrt_ratio_a_x64: u128,
    sqrt_ratio_b_x64: u128,
    liquidity: u128,
    round_up: bool,
) -> Result<u64, QuoteError> {
    u256_to_u64(delta_amount_1_u256(
        sqrt_ratio_a_x64,
        sqrt_ratio_b_x64,
        liquidity,
        round_up,
    )?)
}

fn delta_amount_0_u256(
    sqrt_ratio_a_x64: u128,
    sqrt_ratio_b_x64: u128,
    liquidity: u128,
    round_up: bool,
) -> Result<U256, QuoteError> {
    let (lower, upper) = increasing_price_order(sqrt_ratio_a_x64, sqrt_ratio_b_x64);
    let numerator_1 = U256::from(liquidity) << 64;
    let numerator_2 = U256::from(upper.saturating_sub(lower));
    let intermediate = if round_up {
        mul_div_ceil_u256(numerator_1, numerator_2, U256::from(upper))?
    } else {
        mul_div_floor_u256(numerator_1, numerator_2, U256::from(upper))?
    };
    if round_up {
        div_rounding_up_u256(intermediate, U256::from(lower))
    } else {
        intermediate
            .checked_div(U256::from(lower))
            .ok_or(QuoteError::ArithmeticOverflow)
    }
}

fn delta_amount_1_u256(
    sqrt_ratio_a_x64: u128,
    sqrt_ratio_b_x64: u128,
    liquidity: u128,
    round_up: bool,
) -> Result<U256, QuoteError> {
    let (lower, upper) = increasing_price_order(sqrt_ratio_a_x64, sqrt_ratio_b_x64);
    if round_up {
        Ok(mul_div_ceil_u256(
            U256::from(liquidity),
            U256::from(upper.saturating_sub(lower)),
            U256::from(1u128 << 64),
        )?)
    } else {
        Ok(mul_div_floor_u256(
            U256::from(liquidity),
            U256::from(upper.saturating_sub(lower)),
            U256::from(1u128 << 64),
        )?)
    }
}

fn apply_tick_cross(
    liquidity: u128,
    direction: &DirectionalConcentratedQuoteModel,
    tick_index: i32,
    a_to_b: bool,
) -> Result<u128, QuoteError> {
    let liquidity_net = direction
        .initialized_ticks
        .iter()
        .find(|tick| tick.tick_index == tick_index)
        .map(|tick| tick.liquidity_net)
        .ok_or(QuoteError::ConcentratedWindowExceeded)?;
    if a_to_b {
        if liquidity_net >= 0 {
            liquidity
                .checked_sub(liquidity_net as u128)
                .ok_or_else(|| overflow_at("apply_tick_cross.a_to_b.checked_sub"))
        } else {
            liquidity
                .checked_add(liquidity_net.unsigned_abs())
                .ok_or_else(|| overflow_at("apply_tick_cross.a_to_b.checked_add"))
        }
    } else if liquidity_net >= 0 {
        liquidity
            .checked_add(liquidity_net as u128)
            .ok_or_else(|| overflow_at("apply_tick_cross.b_to_a.checked_add"))
    } else {
        liquidity
            .checked_sub(liquidity_net.unsigned_abs())
            .ok_or_else(|| overflow_at("apply_tick_cross.b_to_a.checked_sub"))
    }
}

fn next_initialized_tick(
    direction: &DirectionalConcentratedQuoteModel,
    current_tick: i32,
    a_to_b: bool,
) -> Option<i32> {
    if a_to_b {
        direction
            .initialized_ticks
            .iter()
            .rev()
            .find(|tick| tick.tick_index <= current_tick)
            .map(|tick| tick.tick_index)
    } else {
        direction
            .initialized_ticks
            .iter()
            .find(|tick| tick.tick_index > current_tick)
            .map(|tick| tick.tick_index)
    }
}

fn tick_sqrt_price(venue: Option<PoolVenue>, tick: i32) -> Result<u128, QuoteError> {
    match venue {
        Some(PoolVenue::RaydiumClmm) => raydium_sqrt_price_at_tick(tick),
        Some(PoolVenue::OrcaWhirlpool) => Ok(orca_sqrt_price_at_tick(tick)),
        _ => Err(QuoteError::InvalidSnapshotPrice),
    }
}

fn tick_from_sqrt_price(venue: Option<PoolVenue>, sqrt_price_x64: u128) -> Result<i32, QuoteError> {
    match venue {
        Some(PoolVenue::RaydiumClmm) => raydium_tick_from_sqrt_price(sqrt_price_x64),
        Some(PoolVenue::OrcaWhirlpool) => Ok(orca_tick_from_sqrt_price(sqrt_price_x64)),
        _ => Err(QuoteError::InvalidSnapshotPrice),
    }
}

fn orca_sqrt_price_at_tick(tick: i32) -> u128 {
    const MUL_SHIFT_96: fn(u128, u128) -> u128 = |left, right| {
        let product = U256::from(left).saturating_mul(U256::from(right));
        (product >> 96).low_u128()
    };
    const MUL_SHIFT_64: fn(u128, u128) -> u128 = |left, right| {
        let product = U256::from(left).saturating_mul(U256::from(right));
        (product >> 64).low_u128()
    };
    if tick >= 0 {
        let mut ratio = if tick & 1 != 0 {
            79_232_123_823_359_799_118_286_999_567u128
        } else {
            79_228_162_514_264_337_593_543_950_336u128
        };
        if tick & 2 != 0 {
            ratio = MUL_SHIFT_96(ratio, 79_236_085_330_515_764_027_303_304_731);
        }
        if tick & 4 != 0 {
            ratio = MUL_SHIFT_96(ratio, 79_244_008_939_048_815_603_706_035_061);
        }
        if tick & 8 != 0 {
            ratio = MUL_SHIFT_96(ratio, 79_259_858_533_276_714_757_314_932_305);
        }
        if tick & 16 != 0 {
            ratio = MUL_SHIFT_96(ratio, 79_291_567_232_598_584_799_939_703_904);
        }
        if tick & 32 != 0 {
            ratio = MUL_SHIFT_96(ratio, 79_355_022_692_464_371_645_785_046_466);
        }
        if tick & 64 != 0 {
            ratio = MUL_SHIFT_96(ratio, 79_482_085_999_252_804_386_437_311_141);
        }
        if tick & 128 != 0 {
            ratio = MUL_SHIFT_96(ratio, 79_736_823_300_114_093_921_829_183_326);
        }
        if tick & 256 != 0 {
            ratio = MUL_SHIFT_96(ratio, 80_248_749_790_819_932_309_965_073_892);
        }
        if tick & 512 != 0 {
            ratio = MUL_SHIFT_96(ratio, 81_282_483_887_344_747_381_513_967_011);
        }
        if tick & 1024 != 0 {
            ratio = MUL_SHIFT_96(ratio, 83_390_072_131_320_151_908_154_831_281);
        }
        if tick & 2048 != 0 {
            ratio = MUL_SHIFT_96(ratio, 87_770_609_709_833_776_024_991_924_138);
        }
        if tick & 4096 != 0 {
            ratio = MUL_SHIFT_96(ratio, 97_234_110_755_111_693_312_479_820_773);
        }
        if tick & 8192 != 0 {
            ratio = MUL_SHIFT_96(ratio, 119_332_217_159_966_728_226_237_229_890);
        }
        if tick & 16_384 != 0 {
            ratio = MUL_SHIFT_96(ratio, 179_736_315_981_702_064_433_883_588_727);
        }
        if tick & 32_768 != 0 {
            ratio = MUL_SHIFT_96(ratio, 407_748_233_172_238_350_107_850_275_304);
        }
        if tick & 65_536 != 0 {
            ratio = MUL_SHIFT_96(ratio, 2_098_478_828_474_011_932_436_660_412_517);
        }
        if tick & 131_072 != 0 {
            ratio = MUL_SHIFT_96(ratio, 55_581_415_166_113_811_149_459_800_483_533);
        }
        if tick & 262_144 != 0 {
            ratio = MUL_SHIFT_96(ratio, 38_992_368_544_603_139_932_233_054_999_993_551);
        }
        ratio >> 32
    } else {
        let abs_tick = tick.abs();
        let mut ratio = if abs_tick & 1 != 0 {
            18_445_821_805_675_392_311u128
        } else {
            18_446_744_073_709_551_616u128
        };
        if abs_tick & 2 != 0 {
            ratio = MUL_SHIFT_64(ratio, 18_444_899_583_751_176_498);
        }
        if abs_tick & 4 != 0 {
            ratio = MUL_SHIFT_64(ratio, 18_443_055_278_223_354_162);
        }
        if abs_tick & 8 != 0 {
            ratio = MUL_SHIFT_64(ratio, 18_439_367_220_385_604_838);
        }
        if abs_tick & 16 != 0 {
            ratio = MUL_SHIFT_64(ratio, 18_431_993_317_065_449_817);
        }
        if abs_tick & 32 != 0 {
            ratio = MUL_SHIFT_64(ratio, 18_417_254_355_718_160_513);
        }
        if abs_tick & 64 != 0 {
            ratio = MUL_SHIFT_64(ratio, 18_387_811_781_193_591_352);
        }
        if abs_tick & 128 != 0 {
            ratio = MUL_SHIFT_64(ratio, 18_329_067_761_203_520_168);
        }
        if abs_tick & 256 != 0 {
            ratio = MUL_SHIFT_64(ratio, 18_212_142_134_806_087_854);
        }
        if abs_tick & 512 != 0 {
            ratio = MUL_SHIFT_64(ratio, 17_980_523_815_641_551_639);
        }
        if abs_tick & 1024 != 0 {
            ratio = MUL_SHIFT_64(ratio, 17_526_086_738_831_147_013);
        }
        if abs_tick & 2048 != 0 {
            ratio = MUL_SHIFT_64(ratio, 16_651_378_430_235_024_244);
        }
        if abs_tick & 4096 != 0 {
            ratio = MUL_SHIFT_64(ratio, 15_030_750_278_693_429_944);
        }
        if abs_tick & 8192 != 0 {
            ratio = MUL_SHIFT_64(ratio, 12_247_334_978_882_834_399);
        }
        if abs_tick & 16_384 != 0 {
            ratio = MUL_SHIFT_64(ratio, 8_131_365_268_884_726_200);
        }
        if abs_tick & 32_768 != 0 {
            ratio = MUL_SHIFT_64(ratio, 3_584_323_654_723_342_297);
        }
        if abs_tick & 65_536 != 0 {
            ratio = MUL_SHIFT_64(ratio, 696_457_651_847_595_233);
        }
        if abs_tick & 131_072 != 0 {
            ratio = MUL_SHIFT_64(ratio, 26_294_789_957_452_057);
        }
        if abs_tick & 262_144 != 0 {
            ratio = MUL_SHIFT_64(ratio, 37_481_735_321_082);
        }
        ratio
    }
}

fn orca_tick_from_sqrt_price(sqrt_price_x64: u128) -> i32 {
    let msb = 128 - sqrt_price_x64.leading_zeros() - 1;
    let log2p_integer_x32 = (msb as i128 - 64) << 32;
    let mut bit = 0x8000_0000_0000_0000i128;
    let mut precision = 0;
    let mut log2p_fraction_x64 = 0i128;
    let mut r = if msb >= 64 {
        sqrt_price_x64 >> (msb - 63)
    } else {
        sqrt_price_x64 << (63 - msb)
    };
    while bit > 0 && precision < 14 {
        r = r.saturating_mul(r);
        let is_r_more_than_two = r >> 127;
        r >>= 63 + is_r_more_than_two;
        log2p_fraction_x64 += bit * is_r_more_than_two as i128;
        bit >>= 1;
        precision += 1;
    }
    let log2p_x32 = log2p_integer_x32 + (log2p_fraction_x64 >> 32);
    let logbp_x64 = log2p_x32 * 59_543_866_431_248i128;
    let tick_low = ((logbp_x64 - 184_467_440_737_095_516i128) >> 64) as i32;
    let tick_high = ((logbp_x64 + 15_793_534_762_490_258_745i128) >> 64) as i32;
    if tick_low == tick_high {
        tick_low
    } else if orca_sqrt_price_at_tick(tick_high) <= sqrt_price_x64 {
        tick_high
    } else {
        tick_low
    }
}

fn raydium_sqrt_price_at_tick(tick: i32) -> Result<u128, QuoteError> {
    let abs_tick = tick.abs() as u32;
    if abs_tick > 443_636 {
        return Err(QuoteError::InvalidSnapshotPrice);
    }
    let mut ratio = if abs_tick & 0x1 != 0 {
        U256::from(0xfffcb933bd6fb800u64)
    } else {
        U256::from(1u128 << 64)
    };
    const FACTORS: [u64; 18] = [
        0xfff97272373d4000,
        0xfff2e50f5f657000,
        0xffe5caca7e10f000,
        0xffcb9843d60f7000,
        0xff973b41fa98e800,
        0xff2ea16466c9b000,
        0xfe5dee046a9a3800,
        0xfcbe86c7900bb000,
        0xf987a7253ac65800,
        0xf3392b0822bb6000,
        0xe7159475a2caf000,
        0xd097f3bdfd2f2000,
        0xa9f746462d9f8000,
        0x70d869a156f31c00,
        0x31be135f97ed3200,
        0x09aa508b5b85a500,
        0x005d6af8dedc582c,
        0x00002216e584f5fa,
    ];
    for (bit, factor) in FACTORS.into_iter().enumerate() {
        if abs_tick & (1 << (bit + 1)) != 0 {
            ratio = (ratio * U256::from(factor)) >> 64;
        }
    }
    if tick > 0 {
        ratio = (U256::one() << 128) / ratio;
    }
    u256_to_u128_ctx(ratio, "raydium_sqrt_price_at_tick.ratio_to_u128")
}

fn raydium_tick_from_sqrt_price(sqrt_price_x64: u128) -> Result<i32, QuoteError> {
    if !(4_295_048_016..79_226_673_521_066_979_257_578_248_091u128).contains(&sqrt_price_x64) {
        return Err(QuoteError::InvalidSnapshotPrice);
    }
    let msb = 128 - sqrt_price_x64.leading_zeros() - 1;
    let log2p_integer_x32 = (msb as i128 - 64) << 32;
    let mut bit = 0x8000_0000_0000_0000i128;
    let mut precision = 0;
    let mut log2p_fraction_x64 = 0i128;
    let mut r = if msb >= 64 {
        sqrt_price_x64 >> (msb - 63)
    } else {
        sqrt_price_x64 << (63 - msb)
    };
    while bit > 0 && precision < 16 {
        r = r.saturating_mul(r);
        let is_r_more_than_two = r >> 127;
        r >>= 63 + is_r_more_than_two;
        log2p_fraction_x64 += bit * is_r_more_than_two as i128;
        bit >>= 1;
        precision += 1;
    }
    let log2p_x32 = log2p_integer_x32 + (log2p_fraction_x64 >> 32);
    let log_sqrt_10001_x64 = log2p_x32 * 59_543_866_431_248i128;
    let tick_low = ((log_sqrt_10001_x64 - 184_467_440_737_095_516i128) >> 64) as i32;
    let tick_high = ((log_sqrt_10001_x64 + 15_793_534_762_490_258_745i128) >> 64) as i32;
    if tick_low == tick_high {
        Ok(tick_low)
    } else if raydium_sqrt_price_at_tick(tick_high)? <= sqrt_price_x64 {
        Ok(tick_high)
    } else {
        Ok(tick_low)
    }
}

fn mul_div_floor_u64(value: u64, numerator: u64, denominator: u64) -> Result<u64, QuoteError> {
    if denominator == 0 {
        return Err(overflow_at("mul_div_floor_u64.denominator_zero"));
    }
    u256_to_u64_ctx(
        U256::from(value)
            .checked_mul(U256::from(numerator))
            .ok_or_else(|| overflow_at("mul_div_floor_u64.checked_mul"))?
            / U256::from(denominator),
        "mul_div_floor_u64.to_u64",
    )
}

fn mul_div_ceil_u64(value: u64, numerator: u64, denominator: u64) -> Result<u64, QuoteError> {
    if denominator == 0 {
        return Err(overflow_at("mul_div_ceil_u64.denominator_zero"));
    }
    u256_to_u64_ctx(
        div_rounding_up_u256_ctx(
            U256::from(value)
                .checked_mul(U256::from(numerator))
                .ok_or_else(|| overflow_at("mul_div_ceil_u64.checked_mul"))?,
            U256::from(denominator),
            "mul_div_ceil_u64.div_rounding_up",
        )?,
        "mul_div_ceil_u64.to_u64",
    )
}

fn mul_div_floor_u256(left: U256, right: U256, denominator: U256) -> Result<U256, QuoteError> {
    left.checked_mul(right)
        .ok_or_else(|| overflow_at("mul_div_floor_u256.checked_mul"))?
        .checked_div(denominator)
        .ok_or_else(|| overflow_at("mul_div_floor_u256.checked_div"))
}

fn mul_div_ceil_u256(left: U256, right: U256, denominator: U256) -> Result<U256, QuoteError> {
    let numerator = left
        .checked_mul(right)
        .ok_or_else(|| overflow_at("mul_div_ceil_u256.checked_mul"))?;
    div_rounding_up_u256_ctx(numerator, denominator, "mul_div_ceil_u256.div_rounding_up")
}

fn div_rounding_up_u256_ctx(
    numerator: U256,
    denominator: U256,
    context: &'static str,
) -> Result<U256, QuoteError> {
    if denominator.is_zero() {
        return Err(overflow_at(context));
    }
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    if remainder.is_zero() {
        Ok(quotient)
    } else {
        quotient
            .checked_add(U256::one())
            .ok_or_else(|| overflow_at(context))
    }
}

fn div_rounding_up_u256(numerator: U256, denominator: U256) -> Result<U256, QuoteError> {
    div_rounding_up_u256_ctx(numerator, denominator, "div_rounding_up_u256")
}

fn u256_to_u64_ctx(value: U256, context: &'static str) -> Result<u64, QuoteError> {
    if value > U256::from(u64::MAX) {
        return Err(overflow_at(context));
    }
    Ok(value.low_u64())
}

fn u256_to_u64(value: U256) -> Result<u64, QuoteError> {
    u256_to_u64_ctx(value, "u256_to_u64")
}

fn u256_to_u128_ctx(value: U256, context: &'static str) -> Result<u128, QuoteError> {
    if value > U256::from(u128::MAX) {
        return Err(overflow_at(context));
    }
    Ok(value.low_u128())
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use domain::quote_models::{
        ConcentratedQuoteModel, DirectionalConcentratedQuoteModel, InitializedTick, TickArrayWindow,
    };
    use state::types::{
        FreshnessState, LiquidityModel, PoolConfidence, PoolId, PoolSnapshot, PoolVenue, RouteId,
    };

    use crate::{
        quote::{
            QuoteError, RouteQuote, SOL_MINT, amount_in_between_prices_u256,
            apply_concentrated_exact_input_update, apply_concentrated_price,
            compute_concentrated_swap_step, orca_sqrt_price_at_tick, orca_tick_from_sqrt_price,
            raydium_sqrt_price_at_tick, raydium_tick_from_sqrt_price,
        },
        route_registry::{RouteDefinition, RouteLeg, RouteSizingPolicy, SizingMode, SwapSide},
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
            max_quote_slot_lag: 32,
            min_trade_size: 1,
            default_trade_size: 1,
            max_trade_size: 10,
            size_ladder: Vec::new(),
            estimated_execution_cost_lamports: 0,
            sizing: RouteSizingPolicy {
                mode: SizingMode::Legacy,
                min_trade_floor_sol_lamports: 100_000_000,
                base_landing_rate_bps: 8_500,
                base_expected_shortfall_bps: 75,
                max_expected_shortfall_bps: 500,
            },
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

    fn concentrated_snapshot(venue: Option<PoolVenue>) -> PoolSnapshot {
        let liquidity_model = LiquidityModel::ConcentratedLiquidity;
        PoolSnapshot {
            pool_id: PoolId("pool-clmm".into()),
            price_bps: 10_000,
            fee_bps: 30,
            reserve_depth: 1_000_000,
            reserve_a: None,
            reserve_b: None,
            active_liquidity: 1_000_000,
            sqrt_price_x64: Some(1u128 << 64),
            venue,
            confidence: PoolConfidence::Executable,
            repair_pending: false,
            liquidity_model,
            slippage_factor_bps: PoolSnapshot::default_slippage_factor_bps(liquidity_model, 64),
            token_mint_a: "SOL".into(),
            token_mint_b: "USDC".into(),
            tick_spacing: 64,
            current_tick_index: Some(0),
            last_update_slot: 1,
            derived_at: SystemTime::now(),
            freshness: FreshnessState::at(1, 1, 1),
        }
    }

    fn concentrated_model() -> ConcentratedQuoteModel {
        ConcentratedQuoteModel {
            pool_id: PoolId("pool-clmm".into()),
            liquidity: 1_000_000,
            sqrt_price_x64: 1u128 << 64,
            current_tick_index: 0,
            tick_spacing: 64,
            required_a_to_b: true,
            required_b_to_a: false,
            a_to_b: Some(DirectionalConcentratedQuoteModel {
                loaded_tick_arrays: 1,
                expected_tick_arrays: 1,
                complete: true,
                windows: vec![TickArrayWindow {
                    start_tick_index: -64,
                    end_tick_index: 0,
                    initialized_tick_count: 0,
                }],
                initialized_ticks: Vec::new(),
            }),
            b_to_a: None,
            last_update_slot: 1,
            write_version: 1,
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
    fn execution_cost_caps_quoted_slot_to_conversion_snapshot_slot() {
        let route = route_definition("USDT", "USDC", Some("pool-sol-usdc"));
        let mut sol_quote_snapshot = snapshot("pool-sol-usdc", SOL_MINT, "USDC", 10_000);
        sol_quote_snapshot.last_update_slot = 7;
        let mut route_quote = quote();
        route_quote.quoted_slot = 12;

        let quote = route_quote
            .with_estimated_execution_cost(&route, Some(&sol_quote_snapshot), 1)
            .expect("sol/usdc conversion should succeed");

        assert_eq!(quote.quoted_slot, 7);
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

    #[test]
    fn concentrated_quote_uses_snapshot_venue_for_tick_math() {
        let model = concentrated_model();
        let missing_venue = concentrated_snapshot(None);
        let with_venue = concentrated_snapshot(Some(PoolVenue::OrcaWhirlpool));

        assert!(matches!(
            apply_concentrated_price(&missing_venue, Some(&model), "SOL", "USDC", 1_000, 30),
            Err(QuoteError::InvalidSnapshotPrice)
        ));

        let priced = apply_concentrated_price(&with_venue, Some(&model), "SOL", "USDC", 1_000, 30)
            .expect("whirlpool venue should unlock concentrated tick math");
        assert!(priced.gross_output >= priced.net_output);
        assert!(priced.net_output > 0);
    }

    #[test]
    fn concentrated_quote_crosses_initialized_tick_when_price_starts_on_boundary() {
        let snapshot = concentrated_snapshot(Some(PoolVenue::OrcaWhirlpool));
        let mut model = concentrated_model();
        model.a_to_b = Some(DirectionalConcentratedQuoteModel {
            loaded_tick_arrays: 1,
            expected_tick_arrays: 1,
            complete: true,
            windows: vec![TickArrayWindow {
                start_tick_index: -64,
                end_tick_index: 0,
                initialized_tick_count: 1,
            }],
            initialized_ticks: vec![InitializedTick {
                tick_index: 0,
                liquidity_net: 100_000,
                liquidity_gross: 100_000,
            }],
        });

        let priced = apply_concentrated_price(&snapshot, Some(&model), "SOL", "USDC", 1_000, 30)
            .expect("price on an initialized boundary should cross the tick instead of failing");

        assert!(priced.gross_output >= priced.net_output);
        assert!(priced.net_output > 0);
    }

    #[test]
    fn concentrated_quote_rejects_direction_that_does_not_cover_current_tick() {
        let snapshot = concentrated_snapshot(Some(PoolVenue::OrcaWhirlpool));
        let mut model = concentrated_model();
        model.current_tick_index = 128;
        model.a_to_b = Some(DirectionalConcentratedQuoteModel {
            loaded_tick_arrays: 1,
            expected_tick_arrays: 3,
            complete: false,
            windows: vec![TickArrayWindow {
                start_tick_index: -64,
                end_tick_index: 0,
                initialized_tick_count: 0,
            }],
            initialized_ticks: Vec::new(),
        });
        let direction = model.a_to_b.as_ref().expect("a_to_b quote model");

        assert!(matches!(
            apply_concentrated_price(&snapshot, Some(&model), "SOL", "USDC", 1_000, 30),
            Err(QuoteError::QuoteModelNotExecutable)
        ));
        assert!(matches!(
            apply_concentrated_exact_input_update(
                PoolVenue::OrcaWhirlpool,
                &model,
                direction,
                true,
                1_000,
                30,
            ),
            Err(QuoteError::QuoteModelNotExecutable)
        ));
    }

    #[test]
    fn orca_negative_tick_sqrt_price_round_trips_large_ticks() {
        for tick in [-1, -32, -1_024, -100_000, -221_818] {
            let sqrt_price = orca_sqrt_price_at_tick(tick);
            assert_eq!(orca_tick_from_sqrt_price(sqrt_price), tick);
        }
    }

    #[test]
    fn raydium_sqrt_price_round_trips_large_positive_and_negative_ticks() {
        for tick in [
            -1, -32, -1_024, -100_000, -221_818, 1, 32, 1_024, 100_000, 221_818,
        ] {
            let sqrt_price =
                raydium_sqrt_price_at_tick(tick).expect("raydium sqrt price should be computed");
            assert_eq!(
                raydium_tick_from_sqrt_price(sqrt_price)
                    .expect("raydium tick should round-trip from sqrt price"),
                tick
            );
        }
    }

    #[test]
    fn concentrated_swap_step_handles_amount_in_max_above_u64() {
        let amount_in_max =
            amount_in_between_prices_u256(1u128 << 64, 2u128 << 64, u128::MAX / 8, false)
                .expect("amount-in-max should be representable in U256");
        assert!(amount_in_max > u64::MAX.into());

        let step = compute_concentrated_swap_step(
            1u128 << 64,
            2u128 << 64,
            u128::MAX / 8,
            1_000_000,
            30,
            false,
        )
        .expect("step should cap against a huge amount-in-max without overflowing");

        assert_eq!(step.sqrt_price_next_x64, 1u128 << 64);
        assert_eq!(step.amount_in, 0);
        assert_eq!(step.amount_out, 0);
        assert_eq!(step.fee_amount, 1_000_000);
    }
}
