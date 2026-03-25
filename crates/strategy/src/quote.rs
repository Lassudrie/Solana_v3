use thiserror::Error;

use crate::route_registry::RouteDefinition;
use state::types::PoolSnapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegQuote {
    pub venue: String,
    pub pool_id: state::types::PoolId,
    pub input_amount: u64,
    pub output_amount: u64,
    pub fee_paid: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteQuote {
    pub quoted_slot: u64,
    pub input_amount: u64,
    pub gross_output_amount: u64,
    pub net_output_amount: u64,
    pub expected_net_profit: i64,
    pub leg_quotes: [LegQuote; 2],
}

#[derive(Debug, Error)]
pub enum QuoteError {
    #[error("expected exactly 2 snapshots for the 2-leg template")]
    InvalidRouteShape,
    #[error("arithmetic overflow during quote")]
    ArithmeticOverflow,
}

pub trait QuoteEngine: Send + Sync {
    fn quote(
        &self,
        route: &RouteDefinition,
        snapshots: [&PoolSnapshot; 2],
        quoted_slot: u64,
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
    ) -> Result<RouteQuote, QuoteError> {
        let input_amount = route.default_trade_size;
        let first = apply_price(input_amount, snapshots[0].price_bps, snapshots[0].fee_bps)?;
        let second = apply_price(
            first.net_output,
            snapshots[1].price_bps,
            snapshots[1].fee_bps,
        )?;
        let expected_net_profit = second
            .net_output
            .checked_sub(input_amount)
            .map(|profit| profit as i64)
            .unwrap_or_else(|| -((input_amount - second.net_output) as i64));

        Ok(RouteQuote {
            quoted_slot,
            input_amount,
            gross_output_amount: second.gross_output,
            net_output_amount: second.net_output,
            expected_net_profit,
            leg_quotes: [
                LegQuote {
                    venue: route.legs[0].venue.clone(),
                    pool_id: route.legs[0].pool_id.clone(),
                    input_amount,
                    output_amount: first.net_output,
                    fee_paid: first.fee_paid,
                },
                LegQuote {
                    venue: route.legs[1].venue.clone(),
                    pool_id: route.legs[1].pool_id.clone(),
                    input_amount: first.net_output,
                    output_amount: second.net_output,
                    fee_paid: second.fee_paid,
                },
            ],
        })
    }
}

struct PricedAmount {
    gross_output: u64,
    net_output: u64,
    fee_paid: u64,
}

fn apply_price(
    input_amount: u64,
    price_bps: u64,
    fee_bps: u16,
) -> Result<PricedAmount, QuoteError> {
    let gross_output: u64 = ((input_amount as u128) * (price_bps as u128) / 10_000u128)
        .try_into()
        .map_err(|_| QuoteError::ArithmeticOverflow)?;
    let fee_paid: u64 = ((gross_output as u128) * (fee_bps as u128) / 10_000u128)
        .try_into()
        .map_err(|_| QuoteError::ArithmeticOverflow)?;
    let net_output = gross_output
        .checked_sub(fee_paid)
        .ok_or(QuoteError::ArithmeticOverflow)?;
    Ok(PricedAmount {
        gross_output,
        net_output,
        fee_paid,
    })
}
