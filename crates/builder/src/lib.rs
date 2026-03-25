pub mod templates;
pub mod transaction_builder;
pub mod types;

pub use templates::AtomicTwoLegTemplate;
pub use transaction_builder::{AtomicArbTransactionBuilder, TransactionBuilder};
pub use types::{
    AtomicLegPlan, BuildRejectionReason, BuildRequest, BuildResult, BuildStatus,
    DynamicBuildParameters, InstructionTemplate, UnsignedTransactionEnvelope,
};

#[cfg(test)]
mod tests {
    use state::types::RouteId;
    use strategy::{opportunity::OpportunityCandidate, quote::LegQuote};

    use crate::{
        AtomicArbTransactionBuilder,
        transaction_builder::TransactionBuilder,
        types::{BuildRequest, BuildStatus, DynamicBuildParameters},
    };

    #[test]
    fn builder_returns_structured_result() {
        let builder = AtomicArbTransactionBuilder::default();
        let candidate = OpportunityCandidate {
            route_id: RouteId("route-a".into()),
            quoted_slot: 42,
            trade_size: 10_000,
            expected_net_output: 10_250,
            expected_net_profit: 250,
            leg_quotes: [
                LegQuote {
                    venue: "venue-a".into(),
                    pool_id: state::types::PoolId("pool-a".into()),
                    input_amount: 10_000,
                    output_amount: 10_120,
                    fee_paid: 5,
                },
                LegQuote {
                    venue: "venue-b".into(),
                    pool_id: state::types::PoolId("pool-b".into()),
                    input_amount: 10_120,
                    output_amount: 10_250,
                    fee_paid: 5,
                },
            ],
        };
        let result = builder.build(BuildRequest {
            candidate,
            dynamic: DynamicBuildParameters {
                recent_blockhash: "blockhash-1".into(),
                compute_unit_limit: 300_000,
                compute_unit_price_micro_lamports: 25_000,
                jito_tip_lamports: 5_000,
                alt_revision: 1,
            },
        });

        assert_eq!(result.status, BuildStatus::Built);
        let envelope = result.envelope.expect("built envelope");
        assert_eq!(envelope.instructions.len(), 4);
        assert_eq!(envelope.route_id, RouteId("route-a".into()));
    }
}
