use crate::{
    templates::AtomicTwoLegTemplate,
    types::{
        BuildRejectionReason, BuildRequest, BuildResult, BuildStatus, UnsignedTransactionEnvelope,
    },
};

pub trait TransactionBuilder: Send + Sync {
    fn build(&self, request: BuildRequest) -> BuildResult;
}

#[derive(Debug, Clone)]
pub struct AtomicArbTransactionBuilder {
    template: AtomicTwoLegTemplate,
}

impl Default for AtomicArbTransactionBuilder {
    fn default() -> Self {
        Self {
            template: AtomicTwoLegTemplate::default(),
        }
    }
}

impl AtomicArbTransactionBuilder {
    pub fn new(template: AtomicTwoLegTemplate) -> Self {
        Self { template }
    }
}

impl TransactionBuilder for AtomicArbTransactionBuilder {
    fn build(&self, request: BuildRequest) -> BuildResult {
        if request.dynamic.recent_blockhash.is_empty() {
            return BuildResult {
                status: BuildStatus::Rejected,
                envelope: None,
                rejection: Some(BuildRejectionReason::MissingBlockhash),
            };
        }

        let leg_plans = self.template.materialize_leg_plans(&request.candidate);
        let instructions = self.template.materialize_instructions(
            &request.candidate.route_id,
            &leg_plans,
            request.dynamic.jito_tip_lamports,
        );
        let message_bytes = format!(
            "template={};route={};slot={};profit={}",
            self.template.template_name,
            request.candidate.route_id.0,
            request.candidate.quoted_slot,
            request.candidate.expected_net_profit
        )
        .into_bytes();

        BuildResult {
            status: BuildStatus::Built,
            envelope: Some(UnsignedTransactionEnvelope {
                route_id: request.candidate.route_id,
                build_slot: request.candidate.quoted_slot,
                recent_blockhash: request.dynamic.recent_blockhash,
                leg_plans,
                instructions,
                message_bytes,
                compute_unit_limit: request.dynamic.compute_unit_limit,
                compute_unit_price_micro_lamports: request
                    .dynamic
                    .compute_unit_price_micro_lamports,
                jito_tip_lamports: request.dynamic.jito_tip_lamports,
            }),
            rejection: None,
        }
    }
}
