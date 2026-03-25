use state::types::RouteId;
use strategy::opportunity::OpportunityCandidate;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicBuildParameters {
    pub recent_blockhash: String,
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
    pub jito_tip_lamports: u64,
    pub alt_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicLegPlan {
    pub venue: String,
    pub pool_id: state::types::PoolId,
    pub input_amount: u64,
    pub min_output_amount: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionTemplate {
    pub label: String,
    pub program: String,
    pub accounts: Vec<String>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildRequest {
    pub candidate: OpportunityCandidate,
    pub dynamic: DynamicBuildParameters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsignedTransactionEnvelope {
    pub route_id: RouteId,
    pub build_slot: u64,
    pub recent_blockhash: String,
    pub leg_plans: [AtomicLegPlan; 2],
    pub instructions: Vec<InstructionTemplate>,
    pub message_bytes: Vec<u8>,
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
    pub jito_tip_lamports: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildStatus {
    Built,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildRejectionReason {
    MissingBlockhash,
    KillSwitchActive,
    UnsupportedRouteShape,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildResult {
    pub status: BuildStatus,
    pub envelope: Option<UnsignedTransactionEnvelope>,
    pub rejection: Option<BuildRejectionReason>,
}
