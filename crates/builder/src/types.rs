use state::types::RouteId;
use strategy::opportunity::OpportunityCandidate;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicBuildParameters {
    pub recent_blockhash: String,
    pub recent_blockhash_slot: Option<u64>,
    pub head_slot: u64,
    pub fee_payer_pubkey: String,
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
    pub jito_tip_lamports: u64,
    pub resolved_lookup_tables: Vec<state::types::LookupTableSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicLegPlan {
    pub venue: String,
    pub pool_id: state::types::PoolId,
    pub input_amount: u64,
    pub min_output_amount: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountSource {
    Static,
    LookupTable { account_key: String, index: u8 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionAccount {
    pub pubkey: String,
    pub is_signer: bool,
    pub is_writable: bool,
    pub source: AccountSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionTemplate {
    pub label: String,
    pub program_id: String,
    pub accounts: Vec<InstructionAccount>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAddressLookupTable {
    pub account_key: String,
    pub addresses: Vec<String>,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
    pub last_extended_slot: u64,
    pub fetched_slot: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageFormat {
    Legacy,
    V0,
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
    pub fee_payer_pubkey: String,
    pub leg_plans: [AtomicLegPlan; 2],
    pub instructions: Vec<InstructionTemplate>,
    pub resolved_lookup_tables: Vec<ResolvedAddressLookupTable>,
    pub compiled_message_bytes: Vec<u8>,
    pub message_format: MessageFormat,
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
    pub base_fee_lamports: u64,
    pub priority_fee_lamports: u64,
    pub estimated_network_fee_lamports: u64,
    pub estimated_total_cost_lamports: u64,
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
    MissingFeePayer,
    KillSwitchActive,
    UnsupportedRouteShape,
    MissingRouteExecution,
    MissingLookupTable,
    LookupTableStale,
    QuoteStaleForExecution,
    MessageCompilationFailed,
    UnsupportedVenue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildResult {
    pub status: BuildStatus,
    pub envelope: Option<UnsignedTransactionEnvelope>,
    pub rejection: Option<BuildRejectionReason>,
}
