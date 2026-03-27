use std::{collections::HashMap, str::FromStr};

use bincode::serialize;
use domain::quote_models::{derive_orca_tick_arrays, derive_raydium_tick_arrays};
use solana_sdk::{
    hash::{Hash, hashv},
    instruction::{AccountMeta, Instruction},
    message::{AddressLookupTableAccount, Message, VersionedMessage, v0},
    pubkey::Pubkey,
};
use solana_system_interface::instruction;

use crate::{
    execution::{
        ExecutionRegistry, MessageMode, OrcaSimplePoolConfig, OrcaWhirlpoolConfig,
        RaydiumClmmConfig, RaydiumSimplePoolConfig, RouteExecutionConfig, VenueExecutionConfig,
    },
    templates::AtomicTwoLegTemplate,
    types::{
        AccountSource, AtomicLegPlan, BuildRejectionReason, BuildRequest, BuildResult, BuildStatus,
        DynamicBuildParameters, InstructionAccount, InstructionTemplate, MessageFormat,
        ResolvedAddressLookupTable, SwapAmountMode, UnsignedTransactionEnvelope,
    },
};

const BASE_FEE_LAMPORTS_PER_SIGNATURE: u64 = 5_000;
const MICRO_LAMPORTS_PER_LAMPORT: u64 = 1_000_000;
const DEFAULT_JITO_TIP_ACCOUNT: &str = "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5";
const COMPUTE_BUDGET_PROGRAM_ID: &str = "ComputeBudget111111111111111111111111111111";
const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const ORCA_SWAP_INSTRUCTION_TAG: u8 = 1;
const RAYDIUM_SWAP_BASE_IN_TAG: u8 = 9;
const RAYDIUM_SWAP_BASE_OUT_TAG: u8 = 11;

pub trait TransactionBuilder: Send + Sync {
    fn build(&self, request: BuildRequest) -> BuildResult;
}

#[derive(Debug, Clone)]
pub struct AtomicArbTransactionBuilder {
    template: AtomicTwoLegTemplate,
    execution_registry: ExecutionRegistry,
}

impl Default for AtomicArbTransactionBuilder {
    fn default() -> Self {
        Self::new(ExecutionRegistry::default())
    }
}

impl AtomicArbTransactionBuilder {
    pub fn new(execution_registry: ExecutionRegistry) -> Self {
        Self {
            template: AtomicTwoLegTemplate::default(),
            execution_registry,
        }
    }
}

impl TransactionBuilder for AtomicArbTransactionBuilder {
    fn build(&self, request: BuildRequest) -> BuildResult {
        let Some(route_execution) = self.execution_registry.get(&request.candidate.route_id) else {
            return rejected(BuildRejectionReason::MissingRouteExecution);
        };

        if request.dynamic.fee_payer_pubkey.is_empty() {
            return rejected(BuildRejectionReason::MissingFeePayer);
        }
        if request.dynamic.recent_blockhash.is_empty() {
            return rejected(BuildRejectionReason::MissingBlockhash);
        }

        let fee_payer = match Pubkey::from_str(&request.dynamic.fee_payer_pubkey) {
            Ok(pubkey) => pubkey,
            Err(_) => return rejected(BuildRejectionReason::MissingFeePayer),
        };
        let recent_blockhash = match Hash::from_str(&request.dynamic.recent_blockhash) {
            Ok(blockhash) => blockhash,
            Err(_) => return rejected(BuildRejectionReason::MissingBlockhash),
        };

        if request
            .dynamic
            .head_slot
            .saturating_sub(request.candidate.oldest_relevant_snapshot_slot())
            > route_execution.max_quote_slot_lag
        {
            return rejected(BuildRejectionReason::QuoteStaleForExecution);
        }

        let leg_plans = self
            .template
            .materialize_leg_plans(&request.candidate, route_execution);
        if !route_matches_execution(route_execution, &leg_plans) {
            return rejected(BuildRejectionReason::UnsupportedVenue);
        }

        let route_lookup_tables =
            match resolve_route_lookup_tables(route_execution, &request.dynamic) {
                Ok(lookup_tables) => lookup_tables,
                Err(reason) => return rejected(reason),
            };

        let compute_unit_limit = effective_u32(
            request.dynamic.compute_unit_limit,
            route_execution.default_compute_unit_limit,
        );
        let compute_unit_price_micro_lamports = effective_u64(
            request.dynamic.compute_unit_price_micro_lamports,
            route_execution.default_compute_unit_price_micro_lamports,
        );
        let jito_tip_lamports = effective_u64(
            request.dynamic.jito_tip_lamports,
            route_execution.default_jito_tip_lamports,
        );

        let mut runtime_instructions = vec![
            compute_budget_set_compute_unit_limit(compute_unit_limit),
            compute_budget_set_compute_unit_price(compute_unit_price_micro_lamports),
        ];
        for (index, leg_plan) in leg_plans.iter().enumerate() {
            let instruction =
                match compile_leg_instruction(&route_execution.legs[index], fee_payer, leg_plan) {
                    Ok(instruction) => instruction,
                    Err(reason) => return rejected(reason),
                };
            runtime_instructions.push(instruction);
        }
        if jito_tip_lamports > 0 {
            let tip_account = parse_static_pubkey(DEFAULT_JITO_TIP_ACCOUNT);
            runtime_instructions.push(instruction::transfer(
                &fee_payer,
                &tip_account,
                jito_tip_lamports,
            ));
        }

        let (versioned_message, message_format) = match compile_message(
            route_execution,
            fee_payer,
            &runtime_instructions,
            &route_lookup_tables,
            recent_blockhash,
        ) {
            Ok(compiled) => compiled,
            Err(reason) => return rejected(reason),
        };
        let compiled_message_bytes = serialize(&versioned_message)
            .expect("versioned Solana message should serialize deterministically");

        let resolved_lookup_tables =
            used_lookup_tables(&versioned_message, &route_lookup_tables).unwrap_or_default();

        let mut instructions = Vec::with_capacity(runtime_instructions.len());
        instructions.push(describe_instruction(
            "compute-budget-limit",
            &runtime_instructions[0],
            &resolved_lookup_tables,
        ));
        instructions.push(describe_instruction(
            "compute-budget-price",
            &runtime_instructions[1],
            &resolved_lookup_tables,
        ));
        instructions.push(describe_instruction(
            &format!("{}-leg-1", request.candidate.route_id.0),
            &runtime_instructions[2],
            &resolved_lookup_tables,
        ));
        instructions.push(describe_instruction(
            &format!("{}-leg-2", request.candidate.route_id.0),
            &runtime_instructions[3],
            &resolved_lookup_tables,
        ));
        if let Some(tip_instruction) = runtime_instructions.get(4) {
            instructions.push(describe_instruction(
                "jito-tip",
                tip_instruction,
                &resolved_lookup_tables,
            ));
        }

        let priority_fee_lamports =
            priority_fee_lamports(compute_unit_limit, compute_unit_price_micro_lamports);
        let estimated_network_fee_lamports =
            BASE_FEE_LAMPORTS_PER_SIGNATURE + priority_fee_lamports;
        let estimated_total_cost_lamports = estimated_network_fee_lamports + jito_tip_lamports;

        BuildResult {
            status: BuildStatus::Built,
            envelope: Some(UnsignedTransactionEnvelope {
                route_id: request.candidate.route_id,
                build_slot: request.candidate.quoted_slot,
                recent_blockhash: request.dynamic.recent_blockhash,
                fee_payer_pubkey: request.dynamic.fee_payer_pubkey,
                leg_plans,
                instructions,
                resolved_lookup_tables,
                compiled_message_bytes,
                message_format,
                compute_unit_limit,
                compute_unit_price_micro_lamports,
                base_fee_lamports: BASE_FEE_LAMPORTS_PER_SIGNATURE,
                priority_fee_lamports,
                estimated_network_fee_lamports,
                estimated_total_cost_lamports,
                jito_tip_lamports,
            }),
            rejection: None,
        }
    }
}

fn rejected(reason: BuildRejectionReason) -> BuildResult {
    BuildResult {
        status: BuildStatus::Rejected,
        envelope: None,
        rejection: Some(reason),
    }
}

fn route_matches_execution(
    route_execution: &RouteExecutionConfig,
    leg_plans: &[AtomicLegPlan; 2],
) -> bool {
    route_execution
        .legs
        .iter()
        .zip(leg_plans.iter())
        .all(|(execution, leg)| execution.venue_name().eq_ignore_ascii_case(&leg.venue))
}

fn resolve_route_lookup_tables(
    route_execution: &RouteExecutionConfig,
    dynamic: &DynamicBuildParameters,
) -> Result<Vec<RouteLookupTable>, BuildRejectionReason> {
    let available_tables = dynamic
        .resolved_lookup_tables
        .iter()
        .map(|table| (table.account_key.as_str(), table))
        .collect::<HashMap<_, _>>();
    let mut route_tables = Vec::with_capacity(route_execution.lookup_tables.len());
    for config in &route_execution.lookup_tables {
        let Some(snapshot) = available_tables.get(config.account_key.as_str()) else {
            return Err(BuildRejectionReason::MissingLookupTable);
        };
        if dynamic.head_slot.saturating_sub(snapshot.fetched_slot)
            > route_execution.max_alt_slot_lag
        {
            return Err(BuildRejectionReason::LookupTableStale);
        }
        let key = parse_pubkey(&snapshot.account_key)?;
        let addresses = snapshot
            .addresses
            .iter()
            .map(|address| parse_pubkey(address))
            .collect::<Result<Vec<_>, _>>()?;
        route_tables.push(RouteLookupTable {
            account: AddressLookupTableAccount { key, addresses },
            metadata: ResolvedAddressLookupTable {
                account_key: snapshot.account_key.clone(),
                addresses: snapshot.addresses.clone(),
                writable_indexes: Vec::new(),
                readonly_indexes: Vec::new(),
                last_extended_slot: snapshot.last_extended_slot,
                fetched_slot: snapshot.fetched_slot,
            },
        });
    }
    Ok(route_tables)
}

fn compile_leg_instruction(
    execution: &VenueExecutionConfig,
    fee_payer: Pubkey,
    leg_plan: &AtomicLegPlan,
) -> Result<Instruction, BuildRejectionReason> {
    match execution {
        VenueExecutionConfig::OrcaSimplePool(config) => {
            compile_orca_simple_pool(config, fee_payer, leg_plan)
        }
        VenueExecutionConfig::OrcaWhirlpool(config) => {
            compile_orca_whirlpool(config, fee_payer, leg_plan)
        }
        VenueExecutionConfig::RaydiumSimplePool(config) => {
            compile_raydium_simple_pool(config, fee_payer, leg_plan)
        }
        VenueExecutionConfig::RaydiumClmm(config) => {
            compile_raydium_clmm(config, fee_payer, leg_plan)
        }
    }
}

fn compile_orca_simple_pool(
    config: &OrcaSimplePoolConfig,
    fee_payer: Pubkey,
    leg_plan: &AtomicLegPlan,
) -> Result<Instruction, BuildRejectionReason> {
    if leg_plan.amount_mode != SwapAmountMode::ExactIn {
        return Err(BuildRejectionReason::UnsupportedVenue);
    }
    let mut data = Vec::with_capacity(17);
    data.push(ORCA_SWAP_INSTRUCTION_TAG);
    data.extend_from_slice(&leg_plan.specified_amount.to_le_bytes());
    data.extend_from_slice(&leg_plan.other_amount_threshold.to_le_bytes());

    let mut accounts = vec![
        AccountMeta::new_readonly(parse_pubkey(&config.swap_account)?, false),
        AccountMeta::new_readonly(parse_pubkey(&config.authority)?, false),
        AccountMeta::new_readonly(fee_payer, true),
        AccountMeta::new(parse_pubkey(&config.user_source_token_account)?, false),
        AccountMeta::new(parse_pubkey(&config.pool_source_token_account)?, false),
        AccountMeta::new(parse_pubkey(&config.pool_destination_token_account)?, false),
        AccountMeta::new(parse_pubkey(&config.user_destination_token_account)?, false),
        AccountMeta::new(parse_pubkey(&config.pool_mint)?, false),
        AccountMeta::new(parse_pubkey(&config.fee_account)?, false),
        AccountMeta::new_readonly(parse_pubkey(&config.token_program_id)?, false),
    ];
    if let Some(host_fee_account) = &config.host_fee_account {
        accounts.push(AccountMeta::new(parse_pubkey(host_fee_account)?, false));
    }

    Ok(Instruction {
        program_id: parse_pubkey(&config.program_id)?,
        accounts,
        data,
    })
}

fn compile_raydium_simple_pool(
    config: &RaydiumSimplePoolConfig,
    fee_payer: Pubkey,
    leg_plan: &AtomicLegPlan,
) -> Result<Instruction, BuildRejectionReason> {
    let mut data = Vec::with_capacity(17);
    let tag = match leg_plan.amount_mode {
        SwapAmountMode::ExactIn => RAYDIUM_SWAP_BASE_IN_TAG,
        SwapAmountMode::ExactOut
            if leg_plan.side == strategy::route_registry::SwapSide::BuyBase =>
        {
            RAYDIUM_SWAP_BASE_OUT_TAG
        }
        SwapAmountMode::ExactOut => return Err(BuildRejectionReason::UnsupportedVenue),
    };
    data.push(tag);
    data.extend_from_slice(&leg_plan.specified_amount.to_le_bytes());
    data.extend_from_slice(&leg_plan.other_amount_threshold.to_le_bytes());

    Ok(Instruction {
        program_id: parse_pubkey(&config.program_id)?,
        accounts: vec![
            AccountMeta::new_readonly(parse_pubkey(&config.token_program_id)?, false),
            AccountMeta::new(parse_pubkey(&config.amm_pool)?, false),
            AccountMeta::new_readonly(parse_pubkey(&config.amm_authority)?, false),
            AccountMeta::new(parse_pubkey(&config.amm_open_orders)?, false),
            AccountMeta::new(parse_pubkey(&config.amm_coin_vault)?, false),
            AccountMeta::new(parse_pubkey(&config.amm_pc_vault)?, false),
            AccountMeta::new_readonly(parse_pubkey(&config.market_program)?, false),
            AccountMeta::new(parse_pubkey(&config.market)?, false),
            AccountMeta::new(parse_pubkey(&config.market_bids)?, false),
            AccountMeta::new(parse_pubkey(&config.market_asks)?, false),
            AccountMeta::new(parse_pubkey(&config.market_event_queue)?, false),
            AccountMeta::new(parse_pubkey(&config.market_coin_vault)?, false),
            AccountMeta::new(parse_pubkey(&config.market_pc_vault)?, false),
            AccountMeta::new_readonly(parse_pubkey(&config.market_vault_signer)?, false),
            AccountMeta::new(parse_pubkey(&config.user_source_token_account)?, false),
            AccountMeta::new(parse_pubkey(&config.user_destination_token_account)?, false),
            AccountMeta::new_readonly(fee_payer, true),
        ],
        data,
    })
}

fn compile_orca_whirlpool(
    config: &OrcaWhirlpoolConfig,
    fee_payer: Pubkey,
    leg_plan: &AtomicLegPlan,
) -> Result<Instruction, BuildRejectionReason> {
    let current_tick_index = leg_plan
        .current_tick_index
        .ok_or(BuildRejectionReason::MissingExecutionHint)?;
    let token_program_id = parse_pubkey(&config.token_program_id)?;
    let token_mint_a = parse_pubkey(&config.token_mint_a)?;
    let token_mint_b = parse_pubkey(&config.token_mint_b)?;
    let whirlpool = parse_pubkey(&config.whirlpool)?;
    let program_id = parse_pubkey(&config.program_id)?;
    let user_token_account_a =
        associated_token_address(&fee_payer, &token_mint_a, &token_program_id);
    let user_token_account_b =
        associated_token_address(&fee_payer, &token_mint_b, &token_program_id);
    let tick_arrays = derive_orca_tick_arrays(
        program_id,
        whirlpool,
        config.tick_spacing,
        current_tick_index,
        config.a_to_b,
    );
    let oracle = derive_orca_oracle(program_id, whirlpool);

    let mut data = anchor_instruction_data("swap");
    data.extend_from_slice(&leg_plan.specified_amount.to_le_bytes());
    data.extend_from_slice(&leg_plan.other_amount_threshold.to_le_bytes());
    data.extend_from_slice(&0u128.to_le_bytes());
    data.push(u8::from(leg_plan.amount_mode == SwapAmountMode::ExactIn));
    data.push(u8::from(config.a_to_b));

    Ok(Instruction {
        program_id,
        accounts: vec![
            AccountMeta::new_readonly(token_program_id, false),
            AccountMeta::new_readonly(fee_payer, true),
            AccountMeta::new(whirlpool, false),
            AccountMeta::new(user_token_account_a, false),
            AccountMeta::new(parse_pubkey(&config.token_vault_a)?, false),
            AccountMeta::new(user_token_account_b, false),
            AccountMeta::new(parse_pubkey(&config.token_vault_b)?, false),
            AccountMeta::new(tick_arrays[0], false),
            AccountMeta::new(tick_arrays[1], false),
            AccountMeta::new(tick_arrays[2], false),
            AccountMeta::new_readonly(oracle, false),
        ],
        data,
    })
}

fn compile_raydium_clmm(
    config: &RaydiumClmmConfig,
    fee_payer: Pubkey,
    leg_plan: &AtomicLegPlan,
) -> Result<Instruction, BuildRejectionReason> {
    let current_tick_index = leg_plan
        .current_tick_index
        .ok_or(BuildRejectionReason::MissingExecutionHint)?;
    let token_program_id = parse_pubkey(&config.token_program_id)?;
    let token_mint_0 = parse_pubkey(&config.token_mint_0)?;
    let token_mint_1 = parse_pubkey(&config.token_mint_1)?;
    let pool_state = parse_pubkey(&config.pool_state)?;
    let program_id = parse_pubkey(&config.program_id)?;
    let user_token_account_0 =
        associated_token_address(&fee_payer, &token_mint_0, &token_program_id);
    let user_token_account_1 =
        associated_token_address(&fee_payer, &token_mint_1, &token_program_id);
    let (
        input_token_account,
        output_token_account,
        input_vault,
        output_vault,
        input_mint,
        output_mint,
    ) = if config.zero_for_one {
        (
            user_token_account_0,
            user_token_account_1,
            parse_pubkey(&config.token_vault_0)?,
            parse_pubkey(&config.token_vault_1)?,
            token_mint_0,
            token_mint_1,
        )
    } else {
        (
            user_token_account_1,
            user_token_account_0,
            parse_pubkey(&config.token_vault_1)?,
            parse_pubkey(&config.token_vault_0)?,
            token_mint_1,
            token_mint_0,
        )
    };
    let tick_arrays = derive_raydium_tick_arrays(
        program_id,
        pool_state,
        config.tick_spacing,
        current_tick_index,
        config.zero_for_one,
    );

    let mut data = anchor_instruction_data("swap_v2");
    data.extend_from_slice(&leg_plan.specified_amount.to_le_bytes());
    data.extend_from_slice(&leg_plan.other_amount_threshold.to_le_bytes());
    data.extend_from_slice(&0u128.to_le_bytes());
    data.push(u8::from(leg_plan.amount_mode == SwapAmountMode::ExactIn));

    let mut accounts = vec![
        AccountMeta::new_readonly(fee_payer, true),
        AccountMeta::new_readonly(parse_pubkey(&config.amm_config)?, false),
        AccountMeta::new(pool_state, false),
        AccountMeta::new(input_token_account, false),
        AccountMeta::new(output_token_account, false),
        AccountMeta::new(input_vault, false),
        AccountMeta::new(output_vault, false),
        AccountMeta::new(parse_pubkey(&config.observation_state)?, false),
        AccountMeta::new_readonly(token_program_id, false),
        AccountMeta::new_readonly(parse_pubkey(&config.token_program_2022_id)?, false),
        AccountMeta::new_readonly(parse_pubkey(&config.memo_program_id)?, false),
        AccountMeta::new_readonly(input_mint, false),
        AccountMeta::new_readonly(output_mint, false),
    ];
    if let Some(ex_bitmap_account) = &config.ex_bitmap_account {
        accounts.push(AccountMeta::new(parse_pubkey(ex_bitmap_account)?, false));
    }
    for tick_array in tick_arrays {
        accounts.push(AccountMeta::new(tick_array, false));
    }

    Ok(Instruction {
        program_id,
        accounts,
        data,
    })
}

fn compile_message(
    route_execution: &RouteExecutionConfig,
    fee_payer: Pubkey,
    runtime_instructions: &[Instruction],
    route_lookup_tables: &[RouteLookupTable],
    recent_blockhash: Hash,
) -> Result<(VersionedMessage, MessageFormat), BuildRejectionReason> {
    let lookup_accounts = route_lookup_tables
        .iter()
        .map(|table| table.account.clone())
        .collect::<Vec<_>>();
    let v0_message = v0::Message::try_compile(
        &fee_payer,
        runtime_instructions,
        &lookup_accounts,
        recent_blockhash,
    );

    match route_execution.message_mode {
        MessageMode::V0Required => v0_message
            .map(|message| (VersionedMessage::V0(message), MessageFormat::V0))
            .map_err(|_| BuildRejectionReason::MessageCompilationFailed),
        MessageMode::V0OrLegacy => {
            if lookup_accounts.is_empty() {
                let legacy_message = Message::new_with_blockhash(
                    runtime_instructions,
                    Some(&fee_payer),
                    &recent_blockhash,
                );
                return Ok((
                    VersionedMessage::Legacy(legacy_message),
                    MessageFormat::Legacy,
                ));
            }
            if let Ok(message) = v0_message {
                return Ok((VersionedMessage::V0(message), MessageFormat::V0));
            }
            let legacy_message = Message::new_with_blockhash(
                runtime_instructions,
                Some(&fee_payer),
                &recent_blockhash,
            );
            Ok((
                VersionedMessage::Legacy(legacy_message),
                MessageFormat::Legacy,
            ))
        }
    }
}

fn used_lookup_tables(
    versioned_message: &VersionedMessage,
    route_lookup_tables: &[RouteLookupTable],
) -> Result<Vec<ResolvedAddressLookupTable>, BuildRejectionReason> {
    let VersionedMessage::V0(message) = versioned_message else {
        return Ok(Vec::new());
    };

    let route_lookup_tables = route_lookup_tables
        .iter()
        .map(|table| (table.account.key, &table.metadata))
        .collect::<HashMap<_, _>>();

    message
        .address_table_lookups
        .iter()
        .map(|lookup| {
            let Some(metadata) = route_lookup_tables.get(&lookup.account_key) else {
                return Err(BuildRejectionReason::MessageCompilationFailed);
            };
            Ok(ResolvedAddressLookupTable {
                account_key: metadata.account_key.clone(),
                addresses: metadata.addresses.clone(),
                writable_indexes: lookup.writable_indexes.clone(),
                readonly_indexes: lookup.readonly_indexes.clone(),
                last_extended_slot: metadata.last_extended_slot,
                fetched_slot: metadata.fetched_slot,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use solana_sdk::{hash::hashv, pubkey::Pubkey};

    use crate::types::SwapAmountMode;

    use super::*;

    fn test_pubkey(label: &str) -> String {
        Pubkey::new_from_array(hashv(&[label.as_bytes()]).to_bytes()).to_string()
    }

    fn read_u64(data: &[u8], offset: usize) -> u64 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&data[offset..offset + 8]);
        u64::from_le_bytes(bytes)
    }

    #[test]
    fn orca_whirlpool_exact_out_instruction_sets_exact_out_flag() {
        let config = OrcaWhirlpoolConfig {
            program_id: test_pubkey("orca-program"),
            token_program_id: test_pubkey("spl-token-program"),
            whirlpool: test_pubkey("whirlpool"),
            token_mint_a: test_pubkey("mint-a"),
            token_vault_a: test_pubkey("vault-a"),
            token_mint_b: test_pubkey("mint-b"),
            token_vault_b: test_pubkey("vault-b"),
            tick_spacing: 64,
            a_to_b: true,
        };
        let fee_payer = parse_static_pubkey(&test_pubkey("fee-payer"));
        let instruction = compile_orca_whirlpool(
            &config,
            fee_payer,
            &AtomicLegPlan {
                venue: "orca_whirlpool".into(),
                pool_id: domain::PoolId("pool-a".into()),
                side: strategy::route_registry::SwapSide::BuyBase,
                amount_mode: SwapAmountMode::ExactOut,
                specified_amount: 123,
                other_amount_threshold: 456,
                current_tick_index: Some(0),
            },
        )
        .expect("orca whirlpool exact-out instruction");

        assert_eq!(read_u64(&instruction.data, 8), 123);
        assert_eq!(read_u64(&instruction.data, 16), 456);
        assert_eq!(instruction.data[40], 0);
        assert_eq!(instruction.data[41], 1);
    }

    #[test]
    fn raydium_clmm_exact_out_instruction_sets_exact_out_flag() {
        let config = RaydiumClmmConfig {
            program_id: test_pubkey("raydium-clmm-program"),
            token_program_id: test_pubkey("spl-token-program"),
            token_program_2022_id: test_pubkey("spl-token-2022"),
            memo_program_id: test_pubkey("memo-program"),
            pool_state: test_pubkey("pool-state"),
            amm_config: test_pubkey("amm-config"),
            observation_state: test_pubkey("observation-state"),
            ex_bitmap_account: None,
            token_mint_0: test_pubkey("mint-0"),
            token_vault_0: test_pubkey("vault-0"),
            token_mint_1: test_pubkey("mint-1"),
            token_vault_1: test_pubkey("vault-1"),
            tick_spacing: 60,
            zero_for_one: true,
        };
        let fee_payer = parse_static_pubkey(&test_pubkey("fee-payer"));
        let instruction = compile_raydium_clmm(
            &config,
            fee_payer,
            &AtomicLegPlan {
                venue: "raydium_clmm".into(),
                pool_id: domain::PoolId("pool-b".into()),
                side: strategy::route_registry::SwapSide::BuyBase,
                amount_mode: SwapAmountMode::ExactOut,
                specified_amount: 321,
                other_amount_threshold: 654,
                current_tick_index: Some(0),
            },
        )
        .expect("raydium clmm exact-out instruction");

        assert_eq!(read_u64(&instruction.data, 8), 321);
        assert_eq!(read_u64(&instruction.data, 16), 654);
        assert_eq!(instruction.data[40], 0);
    }

    #[test]
    fn raydium_simple_buy_base_exact_out_uses_base_out_tag() {
        let config = RaydiumSimplePoolConfig {
            program_id: test_pubkey("raydium-program"),
            token_program_id: test_pubkey("spl-token-program"),
            amm_pool: test_pubkey("amm-pool"),
            amm_authority: test_pubkey("amm-authority"),
            amm_open_orders: test_pubkey("amm-open-orders"),
            amm_coin_vault: test_pubkey("amm-coin-vault"),
            amm_pc_vault: test_pubkey("amm-pc-vault"),
            market_program: test_pubkey("market-program"),
            market: test_pubkey("market"),
            market_bids: test_pubkey("market-bids"),
            market_asks: test_pubkey("market-asks"),
            market_event_queue: test_pubkey("market-event-queue"),
            market_coin_vault: test_pubkey("market-coin-vault"),
            market_pc_vault: test_pubkey("market-pc-vault"),
            market_vault_signer: test_pubkey("market-vault-signer"),
            user_source_token_account: test_pubkey("user-source"),
            user_destination_token_account: test_pubkey("user-destination"),
        };
        let fee_payer = parse_static_pubkey(&test_pubkey("fee-payer"));
        let instruction = compile_raydium_simple_pool(
            &config,
            fee_payer,
            &AtomicLegPlan {
                venue: "raydium".into(),
                pool_id: domain::PoolId("pool-c".into()),
                side: strategy::route_registry::SwapSide::BuyBase,
                amount_mode: SwapAmountMode::ExactOut,
                specified_amount: 777,
                other_amount_threshold: 888,
                current_tick_index: None,
            },
        )
        .expect("raydium simple exact-out instruction");

        assert_eq!(instruction.data[0], RAYDIUM_SWAP_BASE_OUT_TAG);
        assert_eq!(read_u64(&instruction.data, 1), 777);
        assert_eq!(read_u64(&instruction.data, 9), 888);
    }
}

fn parse_pubkey(value: &str) -> Result<Pubkey, BuildRejectionReason> {
    Pubkey::from_str(value).map_err(|_| BuildRejectionReason::MessageCompilationFailed)
}

fn parse_static_pubkey(value: &str) -> Pubkey {
    Pubkey::from_str(value).expect("static Solana address should parse")
}

fn anchor_instruction_data(name: &str) -> Vec<u8> {
    let preimage = format!("global:{name}");
    let hash = hashv(&[preimage.as_bytes()]);
    hash.to_bytes()[..8].to_vec()
}

fn associated_token_address(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), token_program.as_ref(), mint.as_ref()],
        &parse_static_pubkey(ASSOCIATED_TOKEN_PROGRAM_ID),
    )
    .0
}

fn derive_orca_oracle(program_id: Pubkey, whirlpool: Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"oracle", whirlpool.as_ref()], &program_id).0
}

fn effective_u32(value: u32, default_value: u32) -> u32 {
    if value == 0 { default_value } else { value }
}

fn effective_u64(value: u64, default_value: u64) -> u64 {
    if value == 0 { default_value } else { value }
}

fn priority_fee_lamports(compute_unit_limit: u32, compute_unit_price_micro_lamports: u64) -> u64 {
    (compute_unit_limit as u64)
        .saturating_mul(compute_unit_price_micro_lamports)
        .saturating_add(MICRO_LAMPORTS_PER_LAMPORT - 1)
        / MICRO_LAMPORTS_PER_LAMPORT
}

fn compute_budget_set_compute_unit_limit(units: u32) -> Instruction {
    let mut data = Vec::with_capacity(5);
    data.push(2);
    data.extend_from_slice(&units.to_le_bytes());

    Instruction {
        program_id: parse_static_pubkey(COMPUTE_BUDGET_PROGRAM_ID),
        accounts: vec![],
        data,
    }
}

fn compute_budget_set_compute_unit_price(micro_lamports: u64) -> Instruction {
    let mut data = Vec::with_capacity(9);
    data.push(3);
    data.extend_from_slice(&micro_lamports.to_le_bytes());

    Instruction {
        program_id: parse_static_pubkey(COMPUTE_BUDGET_PROGRAM_ID),
        accounts: vec![],
        data,
    }
}

fn describe_instruction(
    label: &str,
    instruction: &Instruction,
    lookup_tables: &[ResolvedAddressLookupTable],
) -> InstructionTemplate {
    InstructionTemplate {
        label: label.into(),
        program_id: instruction.program_id.to_string(),
        accounts: instruction
            .accounts
            .iter()
            .map(|account| InstructionAccount {
                pubkey: account.pubkey.to_string(),
                is_signer: account.is_signer,
                is_writable: account.is_writable,
                source: account_source(account.pubkey, account.is_signer, lookup_tables),
            })
            .collect(),
        data: instruction.data.clone(),
    }
}

fn account_source(
    pubkey: Pubkey,
    is_signer: bool,
    lookup_tables: &[ResolvedAddressLookupTable],
) -> AccountSource {
    if is_signer {
        return AccountSource::Static;
    }
    for lookup_table in lookup_tables {
        if let Some(index) = lookup_table
            .addresses
            .iter()
            .position(|address| address == &pubkey.to_string())
            .and_then(|index| u8::try_from(index).ok())
        {
            return AccountSource::LookupTable {
                account_key: lookup_table.account_key.clone(),
                index,
            };
        }
    }
    AccountSource::Static
}

#[derive(Debug, Clone)]
struct RouteLookupTable {
    account: AddressLookupTableAccount,
    metadata: ResolvedAddressLookupTable,
}
