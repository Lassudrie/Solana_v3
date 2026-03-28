pub mod execution;
pub mod templates;
pub mod transaction_builder;
pub mod types;

pub use execution::{
    ExecutionRegistry, LookupTableUsageConfig, MessageMode, OrcaSimplePoolConfig,
    OrcaWhirlpoolConfig, RaydiumClmmConfig, RaydiumSimplePoolConfig, RouteExecutionConfig,
    VenueExecutionConfig,
};
pub use templates::AtomicTwoLegTemplate;
pub use transaction_builder::{AtomicArbTransactionBuilder, TransactionBuilder};
pub use types::{
    AccountSource, AtomicLegPlan, BuildRejectionReason, BuildRequest, BuildResult, BuildStatus,
    DynamicBuildParameters, InstructionAccount, InstructionTemplate, MessageFormat,
    ResolvedAddressLookupTable, SwapAmountMode, UnsignedTransactionEnvelope,
};

#[cfg(test)]
mod tests {
    use bincode::deserialize;
    use domain::{LookupTableSnapshot, PoolId, RouteId};
    use solana_sdk::signer::{SeedDerivable, Signer as SolanaSigner, keypair::Keypair};
    use solana_sdk::{hash::hashv, message::VersionedMessage, pubkey::Pubkey};
    use strategy::{opportunity::OpportunityCandidate, quote::LegQuote, route_registry::SwapSide};

    use crate::{
        AtomicArbTransactionBuilder, BuildRejectionReason, ExecutionRegistry,
        LookupTableUsageConfig, MessageFormat, MessageMode, OrcaSimplePoolConfig,
        RaydiumSimplePoolConfig, RouteExecutionConfig, VenueExecutionConfig,
        transaction_builder::TransactionBuilder,
        types::{BuildRequest, BuildStatus, DynamicBuildParameters},
    };

    fn fee_payer_pubkey() -> String {
        let seed = hashv(&[b"builder-test-fee-payer"]).to_bytes();
        Keypair::from_seed(&seed)
            .expect("test seed should derive keypair")
            .pubkey()
            .to_string()
    }

    fn test_pubkey(label: &str) -> String {
        Pubkey::new_from_array(hashv(&[label.as_bytes()]).to_bytes()).to_string()
    }

    fn recent_blockhash() -> String {
        hashv(&[b"builder-test-blockhash"]).to_string()
    }

    fn candidate() -> OpportunityCandidate {
        OpportunityCandidate {
            route_id: RouteId("route-a".into()),
            quoted_slot: 42,
            leg_snapshot_slots: [42, 42],
            sol_quote_conversion_snapshot_slot: None,
            trade_size: 10_000,
            active_execution_buffer_bps: None,
            expected_net_output: 10_250,
            minimum_acceptable_output: 10_025,
            expected_gross_profit_quote_atoms: 250,
            estimated_execution_cost_lamports: 0,
            estimated_execution_cost_quote_atoms: 0,
            expected_net_profit_quote_atoms: 250,
            leg_quotes: [
                LegQuote {
                    venue: "orca".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    input_amount: 10_000,
                    output_amount: 10_120,
                    fee_paid: 5,
                    current_tick_index: None,
                },
                LegQuote {
                    venue: "raydium".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    input_amount: 10_120,
                    output_amount: 10_250,
                    fee_paid: 5,
                    current_tick_index: None,
                },
            ],
        }
    }

    fn execution_registry(message_mode: MessageMode, with_lookup_table: bool) -> ExecutionRegistry {
        let mut registry = ExecutionRegistry::default();
        registry.register(RouteExecutionConfig {
            route_id: RouteId("route-a".into()),
            message_mode,
            lookup_tables: if with_lookup_table {
                vec![LookupTableUsageConfig {
                    account_key: test_pubkey("route-alt"),
                }]
            } else {
                Vec::new()
            },
            default_compute_unit_limit: 300_000,
            default_compute_unit_price_micro_lamports: 25_000,
            default_jito_tip_lamports: 5_000,
            max_quote_slot_lag: 4,
            max_alt_slot_lag: 4,
            legs: [
                VenueExecutionConfig::OrcaSimplePool(OrcaSimplePoolConfig {
                    program_id: test_pubkey("orca-program"),
                    token_program_id: test_pubkey("spl-token-program"),
                    swap_account: test_pubkey("orca-swap"),
                    authority: test_pubkey("orca-authority"),
                    pool_source_token_account: test_pubkey("orca-pool-source"),
                    pool_destination_token_account: test_pubkey("orca-pool-destination"),
                    pool_mint: test_pubkey("orca-pool-mint"),
                    fee_account: test_pubkey("orca-fee-account"),
                    user_source_token_account: test_pubkey("route-input-ata"),
                    user_destination_token_account: test_pubkey("route-mid-ata"),
                    host_fee_account: None,
                }),
                VenueExecutionConfig::RaydiumSimplePool(RaydiumSimplePoolConfig {
                    program_id: test_pubkey("raydium-program"),
                    token_program_id: test_pubkey("spl-token-program"),
                    amm_pool: test_pubkey("raydium-amm-pool"),
                    amm_authority: test_pubkey("raydium-amm-authority"),
                    amm_open_orders: test_pubkey("raydium-open-orders"),
                    amm_coin_vault: test_pubkey("raydium-coin-vault"),
                    amm_pc_vault: test_pubkey("raydium-pc-vault"),
                    market_program: test_pubkey("serum-program"),
                    market: test_pubkey("serum-market"),
                    market_bids: test_pubkey("serum-bids"),
                    market_asks: test_pubkey("serum-asks"),
                    market_event_queue: test_pubkey("serum-event-queue"),
                    market_coin_vault: test_pubkey("serum-coin-vault"),
                    market_pc_vault: test_pubkey("serum-pc-vault"),
                    market_vault_signer: test_pubkey("serum-vault-signer"),
                    user_source_token_account: Some(test_pubkey("route-mid-ata")),
                    user_destination_token_account: Some(test_pubkey("route-output-ata")),
                    user_source_mint: None,
                    user_destination_mint: None,
                }),
            ],
        });
        registry
    }

    fn lookup_table_snapshot(fetched_slot: u64) -> LookupTableSnapshot {
        LookupTableSnapshot {
            account_key: test_pubkey("route-alt"),
            addresses: vec![
                test_pubkey("orca-swap"),
                test_pubkey("orca-authority"),
                test_pubkey("orca-pool-source"),
                test_pubkey("orca-pool-destination"),
                test_pubkey("orca-pool-mint"),
                test_pubkey("orca-fee-account"),
                test_pubkey("route-input-ata"),
                test_pubkey("route-mid-ata"),
                test_pubkey("raydium-amm-pool"),
                test_pubkey("raydium-amm-authority"),
                test_pubkey("raydium-open-orders"),
                test_pubkey("raydium-coin-vault"),
                test_pubkey("raydium-pc-vault"),
                test_pubkey("serum-market"),
                test_pubkey("serum-bids"),
                test_pubkey("serum-asks"),
                test_pubkey("serum-event-queue"),
                test_pubkey("serum-coin-vault"),
                test_pubkey("serum-pc-vault"),
                test_pubkey("serum-vault-signer"),
                test_pubkey("route-output-ata"),
            ],
            last_extended_slot: fetched_slot.saturating_sub(1),
            fetched_slot,
        }
    }

    fn dynamic_params(
        lookup_tables: Vec<LookupTableSnapshot>,
        head_slot: u64,
    ) -> DynamicBuildParameters {
        DynamicBuildParameters {
            recent_blockhash: recent_blockhash(),
            recent_blockhash_slot: Some(head_slot),
            head_slot,
            fee_payer_pubkey: fee_payer_pubkey(),
            compute_unit_limit: 300_000,
            compute_unit_price_micro_lamports: 25_000,
            jito_tip_lamports: 5_000,
            resolved_lookup_tables: lookup_tables,
        }
    }

    #[test]
    fn builder_returns_structured_v0_result_with_lookup_tables() {
        let builder =
            AtomicArbTransactionBuilder::new(execution_registry(MessageMode::V0Required, true));
        let result = builder.build(BuildRequest {
            candidate: candidate(),
            dynamic: dynamic_params(vec![lookup_table_snapshot(43)], 43),
        });

        assert_eq!(result.status, BuildStatus::Built);
        let envelope = result.envelope.expect("built envelope");
        assert_eq!(envelope.instructions.len(), 5);
        assert_eq!(envelope.message_format, MessageFormat::V0);
        assert_eq!(envelope.resolved_lookup_tables.len(), 1);
        assert!(!envelope.compiled_message_bytes.is_empty());
        assert_eq!(envelope.base_fee_lamports, 5_000);
        assert_eq!(envelope.route_id, RouteId("route-a".into()));
        let message: VersionedMessage =
            deserialize(&envelope.compiled_message_bytes).expect("versioned message");
        assert!(matches!(message, VersionedMessage::V0(_)));
        assert_eq!(
            envelope.instructions[2].program_id,
            test_pubkey("orca-program")
        );
        assert_eq!(
            envelope.instructions[3].program_id,
            test_pubkey("raydium-program")
        );
    }

    #[test]
    fn builder_supports_legacy_fallback_when_route_allows_it() {
        let builder =
            AtomicArbTransactionBuilder::new(execution_registry(MessageMode::V0OrLegacy, false));
        let result = builder.build(BuildRequest {
            candidate: candidate(),
            dynamic: dynamic_params(Vec::new(), 43),
        });

        assert_eq!(result.status, BuildStatus::Built);
        let envelope = result.envelope.expect("built envelope");
        assert_eq!(envelope.message_format, MessageFormat::Legacy);
        let message: VersionedMessage =
            deserialize(&envelope.compiled_message_bytes).expect("versioned message");
        assert!(matches!(message, VersionedMessage::Legacy(_)));
    }

    #[test]
    fn builder_rejects_missing_lookup_table() {
        let builder =
            AtomicArbTransactionBuilder::new(execution_registry(MessageMode::V0Required, true));
        let result = builder.build(BuildRequest {
            candidate: candidate(),
            dynamic: dynamic_params(Vec::new(), 43),
        });

        assert_eq!(result.status, BuildStatus::Rejected);
        assert_eq!(
            result.rejection,
            Some(BuildRejectionReason::MissingLookupTable)
        );
    }

    #[test]
    fn builder_rejects_stale_lookup_table() {
        let builder =
            AtomicArbTransactionBuilder::new(execution_registry(MessageMode::V0Required, true));
        let result = builder.build(BuildRequest {
            candidate: candidate(),
            dynamic: dynamic_params(vec![lookup_table_snapshot(37)], 43),
        });

        assert_eq!(result.status, BuildStatus::Rejected);
        assert_eq!(
            result.rejection,
            Some(BuildRejectionReason::LookupTableStale)
        );
    }

    #[test]
    fn builder_rejects_stale_leg_snapshot_even_when_quoted_slot_is_recent() {
        let builder =
            AtomicArbTransactionBuilder::new(execution_registry(MessageMode::V0OrLegacy, false));
        let mut candidate = candidate();
        candidate.quoted_slot = 43;
        candidate.leg_snapshot_slots = [38, 43];

        let result = builder.build(BuildRequest {
            candidate,
            dynamic: dynamic_params(Vec::new(), 43),
        });

        assert_eq!(result.status, BuildStatus::Rejected);
        assert_eq!(
            result.rejection,
            Some(BuildRejectionReason::QuoteStaleForExecution)
        );
    }

    #[test]
    fn builder_rejects_stale_conversion_snapshot_even_when_leg_snapshots_are_recent() {
        let builder =
            AtomicArbTransactionBuilder::new(execution_registry(MessageMode::V0OrLegacy, false));
        let mut candidate = candidate();
        candidate.quoted_slot = 43;
        candidate.leg_snapshot_slots = [43, 42];
        candidate.sol_quote_conversion_snapshot_slot = Some(38);

        let result = builder.build(BuildRequest {
            candidate,
            dynamic: dynamic_params(Vec::new(), 43),
        });

        assert_eq!(result.status, BuildStatus::Rejected);
        assert_eq!(
            result.rejection,
            Some(BuildRejectionReason::QuoteStaleForExecution)
        );
    }

    #[test]
    fn builder_rejects_route_without_execution_spec() {
        let builder = AtomicArbTransactionBuilder::default();
        let result = builder.build(BuildRequest {
            candidate: candidate(),
            dynamic: dynamic_params(Vec::new(), 43),
        });

        assert_eq!(result.status, BuildStatus::Rejected);
        assert_eq!(
            result.rejection,
            Some(BuildRejectionReason::MissingRouteExecution)
        );
    }
}
