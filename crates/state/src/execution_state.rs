use crate::types::{ExecutionStateSnapshot, LookupTableSnapshot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionState {
    rpc_slot: Option<u64>,
    latest_blockhash: Option<String>,
    blockhash_slot: Option<u64>,
    alt_revision: u64,
    lookup_tables: Vec<LookupTableSnapshot>,
    wallet_balance_lamports: u64,
    wallet_ready: bool,
    kill_switch_enabled: bool,
}

impl Default for ExecutionState {
    fn default() -> Self {
        Self {
            rpc_slot: None,
            latest_blockhash: None,
            blockhash_slot: None,
            alt_revision: 0,
            lookup_tables: Vec::new(),
            wallet_balance_lamports: 0,
            wallet_ready: false,
            kill_switch_enabled: false,
        }
    }
}

impl ExecutionState {
    pub fn snapshot(&self, head_slot: u64) -> ExecutionStateSnapshot {
        ExecutionStateSnapshot {
            head_slot,
            rpc_slot: self.rpc_slot,
            latest_blockhash: self.latest_blockhash.clone(),
            blockhash_slot: self.blockhash_slot,
            alt_revision: self.alt_revision,
            lookup_tables: self.lookup_tables.clone(),
            wallet_balance_lamports: self.wallet_balance_lamports,
            wallet_ready: self.wallet_ready,
            kill_switch_enabled: self.kill_switch_enabled,
        }
    }

    pub fn set_blockhash(&mut self, blockhash: impl Into<String>, slot: u64) {
        self.latest_blockhash = Some(blockhash.into());
        self.blockhash_slot = Some(slot);
    }

    pub fn set_rpc_slot(&mut self, slot: u64) {
        self.rpc_slot = Some(self.rpc_slot.unwrap_or(0).max(slot));
    }

    pub fn set_wallet_state(&mut self, balance_lamports: u64, ready: bool) {
        self.wallet_balance_lamports = balance_lamports;
        self.wallet_ready = ready;
    }

    pub fn set_alt_revision(&mut self, revision: u64) {
        self.alt_revision = revision;
    }

    pub fn set_lookup_tables(&mut self, lookup_tables: Vec<LookupTableSnapshot>) {
        self.lookup_tables = lookup_tables;
    }

    pub fn set_kill_switch(&mut self, enabled: bool) {
        self.kill_switch_enabled = enabled;
    }
}
