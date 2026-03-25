use crate::types::ExecutionStateSnapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionState {
    latest_blockhash: Option<String>,
    blockhash_slot: Option<u64>,
    alt_revision: u64,
    wallet_balance_lamports: u64,
    wallet_ready: bool,
    kill_switch_enabled: bool,
}

impl Default for ExecutionState {
    fn default() -> Self {
        Self {
            latest_blockhash: None,
            blockhash_slot: None,
            alt_revision: 0,
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
            latest_blockhash: self.latest_blockhash.clone(),
            blockhash_slot: self.blockhash_slot,
            alt_revision: self.alt_revision,
            wallet_balance_lamports: self.wallet_balance_lamports,
            wallet_ready: self.wallet_ready,
            kill_switch_enabled: self.kill_switch_enabled,
        }
    }

    pub fn set_blockhash(&mut self, blockhash: impl Into<String>, slot: u64) {
        self.latest_blockhash = Some(blockhash.into());
        self.blockhash_slot = Some(slot);
    }

    pub fn set_wallet_state(&mut self, balance_lamports: u64, ready: bool) {
        self.wallet_balance_lamports = balance_lamports;
        self.wallet_ready = ready;
    }

    pub fn set_alt_revision(&mut self, revision: u64) {
        self.alt_revision = revision;
    }

    pub fn set_kill_switch(&mut self, enabled: bool) {
        self.kill_switch_enabled = enabled;
    }
}
