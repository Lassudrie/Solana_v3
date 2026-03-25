#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletStatus {
    Ready,
    Refreshing,
    MissingSigner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletPrecondition {
    Ok,
    WalletNotReady,
    InsufficientBalance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotWallet {
    pub wallet_id: String,
    pub owner_pubkey: String,
    pub balance_lamports: u64,
    pub status: WalletStatus,
}

impl HotWallet {
    pub fn precondition(&self) -> WalletPrecondition {
        match self.status {
            WalletStatus::Ready if self.balance_lamports > 0 => WalletPrecondition::Ok,
            WalletStatus::Ready => WalletPrecondition::InsufficientBalance,
            _ => WalletPrecondition::WalletNotReady,
        }
    }
}
