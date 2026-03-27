pub mod protocol;
pub mod signer;
pub mod wallet;

use std::time::SystemTime;

use builder::UnsignedTransactionEnvelope;
use domain::RouteId;

pub use signer::{LocalWalletSigner, SecureUnixWalletSigner, Signer, SigningError};
pub use wallet::{HotWallet, WalletPrecondition, WalletStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigningRequest {
    pub envelope: UnsignedTransactionEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedTransactionEnvelope {
    pub route_id: RouteId,
    pub recent_blockhash: String,
    pub signature: String,
    pub signer_id: String,
    pub signed_message: Vec<u8>,
    pub build_slot: u64,
    pub signed_at: SystemTime,
}
