use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::SystemTime;

use builder::UnsignedTransactionEnvelope;
use thiserror::Error;

use crate::{
    SignedTransactionEnvelope, SigningRequest,
    wallet::{HotWallet, WalletPrecondition},
};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SigningError {
    #[error("wallet is not ready for signing")]
    WalletNotReady,
    #[error("wallet has insufficient balance")]
    InsufficientBalance,
}

pub trait Signer: Send + Sync {
    fn sign(
        &self,
        wallet: &HotWallet,
        request: SigningRequest,
    ) -> Result<SignedTransactionEnvelope, SigningError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalWalletSigner {
    signer_id: String,
}

impl LocalWalletSigner {
    pub fn new(signer_id: impl Into<String>) -> Self {
        Self {
            signer_id: signer_id.into(),
        }
    }

    fn signature_for(&self, envelope: &UnsignedTransactionEnvelope) -> String {
        let mut hasher = DefaultHasher::new();
        self.signer_id.hash(&mut hasher);
        envelope.route_id.hash(&mut hasher);
        envelope.message_bytes.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }
}

impl Signer for LocalWalletSigner {
    fn sign(
        &self,
        wallet: &HotWallet,
        request: SigningRequest,
    ) -> Result<SignedTransactionEnvelope, SigningError> {
        match wallet.precondition() {
            WalletPrecondition::Ok => {}
            WalletPrecondition::WalletNotReady => return Err(SigningError::WalletNotReady),
            WalletPrecondition::InsufficientBalance => {
                return Err(SigningError::InsufficientBalance);
            }
        }

        let signature = self.signature_for(&request.envelope);
        Ok(SignedTransactionEnvelope {
            route_id: request.envelope.route_id,
            recent_blockhash: request.envelope.recent_blockhash,
            signature,
            signer_id: self.signer_id.clone(),
            signed_message: request.envelope.message_bytes,
            build_slot: request.envelope.build_slot,
            signed_at: SystemTime::now(),
        })
    }
}
