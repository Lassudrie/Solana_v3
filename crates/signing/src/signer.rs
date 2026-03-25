use std::{sync::Arc, time::SystemTime};

use bs58::decode as base58_decode;
use builder::UnsignedTransactionEnvelope;
use solana_sdk::signer::{
    Signer as SolanaCurveSigner,
    keypair::{Keypair, read_keypair_file},
};
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
    #[error("wallet signer material is missing")]
    MissingSigner,
    #[error("wallet signer config is invalid: {0}")]
    InvalidSignerConfig(String),
    #[error("wallet pubkey mismatch; expected {expected}, got {actual}")]
    PubkeyMismatch { expected: String, actual: String },
    #[error("signing failed: {0}")]
    SigningFailed(String),
}

pub trait Signer: Send + Sync {
    fn pubkey_string(&self) -> Result<String, SigningError>;

    fn sign(
        &self,
        wallet: &HotWallet,
        request: SigningRequest,
    ) -> Result<SignedTransactionEnvelope, SigningError>;
}

#[derive(Debug, Clone)]
pub struct LocalWalletSigner {
    signer_id: String,
    signer_material: SignerMaterial,
}

#[derive(Debug, Clone)]
enum SignerMaterial {
    Loaded(Arc<Keypair>),
    Missing,
    Invalid(String),
}

impl LocalWalletSigner {
    pub fn new(
        signer_id: impl Into<String>,
        keypair_path: Option<String>,
        keypair_base58: Option<String>,
    ) -> Self {
        Self {
            signer_id: signer_id.into(),
            signer_material: Self::load_signer_material(keypair_path, keypair_base58),
        }
    }

    pub fn has_signer_material(&self) -> bool {
        matches!(self.signer_material, SignerMaterial::Loaded(_))
    }

    pub fn pubkey_string(&self) -> Result<String, SigningError> {
        Ok(self.keypair()?.pubkey().to_string())
    }

    fn load_signer_material(
        keypair_path: Option<String>,
        keypair_base58: Option<String>,
    ) -> SignerMaterial {
        match (keypair_path, keypair_base58) {
            (Some(_path), Some(_)) => SignerMaterial::Invalid(
                "set either signing.keypair_path or signing.keypair_base58, not both".into(),
            ),
            (Some(path), None) => match read_keypair_file(&path) {
                Ok(keypair) => SignerMaterial::Loaded(Arc::new(keypair)),
                Err(error) => {
                    SignerMaterial::Invalid(format!("failed to load keypair file {path}: {error}"))
                }
            },
            (None, Some(base58)) => {
                let decoded = match base58_decode(base58.trim()).into_vec() {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        return SignerMaterial::Invalid(format!(
                            "invalid base58-encoded keypair: {error}"
                        ));
                    }
                };
                match Keypair::try_from(decoded.as_slice()) {
                    Ok(keypair) => SignerMaterial::Loaded(Arc::new(keypair)),
                    Err(error) => SignerMaterial::Invalid(format!(
                        "invalid keypair byte layout from base58 input: {error}"
                    )),
                }
            }
            (None, None) => SignerMaterial::Missing,
        }
    }

    fn keypair(&self) -> Result<&Keypair, SigningError> {
        match &self.signer_material {
            SignerMaterial::Loaded(keypair) => Ok(keypair.as_ref()),
            SignerMaterial::Missing => Err(SigningError::MissingSigner),
            SignerMaterial::Invalid(reason) => {
                Err(SigningError::InvalidSignerConfig(reason.clone()))
            }
        }
    }

    fn serialize_transaction(
        &self,
        signature: &solana_sdk::signature::Signature,
        envelope: &UnsignedTransactionEnvelope,
    ) -> Vec<u8> {
        let mut serialized = Vec::with_capacity(
            1 + signature.as_ref().len() + envelope.compiled_message_bytes.len(),
        );
        write_shortvec_len(1, &mut serialized);
        serialized.extend_from_slice(signature.as_ref());
        serialized.extend_from_slice(&envelope.compiled_message_bytes);
        serialized
    }
}

impl Signer for LocalWalletSigner {
    fn pubkey_string(&self) -> Result<String, SigningError> {
        LocalWalletSigner::pubkey_string(self)
    }

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
            WalletPrecondition::MissingSigner => return Err(SigningError::MissingSigner),
        }

        let keypair = self.keypair()?;
        let actual_pubkey = keypair.pubkey().to_string();
        if !wallet.owner_pubkey.is_empty() && wallet.owner_pubkey != actual_pubkey {
            return Err(SigningError::PubkeyMismatch {
                expected: wallet.owner_pubkey.clone(),
                actual: actual_pubkey,
            });
        }

        let signature = keypair
            .try_sign_message(&request.envelope.compiled_message_bytes)
            .map_err(|error| SigningError::SigningFailed(error.to_string()))?;
        let envelope = request.envelope;
        let route_id = envelope.route_id.clone();
        let build_slot = envelope.build_slot;
        let recent_blockhash = envelope.recent_blockhash.clone();
        Ok(SignedTransactionEnvelope {
            route_id,
            recent_blockhash,
            signature: signature.to_string(),
            signer_id: self.signer_id.clone(),
            signed_message: self.serialize_transaction(&signature, &envelope),
            build_slot,
            signed_at: SystemTime::now(),
        })
    }
}

fn write_shortvec_len(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let mut element = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            element |= 0x80;
        }
        out.push(element);
        if value == 0 {
            break;
        }
    }
}
