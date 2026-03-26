use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignerRequest {
    GetPublicKey,
    SignMessage { message_base64: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignerResponse {
    PublicKey { pubkey: String },
    Signature { signature: String },
    Error { code: String, message: String },
}
