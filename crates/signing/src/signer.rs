use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bs58::decode as base58_decode;
use builder::UnsignedTransactionEnvelope;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    signer::{
        Signer as SolanaCurveSigner,
        keypair::{Keypair, read_keypair_file},
    },
};
use thiserror::Error;

use crate::{
    SignedTransactionEnvelope, SigningRequest,
    protocol::{SignerRequest, SignerResponse},
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
    #[error("signer transport failed: {0}")]
    Transport(String),
    #[error("signer transport timed out: {0}")]
    TransportTimeout(String),
    #[error("signer response was malformed: {0}")]
    MalformedResponse(String),
    #[error("signer returned a signature that failed local verification")]
    InvalidSignature,
    #[error("signing failed: {0}")]
    SigningFailed(String),
}

pub trait Signer: Send + Sync {
    fn pubkey_string(&self) -> Result<String, SigningError>;

    fn is_available(&self) -> bool;

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
pub struct SecureUnixWalletSigner {
    signer_id: String,
    socket_path: PathBuf,
    signer_pubkey: String,
    connect_timeout: Duration,
    io_timeout: Duration,
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

    fn load_signer_material(
        keypair_path: Option<String>,
        keypair_base58: Option<String>,
    ) -> SignerMaterial {
        match (keypair_path, keypair_base58) {
            (Some(_), Some(_)) => SignerMaterial::Invalid(
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
}

impl SecureUnixWalletSigner {
    pub fn new(
        signer_id: impl Into<String>,
        socket_path: impl Into<PathBuf>,
        expected_pubkey: Option<String>,
        connect_timeout_ms: u64,
        io_timeout_ms: u64,
    ) -> Result<Self, SigningError> {
        let socket_path = socket_path.into();
        if socket_path.as_os_str().is_empty() {
            return Err(SigningError::InvalidSignerConfig(
                "signing.socket_path must not be empty".into(),
            ));
        }

        let signer_id = signer_id.into();
        let signer = Self {
            signer_id,
            socket_path,
            signer_pubkey: String::new(),
            connect_timeout: Duration::from_millis(connect_timeout_ms),
            io_timeout: Duration::from_millis(io_timeout_ms),
        };

        let actual_pubkey = signer.request_public_key()?;
        if let Some(expected_pubkey) = expected_pubkey.filter(|value| !value.is_empty()) {
            verify_expected_pubkey(&expected_pubkey, &actual_pubkey)?;
        }

        Ok(Self {
            signer_pubkey: actual_pubkey,
            ..signer
        })
    }

    fn connect(&self) -> Result<UnixStream, SigningError> {
        let deadline = Instant::now() + self.connect_timeout;
        loop {
            match UnixStream::connect(&self.socket_path) {
                Ok(stream) => {
                    stream
                        .set_read_timeout(Some(self.io_timeout))
                        .map_err(|error| {
                            map_transport_error(error, "failed to set read timeout")
                        })?;
                    stream
                        .set_write_timeout(Some(self.io_timeout))
                        .map_err(|error| {
                            map_transport_error(error, "failed to set write timeout")
                        })?;
                    return Ok(stream);
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                    ) && Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => {
                    return Err(map_transport_error(
                        error,
                        &format!("failed to connect to {}", self.socket_path.display()),
                    ));
                }
            }
        }
    }

    fn request_public_key(&self) -> Result<String, SigningError> {
        match self.send_request(SignerRequest::GetPublicKey)? {
            SignerResponse::PublicKey { pubkey } => parse_pubkey(&pubkey, true).map(|_| pubkey),
            other => Err(SigningError::MalformedResponse(format!(
                "expected public_key response, got {other:?}"
            ))),
        }
    }

    fn request_signature(&self, message_bytes: &[u8]) -> Result<Signature, SigningError> {
        let request = SignerRequest::SignMessage {
            message_base64: BASE64_STANDARD.encode(message_bytes),
        };
        match self.send_request(request)? {
            SignerResponse::Signature { signature } => Signature::from_str(signature.trim())
                .map_err(|error| {
                    SigningError::MalformedResponse(format!(
                        "invalid signature returned by signer: {error}"
                    ))
                }),
            other => Err(SigningError::MalformedResponse(format!(
                "expected signature response, got {other:?}"
            ))),
        }
    }

    fn send_request(&self, request: SignerRequest) -> Result<SignerResponse, SigningError> {
        let mut stream = self.connect()?;
        let payload = serde_json::to_vec(&request).map_err(|error| {
            SigningError::SigningFailed(format!("failed to serialize signer request: {error}"))
        })?;
        stream
            .write_all(&payload)
            .map_err(|error| map_transport_error(error, "failed to write signer request"))?;
        stream
            .write_all(b"\n")
            .map_err(|error| map_transport_error(error, "failed to finalize signer request"))?;
        stream
            .flush()
            .map_err(|error| map_transport_error(error, "failed to flush signer request"))?;

        let mut line = String::new();
        let mut reader = BufReader::new(stream);
        reader
            .read_line(&mut line)
            .map_err(|error| map_transport_error(error, "failed to read signer response"))?;
        if line.trim().is_empty() {
            return Err(SigningError::MalformedResponse(
                "signer returned an empty response".into(),
            ));
        }

        match serde_json::from_str::<SignerResponse>(line.trim_end()) {
            Ok(SignerResponse::Error { code, message }) => {
                Err(SigningError::SigningFailed(format!("{code}: {message}")))
            }
            Ok(response) => Ok(response),
            Err(error) => Err(SigningError::MalformedResponse(format!(
                "signer response was not valid JSON: {error}"
            ))),
        }
    }
}

impl Signer for LocalWalletSigner {
    fn pubkey_string(&self) -> Result<String, SigningError> {
        Ok(self.keypair()?.pubkey().to_string())
    }

    fn is_available(&self) -> bool {
        matches!(self.signer_material, SignerMaterial::Loaded(_))
    }

    fn sign(
        &self,
        wallet: &HotWallet,
        request: SigningRequest,
    ) -> Result<SignedTransactionEnvelope, SigningError> {
        ensure_wallet_ready(wallet)?;

        let keypair = self.keypair()?;
        let actual_pubkey = keypair.pubkey().to_string();
        verify_expected_pubkey(&wallet.owner_pubkey, &actual_pubkey)?;

        let signature = keypair
            .try_sign_message(&request.envelope.compiled_message_bytes)
            .map_err(|error| SigningError::SigningFailed(error.to_string()))?;
        finalize_signed_envelope(&self.signer_id, &actual_pubkey, signature, request.envelope)
    }
}

impl Signer for SecureUnixWalletSigner {
    fn pubkey_string(&self) -> Result<String, SigningError> {
        Ok(self.signer_pubkey.clone())
    }

    fn is_available(&self) -> bool {
        !self.signer_pubkey.is_empty()
    }

    fn sign(
        &self,
        wallet: &HotWallet,
        request: SigningRequest,
    ) -> Result<SignedTransactionEnvelope, SigningError> {
        ensure_wallet_ready(wallet)?;
        verify_expected_pubkey(&wallet.owner_pubkey, &self.signer_pubkey)?;

        let signature = self.request_signature(&request.envelope.compiled_message_bytes)?;
        finalize_signed_envelope(
            &self.signer_id,
            &self.signer_pubkey,
            signature,
            request.envelope,
        )
    }
}

fn ensure_wallet_ready(wallet: &HotWallet) -> Result<(), SigningError> {
    match wallet.precondition() {
        WalletPrecondition::Ok => Ok(()),
        WalletPrecondition::WalletNotReady => Err(SigningError::WalletNotReady),
        WalletPrecondition::InsufficientBalance => Err(SigningError::InsufficientBalance),
        WalletPrecondition::MissingSigner => Err(SigningError::MissingSigner),
    }
}

fn verify_expected_pubkey(expected: &str, actual: &str) -> Result<(), SigningError> {
    if !expected.is_empty() && expected != actual {
        return Err(SigningError::PubkeyMismatch {
            expected: expected.to_string(),
            actual: actual.to_string(),
        });
    }
    Ok(())
}

fn finalize_signed_envelope(
    signer_id: &str,
    signer_pubkey: &str,
    signature: Signature,
    envelope: UnsignedTransactionEnvelope,
) -> Result<SignedTransactionEnvelope, SigningError> {
    let pubkey = parse_pubkey(signer_pubkey, false)?;
    if !signature.verify(pubkey.as_ref(), &envelope.compiled_message_bytes) {
        return Err(SigningError::InvalidSignature);
    }

    let route_id = envelope.route_id.clone();
    let build_slot = envelope.build_slot;
    let recent_blockhash = envelope.recent_blockhash.clone();
    Ok(SignedTransactionEnvelope {
        route_id,
        recent_blockhash,
        signature: signature.to_string(),
        signer_id: signer_id.to_string(),
        signed_message: serialize_transaction(&signature, &envelope),
        build_slot,
        signed_at: SystemTime::now(),
    })
}

fn parse_pubkey(value: &str, remote: bool) -> Result<Pubkey, SigningError> {
    Pubkey::from_str(value.trim()).map_err(|error| {
        if remote {
            SigningError::MalformedResponse(format!("invalid signer pubkey {value}: {error}"))
        } else {
            SigningError::InvalidSignerConfig(format!("invalid signer pubkey {value}: {error}"))
        }
    })
}

fn serialize_transaction(signature: &Signature, envelope: &UnsignedTransactionEnvelope) -> Vec<u8> {
    let mut serialized =
        Vec::with_capacity(1 + signature.as_ref().len() + envelope.compiled_message_bytes.len());
    write_shortvec_len(1, &mut serialized);
    serialized.extend_from_slice(signature.as_ref());
    serialized.extend_from_slice(&envelope.compiled_message_bytes);
    serialized
}

fn map_transport_error(error: std::io::Error, context: &str) -> SigningError {
    match error.kind() {
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => {
            SigningError::TransportTimeout(format!("{context}: {error}"))
        }
        _ => SigningError::Transport(format!("{context}: {error}")),
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

#[cfg(test)]
mod tests {
    use std::{
        env,
        io::{BufRead, BufReader, Write},
        os::unix::net::UnixListener,
        path::PathBuf,
        process,
        sync::mpsc,
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    use builder::{AtomicLegPlan, MessageFormat};
    use solana_sdk::{hash::hashv, signer::keypair::Keypair};
    use state::types::{PoolId, RouteId};
    use strategy::route_registry::SwapSide;

    use super::*;
    use crate::wallet::WalletStatus;

    #[test]
    fn local_signer_produces_verifiable_signature() {
        let keypair = Keypair::new_from_array([7; 32]);
        let signer = LocalWalletSigner::new("wallet-a", None, Some(keypair.to_base58_string()));
        let wallet = ready_wallet(keypair.pubkey().to_string());
        let message = b"arb-message".to_vec();

        let signed = signer
            .sign(
                &wallet,
                SigningRequest {
                    envelope: unsigned_envelope(message.clone()),
                },
            )
            .expect("local signature");

        let signature = Signature::from_str(&signed.signature).expect("signature");
        assert!(signature.verify(keypair.pubkey().as_ref(), &message));
        assert_eq!(signed.signed_message[0], 1);
    }

    #[test]
    fn secure_unix_signer_produces_verifiable_signature() {
        let keypair = Keypair::new_from_array([8; 32]);
        let socket_path = temp_socket_path("secure-unix-valid");
        let server = spawn_mock_signer_server(socket_path.clone(), keypair, MockBehavior::Valid, 2);
        let signer = SecureUnixWalletSigner::new(
            "wallet-b",
            &socket_path,
            Some(server.pubkey.clone()),
            50,
            50,
        )
        .expect("secure unix signer");
        let wallet = ready_wallet(server.pubkey.clone());
        let message = b"verified-message".to_vec();

        let signed = signer
            .sign(
                &wallet,
                SigningRequest {
                    envelope: unsigned_envelope(message.clone()),
                },
            )
            .expect("secure signature");

        let signature = Signature::from_str(&signed.signature).expect("signature");
        let pubkey = Pubkey::from_str(&server.pubkey).expect("pubkey");
        assert!(signature.verify(pubkey.as_ref(), &message));
        server.join();
    }

    #[test]
    fn secure_unix_signer_rejects_invalid_remote_signature() {
        let keypair = Keypair::new_from_array([9; 32]);
        let socket_path = temp_socket_path("secure-unix-invalid-signature");
        let server = spawn_mock_signer_server(
            socket_path.clone(),
            keypair,
            MockBehavior::WrongSignature,
            2,
        );
        let signer = SecureUnixWalletSigner::new(
            "wallet-c",
            &socket_path,
            Some(server.pubkey.clone()),
            50,
            50,
        )
        .expect("secure unix signer");
        let wallet = ready_wallet(server.pubkey.clone());

        let error = signer
            .sign(
                &wallet,
                SigningRequest {
                    envelope: unsigned_envelope(b"invalid-signature".to_vec()),
                },
            )
            .expect_err("signature must be rejected");
        assert_eq!(error, SigningError::InvalidSignature);
        server.join();
    }

    #[test]
    fn secure_unix_signer_rejects_pubkey_mismatch() {
        let keypair = Keypair::new_from_array([10; 32]);
        let socket_path = temp_socket_path("secure-unix-mismatch");
        let server = spawn_mock_signer_server(socket_path.clone(), keypair, MockBehavior::Valid, 1);

        let error = SecureUnixWalletSigner::new(
            "wallet-d",
            &socket_path,
            Some(Keypair::new_from_array([11; 32]).pubkey().to_string()),
            50,
            50,
        )
        .expect_err("pubkey mismatch should fail");

        assert!(matches!(error, SigningError::PubkeyMismatch { .. }));
        server.join();
    }

    #[test]
    fn secure_unix_signer_rejects_malformed_response() {
        let keypair = Keypair::new_from_array([12; 32]);
        let socket_path = temp_socket_path("secure-unix-malformed");
        let server =
            spawn_mock_signer_server(socket_path.clone(), keypair, MockBehavior::Malformed, 1);

        let error = SecureUnixWalletSigner::new("wallet-e", &socket_path, None, 50, 50)
            .expect_err("malformed response should fail");

        assert!(matches!(error, SigningError::MalformedResponse(_)));
        server.join();
    }

    fn ready_wallet(owner_pubkey: String) -> HotWallet {
        HotWallet {
            wallet_id: "wallet".into(),
            owner_pubkey,
            balance_lamports: 10,
            status: WalletStatus::Ready,
        }
    }

    fn unsigned_envelope(message: Vec<u8>) -> UnsignedTransactionEnvelope {
        UnsignedTransactionEnvelope {
            route_id: RouteId("route-a".into()),
            build_slot: 42,
            recent_blockhash: hashv(&[b"blockhash"]).to_string(),
            fee_payer_pubkey: Keypair::new_from_array([13; 32]).pubkey().to_string(),
            leg_plans: [
                AtomicLegPlan {
                    venue: "orca".into(),
                    pool_id: PoolId("pool-a".into()),
                    side: SwapSide::BuyBase,
                    input_amount: 10,
                    min_output_amount: 11,
                    current_tick_index: None,
                },
                AtomicLegPlan {
                    venue: "raydium".into(),
                    pool_id: PoolId("pool-b".into()),
                    side: SwapSide::SellBase,
                    input_amount: 11,
                    min_output_amount: 12,
                    current_tick_index: None,
                },
            ],
            instructions: Vec::new(),
            resolved_lookup_tables: Vec::new(),
            compiled_message_bytes: message,
            message_format: MessageFormat::Legacy,
            compute_unit_limit: 1,
            compute_unit_price_micro_lamports: 1,
            base_fee_lamports: 5_000,
            priority_fee_lamports: 0,
            estimated_network_fee_lamports: 5_000,
            estimated_total_cost_lamports: 5_000,
            jito_tip_lamports: 0,
        }
    }

    struct MockSignerServer {
        pubkey: String,
        join_handle: thread::JoinHandle<()>,
        socket_path: PathBuf,
    }

    impl MockSignerServer {
        fn join(self) {
            self.join_handle.join().expect("mock signer thread");
            let _ = std::fs::remove_file(self.socket_path);
        }
    }

    #[derive(Clone, Copy)]
    enum MockBehavior {
        Valid,
        WrongSignature,
        Malformed,
    }

    fn spawn_mock_signer_server(
        socket_path: PathBuf,
        keypair: Keypair,
        behavior: MockBehavior,
        request_count: usize,
    ) -> MockSignerServer {
        let pubkey = keypair.pubkey().to_string();
        let (ready_tx, ready_rx) = mpsc::channel();
        let listener_path = socket_path.clone();
        let join_handle = thread::spawn(move || {
            let listener = UnixListener::bind(&listener_path).expect("bind unix listener");
            ready_tx.send(()).expect("signal ready");
            for _ in 0..request_count {
                let (mut stream, _) = listener.accept().expect("accept signer connection");
                let mut line = String::new();
                BufReader::new(stream.try_clone().expect("clone stream"))
                    .read_line(&mut line)
                    .expect("read signer request");
                let request: SignerRequest =
                    serde_json::from_str(line.trim_end()).expect("parse signer request");
                match behavior {
                    MockBehavior::Malformed => {
                        stream
                            .write_all(b"not-json\n")
                            .expect("write malformed response");
                    }
                    MockBehavior::Valid | MockBehavior::WrongSignature => {
                        let response = match request {
                            SignerRequest::GetPublicKey => SignerResponse::PublicKey {
                                pubkey: keypair.pubkey().to_string(),
                            },
                            SignerRequest::SignMessage { message_base64 } => {
                                let message = BASE64_STANDARD
                                    .decode(message_base64)
                                    .expect("decode message");
                                let signature = match behavior {
                                    MockBehavior::Valid => keypair
                                        .try_sign_message(&message)
                                        .expect("sign message")
                                        .to_string(),
                                    MockBehavior::WrongSignature => {
                                        Keypair::new_from_array([3; 32])
                                            .try_sign_message(&message)
                                            .expect("sign wrong message")
                                            .to_string()
                                    }
                                    MockBehavior::Malformed => unreachable!(),
                                };
                                SignerResponse::Signature { signature }
                            }
                        };
                        serde_json::to_writer(&mut stream, &response)
                            .expect("write signer response");
                        stream.write_all(b"\n").expect("finalize signer response");
                    }
                }
            }
        });
        ready_rx.recv().expect("wait for signer ready");
        MockSignerServer {
            pubkey,
            join_handle,
            socket_path,
        }
    }

    fn temp_socket_path(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        env::temp_dir().join(format!("{label}-{}-{unique}.sock", process::id()))
    }
}
