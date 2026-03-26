use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::PathBuf,
    sync::Arc,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use signing::protocol::{SignerRequest, SignerResponse};
use solana_sdk::signer::{
    Signer as SolanaCurveSigner,
    keypair::{Keypair, read_keypair_file},
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecureSignerServiceConfig {
    pub socket_path: PathBuf,
    pub keypair_path: PathBuf,
}

pub struct SecureSignerService {
    socket_path: PathBuf,
    listener: UnixListener,
    keypair: Arc<Keypair>,
}

#[derive(Debug, Error)]
pub enum SecureSignerServiceError {
    #[error("signer config is invalid: {0}")]
    InvalidConfig(String),
    #[error("failed to prepare signer socket path {path}: {source}")]
    PrepareSocket {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to load keypair file {path}: {detail}")]
    LoadKeypair { path: String, detail: String },
    #[error("signer I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to encode signer response: {0}")]
    Encode(#[from] serde_json::Error),
}

impl SecureSignerService {
    pub fn bind(config: SecureSignerServiceConfig) -> Result<Self, SecureSignerServiceError> {
        if config.socket_path.as_os_str().is_empty() {
            return Err(SecureSignerServiceError::InvalidConfig(
                "socket path must not be empty".into(),
            ));
        }
        if config.keypair_path.as_os_str().is_empty() {
            return Err(SecureSignerServiceError::InvalidConfig(
                "keypair path must not be empty".into(),
            ));
        }

        let socket_path = config.socket_path;
        prepare_socket_path(&socket_path)?;
        let listener = UnixListener::bind(&socket_path)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).map_err(|source| {
            SecureSignerServiceError::PrepareSocket {
                path: socket_path.display().to_string(),
                source,
            }
        })?;

        let keypair = read_keypair_file(&config.keypair_path).map_err(|source| {
            SecureSignerServiceError::LoadKeypair {
                path: config.keypair_path.display().to_string(),
                detail: source.to_string(),
            }
        })?;

        Ok(Self {
            socket_path,
            listener,
            keypair: Arc::new(keypair),
        })
    }

    pub fn serve_forever(&self) -> Result<(), SecureSignerServiceError> {
        loop {
            self.serve_one()?;
        }
    }

    pub fn serve_one(&self) -> Result<(), SecureSignerServiceError> {
        let (stream, _) = self.listener.accept()?;
        self.handle_client(stream)
    }

    fn handle_client(&self, mut stream: UnixStream) -> Result<(), SecureSignerServiceError> {
        let mut line = String::new();
        BufReader::new(stream.try_clone()?).read_line(&mut line)?;
        let response = match serde_json::from_str::<SignerRequest>(line.trim_end()) {
            Ok(SignerRequest::GetPublicKey) => SignerResponse::PublicKey {
                pubkey: self.keypair.pubkey().to_string(),
            },
            Ok(SignerRequest::SignMessage { message_base64 }) => {
                match BASE64_STANDARD.decode(message_base64) {
                    Ok(message) => match self.keypair.try_sign_message(&message) {
                        Ok(signature) => SignerResponse::Signature {
                            signature: signature.to_string(),
                        },
                        Err(error) => SignerResponse::Error {
                            code: "sign_failed".into(),
                            message: error.to_string(),
                        },
                    },
                    Err(error) => SignerResponse::Error {
                        code: "invalid_message".into(),
                        message: error.to_string(),
                    },
                }
            }
            Err(error) => SignerResponse::Error {
                code: "invalid_request".into(),
                message: error.to_string(),
            },
        };

        serde_json::to_writer(&mut stream, &response)?;
        stream.write_all(b"\n")?;
        stream.flush()?;
        Ok(())
    }
}

impl Drop for SecureSignerService {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.socket_path);
    }
}

fn prepare_socket_path(socket_path: &PathBuf) -> Result<(), SecureSignerServiceError> {
    let Some(parent) = socket_path.parent() else {
        return Err(SecureSignerServiceError::InvalidConfig(format!(
            "socket path {} has no parent directory",
            socket_path.display()
        )));
    };
    fs::create_dir_all(parent).map_err(|source| SecureSignerServiceError::PrepareSocket {
        path: parent.display().to_string(),
        source,
    })?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).map_err(|source| {
        SecureSignerServiceError::PrepareSocket {
            path: parent.display().to_string(),
            source,
        }
    })?;

    if socket_path.exists() {
        let metadata = fs::metadata(socket_path).map_err(|source| {
            SecureSignerServiceError::PrepareSocket {
                path: socket_path.display().to_string(),
                source,
            }
        })?;
        if metadata.file_type().is_socket() {
            fs::remove_file(socket_path).map_err(|source| {
                SecureSignerServiceError::PrepareSocket {
                    path: socket_path.display().to_string(),
                    source,
                }
            })?;
        } else {
            return Err(SecureSignerServiceError::InvalidConfig(format!(
                "refusing to replace non-socket path {}",
                socket_path.display()
            )));
        }
    }

    Ok(())
}
