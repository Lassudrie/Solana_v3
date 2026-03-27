use std::{
    fs,
    io::{BufRead, BufReader, Write},
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::PathBuf,
    sync::Arc,
    thread,
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
    #[error("failed to spawn signer client worker: {0}")]
    Spawn(std::io::Error),
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
            let (stream, _) = self.listener.accept()?;
            let keypair = Arc::clone(&self.keypair);
            thread::Builder::new()
                .name("signerd-client".into())
                .spawn(move || {
                    let _ = handle_client(keypair, stream);
                })
                .map_err(SecureSignerServiceError::Spawn)?;
        }
    }

    pub fn serve_one(&self) -> Result<(), SecureSignerServiceError> {
        let (stream, _) = self.listener.accept()?;
        handle_client(Arc::clone(&self.keypair), stream)
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

fn handle_client(
    keypair: Arc<Keypair>,
    mut stream: UnixStream,
) -> Result<(), SecureSignerServiceError> {
    let mut reader = BufReader::new(stream.try_clone()?);
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            return Ok(());
        }
        let response = match serde_json::from_str::<SignerRequest>(line.trim_end()) {
            Ok(SignerRequest::GetPublicKey) => SignerResponse::PublicKey {
                pubkey: keypair.pubkey().to_string(),
            },
            Ok(SignerRequest::SignMessage { message_base64 }) => {
                match BASE64_STANDARD.decode(message_base64) {
                    Ok(message) => match keypair.try_sign_message(&message) {
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
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader, Write},
        os::unix::net::UnixStream,
        sync::Arc,
        thread,
    };

    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use signing::protocol::{SignerRequest, SignerResponse};
    use solana_sdk::signer::keypair::Keypair;

    use super::handle_client;

    #[test]
    fn handle_client_serves_multiple_requests_on_one_connection() {
        let keypair = Arc::new(Keypair::new_from_array([21; 32]));
        let (mut client, server) = UnixStream::pair().expect("unix pair");

        let join = thread::spawn(move || handle_client(keypair, server).expect("handle client"));

        serde_json::to_writer(&mut client, &SignerRequest::GetPublicKey)
            .expect("write pubkey request");
        client.write_all(b"\n").expect("newline");
        serde_json::to_writer(
            &mut client,
            &SignerRequest::SignMessage {
                message_base64: BASE64_STANDARD.encode(b"message"),
            },
        )
        .expect("write sign request");
        client.write_all(b"\n").expect("newline");
        client.flush().expect("flush requests");

        let mut reader = BufReader::new(client.try_clone().expect("clone client"));
        let mut first = String::new();
        reader.read_line(&mut first).expect("read first response");
        let mut second = String::new();
        reader.read_line(&mut second).expect("read second response");

        let first: SignerResponse =
            serde_json::from_str(first.trim_end()).expect("parse first response");
        let second: SignerResponse =
            serde_json::from_str(second.trim_end()).expect("parse second response");

        assert!(matches!(first, SignerResponse::PublicKey { .. }));
        assert!(matches!(second, SignerResponse::Signature { .. }));

        drop(reader);
        drop(client);
        join.join().expect("join handler");
    }
}
