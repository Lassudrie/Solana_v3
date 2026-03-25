use thiserror::Error;

use crate::types::{SubmitRequest, SubmitResult};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SubmitError {
    #[error("submit transport unavailable")]
    TransportUnavailable,
}

pub trait Submitter: Send + Sync {
    fn submit(&self, request: SubmitRequest) -> Result<SubmitResult, SubmitError>;
}
