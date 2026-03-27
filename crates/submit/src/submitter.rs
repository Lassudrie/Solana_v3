use std::{thread, time::Duration};

use thiserror::Error;

use crate::types::{SubmitRequest, SubmitResult};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SubmitError {
    #[error("submit transport unavailable")]
    TransportUnavailable,
    #[error("submit upstream protocol error")]
    UpstreamProtocol,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitAttemptOutcome {
    Terminal(SubmitResult),
    Retry {
        terminal_result: SubmitResult,
        retry_after: Option<Duration>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmitRetryPolicy {
    pub max_attempts: usize,
    pub retry_backoff: Duration,
}

impl Default for SubmitRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            retry_backoff: Duration::ZERO,
        }
    }
}

impl SubmitRetryPolicy {
    pub fn retry_delay(&self, attempt_index: usize) -> Duration {
        let multiplier = 1_u32 << attempt_index.min(8);
        self.retry_backoff.saturating_mul(multiplier)
    }
}

pub trait Submitter: Send + Sync {
    fn submit_attempt(&self, request: SubmitRequest) -> Result<SubmitAttemptOutcome, SubmitError>;

    fn retry_policy(&self) -> SubmitRetryPolicy {
        SubmitRetryPolicy::default()
    }

    fn submit(&self, request: SubmitRequest) -> Result<SubmitResult, SubmitError> {
        submit_with_retry_policy(self, request, self.retry_policy())
    }
}

pub fn submit_with_retry_policy<S: Submitter + ?Sized>(
    submitter: &S,
    request: SubmitRequest,
    policy: SubmitRetryPolicy,
) -> Result<SubmitResult, SubmitError> {
    let max_attempts = policy.max_attempts.max(1);
    let mut attempt_index = 0;

    loop {
        match submitter.submit_attempt(request.clone())? {
            SubmitAttemptOutcome::Terminal(result) => return Ok(result),
            SubmitAttemptOutcome::Retry {
                terminal_result,
                retry_after,
            } => {
                if attempt_index + 1 >= max_attempts {
                    return Ok(terminal_result);
                }
                thread::sleep(retry_after.unwrap_or_else(|| policy.retry_delay(attempt_index)));
                attempt_index += 1;
            }
        }
    }
}
