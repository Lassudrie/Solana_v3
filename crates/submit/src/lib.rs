pub mod jito;
pub mod submitter;
pub mod types;

pub use jito::{JitoConfig, JitoSubmitter};
pub use submitter::{
    SubmitAttemptOutcome, SubmitError, SubmitRetryPolicy, Submitter, submit_with_retry_policy,
};
pub use types::{
    SubmissionId, SubmitMode, SubmitRejectionReason, SubmitRequest, SubmitResult, SubmitStatus,
};

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use domain::RouteId;
    use signing::SignedTransactionEnvelope;

    use crate::{
        JitoConfig, JitoSubmitter, SubmitAttemptOutcome, SubmitRetryPolicy,
        submitter::Submitter,
        types::{SubmitMode, SubmitRequest, SubmitStatus},
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug)]
    struct RetryingSubmitter {
        attempts: AtomicUsize,
        outcomes: Mutex<Vec<SubmitAttemptOutcome>>,
        policy: SubmitRetryPolicy,
    }

    impl Submitter for RetryingSubmitter {
        fn submit_attempt(
            &self,
            _request: SubmitRequest,
        ) -> Result<SubmitAttemptOutcome, crate::SubmitError> {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            Ok(self.outcomes.lock().expect("outcomes lock").remove(0))
        }

        fn retry_policy(&self) -> SubmitRetryPolicy {
            self.policy
        }
    }

    #[test]
    fn submitter_mock_returns_structured_result() {
        let submitter = JitoSubmitter::new(JitoConfig {
            endpoint: "mock://jito".into(),
            ws_endpoint: "mock://jito-tip-stream".into(),
            ..JitoConfig::default()
        });
        let result = submitter
            .submit(SubmitRequest {
                envelope: SignedTransactionEnvelope {
                    route_id: RouteId("route-a".into()),
                    recent_blockhash: "blockhash-1".into(),
                    signature: "sig".into(),
                    signer_id: "wallet".into(),
                    signed_message: vec![1, 2, 3],
                    build_slot: 10,
                    signed_at: SystemTime::now(),
                },
                mode: SubmitMode::SingleTransaction,
            })
            .unwrap();

        assert_eq!(result.status, SubmitStatus::Accepted);
        assert_eq!(result.submission_id.0, "jito-single-sig");
    }

    #[test]
    fn submitter_minimally_deduplicates_same_signature() {
        let submitter = JitoSubmitter::new(JitoConfig {
            endpoint: "mock://jito".into(),
            ws_endpoint: "mock://jito-tip-stream".into(),
            ..JitoConfig::default()
        });
        let request = SubmitRequest {
            envelope: SignedTransactionEnvelope {
                route_id: RouteId("route-a".into()),
                recent_blockhash: "blockhash-1".into(),
                signature: "sig".into(),
                signer_id: "wallet".into(),
                signed_message: vec![1, 2, 3],
                build_slot: 10,
                signed_at: SystemTime::now(),
            },
            mode: SubmitMode::SingleTransaction,
        };

        let first = submitter.submit(request.clone()).unwrap();
        let second = submitter.submit(request).unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn submitter_wrapper_retries_until_terminal_success() {
        let submitter = RetryingSubmitter {
            attempts: AtomicUsize::new(0),
            outcomes: Mutex::new(vec![
                SubmitAttemptOutcome::Retry {
                    terminal_result: crate::SubmitResult {
                        status: crate::SubmitStatus::Rejected,
                        submission_id: crate::SubmissionId("retry-terminal".into()),
                        endpoint: "mock://jito/api/v1/transactions".into(),
                        rejection: Some(crate::SubmitRejectionReason::ChannelUnavailable),
                    },
                    retry_after: Some(std::time::Duration::ZERO),
                },
                SubmitAttemptOutcome::Terminal(crate::SubmitResult {
                    status: crate::SubmitStatus::Accepted,
                    submission_id: crate::SubmissionId("accepted-after-retry".into()),
                    endpoint: "mock://jito/api/v1/transactions".into(),
                    rejection: None,
                }),
            ]),
            policy: SubmitRetryPolicy {
                max_attempts: 2,
                retry_backoff: std::time::Duration::ZERO,
            },
        };
        let result = submitter
            .submit(SubmitRequest {
                envelope: SignedTransactionEnvelope {
                    route_id: RouteId("route-a".into()),
                    recent_blockhash: "blockhash-1".into(),
                    signature: "sig".into(),
                    signer_id: "wallet".into(),
                    signed_message: vec![1, 2, 3],
                    build_slot: 10,
                    signed_at: SystemTime::now(),
                },
                mode: SubmitMode::SingleTransaction,
            })
            .unwrap();

        assert_eq!(result.status, SubmitStatus::Accepted);
        assert_eq!(submitter.attempts.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn submitter_wrapper_returns_terminal_retry_result_when_budget_is_exhausted() {
        let submitter = RetryingSubmitter {
            attempts: AtomicUsize::new(0),
            outcomes: Mutex::new(vec![SubmitAttemptOutcome::Retry {
                terminal_result: crate::SubmitResult {
                    status: crate::SubmitStatus::Rejected,
                    submission_id: crate::SubmissionId("channel-unavailable".into()),
                    endpoint: "mock://jito/api/v1/transactions".into(),
                    rejection: Some(crate::SubmitRejectionReason::ChannelUnavailable),
                },
                retry_after: Some(std::time::Duration::ZERO),
            }]),
            policy: SubmitRetryPolicy {
                max_attempts: 1,
                retry_backoff: std::time::Duration::ZERO,
            },
        };
        let result = submitter
            .submit(SubmitRequest {
                envelope: SignedTransactionEnvelope {
                    route_id: RouteId("route-a".into()),
                    recent_blockhash: "blockhash-1".into(),
                    signature: "sig".into(),
                    signer_id: "wallet".into(),
                    signed_message: vec![1, 2, 3],
                    build_slot: 10,
                    signed_at: SystemTime::now(),
                },
                mode: SubmitMode::SingleTransaction,
            })
            .unwrap();

        assert_eq!(
            result.rejection,
            Some(crate::SubmitRejectionReason::ChannelUnavailable)
        );
        assert_eq!(submitter.attempts.load(Ordering::Relaxed), 1);
    }
}
