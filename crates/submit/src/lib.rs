pub mod jito;
pub mod router;
pub mod rpc;
pub mod submitter;
pub mod types;

pub use jito::{JitoConfig, JitoSubmitter};
pub use router::{RoutedSubmitter, SingleTransactionRoutingPolicy};
pub use rpc::{RpcConfig, RpcSubmitter};
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
        JitoConfig, JitoSubmitter, RoutedSubmitter, RpcConfig, RpcSubmitter,
        SingleTransactionRoutingPolicy, SubmissionId, SubmitAttemptOutcome, SubmitRejectionReason,
        SubmitResult, SubmitRetryPolicy,
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

    #[derive(Debug)]
    struct StaticSubmitter {
        attempts: AtomicUsize,
        outcome: Result<SubmitAttemptOutcome, crate::SubmitError>,
    }

    impl Submitter for StaticSubmitter {
        fn submit_attempt(
            &self,
            _request: SubmitRequest,
        ) -> Result<SubmitAttemptOutcome, crate::SubmitError> {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            self.outcome.clone()
        }
    }

    #[test]
    fn submitter_mock_returns_structured_result() {
        let submitter = JitoSubmitter::new(JitoConfig {
            endpoint: "mock://jito".into(),
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
                    quoted_slot: 10,
                    blockhash_slot: Some(11),
                    signed_at: SystemTime::now(),
                },
                mode: SubmitMode::SingleTransaction,
                leader: None,
            })
            .unwrap();

        assert_eq!(result.status, SubmitStatus::Accepted);
        assert_eq!(result.submission_id.0, "jito-single-sig");
    }

    #[test]
    fn submitter_minimally_deduplicates_same_signature() {
        let submitter = JitoSubmitter::new(JitoConfig {
            endpoint: "mock://jito".into(),
            ..JitoConfig::default()
        });
        let request = SubmitRequest {
            envelope: SignedTransactionEnvelope {
                route_id: RouteId("route-a".into()),
                recent_blockhash: "blockhash-1".into(),
                signature: "sig".into(),
                signer_id: "wallet".into(),
                signed_message: vec![1, 2, 3],
                quoted_slot: 10,
                blockhash_slot: Some(11),
                signed_at: SystemTime::now(),
            },
            mode: SubmitMode::SingleTransaction,
            leader: None,
        };

        let first = submitter.submit(request.clone()).unwrap();
        let second = submitter.submit(request).unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn rpc_submitter_mock_returns_structured_result() {
        let submitter = RpcSubmitter::new(RpcConfig {
            endpoint: "mock://solana-rpc".into(),
            ..RpcConfig::default()
        });
        let result = submitter
            .submit(SubmitRequest {
                envelope: SignedTransactionEnvelope {
                    route_id: RouteId("route-a".into()),
                    recent_blockhash: "blockhash-1".into(),
                    signature: "sig".into(),
                    signer_id: "wallet".into(),
                    signed_message: vec![1, 2, 3],
                    quoted_slot: 10,
                    blockhash_slot: Some(11),
                    signed_at: SystemTime::now(),
                },
                mode: SubmitMode::SingleTransaction,
                leader: None,
            })
            .unwrap();

        assert_eq!(result.status, SubmitStatus::Accepted);
        assert_eq!(result.submission_id.0, "rpc-single-sig");
    }

    #[test]
    fn routed_submitter_leader_aware_prefers_rpc_lane() {
        let rpc = std::sync::Arc::new(StaticSubmitter {
            attempts: AtomicUsize::new(0),
            outcome: Ok(SubmitAttemptOutcome::Terminal(SubmitResult {
                status: SubmitStatus::Accepted,
                submission_id: SubmissionId("rpc-accepted".into()),
                endpoint: "mock://solana-rpc".into(),
                rejection: None,
            })),
        });
        let jito = std::sync::Arc::new(StaticSubmitter {
            attempts: AtomicUsize::new(0),
            outcome: Ok(SubmitAttemptOutcome::Terminal(SubmitResult {
                status: SubmitStatus::Rejected,
                submission_id: SubmissionId("jito-rejected".into()),
                endpoint: "mock://jito/api/v1/transactions".into(),
                rejection: Some(SubmitRejectionReason::RemoteRejected),
            })),
        });
        let submitter = RoutedSubmitter::new(
            Some(jito.clone()),
            Some(rpc.clone()),
            SingleTransactionRoutingPolicy::LeaderAware,
        );

        let result = submitter
            .submit(SubmitRequest {
                envelope: SignedTransactionEnvelope {
                    route_id: RouteId("route-a".into()),
                    recent_blockhash: "blockhash-1".into(),
                    signature: "sig".into(),
                    signer_id: "wallet".into(),
                    signed_message: vec![1, 2, 3],
                    quoted_slot: 10,
                    blockhash_slot: Some(11),
                    signed_at: SystemTime::now(),
                },
                mode: SubmitMode::SingleTransaction,
                leader: Some("leader-a".into()),
            })
            .unwrap();

        assert_eq!(result.submission_id.0, "rpc-accepted");
        assert_eq!(rpc.attempts.load(Ordering::Relaxed), 1);
        assert_eq!(jito.attempts.load(Ordering::Relaxed), 0);
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
                    quoted_slot: 10,
                    blockhash_slot: Some(11),
                    signed_at: SystemTime::now(),
                },
                mode: SubmitMode::SingleTransaction,
                leader: None,
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
                    quoted_slot: 10,
                    blockhash_slot: Some(11),
                    signed_at: SystemTime::now(),
                },
                mode: SubmitMode::SingleTransaction,
                leader: None,
            })
            .unwrap();

        assert_eq!(
            result.rejection,
            Some(crate::SubmitRejectionReason::ChannelUnavailable)
        );
        assert_eq!(submitter.attempts.load(Ordering::Relaxed), 1);
    }
}
