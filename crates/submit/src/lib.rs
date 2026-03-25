pub mod jito;
pub mod submitter;
pub mod types;

pub use jito::{JitoConfig, JitoSubmitter};
pub use submitter::{SubmitError, Submitter};
pub use types::{
    SubmissionId, SubmitMode, SubmitRejectionReason, SubmitRequest, SubmitResult, SubmitStatus,
};

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use signing::SignedTransactionEnvelope;
    use state::types::RouteId;

    use crate::{
        JitoConfig, JitoSubmitter,
        submitter::Submitter,
        types::{SubmitMode, SubmitRequest, SubmitStatus},
    };

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
}
