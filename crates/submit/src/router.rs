use std::{
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use crate::{
    SubmissionId, SubmitAttemptOutcome, SubmitError, SubmitMode, SubmitRejectionReason,
    SubmitRequest, SubmitResult, SubmitRetryPolicy, SubmitStatus, Submitter,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SingleTransactionRoutingPolicy {
    JitoOnly,
    RpcOnly,
    Fanout,
    LeaderAware,
}

#[derive(Clone)]
enum SubmitLane {
    Jito(Arc<dyn Submitter>),
    Rpc(Arc<dyn Submitter>),
}

impl SubmitLane {
    fn submit_attempt(&self, request: SubmitRequest) -> Result<SubmitAttemptOutcome, SubmitError> {
        match self {
            Self::Jito(submitter) | Self::Rpc(submitter) => submitter.submit_attempt(request),
        }
    }
}

pub struct RoutedSubmitter {
    jito: Option<Arc<dyn Submitter>>,
    rpc: Option<Arc<dyn Submitter>>,
    single_transaction_policy: SingleTransactionRoutingPolicy,
}

impl RoutedSubmitter {
    pub fn new(
        jito: Option<Arc<dyn Submitter>>,
        rpc: Option<Arc<dyn Submitter>>,
        single_transaction_policy: SingleTransactionRoutingPolicy,
    ) -> Self {
        Self {
            jito,
            rpc,
            single_transaction_policy,
        }
    }

    fn lanes_for(&self, request: &SubmitRequest) -> Vec<SubmitLane> {
        match request.mode {
            SubmitMode::Bundle => self
                .jito
                .as_ref()
                .map(|submitter| vec![SubmitLane::Jito(Arc::clone(submitter))])
                .unwrap_or_default(),
            SubmitMode::SingleTransaction => match self.single_transaction_policy {
                SingleTransactionRoutingPolicy::JitoOnly => self
                    .jito
                    .as_ref()
                    .map(|submitter| vec![SubmitLane::Jito(Arc::clone(submitter))])
                    .unwrap_or_default(),
                SingleTransactionRoutingPolicy::RpcOnly => self
                    .rpc
                    .as_ref()
                    .map(|submitter| vec![SubmitLane::Rpc(Arc::clone(submitter))])
                    .unwrap_or_default(),
                SingleTransactionRoutingPolicy::Fanout => self.all_single_transaction_lanes(),
                SingleTransactionRoutingPolicy::LeaderAware => {
                    if request.leader.is_some() {
                        self.rpc
                            .as_ref()
                            .map(|submitter| vec![SubmitLane::Rpc(Arc::clone(submitter))])
                            .or_else(|| {
                                self.jito
                                    .as_ref()
                                    .map(|submitter| vec![SubmitLane::Jito(Arc::clone(submitter))])
                            })
                            .unwrap_or_default()
                    } else {
                        self.all_single_transaction_lanes()
                    }
                }
            },
        }
    }

    fn all_single_transaction_lanes(&self) -> Vec<SubmitLane> {
        let mut lanes = Vec::new();
        if let Some(submitter) = &self.rpc {
            lanes.push(SubmitLane::Rpc(Arc::clone(submitter)));
        }
        if let Some(submitter) = &self.jito {
            lanes.push(SubmitLane::Jito(Arc::clone(submitter)));
        }
        lanes
    }
}

impl Submitter for RoutedSubmitter {
    fn submit_attempt(&self, request: SubmitRequest) -> Result<SubmitAttemptOutcome, SubmitError> {
        let lanes = self.lanes_for(&request);
        if lanes.is_empty() {
            let rejection = match request.mode {
                SubmitMode::Bundle => SubmitRejectionReason::BundleDisabled,
                SubmitMode::SingleTransaction => SubmitRejectionReason::ChannelUnavailable,
            };
            return Ok(SubmitAttemptOutcome::Terminal(SubmitResult {
                status: SubmitStatus::Rejected,
                submission_id: SubmissionId(format!(
                    "router-{}-{}",
                    rejection_code(&rejection),
                    request.envelope.signature
                )),
                endpoint: "router://unavailable".into(),
                rejection: Some(rejection),
            }));
        }
        if lanes.len() == 1 {
            return lanes[0].submit_attempt(request);
        }

        let (sender, receiver) = mpsc::channel();
        for lane in lanes {
            let sender = sender.clone();
            let request = request.clone();
            thread::spawn(move || {
                let _ = sender.send(lane.submit_attempt(request));
            });
        }
        drop(sender);

        let mut first_terminal: Option<SubmitAttemptOutcome> = None;
        let mut first_error: Option<SubmitError> = None;
        while let Ok(result) = receiver.recv_timeout(Duration::from_secs(1)) {
            match result {
                Ok(SubmitAttemptOutcome::Terminal(result))
                    if result.status == SubmitStatus::Accepted =>
                {
                    return Ok(SubmitAttemptOutcome::Terminal(result));
                }
                Ok(outcome) => {
                    if first_terminal.is_none() {
                        first_terminal = Some(outcome);
                    }
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }

        if let Some(outcome) = first_terminal {
            Ok(outcome)
        } else if let Some(error) = first_error {
            Err(error)
        } else {
            Err(SubmitError::TransportUnavailable)
        }
    }

    fn retry_policy(&self) -> SubmitRetryPolicy {
        SubmitRetryPolicy::default()
    }
}

fn rejection_code(reason: &SubmitRejectionReason) -> &'static str {
    match reason {
        SubmitRejectionReason::InvalidEnvelope => "invalid-envelope",
        SubmitRejectionReason::InvalidRequest => "invalid-request",
        SubmitRejectionReason::BundleDisabled => "bundle-disabled",
        SubmitRejectionReason::Unauthorized => "unauthorized",
        SubmitRejectionReason::RateLimited => "rate-limited",
        SubmitRejectionReason::DuplicateSubmission => "duplicate",
        SubmitRejectionReason::TipTooLow => "tip-too-low",
        SubmitRejectionReason::PathCongested => "path-congested",
        SubmitRejectionReason::ChannelUnavailable => "channel-unavailable",
        SubmitRejectionReason::RemoteRejected => "remote-rejected",
    }
}
