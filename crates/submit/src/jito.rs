use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    submitter::{SubmitError, Submitter},
    types::{
        SubmissionId, SubmitMode, SubmitRejectionReason, SubmitRequest, SubmitResult, SubmitStatus,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JitoConfig {
    pub endpoint: String,
    pub bundle_enabled: bool,
}

impl Default for JitoConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://mainnet.block-engine.jito.wtf".into(),
            bundle_enabled: true,
        }
    }
}

#[derive(Debug)]
pub struct JitoSubmitter {
    config: JitoConfig,
    counter: AtomicU64,
}

impl JitoSubmitter {
    pub fn new(config: JitoConfig) -> Self {
        Self {
            config,
            counter: AtomicU64::new(1),
        }
    }
}

impl Submitter for JitoSubmitter {
    fn submit(&self, request: SubmitRequest) -> Result<SubmitResult, SubmitError> {
        if request.envelope.signed_message.is_empty() {
            return Ok(SubmitResult {
                status: SubmitStatus::Rejected,
                submission_id: SubmissionId("jito-rejected".into()),
                endpoint: self.config.endpoint.clone(),
                rejection: Some(SubmitRejectionReason::InvalidEnvelope),
            });
        }

        let mode_suffix = match request.mode {
            SubmitMode::SingleTransaction => "single",
            SubmitMode::Bundle => "bundle",
        };
        let id = self.counter.fetch_add(1, Ordering::Relaxed);

        Ok(SubmitResult {
            status: SubmitStatus::Accepted,
            submission_id: SubmissionId(format!("jito-{mode_suffix}-{id}")),
            endpoint: self.config.endpoint.clone(),
            rejection: None,
        })
    }
}
