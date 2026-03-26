use crate::tracker::InclusionStatus;
use submit::SubmitResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureClass {
    SubmitRejected,
    TransportFailed,
    ChainDropped,
    ChainExecutionFailed,
    ChainExecutionTooLittleOutput,
    Expired,
    Unknown,
}

#[derive(Debug, Default)]
pub struct OutcomeClassifier;

impl OutcomeClassifier {
    pub fn classify_submit(result: &SubmitResult) -> Option<FailureClass> {
        matches!(result.status, submit::SubmitStatus::Rejected)
            .then_some(FailureClass::SubmitRejected)
    }

    pub fn classify_inclusion(status: &InclusionStatus) -> Option<FailureClass> {
        match status {
            InclusionStatus::Dropped => Some(FailureClass::ChainDropped),
            InclusionStatus::Expired { .. } => Some(FailureClass::Expired),
            InclusionStatus::Failed(class) => Some(class.clone()),
            _ => None,
        }
    }
}
