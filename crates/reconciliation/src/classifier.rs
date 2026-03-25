use crate::tracker::InclusionStatus;
use submit::SubmitResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureClass {
    SubmitRejected,
    ChainDropped,
    ChainExecutionFailed,
    Expired,
    Unknown,
}

#[derive(Debug, Default)]
pub struct OutcomeClassifier;

impl OutcomeClassifier {
    pub fn classify_submit(result: &SubmitResult) -> Option<FailureClass> {
        result
            .rejection
            .as_ref()
            .map(|_| FailureClass::SubmitRejected)
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
