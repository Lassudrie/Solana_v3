use signing::SignedTransactionEnvelope;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubmitMode {
    SingleTransaction,
    Bundle,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubmissionId(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitRequest {
    pub envelope: SignedTransactionEnvelope,
    pub mode: SubmitMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitStatus {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitRejectionReason {
    InvalidEnvelope,
    InvalidRequest,
    BundleDisabled,
    Unauthorized,
    RateLimited,
    DuplicateSubmission,
    TipTooLow,
    PathCongested,
    ChannelUnavailable,
    RemoteRejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitResult {
    pub status: SubmitStatus,
    pub submission_id: SubmissionId,
    pub endpoint: String,
    pub rejection: Option<SubmitRejectionReason>,
}
