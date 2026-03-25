use std::collections::HashMap;

use submit::SubmissionId;

use crate::tracker::ExecutionRecord;

#[derive(Debug, Default)]
pub struct ExecutionHistory {
    records: HashMap<SubmissionId, ExecutionRecord>,
}

impl ExecutionHistory {
    pub fn insert(&mut self, record: ExecutionRecord) {
        self.records.insert(record.submission_id.clone(), record);
    }

    pub fn get(&self, submission_id: &SubmissionId) -> Option<&ExecutionRecord> {
        self.records.get(submission_id)
    }

    pub fn get_mut(&mut self, submission_id: &SubmissionId) -> Option<&mut ExecutionRecord> {
        self.records.get_mut(submission_id)
    }

    pub fn values(&self) -> impl Iterator<Item = &ExecutionRecord> {
        self.records.values()
    }
}
