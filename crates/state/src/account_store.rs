use std::collections::HashMap;

use crate::types::{AccountKey, AccountRecord, AccountUpdateStatus};

#[derive(Debug, Default)]
pub struct AccountStore {
    records: HashMap<AccountKey, AccountRecord>,
}

impl AccountStore {
    pub fn upsert(&mut self, record: AccountRecord) -> AccountUpdateStatus {
        match self.records.get(&record.key) {
            Some(existing)
                if existing.slot > record.slot
                    || (existing.slot == record.slot
                        && existing.write_version >= record.write_version) =>
            {
                AccountUpdateStatus::StaleRejected
            }
            _ => {
                self.records.insert(record.key.clone(), record);
                AccountUpdateStatus::Applied
            }
        }
    }

    pub fn get(&self, key: &AccountKey) -> Option<&AccountRecord> {
        self.records.get(key)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::AccountStore;
    use crate::types::{AccountKey, AccountRecord, AccountUpdateStatus};

    #[test]
    fn rejects_stale_updates() {
        let key = AccountKey("acct-1".into());
        let mut store = AccountStore::default();
        let first = AccountRecord {
            key: key.clone(),
            owner: "owner".into(),
            lamports: 1,
            data: vec![1],
            slot: 11,
            write_version: 3,
            observed_at: SystemTime::now(),
        };
        let stale = AccountRecord {
            key: key.clone(),
            owner: "owner".into(),
            lamports: 1,
            data: vec![2],
            slot: 11,
            write_version: 2,
            observed_at: SystemTime::now(),
        };

        assert_eq!(store.upsert(first.clone()), AccountUpdateStatus::Applied);
        assert_eq!(store.upsert(stale), AccountUpdateStatus::StaleRejected);
        assert_eq!(store.get(&key).unwrap().data, first.data);
    }
}
