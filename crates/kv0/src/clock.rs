//! Sequence numbers: a hybrid logical clock in nanoseconds since the Unix epoch.
//!
//! A new sequence number is larger than the wall clock reading and than every sequence number
//! this handle has seen, so local writes always win over what was loaded, and concurrent writers
//! order roughly by wall-clock time.

use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[derive(Default)]
pub(crate) struct Clock {
    last: u64,
}

impl Clock {
    pub fn observe(&mut self, seq: u64) {
        const DAY: u64 = 86_400 * 1_000_000_000;
        if seq > now_nanos().saturating_add(DAY) {
            tracing::warn!(
                seq,
                "kv0: a sequence number is more than a day in the future (clock skew or a damaged row)"
            );
        }
        self.last = self.last.max(seq);
    }

    /// The last number issued or observed.
    pub fn last(&self) -> u64 {
        self.last
    }

    pub fn next(&mut self) -> u64 {
        self.last = self.last.saturating_add(1).max(now_nanos());
        self.last
    }
}
