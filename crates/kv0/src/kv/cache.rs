//! A byte-bounded LRU cache of values read from Lance, keyed by row address.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;

pub(crate) struct ValueCache {
    capacity: usize,
    used: usize,
    tick: u64,
    /// Row address to (value, last use).
    entries: HashMap<u64, (Bytes, u64)>,
    /// Last use to row address, oldest first.
    order: BTreeMap<u64, u64>,
}

impl ValueCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            used: 0,
            tick: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    pub fn get(&mut self, addr: u64) -> Option<Bytes> {
        self.tick += 1;
        let (v, used) = self.entries.get_mut(&addr)?;
        self.order.remove(used);
        *used = self.tick;
        self.order.insert(self.tick, addr);
        Some(v.clone())
    }

    pub fn put(&mut self, addr: u64, v: Bytes) {
        if v.len() > self.capacity {
            return;
        }
        self.tick += 1;
        if let Some((old, used)) = self.entries.insert(addr, (v.clone(), self.tick)) {
            self.order.remove(&used);
            self.used -= old.len();
        }
        self.order.insert(self.tick, addr);
        self.used += v.len();
        while self.used > self.capacity {
            let Some((_, a)) = self.order.pop_first() else {
                break;
            };
            if let Some((old, _)) = self.entries.remove(&a) {
                self.used -= old.len();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_least_recently_used_beyond_capacity() {
        let mut c = ValueCache::new(10);
        c.put(1, Bytes::from_static(b"aaaa"));
        c.put(2, Bytes::from_static(b"bbbb"));
        assert!(c.get(1).is_some());
        c.put(3, Bytes::from_static(b"cccc"));
        assert!(c.get(2).is_none());
        assert!(c.get(1).is_some());
        assert!(c.get(3).is_some());
        c.put(4, Bytes::from(vec![0; 11]));
        assert!(c.get(4).is_none());
    }
}
