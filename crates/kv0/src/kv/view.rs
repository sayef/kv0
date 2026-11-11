//! The in-memory ordered view of a key-value database.
//!
//! Every row stored in Lance is an operation stamped with a sequence number: a put, a point
//! delete or a range delete. The view applies operations newest-wins by sequence number, so
//! applying the same row twice, or applying rows out of order, converges to the same state.

use std::collections::BTreeSet;
use std::ops::Bound;

use bytes::Bytes;

pub(crate) const OP_PUT: u8 = 0;
pub(crate) const OP_DELETE: u8 = 1;
pub(crate) const OP_DELETE_RANGE: u8 = 2;
/// A WAL high-water mark: `key` is a writer id, `value` the highest sequence number (u64 LE) of
/// that writer's operations committed so far. Written in the same fragment as the operations it
/// covers and carried forward by compaction, so it survives version cleanup.
pub(crate) const OP_MARK: u8 = 3;

/// One logged operation. For a range delete, `key` is the inclusive start and `value` the
/// exclusive end.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Op {
    pub seq: u64,
    pub kind: u8,
    pub key: Bytes,
    pub value: Option<Bytes>,
}

impl Op {
    pub fn size(&self) -> usize {
        24 + self.key.len() + self.value.as_ref().map_or(0, |v| v.len())
    }
}

/// A key as a byte string ordered byte-wise; what [`Slot`]s are looked up by.
#[repr(transparent)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) struct Key([u8]);

impl Key {
    pub fn new(k: &[u8]) -> &Key {
        // SAFETY: `Key` is a transparent wrapper around `[u8]`.
        unsafe { &*(k as *const [u8] as *const Key) }
    }
}

const TOMBSTONE: u8 = 1;
/// The value stays in Lance: the slot holds its row address (8 bytes) and length (4 bytes).
const LAZY: u8 = 2;

/// A live value: in memory, or in Lance at a row address of the view's dataset version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Val {
    Here(Bytes),
    Stored { addr: u64, len: usize },
}

/// A row read from storage at row address `addr`. When the reader left a large value in Lance,
/// `op.value` is `None` and `stored_len` holds its length.
#[derive(Clone, Debug)]
pub(crate) struct Row {
    pub op: Op,
    pub addr: u64,
    pub stored_len: Option<usize>,
}

/// Part of a view snapshot as columns (`seq`, `op`, `key`, `value`). Values kept in Lance are
/// null in `value`, listed in `stored` as (row, address) to be read when writing.
pub(crate) struct SnapChunk {
    pub seq: Vec<u64>,
    pub op: Vec<u8>,
    pub key: arrow_array::builder::BinaryBuilder,
    pub value: arrow_array::builder::BinaryBuilder,
    pub stored: Vec<(usize, u64)>,
    /// Bytes of values in this chunk, stored ones included.
    pub bytes: usize,
}

impl SnapChunk {
    fn new() -> Self {
        Self {
            seq: Vec::new(),
            op: Vec::new(),
            key: arrow_array::builder::BinaryBuilder::new(),
            value: arrow_array::builder::BinaryBuilder::new(),
            stored: Vec::new(),
            bytes: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.seq.len()
    }

    fn push(&mut self, seq: u64, op: u8, key: &[u8], value: Option<&[u8]>) {
        self.seq.push(seq);
        self.op.push(op);
        self.key.append_value(key);
        self.value.append_option(value);
        self.bytes += value.map_or(0, <[u8]>::len);
    }
}

/// A key's latest state in one allocation: sequence number (8 bytes, little-endian), flags
/// (1 byte), key length (4 bytes, little-endian), key, value. The fixed-size header keeps key
/// comparisons, which the ordered set makes on every lookup, to one load. A tombstone has no
/// value and hides older puts. Ordered and compared by key only.
pub(crate) struct Slot(Box<[u8]>);

/// Bytes before the key: sequence number, flags, key length.
const HEADER: usize = 13;

impl Slot {
    fn new(seq: u64, value: Option<&[u8]>, key: &[u8]) -> Slot {
        Slot::build(
            seq,
            if value.is_some() { 0 } else { TOMBSTONE },
            key,
            value.unwrap_or_default(),
        )
    }

    fn lazy(seq: u64, key: &[u8], addr: u64, len: usize) -> Slot {
        let mut v = [0u8; 12];
        v[..8].copy_from_slice(&addr.to_le_bytes());
        v[8..].copy_from_slice(&(len as u32).to_le_bytes());
        Slot::build(seq, LAZY, key, &v)
    }

    fn build(seq: u64, flags: u8, key: &[u8], v: &[u8]) -> Slot {
        let mut b = Vec::with_capacity(HEADER + key.len() + v.len());
        b.extend_from_slice(&seq.to_le_bytes());
        b.push(flags);
        b.extend_from_slice(&(key.len() as u32).to_le_bytes());
        b.extend_from_slice(key);
        b.extend_from_slice(v);
        Slot(b.into_boxed_slice())
    }

    /// Whether `b` parses as a slot (header, key, and a 12-byte payload when lazy).
    fn well_formed(b: &[u8]) -> bool {
        if b.len() < HEADER {
            return false;
        }
        let n = u32::from_le_bytes(b[9..HEADER].try_into().expect("4 bytes")) as usize;
        let Some(rest) = (b.len() - HEADER).checked_sub(n) else {
            return false;
        };
        b[8] & LAZY == 0 || rest == 12
    }

    pub fn seq(&self) -> u64 {
        u64::from_le_bytes(self.0[..8].try_into().expect("slot header"))
    }

    pub fn is_live(&self) -> bool {
        self.0[8] & TOMBSTONE == 0
    }

    fn key_len(&self) -> usize {
        u32::from_le_bytes(self.0[9..HEADER].try_into().expect("slot header")) as usize
    }

    pub fn key(&self) -> &[u8] {
        &self.0[HEADER..HEADER + self.key_len()]
    }

    fn payload(&self) -> &[u8] {
        &self.0[HEADER + self.key_len()..]
    }

    /// Row address and length of a value kept in Lance.
    fn stored(&self) -> Option<(u64, usize)> {
        (self.0[8] & LAZY != 0).then(|| {
            let p = self.payload();
            (
                u64::from_le_bytes(p[..8].try_into().expect("lazy slot")),
                u32::from_le_bytes(p[8..12].try_into().expect("lazy slot")) as usize,
            )
        })
    }

    /// The value; `None` for a tombstone.
    pub fn value(&self) -> Option<Val> {
        if !self.is_live() {
            return None;
        }
        Some(match self.stored() {
            Some((addr, len)) => Val::Stored { addr, len },
            None => Val::Here(Bytes::copy_from_slice(self.payload())),
        })
    }

    /// Length of a live value, wherever it is.
    fn value_len(&self) -> usize {
        self.stored().map_or(self.payload().len(), |(_, len)| len)
    }
}

impl std::borrow::Borrow<Key> for Slot {
    fn borrow(&self) -> &Key {
        Key::new(self.key())
    }
}

impl PartialEq for Slot {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for Slot {}

impl PartialOrd for Slot {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Slot {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(other.key())
    }
}

impl std::fmt::Debug for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slot")
            .field("seq", &self.seq())
            .field("key", &self.key())
            .field("live", &self.is_live())
            .finish()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RangeTombstone {
    pub begin: Bytes,
    pub end: Bytes,
    pub seq: u64,
}

fn bound(b: Bound<&[u8]>) -> Bound<&Key> {
    match b {
        Bound::Included(k) => Bound::Included(Key::new(k)),
        Bound::Excluded(k) => Bound::Excluded(Key::new(k)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[derive(Default)]
pub(crate) struct View {
    /// Values longer than this many bytes stay in Lance once committed; 0 keeps every value
    /// in memory.
    pub lazy_threshold: usize,
    map: BTreeSet<Slot>,
    /// Live entries whose value is kept in Lance.
    stored: usize,
    pub ranges: Vec<RangeTombstone>,
    /// Number of entries holding a value (not tombstones).
    pub live: usize,
    /// Highest sequence number applied.
    pub max_seq: u64,
    max_range_seq: u64,
    /// WAL high-water marks by writer id.
    pub marks: std::collections::HashMap<Bytes, u64>,
}

impl View {
    fn hidden_by_range(&self, key: &[u8], seq: u64) -> bool {
        seq < self.max_range_seq
            && self
                .ranges
                .iter()
                .any(|r| r.seq > seq && key >= &r.begin[..] && key < &r.end[..])
    }

    /// Entries held, tombstones included.
    pub fn entries(&self) -> usize {
        self.map.len()
    }

    pub fn apply(&mut self, op: Op) {
        self.apply_parts(op.seq, op.kind, &op.key, op.value.as_deref(), None);
    }

    /// Applies a row read from storage. A row whose value was left in Lance while reading
    /// (`stored_len`) becomes an entry pointing at it.
    pub fn apply_row(&mut self, row: Row) {
        match row.stored_len {
            Some(len) => self.apply_put(row.op.seq, &row.op.key, None, Some((row.addr, len))),
            None => self.apply_parts(
                row.op.seq,
                row.op.kind,
                &row.op.key,
                row.op.value.as_deref(),
                Some(row.addr),
            ),
        }
    }

    /// Applies a put (`value`, or a value in Lance at `lazy = (addr, len)`) or a point delete
    /// (neither), already checked against range tombstones.
    fn apply_put(
        &mut self,
        seq: u64,
        key: &[u8],
        value: Option<&[u8]>,
        lazy: Option<(u64, usize)>,
    ) {
        self.max_seq = self.max_seq.max(seq);
        if self.hidden_by_range(key, seq) {
            return;
        }
        let live = value.is_some() || lazy.is_some();
        let was_live = match self.map.get(Key::new(key)) {
            // The same row again, possibly moved by a compaction: keep its new address.
            Some(e) if e.seq() == seq && e.is_live() && lazy.is_some() => {
                let (a, len) = lazy.expect("checked");
                self.put_slot(Slot::lazy(seq, key, a, len));
                return;
            }
            Some(e) if e.seq() >= seq => return,
            Some(e) => Some(e.is_live()),
            None => None,
        };
        self.put_slot(match lazy {
            Some((a, len)) => Slot::lazy(seq, key, a, len),
            None => Slot::new(seq, value, key),
        });
        match (was_live, live) {
            (Some(true), false) => self.live -= 1,
            (Some(false) | None, true) => self.live += 1,
            _ => {}
        }
    }

    /// Inserts or replaces an entry, keeping the count of stored values.
    fn put_slot(&mut self, slot: Slot) {
        let new = slot.stored().is_some();
        let old = self.map.replace(slot).is_some_and(|o| o.stored().is_some());
        match (old, new) {
            (false, true) => self.stored += 1,
            (true, false) => self.stored -= 1,
            _ => {}
        }
    }

    /// Live values kept in Lance.
    pub fn stored_count(&self) -> usize {
        self.stored
    }

    fn keeps_in_lance(&self, len: usize) -> bool {
        self.lazy_threshold > 0 && len > self.lazy_threshold
    }

    /// Applies one operation given by its parts, so rows read from storage need no copy first.
    /// `addr` is the row's address when it was read from storage.
    pub fn apply_parts(
        &mut self,
        seq: u64,
        kind: u8,
        key: &[u8],
        value: Option<&[u8]>,
        addr: Option<u64>,
    ) {
        self.max_seq = self.max_seq.max(seq);
        match kind {
            OP_PUT | OP_DELETE => {
                let value = if kind == OP_PUT {
                    Some(value.unwrap_or_default())
                } else {
                    None
                };
                let lazy = match (addr, value) {
                    (Some(a), Some(v)) if self.keeps_in_lance(v.len()) => Some((a, v.len())),
                    _ => None,
                };
                self.apply_put(seq, key, value, lazy);
            }
            OP_DELETE_RANGE => {
                let end = value.unwrap_or_default();
                if key >= end {
                    return;
                }
                let doomed: Vec<Box<[u8]>> = self
                    .map
                    .range::<Key, _>((
                        Bound::Included(Key::new(key)),
                        Bound::Excluded(Key::new(end)),
                    ))
                    .filter(|e| e.seq() < seq)
                    .map(|e| e.key().into())
                    .collect();
                for k in doomed {
                    if let Some(e) = self.map.take(Key::new(&k)) {
                        if e.is_live() {
                            self.live -= 1;
                        }
                        if e.stored().is_some() {
                            self.stored -= 1;
                        }
                    }
                }
                self.max_range_seq = self.max_range_seq.max(seq);
                self.ranges.push(RangeTombstone {
                    begin: Bytes::copy_from_slice(key),
                    end: Bytes::copy_from_slice(end),
                    seq,
                });
            }
            OP_MARK => {
                let v = value
                    .and_then(|v| v.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                match self.marks.get_mut(key) {
                    Some(m) => *m = (*m).max(v),
                    None => {
                        self.marks.insert(Bytes::copy_from_slice(key), v);
                    }
                }
            }
            _ => {}
        }
    }

    /// Applies operations in any order; the result does not depend on it.
    #[cfg(test)]
    pub fn apply_all(&mut self, ops: Vec<Op>) {
        for op in ops {
            self.apply(op);
        }
    }

    /// Records that the committed row `(key, seq)` is at `addr`: a value already kept in Lance
    /// follows it, and a large value still in memory moves out to it.
    pub fn relocate(&mut self, key: &[u8], seq: u64, addr: u64) {
        let len = match self.map.get(Key::new(key)) {
            Some(e) if e.seq() == seq && e.is_live() => e.value_len(),
            _ => return,
        };
        if self.keeps_in_lance(len) {
            self.put_slot(Slot::lazy(seq, key, addr, len));
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Val> {
        self.map.get(Key::new(key)).and_then(Slot::value)
    }

    /// The sequence number of the live put of `key`, if there is one.
    pub fn live_seq(&self, key: &[u8]) -> Option<u64> {
        self.map
            .get(Key::new(key))
            .filter(|e| e.is_live())
            .map(|e| e.seq())
    }

    /// Up to `limit` live entries after `from`, in key order. Also returns the last key visited,
    /// so a caller can resume after it even when every visited key was a tombstone.
    pub fn scan_forward(
        &self,
        from: Bound<&[u8]>,
        limit: usize,
    ) -> (Vec<(Bytes, Val)>, Option<Bytes>, bool) {
        let mut out = Vec::with_capacity(limit.min(1024));
        let mut last = None;
        let mut iter = self.map.range::<Key, _>((bound(from), Bound::Unbounded));
        let mut last_slot = None;
        for e in iter.by_ref() {
            last_slot = Some(e);
            if let Some(v) = e.value() {
                out.push((Bytes::copy_from_slice(e.key()), v));
                if out.len() >= limit {
                    break;
                }
            }
        }
        if let Some(e) = last_slot {
            last = Some(match out.last() {
                Some((k, _)) if &k[..] == e.key() => k.clone(),
                _ => Bytes::copy_from_slice(e.key()),
            });
        }
        let exhausted = out.len() < limit;
        (out, last, exhausted)
    }

    /// The last live entry at or before `upto` (or the last entry overall).
    pub fn scan_back(&self, upto: Bound<&[u8]>) -> Option<(Bytes, Val)> {
        self.map
            .range::<Key, _>((Bound::Unbounded, bound(upto)))
            .rev()
            .find_map(|e| e.value().map(|v| (Bytes::copy_from_slice(e.key()), v)))
    }

    /// Drops what compaction no longer stores: tombstones at or before `keep_after` and marks
    /// at or before `marks_after`, so the view stays equal to what a reopen would load.
    pub fn prune(&mut self, keep_after: u64, marks_after: u64) {
        self.map.retain(|e| e.is_live() || e.seq() > keep_after);
        self.ranges.retain(|r| r.seq > keep_after);
        self.max_range_seq = self.ranges.iter().map(|r| r.seq).max().unwrap_or(0);
        self.marks.retain(|_, s| *s > marks_after);
    }

    /// The view as rows that reproduce it (live puts, tombstones newer than `keep_after`, marks
    /// newer than `marks_after`), in chunks of at most `rows` rows and about `bytes` bytes of
    /// values, built as columns so a snapshot of a large view costs little more than its data.
    pub fn snapshot(
        &self,
        keep_after: u64,
        marks_after: u64,
        rows: usize,
        bytes: usize,
    ) -> Vec<SnapChunk> {
        let mut out = vec![SnapChunk::new()];
        let room = |out: &mut Vec<SnapChunk>, len: usize| {
            let c = out.last().expect("a chunk");
            if c.len() >= rows || (c.len() > 0 && c.bytes + len > bytes) {
                out.push(SnapChunk::new());
            }
        };
        for r in &self.ranges {
            if r.seq > keep_after {
                room(&mut out, r.end.len());
                let c = out.last_mut().expect("a chunk");
                c.push(r.seq, OP_DELETE_RANGE, &r.begin, Some(&r.end));
            }
        }
        for e in &self.map {
            if e.is_live() {
                match e.stored() {
                    Some((addr, len)) => {
                        room(&mut out, len);
                        let c = out.last_mut().expect("a chunk");
                        c.stored.push((c.len(), addr));
                        c.push(e.seq(), OP_PUT, e.key(), None);
                        c.bytes += len;
                    }
                    None => {
                        room(&mut out, e.payload().len());
                        let c = out.last_mut().expect("a chunk");
                        c.push(e.seq(), OP_PUT, e.key(), Some(e.payload()));
                    }
                }
            } else if e.seq() > keep_after {
                room(&mut out, 0);
                let c = out.last_mut().expect("a chunk");
                c.push(e.seq(), OP_DELETE, e.key(), None);
            }
        }
        for (w, s) in &self.marks {
            if *s > marks_after {
                room(&mut out, 8);
                let c = out.last_mut().expect("a chunk");
                c.push(*s, OP_MARK, w, Some(&s.to_le_bytes()));
            }
        }
        out.retain(|c| c.len() > 0);
        out
    }
}

fn put_u64(w: &mut impl std::io::Write, v: u64) -> std::io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn put_bytes(w: &mut impl std::io::Write, b: &[u8]) -> std::io::Result<()> {
    w.write_all(&(b.len() as u32).to_le_bytes())?;
    w.write_all(b)
}

fn get_u64(r: &mut impl std::io::Read) -> std::io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn get_bytes(r: &mut impl std::io::Read) -> std::io::Result<Vec<u8>> {
    let mut n = [0u8; 4];
    r.read_exact(&mut n)?;
    let mut b = vec![0u8; u32::from_le_bytes(n) as usize];
    r.read_exact(&mut b)?;
    Ok(b)
}

impl View {
    /// Writes the view: counters, marks, range tombstones, then every entry in key order as
    /// its raw slot.
    pub fn write_to(&self, w: &mut impl std::io::Write) -> std::io::Result<()> {
        put_u64(w, self.lazy_threshold as u64)?;
        put_u64(w, self.max_seq)?;
        put_u64(w, self.live as u64)?;
        put_u64(w, self.marks.len() as u64)?;
        for (k, s) in &self.marks {
            put_bytes(w, k)?;
            put_u64(w, *s)?;
        }
        put_u64(w, self.ranges.len() as u64)?;
        for r in &self.ranges {
            put_u64(w, r.seq)?;
            put_bytes(w, &r.begin)?;
            put_bytes(w, &r.end)?;
        }
        put_u64(w, self.map.len() as u64)?;
        for e in &self.map {
            put_bytes(w, &e.0)?;
        }
        Ok(())
    }

    /// Reads a view written by [`View::write_to`].
    pub fn read_from(r: &mut impl std::io::Read) -> std::io::Result<View> {
        let bad = |m: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, m.to_string());
        let mut v = View {
            lazy_threshold: get_u64(r)? as usize,
            max_seq: get_u64(r)?,
            live: get_u64(r)? as usize,
            ..View::default()
        };
        for _ in 0..get_u64(r)? {
            let k = get_bytes(r)?;
            let s = get_u64(r)?;
            v.marks.insert(Bytes::from(k), s);
        }
        for _ in 0..get_u64(r)? {
            let seq = get_u64(r)?;
            let begin = Bytes::from(get_bytes(r)?);
            let end = Bytes::from(get_bytes(r)?);
            v.max_range_seq = v.max_range_seq.max(seq);
            v.ranges.push(RangeTombstone { begin, end, seq });
        }
        let n = get_u64(r)? as usize;
        let mut slots: Vec<Slot> = Vec::with_capacity(n.min(1 << 24));
        for _ in 0..n {
            let b = get_bytes(r)?;
            if !Slot::well_formed(&b) {
                return Err(bad("a view entry is malformed"));
            }
            let slot = Slot(b.into_boxed_slice());
            if slots.last().is_some_and(|l| l.key() >= slot.key()) {
                return Err(bad("view entries are out of order"));
            }
            slots.push(slot);
        }
        v.stored = slots.iter().filter(|e| e.stored().is_some()).count();
        // Sorted and unique: the set is built without comparisons beyond the check above.
        v.map = slots.into_iter().collect();
        Ok(v)
    }
}

/// The mark row recording that `writer` committed everything up to `seq`.
pub(crate) fn mark(writer: Bytes, seq: u64) -> Op {
    Op {
        seq,
        kind: OP_MARK,
        key: writer,
        value: Some(Bytes::copy_from_slice(&seq.to_le_bytes())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(seq: u64, k: &str, v: &str) -> Op {
        Op {
            seq,
            kind: OP_PUT,
            key: Bytes::copy_from_slice(k.as_bytes()),
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
        }
    }

    #[test]
    fn newest_wins_regardless_of_order() {
        let mut a = View::default();
        a.apply_all(vec![put(2, "k", "new"), put(1, "k", "old")]);
        assert_eq!(a.get(b"k").unwrap(), Val::Here(Bytes::from_static(b"new")));
        assert_eq!(a.live, 1);
    }

    #[test]
    fn late_older_put_stays_hidden_by_range() {
        let mut v = View::default();
        v.apply(Op {
            seq: 10,
            kind: OP_DELETE_RANGE,
            key: Bytes::from_static(b"a"),
            value: Some(Bytes::from_static(b"b")),
        });
        v.apply(put(5, "a1", "x"));
        v.apply(put(11, "a2", "y"));
        assert!(v.get(b"a1").is_none());
        assert!(v.get(b"a2").is_some());
    }

    #[test]
    fn tombstone_blocks_older_put() {
        let mut v = View::default();
        v.apply(Op {
            seq: 3,
            kind: OP_DELETE,
            key: Bytes::from_static(b"k"),
            value: None,
        });
        v.apply(put(2, "k", "v"));
        assert!(v.get(b"k").is_none());
        assert_eq!(v.live, 0);
    }

    #[test]
    fn slots_hold_long_keys_and_empty_values() {
        let long = "k".repeat(300);
        let mut v = View::default();
        v.apply(put(1, &long, ""));
        v.apply(put(2, "a", "x"));
        assert_eq!(v.get(long.as_bytes()).unwrap(), Val::Here(Bytes::new()));
        let (items, _, _) = v.scan_forward(Bound::Unbounded, 10);
        assert_eq!(items.len(), 2);
        assert_eq!(&items[1].0[..], long.as_bytes());
    }

    #[test]
    fn any_order_gives_the_same_view() {
        let mut ops = Vec::new();
        let mut x: u64 = 7;
        for seq in 1..400u64 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let k = format!("k{:02}", (x >> 33) % 40);
            ops.push(match (x >> 20) % 10 {
                0 => Op {
                    seq,
                    kind: OP_DELETE,
                    key: Bytes::from(k),
                    value: None,
                },
                1 => Op {
                    seq,
                    kind: OP_DELETE_RANGE,
                    key: Bytes::from(k),
                    value: Some(Bytes::from(format!("k{:02}", (x >> 40) % 40 + 1))),
                },
                _ => put(seq, &k, &format!("v{seq}")),
            });
        }
        let dump = |v: &View| v.scan_forward(Bound::Unbounded, 1000).0;
        let mut a = View::default();
        a.apply_all(ops.clone());
        let mut b = View::default();
        ops.reverse();
        b.apply_all(ops);
        assert_eq!(dump(&a), dump(&b));
        assert_eq!(a.live, b.live);
    }
}
