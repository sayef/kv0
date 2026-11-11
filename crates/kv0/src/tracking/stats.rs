//! Per-series statistics, so a tracker can show and sort by the last, minimum and maximum of
//! every metric without reading points.
//!
//! A flush writes, for each series it adds points to, a **delta**: the totals of those points.
//! Deltas are reserved rows of the metrics table (`series_id` 0, the series id in `step`, the
//! encoded delta in `stats`), written in the same append as the points they describe, so they
//! commit atomically with them and cost no extra commit. Merging deltas is associative and
//! commutative, so concurrent writers to one series never overwrite each other. `optimize`
//! replaces a series' deltas by one exact delta computed from its (deduplicated) points.
//!
//! A step written twice is counted once by reads but twice by naive totals. A writer that
//! writes a step at or below one it already wrote to that series, and every replayed write-ahead
//! log, marks the delta *dirty*; reads compute dirty series exactly from their points, so the
//! totals always agree with reads, except when a step is re-logged by a handle that never wrote
//! or resumed that series (such totals are exact again after `optimize`).

use std::collections::HashMap;

use crate::error::{Error, Result};

/// Totals of one series. Steps and timestamps as stored; `min` and `max` ignore NaN and
/// infinities, which `non_finite` counts.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SeriesStats {
    pub count: u64,
    pub first_step: i64,
    pub last_step: i64,
    /// The value at `last_step` (the later write when a step was written twice).
    pub last: f64,
    pub min: Option<f64>,
    pub min_step: Option<i64>,
    pub max: Option<f64>,
    pub max_step: Option<i64>,
    pub non_finite: u64,
    pub last_timestamp: i64,
}

impl SeriesStats {
    fn of(step: i64, value: f64, ts: i64) -> Self {
        let finite = value.is_finite();
        SeriesStats {
            count: 1,
            first_step: step,
            last_step: step,
            last: value,
            min: finite.then_some(value),
            min_step: finite.then_some(step),
            max: finite.then_some(value),
            max_step: finite.then_some(step),
            non_finite: u64::from(!finite),
            last_timestamp: ts,
        }
    }

    /// Adds `later`, whose points were written after this one's.
    pub fn merge(&mut self, later: &SeriesStats) {
        let mut d = Delta {
            s: *self,
            last_seq: 0,
            dirty: false,
        };
        d.merge(&Delta {
            s: *later,
            last_seq: 1,
            dirty: false,
        });
        *self = d.s;
    }
}

/// Totals of some points of a series, as written by one flush or one `optimize`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Delta {
    pub s: SeriesStats,
    /// Orders `last` between deltas with the same `last_step`: the later write wins.
    pub last_seq: u64,
    /// The points may overwrite steps counted elsewhere: compute the series from its points.
    pub dirty: bool,
}

const VERSION: u8 = 1;
const HAS_MIN: u8 = 1;
const HAS_MAX: u8 = 2;
const DIRTY: u8 = 4;
const EMPTY: u8 = 8;

impl Delta {
    /// The exact delta of one point, as `optimize` writes it.
    pub fn of_point(step: i64, value: f64, ts: i64) -> Self {
        Delta {
            s: SeriesStats::of(step, value, ts),
            last_seq: 0,
            dirty: false,
        }
    }

    /// A dirty delta without points: the series' totals must come from its points.
    pub fn dirty_only() -> Self {
        let mut s = SeriesStats::of(0, 0.0, 0);
        s.count = 0;
        Delta {
            s,
            last_seq: 0,
            dirty: true,
        }
    }

    fn is_empty(&self) -> bool {
        self.dirty && self.s.count == 0
    }

    /// Merges `o` into `self`; the result does not depend on the order of merging.
    pub fn merge(&mut self, o: &Delta) {
        if o.is_empty() {
            self.dirty = true;
            return;
        }
        if self.is_empty() {
            *self = Delta { dirty: true, ..*o };
            return;
        }
        let (a, b) = (&mut self.s, &o.s);
        a.count += b.count;
        a.first_step = a.first_step.min(b.first_step);
        if (b.last_step, o.last_seq) > (a.last_step, self.last_seq) {
            a.last_step = b.last_step;
            a.last = b.last;
            self.last_seq = o.last_seq;
        }
        if let (Some(m), Some(st)) = (b.min, b.min_step) {
            if a.min
                .is_none_or(|x| (m, st) < (x, a.min_step.unwrap_or(i64::MAX)))
            {
                a.min = Some(m);
                a.min_step = Some(st);
            }
        }
        if let (Some(m), Some(st)) = (b.max, b.max_step) {
            // The highest value; among equal values, the earliest step.
            if a.max
                .is_none_or(|x| m > x || (m == x && st < a.max_step.unwrap_or(i64::MAX)))
            {
                a.max = Some(m);
                a.max_step = Some(st);
            }
        }
        a.non_finite += b.non_finite;
        a.last_timestamp = a.last_timestamp.max(b.last_timestamp);
        self.dirty |= o.dirty;
    }

    pub fn encode(&self) -> Vec<u8> {
        let s = &self.s;
        let mut flags = 0;
        if s.min.is_some() {
            flags |= HAS_MIN;
        }
        if s.max.is_some() {
            flags |= HAS_MAX;
        }
        if self.dirty {
            flags |= DIRTY;
        }
        if self.is_empty() {
            flags |= EMPTY;
        }
        let mut b = Vec::with_capacity(2 + 11 * 8);
        b.push(VERSION);
        b.push(flags);
        for v in [
            s.count,
            s.first_step as u64,
            s.last_step as u64,
            s.last.to_bits(),
            self.last_seq,
            s.min.unwrap_or(0.0).to_bits(),
            s.min_step.unwrap_or(0) as u64,
            s.max.unwrap_or(0.0).to_bits(),
            s.max_step.unwrap_or(0) as u64,
            s.non_finite,
            s.last_timestamp as u64,
        ] {
            b.extend_from_slice(&v.to_le_bytes());
        }
        b
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        if b.len() != 2 + 11 * 8 || b[0] != VERSION {
            return Err(Error::Corruption(
                "a series statistics row has an unknown format".into(),
            ));
        }
        let flags = b[1];
        let w = |i: usize| u64::from_le_bytes(b[2 + i * 8..10 + i * 8].try_into().expect("8"));
        let (has_min, has_max) = (flags & HAS_MIN != 0, flags & HAS_MAX != 0);
        let mut d = Delta {
            s: SeriesStats {
                count: w(0),
                first_step: w(1) as i64,
                last_step: w(2) as i64,
                last: f64::from_bits(w(3)),
                min: has_min.then(|| f64::from_bits(w(5))),
                min_step: has_min.then(|| w(6) as i64),
                max: has_max.then(|| f64::from_bits(w(7))),
                max_step: has_max.then(|| w(8) as i64),
                non_finite: w(9),
                last_timestamp: w(10) as i64,
            },
            last_seq: w(4),
            dirty: flags & DIRTY != 0,
        };
        if flags & EMPTY != 0 {
            d = Delta::dirty_only();
        }
        Ok(d)
    }
}

/// Deltas of points given in write order, by series; series id 0 (reserved rows) is skipped.
/// A step written twice in the batch, or a series in `relogged`, makes that series' delta dirty;
/// every series in `relogged` gets a delta, with or without points.
pub(crate) fn of_points(
    ids: &[u64],
    steps: &[i64],
    values: &[f64],
    ts: &[i64],
    seq: u64,
    relogged: &std::collections::HashSet<u64>,
) -> HashMap<u64, Delta> {
    let mut out: HashMap<u64, Delta> = HashMap::new();
    let mut seen: std::collections::HashSet<(u64, i64)> = std::collections::HashSet::new();
    for i in 0..ids.len() {
        if ids[i] == 0 {
            continue;
        }
        let dup = !seen.insert((ids[i], steps[i]));
        let p = Delta {
            s: SeriesStats::of(steps[i], values[i], ts[i]),
            // Later points of the batch win `last` at an equal step.
            last_seq: seq.saturating_sub((ids.len() - i) as u64),
            dirty: dup,
        };
        match out.get_mut(&ids[i]) {
            Some(d) => d.merge(&p),
            None => {
                out.insert(ids[i], p);
            }
        }
    }
    for id in relogged {
        match out.get_mut(id) {
            Some(d) => d.dirty = true,
            None => {
                out.insert(*id, Delta::dirty_only());
            }
        }
    }
    out
}

/// Exact totals of points given in storage order: per series, a step written more than once
/// counts once, with its last value (as reads return it).
pub(crate) fn exact(
    ids: &[u64],
    steps: &[i64],
    values: &[f64],
    ts: &[i64],
) -> HashMap<u64, SeriesStats> {
    let mut last: HashMap<(u64, i64), (f64, i64)> = HashMap::new();
    for i in 0..ids.len() {
        if ids[i] != 0 {
            last.insert((ids[i], steps[i]), (values[i], ts[i]));
        }
    }
    let mut pts: Vec<_> = last.into_iter().collect();
    pts.sort_unstable_by_key(|((id, step), _)| (*id, *step));
    let mut out: HashMap<u64, SeriesStats> = HashMap::new();
    for ((id, step), (v, t)) in pts {
        let p = SeriesStats::of(step, v, t);
        match out.get_mut(&id) {
            Some(s) => s.merge(&p),
            None => {
                out.insert(id, p);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deltas_merge_in_any_order_and_round_trip() {
        let none = std::collections::HashSet::new();
        let a = of_points(&[7, 7], &[0, 1], &[3.0, 1.0], &[10, 11], 10, &none)[&7];
        let b = of_points(&[7, 7], &[2, 3], &[5.0, f64::NAN], &[12, 13], 20, &none)[&7];
        let (mut x, mut y) = (a, b);
        x.merge(&b);
        y.merge(&a);
        assert_eq!(x.encode(), y.encode());
        assert_eq!(Delta::decode(&x.encode()).unwrap().encode(), x.encode());
        assert_eq!(
            (x.s.count, x.s.min, x.s.max, x.s.non_finite),
            (4, Some(1.0), Some(5.0), 1)
        );
        assert_eq!(x.s.last_step, 3);
        let e = Delta::dirty_only();
        let mut z = x;
        z.merge(&e);
        assert!(z.dirty && z.s.count == 4);
        assert!(Delta::decode(&e.encode()).unwrap().is_empty());
    }

    #[test]
    fn exact_totals_count_a_step_once_with_its_last_value() {
        let s = exact(&[1, 1, 1], &[5, 6, 5], &[1000.0, 2.0, 1.0], &[1, 2, 3])[&1];
        assert_eq!((s.count, s.max, s.min), (2, Some(2.0), Some(1.0)));
    }
}
