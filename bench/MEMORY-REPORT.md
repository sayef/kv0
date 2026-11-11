# Memory and reopen: native improvements

The same harness and machine as [REPORT.md](REPORT.md), 5 repetitions per cell, the same
significance rule (non-overlapping ranges and more than 20% apart). The two builds were measured
one after the other on the same day and machine, on local storage only. The machine is shared:
the last remeasurement of the bulk, large-value and reopen cells started at a load average of 13
and ended at 3, which widens the write-throughput ranges; the memory and reopen results do not
depend on it.

- **before**: kv0 0.2 before these changes: the view as a `BTreeMap` of shared byte buffers, every value
  in memory, every open a full scan of the dataset.
- **after**: with these changes: one allocation per view entry, values over 4 KiB left in Lance
  (`lazy_value_threshold`) behind a 64 MiB cache, rows applied batch by batch while loading, and
  a view snapshot saved on close (`snapshot_min_entries`, 100 000) that the next open loads
  before reading only newer fragments.

The read cells open a dataset that the `bulk` cell built and closed, so "after" reopens from the
snapshot that close saved; the first open on a machine without a snapshot costs what "before"
costs, less the memory saved by the compact entries. Writable reopen opens the same dataset
without `read_only`.

## Verdict

**Memory and reopen are fixed inside the native design, and reads got faster.** All rows below are
significant under the rule unless marked.

| What | before | after | Change |
|---|---:|---:|---|
| Reopen, 1M / 10M keys (read-only) | 451 ms / 5.57 s | 74 ms / 705 ms | 6.1× / 7.9× faster |
| Reopen, 1M / 10M keys (writable) | 458 ms / 5.57 s | 75 ms / 715 ms | 6.1× / 7.8× faster |
| Memory after reopen, 1M / 10M keys | 547 MB / 3.1 GB | 151 MB / 1.0 GB | 3.6× / 3.0× less |
| First open without a snapshot, 10M keys | 5.57 s, 3.1 GB | 5.41 s, 1.4 GB | same time, 2.3× less memory |
| 100k × 16 KiB values: reopen | 886 ms, 2.6 GB | 20 ms, 90 MB | 45× faster, 28× less memory |
| 100k × 16 KiB values: first open without a snapshot | 886 ms, 2.6 GB | 702 ms, 1.6 GB | 1.6× less memory |
| Bulk load 1M: writer memory | 441 MB | 293 MB | 50% less |
| Hot point reads | 1.15M gets/s | 1.66M gets/s | 44% faster |
| Seek + 100 × next, p50 | 25.0 µs | 10.9 µs | 2.3× faster |
| Full scan | 6.7M items/s | 15.1M items/s | 2.2× faster |
| Tracker reader session (cold open, both databases) | 182 ms | 78 ms | 2.3× faster |
| Crash recovery, 200k unflushed puts | 173 ms | 218 ms | **26% slower** |

**What each measure contributes.**

- **The compact entries** account for the memory of a first open without a snapshot. At
  10M keys the view drops from about 310 to about 140 bytes per entry, process included.
- **The snapshot** accounts for reopen time. The remaining 785 ms at 10M keys is reading and
  allocating 10M entries from a 660 MB local file.
- **Lazy values** account for the large-value rows.
- **Reads are faster** because a lookup compares keys in one contiguous allocation with a
  fixed header, instead of following a shared buffer per key and per value.

**Costs.**

- **Crash recovery is 26% slower.** Replaying the WAL now builds an entry allocation per
  operation, and replayed writes are copied into the pending buffer instead of sharing it.
- **Closing a writer saves a snapshot when the view holds 100 000 entries or more.** The tracker run
  (300 000 entries in its sequences database) closes in 59 ms instead of 32 ms. This is within
  noise under the rule, but real; it is what makes the next open 2.6× faster.
- **Write throughput:** puts are 8–15% lower and two concurrent writers 25% lower. None of
  these is significant under the rule, but all point the same way: each put now allocates its
  entry instead of sharing the buffer it was given.
- **The snapshot file costs local disk:** 66 MB at 1M keys and 660 MB at 10M keys, next to the
  WAL.

**Records bounded by RAM.**

- Lazy values cover records whose values exceed `lazy_value_threshold` (4 KiB): datasets with
  inputs and outputs, evaluation results, samples. For these, memory no longer grows with the
  data. 100k records of 16 KiB take 90 MB instead of 2.6 GB.
- Small records (labels, scores, alerts) stay resident, at about 100 bytes per entry plus key
  and value. 10M of them take about 1 GB.
- Where that is too much, records tables (see [DESIGN.md](../DESIGN.md)) keep nothing in
  memory.

**Still open.** These are listed in `TODO.md`:

- The first open of a large-value database without a snapshot still peaks at 1.1–1.6 GB, because
  Lance decodes value pages before the view drops them. Capping the scan's batch size (16 MiB) and
  I/O buffer (64 MiB) was measured and did not lower the peak, so it was not kept. The remaining
  fix is a two-pass open: keys, sequence numbers and row addresses first, then only the values at
  or below the threshold, by address.
- Shared key prefixes are stored once per key. Front coding would cut the 10M-key view further.


## Tables: before against after

| Workload | Size | before median [min–max] | after median [min–max] | Change | Verdict |
|---|---|---:|---:|---:|---|
| Tracker run: puts | 5,000 | 417,866 [353,875–433,243] | 386,113 [325,158–413,587] | 8% worse | same |
| Tracker run: put p50 | 5,000 | 1.0 µs [1.0 µs–1.1 µs] | 1.1 µs [1.1 µs–1.1 µs] | 5% worse | same |
| Tracker run: put p99 | 5,000 | 2.0 µs [2.0 µs–3.3 µs] | 4.7 µs [2.5 µs–5.2 µs] | 2.3× worse | 2.3× worse (not significant) |
| Tracker run: close (final flush) | 5,000 | 32 ms [27 ms–54 ms] | 59 ms [39 ms–92 ms] | 87% worse | 87% worse (not significant) |
| Tracker run: writer peak RSS | 5,000 | 281 MB [280 MB–287 MB] | 243 MB [239 MB–245 MB] | 16% better | same |
| Tracker run: stored size | 5,000 | 22 MB [22 MB–22 MB] | 22 MB [22 MB–22 MB] | 0% worse | same |
| Tracker run: S3 requests while writing | 5,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Tracker reader: open both DBs (cold) | 5,000 | 156 ms [147 ms–186 ms] | 61 ms [53 ms–88 ms] | 2.6× better | **better** |
| Tracker reader: list run metadata | 5,000 | 0 ms [0 ms–0 ms] | 0 ms [0 ms–0 ms] | 12% better | same |
| Tracker reader: read one series (val+time) p50 | 5,000 | 1.2 ms [1.2 ms–1.3 ms] | 871.7 µs [846.1 µs–909.7 µs] | 41% better | **better** |
| Tracker reader: whole session | 5,000 | 182 ms [172 ms–212 ms] | 78 ms [70 ms–107 ms] | 2.3× better | **better** |
| Tracker reader: S3 requests | 5,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Small tracker run: puts | 200 | 737,463 [692,738–750,888] | 663,429 [537,635–670,873] | 11% worse | same |
| Small tracker run: put p99 | 200 | 1.9 µs [1.8 µs–2.4 µs] | 2.1 µs [2.0 µs–5.2 µs] | 11% worse | same |
| Bulk load 1M (batches of 1000) | 1,000,000 | 617,958 [568,545–634,647] | 524,136 [180,317–571,485] | 18% worse | same |
| Bulk load: batch p99 | 1,000,000 | 28.9 ms [26.5 ms–41.4 ms] | 24.2 ms [20.1 ms–208.2 ms] | 20% better | same |
| Bulk load: peak RSS | 1,000,000 | 441 MB [426 MB–446 MB] | 293 MB [268 MB–304 MB] | 50% better | **better** |
| Bulk load: stored size | 1,000,000 | 65 MB [65 MB–65 MB] | 65 MB [65 MB–65 MB] | 0% better | same |
| Bulk load: S3 requests | 1,000,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Point reads: open (cold cache) | 1,000,000 | 449 ms [442 ms–455 ms] | 76 ms [73 ms–78 ms] | 5.9× better | **better** |
| Point reads cold: get p50 | 1,000,000 | 0.8 µs [0.8 µs–0.8 µs] | 0.5 µs [0.5 µs–0.6 µs] | 47% better | **better** |
| Point reads cold: get p99 | 1,000,000 | 1.3 µs [1.3 µs–1.5 µs] | 1.0 µs [1.0 µs–1.1 µs] | 35% better | **better** |
| Point reads hot: gets | 1,000,000 | 1,151,215 [884,717–1,188,789] | 1,630,101 [1,266,700–1,661,936] | 42% better | **better** |
| Point reads hot: get p99 | 1,000,000 | 1.3 µs [1.2 µs–2.1 µs] | 0.9 µs [0.9 µs–1.5 µs] | 46% better | 46% better (not significant) |
| Point reads: peak RSS | 1,000,000 | 569 MB [536 MB–578 MB] | 154 MB [151 MB–154 MB] | 3.7× better | **better** |
| Range scan: seek + 100 next p50 | 1,000,000 | 25.0 µs [24.4 µs–31.8 µs] | 11.2 µs [10.8 µs–12.7 µs] | 2.2× better | **better** |
| Range scan: seek + 100 next p99 | 1,000,000 | 56.5 µs [50.0 µs–193.9 µs] | 18.7 µs [16.0 µs–24.6 µs] | 3.0× better | **better** |
| Full scan | 1,000,000 | 6,716,272 [6,492,338–6,779,343] | 14,822,088 [12,478,299–15,158,200] | 2.2× better | **better** |
| Reopen + first get (cold cache) | 10,000 | 10 ms [9 ms–11 ms] | 13 ms [11 ms–20 ms] | 32% worse | **worse** |
| Reopen + first get (cold cache) | 1,000,000 | 451 ms [441 ms–458 ms] | 74 ms [73 ms–76 ms] | 6.1× better | **better** |
| Reopen + first get (cold cache) | 10,000,000 | 5.57 s [5.49 s–5.60 s] | 705 ms [694 ms–754 ms] | 7.9× better | **better** |
| Reopen: peak RSS | 10,000 | 98 MB [98 MB–100 MB] | 95 MB [92 MB–97 MB] | 4% better | same |
| Reopen: peak RSS | 1,000,000 | 547 MB [541 MB–565 MB] | 151 MB [151 MB–151 MB] | 3.6× better | **better** |
| Reopen: peak RSS | 10,000,000 | 3,097 MB [3,040 MB–3,146 MB] | 1,019 MB [1,019 MB–1,019 MB] | 3.0× better | **better** |
| Reopen: S3 requests | 10,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Reopen: S3 requests | 1,000,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Reopen: S3 requests | 10,000,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Large values (100k × 16 KiB): load | 100,000 | 3,862 [2,993–5,263] | 4,762 [3,953–5,000] | 23% better | 23% better (not significant) |
| Large values: reopen | 100,000 | 886 ms [872 ms–926 ms] | 20 ms [14 ms–32 ms] | 45.3× better | **better** |
| Large values: reopen peak RSS | 100,000 | 2,561 MB [2,306 MB–2,741 MB] | 90 MB [88 MB–90 MB] | 28.6× better | **better** |
| Large values: stored size | 100,000 | 4,852 MB [4,852 MB–4,852 MB] | 4,852 MB [4,852 MB–4,852 MB] | 0% worse | same |
| Two writers: combined keys | 200,000 | 1,022,280 [945,402–1,038,494] | 819,434 [740,182–877,917] | 25% worse | same |
| Two writers: keys visible after | 200,000 | 400,001 [400,001–400,001] | 400,001 [400,001–400,001] | 0% better | same |
| Crash recovery: reopen after SIGKILL | 200,000 | 173 ms [166 ms–175 ms] | 218 ms [202 ms–262 ms] | 26% worse | **worse** |
| Crash recovery: keys recovered of 200k | 200,000 | 200,000 [200,000–200,000] | 200,000 [200,000–200,000] | 0% better | same |
| Writable reopen + first get (cold cache) | 1,000,000 | 458 ms [430 ms–465 ms] | 75 ms [74 ms–79 ms] | 6.1× better | **better** |
| Writable reopen + first get (cold cache) | 10,000,000 | 5.57 s [5.51 s–7.31 s] | 715 ms [709 ms–737 ms] | 7.8× better | **better** |
| Writable reopen: peak RSS | 1,000,000 | 551 MB [526 MB–587 MB] | 149 MB [149 MB–151 MB] | 3.7× better | **better** |
| Writable reopen: peak RSS | 10,000,000 | 3,124 MB [3,050 MB–3,150 MB] | 1,019 MB [1,019 MB–1,019 MB] | 3.1× better | **better** |

## First open without a snapshot: before against after

The same build as "after" with the snapshot file removed before every open, as on a machine that has not opened the database before: a full scan into the compact view.

| Workload | Size | before median [min–max] | after, no snapshot median [min–max] | Change | Verdict |
|---|---|---:|---:|---:|---|
| Reopen + first get (cold cache) | 1,000,000 | 451 ms [441 ms–458 ms] | 453 ms [444 ms–457 ms] | 0% worse | same |
| Reopen + first get (cold cache) | 10,000,000 | 5.57 s [5.49 s–5.60 s] | 5.41 s [5.33 s–5.62 s] | 3% better | same |
| Reopen: peak RSS | 1,000,000 | 547 MB [541 MB–565 MB] | 378 MB [366 MB–394 MB] | 45% better | **better** |
| Reopen: peak RSS | 10,000,000 | 3,097 MB [3,040 MB–3,146 MB] | 1,370 MB [1,355 MB–1,404 MB] | 2.3× better | **better** |
| Reopen: S3 requests | 1,000,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Reopen: S3 requests | 10,000,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Large values (100k × 16 KiB): load | 100,000 | 3,862 [2,993–5,263] | 4,850 [3,982–5,596] | 26% better | 26% better (not significant) |
| Large values: reopen | 100,000 | 886 ms [872 ms–926 ms] | 702 ms [679 ms–712 ms] | 26% better | **better** |
| Large values: reopen peak RSS | 100,000 | 2,561 MB [2,306 MB–2,741 MB] | 1,567 MB [1,122 MB–1,614 MB] | 63% better | **better** |
| Large values: stored size | 100,000 | 4,852 MB [4,852 MB–4,852 MB] | 4,852 MB [4,852 MB–4,852 MB] | 0% worse | same |

## What the tests cover

Each test of these changes and what it pins down, followed by known gaps.

**View entries** (`crates/kv0/src/kv/view.rs`, unit tests)

- `newest_wins_regardless_of_order`, `tombstone_blocks_older_put`,
  `late_older_put_stays_hidden_by_range`: the newest-wins rules on the new entry layout.
- `slots_hold_long_keys_and_empty_values`: key lengths above 127 bytes (multi-byte length
  prefix) and empty values.
- `any_order_gives_the_same_view`: 400 random puts, deletes and range deletes applied forwards
  and backwards give the same entries and live count. Loading no longer sorts rows by sequence
  number, so this property is what makes streaming the load correct.

**Value cache** (`crates/kv0/src/kv/cache.rs`)

- `evicts_least_recently_used_beyond_capacity`: least-recently-used eviction, a hit refreshes
  recency, a value larger than the whole capacity is not cached, and the byte count stays within
  capacity.

**Lazy values** (`crates/kv0/tests/lazy.rs`, threshold 16 bytes)

- `large_values_leave_memory_and_read_back`:
  - Unflushed values stay in memory; after a flush, exactly the large ones are stored in Lance.
  - `get`, `multi_get`, forward iteration and `seek_for_prev` return the right bytes.
  - Overwrites read back correctly before and after their flush.
  - A full rewrite (`compact`) moves every row, and the view follows the new addresses.
  - Reopen, and a handle with `lazy_value_threshold=0`, read identical data.
- `file_merging_compaction_moves_stored_values`: automatic compaction through Lance's file
  merge (the path that keeps superseded rows), checked after every flush, then at the end.
- `another_writers_compaction_is_followed_on_refresh`:
  - Handle B reads stored values that handle A committed.
  - A compacts. Before B refreshes, B still reads correctly from the version its addresses refer
    to; after refresh, B reads from the new addresses.
  - A read-only handle with `value_cache_size=0` reads correctly.
  - The refresh reads A's rewritten fragments without copying large values: rows over the
    threshold are applied as addresses (`Row::stored_len`).

- `values_moved_and_cleaned_up_by_another_writer_still_read` (review M1): a reader with
  `refresh_interval` None and then 1 s, a cache of 0 bytes and no snapshot reads a value; another
  writer compacts and runs `cleanup_old_versions(0)`, deleting the files the reader's addresses
  point at. `get`, then `multi_get` and a full iteration after further compactions and cleanups,
  return the right values: the read refreshes once and resolves the keys again.
- `concurrent_misses_on_one_value_read_correctly` (review M2): 8 threads × 20 reads of one
  uncached 100 kB value; misses on one address share a single read.

**Snapshots** (`crates/kv0/tests/snapshot.rs`, minimum 10 entries)

- `reopen_loads_the_snapshot_and_catches_up`:
  - A close saves a snapshot and the next open uses it, with stored values and a range tombstone
    included.
  - Later commits (new keys, a delete, an overwrite) by a handle that does not save a snapshot
    are applied on the next open from the old snapshot.
  - The result equals a full scan.
- `compaction_invalidates_the_snapshot`: another handle deletes a key and compacts with no
  tombstone grace, so the tombstone is gone. The next open must not use the snapshot, which
  still holds the key, and must not show the key.
- `a_read_only_handle_saves_a_snapshot_when_dropped`: a read-only handle (no writes, no flush)
  saves a snapshot when dropped, and the next read-only open uses it.
- `a_damaged_or_foreign_snapshot_is_ignored`:
  - A flipped byte fails the checksum and causes a full scan; the next close writes a good
    snapshot.
  - A dataset deleted and created again at the same path is not matched to the old snapshot.

**Known gaps** (not covered by a test yet)

- Iterators that span a compaction by another writer are covered by the M1 test only after the
  compaction, not with a chunk in flight during it.
- Fsyncing the snapshot (review M3) and the deletion-file identity (review M4) are not exercised by
  a test: the first needs a power cut, the second a deletion that kv0 itself never makes.
- Lazy values and snapshots on S3: covered only by local tests; `tests/s3.rs` runs the basic
  key-value suite.
- The cache bound counts value bytes only, not per-entry bookkeeping (about 60 bytes each).
