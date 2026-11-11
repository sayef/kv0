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
