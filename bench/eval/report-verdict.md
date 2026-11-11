## Verdict

**0.2 is significantly better at everything that moves data** (writing, reading, scanning,
concurrent writers, S3 traffic for a training run) and **significantly worse at memory, cold start
and storage overhead for large values**. Both directions are large, not marginal. Every row below
is consistent across 5 repetitions (non-overlapping ranges, more than 20% apart) unless marked.

| Workload | Like for like (0.1 vs 0.2 default, local) | On S3 (RustFS) | Verdict |
|---|---|---|---|
| (a) Tracker run, writing (≈520k puts) | 10.4k → 533k puts/s (**51×**); p99 88 → 2.5 µs | 10.6k → 276k puts/s (**26×**); 79 → 46 requests | better |
| (a) Tracker run, writer memory | 43 → 287 MB (**6.7× worse**) | 85 → 301 MB (3.6× worse) | worse |
| (a) Tracker reader session (open, list, read 20 series, cold cache) | 373 → 192 ms (**1.9×**); opening alone 1 → 167 ms (worse) | 1.83 s → 376 ms (**4.9×**); 21 → 45 requests (worse) | better overall |
| (b) Bulk load 1M keys | 20k → 716k keys/s (**36×**); 137 → 65 MB on disk | 18k → 199k keys/s (**11×**); 41 → 77 requests (worse) | better |
| (c) Point reads, hot | 240k → 1.26M gets/s (**5.2×**), p99 5.4 → 1.2 µs | 249k → 1.22M (**4.9×**) | better |
| (c) Point reads, cold page cache | p50 96 µs → 0.7 µs (**130×**), but open 0 → 455 ms | open 2.96 s → 0.77 s (**3.8× better**) | better per read, worse to open |
| (d) Range scans | seek+100 p50 498 → 24 µs (**21×**); full scan 0.38M → 7.2M items/s (**19×**) | p50 58 → 24 µs (2.4×); p99 not significant | better |
| (e) Reopen, 10k / 1M / 10M keys | 1 ms → 11 ms / **468 ms** / **5.8 s** | 10k: 173 → 135 ms; 1M: 3.6 s → 0.8 s (**4.4× better**) | worse locally, better on S3 |
| (e) Memory after reopen, 10k / 1M / 10M | 24 MB → 98 MB / **559 MB** / **3.08 GB** | 120 → 685 MB at 1M | worse |
| (e) Large values, 100k × 16 KiB | load 7.0k → 5.2k/s (**25% worse**); reopen 8 ms → 903 ms; RSS 23 MB → **2.7 GB**; disk 1.70 → **4.85 GB** | not measured | worse |
| (f) Two concurrent writers | 395k → 1.0M keys/s (**2.6×**); all 400k visible in both | 129k → 191k keys/s (48%); all visible | better |
| (g) Crash recovery, 200k unflushed puts | all 200k recovered by both; reopen 0.5 ms → 164 ms | — | same data, slower reopen |

**Stronger durability (`wal_sync="always"`, fsync per write)** is not what 0.1 offered (0.1 never
fsynced a put): on the small tracker run it is **94× slower than 0.1** (15.3k → 163 puts/s, p99 21 µs →
13 ms, an fsync on this disk); on bulk loads, where one fsync covers 1000 keys, it is still
**4.5× faster** than 0.1 (20k → 90k keys/s). Recovery after SIGKILL at 20k fsynced puts recovers all.

### The regressions, named

1. **Memory.** 0.2 keeps every live key and value in an in-memory ordered map. Peak RSS is
   20–130× 0.1's at the same data: 559 MB at 1M small keys, 3.1 GB at 10M, 2.7 GB for 1.6 GB of
   values. 0.1's SQLite file kept RSS at 24 MB regardless of size. This is the largest regression
   and it scales with data, not with load.
2. **Cold start.** Opening reads the whole operation log into that map: 0.47 s at 1M keys, 5.8 s
   at 10M, 0.9 s for 1.6 GB of values, against about 1 ms for 0.1 locally. (On S3 0.2 still wins,
   because 0.1 has to download and merge segments into a new local cache.)
3. **Storage overhead for large values.** Automatic compaction rewrites fragments, and the
   superseded files stay until version cleanup (7-day retention by default): 4.85 GB on disk for
   1.70 GB of incompressible values, 2.9× 0.1. For small tracker-shaped values Lance's encoding wins
   instead (65 MB against 137 MB).
4. **Synchronous durability.** `sync=True` (commit to Lance) and `wal_sync="always"` (fsync each
   write) cost an fsync or a Lance commit per call; 0.1 offered neither guarantee, so there is no
   like-for-like row, but code that relies on them is 10²–10³× slower per write.
5. **S3 requests on bulk load and reopen.** 77 against 41 requests for a 1M bulk load, 92 against
   11 to reopen 1M keys (Lance reads manifests, fragment metadata and data files separately). A
   training run writes with fewer requests (46 against 79).
6. **Crash recovery** replays and commits the WAL on open: 164 ms for 200k writes, where 0.1 had
   nothing to recover. Correctness is equal (all acknowledged writes come back in both).

### Is 0.2 significantly better?

For the workload kv0 exists for (an experiment tracker writing metrics and a reader opening them),
**yes**: writes are 26–51× faster, UI sessions 2–5× faster, scans and reads 5–20× faster, and
concurrent writers and S3 work correctly with fewer requests while training. It is **not** better
for a process that opens large databases often or must run in little memory: at 10M keys it needs
3 GB and 6 s to open what 0.1 opened in 1 ms with 24 MB.

### Decision: fix memory and reopen natively

Regressions 1–2 come from one design choice: the whole view in process memory, rebuilt from Lance
on every open. I keep that design (the in-memory view, the local WAL and Lance) and do not add
a second storage engine, so the fixes stay inside it:

- **Large values out of the view.** Values above a size threshold stay in Lance and are fetched by
  row id on demand, through a bounded cache. This targets the 2.7 GB of large-value memory.
- **Compact keys and entries.** Less per-entry overhead in the view, which targets the 3.1 GB at
  10M small keys.
- **A snapshot for reopen.** A compact snapshot of the view, tagged with the Lance version it
  reflects, is loaded at open, and only fragments committed since are applied. This targets the
  5.8 s reopen.
- **Storage (regression 3)** is separate: compaction should clean up the versions it supersedes
  once readers no longer need them, instead of waiting for the retention period.

I built and measured an SQLite local tier as an alternative. It fixed memory and reopen (10M keys:
56 MB and 4 ms) but cost 7–22× throughput and slower crash recovery, so I dropped it. The
record is in [docs/design/sqlite-tier-experiment.md](../docs/design/sqlite-tier-experiment.md).
I rejected RocksDB, LMDB, redb and fjall as well; Lance stays the durable, shared source of truth.
