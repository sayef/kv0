The question: is kv0 (Rust, Lance, local write-ahead log) significantly better than the pure-Python
prototype it replaced (pure Python, a local SQLite file per database, optional S3 sync of segment files)? Everything here is
reproducible with `python bench/eval/run_all.py` followed by `python bench/eval/report.py`; raw
results are in `bench/eval/results.jsonl`.

## Method

**Machine.** Intel Core i5-13500 (6 P-cores + 8 E-cores, 20 threads), 62 GB RAM, two Samsung PM9A1
NVMe drives in software RAID (md), Linux 6.12 (Debian), Python 3.13. One machine for everything.
kv0 is the release build (thin LTO) of this repository; the prototype is the last pure-Python version,
installed in its own virtual environment.

**S3.** RustFS (an S3-compatible server) in Docker on the same machine, behind a small counting
proxy (`bench/eval/s3proxy.py`) so request counts are exact for both versions. Latency is loopback,
so S3 timings are a lower bound; request counts carry over to real S3 directly. The prototype talks to S3
through its own sync layer (`KV0_S3_*`, background flush every 5 s, `flush()`/`close()` push);
kv0 opens `s3://…` through Lance.

**Durability.** The prototype's SQLite runs in WAL mode with `synchronous=NORMAL`: every write
is in the SQLite log before it returns (it survives a killed process) but is not fsynced, so a power
loss can lose recent writes. kv0's default `wal_sync="batch"` is the same guarantee class (written to
the OS before returning, fsync about every 100 ms), so **the prototype against kv0's default is the
like-for-like comparison**. `wal_sync="always"` fsyncs every write, which is *stronger* than the prototype; it is included as
asked, on the smaller workloads (an fsync costs about 6 ms on this disk).

**Repetitions and statistics.** Every cell runs 5 times, each in a fresh process (write cells on a
fresh database). Tables give the median and the min–max range. A difference is **better** or
**worse** only when the two ranges do not overlap (for 5 against 5 that is the most extreme
Mann–Whitney U, two-sided p = 0.008) *and* the medians differ by more than 20%; otherwise it is
"same" or "not significant". Read-cell datasets are built once per engine and size and reused.
"Cold" cells run in a new process after evicting the dataset files from the page cache with `posix_fadvise(DONTNEED)` (`/proc/sys/vm/drop_caches` is read-only in this container); "hot" is a second pass in
the same process.

**Workloads** (synthetic data only):

- **Tracker run (a).** The key-value traffic of an experiment tracker logging one training run,
  synthesised: 20 series (10 metrics × 2 contexts) × 5000 steps. Per tracked value the tracker
  writes three puts to the run's `seqs` database (`val`, `epoch`, `time`, keyed by step) and two to
  its `meta` database (`last`, `last_step`), plus `min`/`max` when they change and four puts when a
  series starts: about 520 000 single-key puts into two databases, then `close()`. The **tracker
  reader** part opens both databases read-only in a new process (cold page cache), lists the run's
  metadata (full scan of `meta`), reads every series (seek to the `val` and `time` prefixes and
  iterate to the end) and reads each series' `last`. A **small tracker run** (200 steps, about
  21 000 puts) is used where every write is fsynced.
- **Bulk load (b).** 1 000 000 tracker-shaped keys (8-byte values) in `WriteBatch`es of 1000, then
  `close()`.
- **Point reads (c).** 20 000 random `get`s on a 1M-key database: cold (first pass after opening in a
  new process with the dataset evicted from the page cache) and hot (second pass).
- **Range scans (d).** 200 × (`seek` to a random prefix + 100 `next()`), and one full scan.
- **Reopen / cold start (e).** Open an existing database in a new process (dataset evicted from the page cache) and
  read one key, at 10 000, 1 000 000 and 10 000 000 keys (S3: 10 000 and 1 000 000, from a new empty
  local directory, which is a new machine for the prototype). Plus **large values**: 100 000 × 16 KiB.
- **Two writers (f).** Two processes write 200 000 keys each (batches of 100) to one database at the
  same time; a third process then counts what is visible.
- **Crash recovery (g).** A process writes 200 000 single puts, never flushes and SIGKILLs itself; the
  reopen that recovers is timed and the recovered keys counted.

Measured: throughput, p50/p99 latency per operation, peak RSS (`ru_maxrss`), bytes on disk (or in the
bucket) and S3 requests.
