# Experiment: an SQLite local tier (rejected)

> **Status: rejected.** This is the evidence record for the decision in
> [DESIGN.md](../../DESIGN.md) ("Alternatives I rejected"). I built the tier as an experiment and
> did not merge it; it is not part of kv0. kv0 keeps the native design: the
> in-memory ordered view, the local write-ahead log and Lance. Links below to `REPORT.md` refer to
> `bench/REPORT.md`.


The same harness and machine as [REPORT.md](../../bench/REPORT.md) (i5-13500, NVMe RAID, Linux 6.12,
Python 3.13), 5 repetitions per cell, the same significance rule. "In-memory" is the `rust-lance`
release build (the whole view in a `BTreeMap`, a custom write-ahead log); "SQLite tier" is this
branch's release build (the view and the pending writes in a per-writer SQLite file, WAL journal
mode, `synchronous=NORMAL` for the default `wal_sync="batch"`). Both survive a killed process.

Reopen is measured two ways, because the tier's fast reopen depends on adopting an existing file:

- **Read-only reopen** (`reopen`): a read-only handle keeps its own cache under `readers/`. The first
  repetition on a machine builds it from Lance; later repetitions reuse it. The table's range shows
  both (the maximum is the first build).
- **Writable reopen** (`reopen-rw`): the writer that loaded the data closed cleanly; the reopen
  adopts its file.


## Verdict

**The tier fixes what it was built for, memory and cold start, decisively, and it costs throughput
across the board.** Every row below is significant under the report's rule (non-overlapping ranges over 5 repetitions,
more than 20% apart), except where a footnote says otherwise.

| What | In-memory view | SQLite tier | Verdict |
|---|---:|---:|---|
| Memory after reopen, 1M / 10M keys | 526 MB / 3.0 GB | 56 MB / 56 MB | **better** (9× / 54×) |
| Reopen (writable, adopting the file), 1M / 10M keys | 450 ms / 5.6 s | 4 ms / 4 ms | **better** (100× / 1300×) |
| Reopen (read-only), 1M keys | 495 ms | 4 ms | **better**¹ |
| Writer memory, tracker run / bulk load | 268 / 420 MB | 215 / 223 MB | **better** (20% / 47% less) |
| Tracker run, tracked puts/s | 505k | 34k | **worse** (15×) |
| Tracker run, close (final flush) | 36 ms | 1.04 s | **worse** (29×) |
| Bulk load 1M, keys/s | 638k | 61k | **worse** (10×) |
| Hot point reads, gets/s | 973k | 481k | **worse** (2×) |
| Seek + 100 × next, p50 | 28 µs | 606 µs | **worse** (22×) |
| Full scan, items/s | 5.5M | 0.79M | **worse** (7×) |
| Tracker reader session (cold) | 187 ms | 698 ms | **worse** (3.7×) |
| Two concurrent writers, keys/s | 1.05M | 204k | **worse** (5×) |
| Crash recovery, 200k unflushed puts | 180 ms | 7.6 s | **worse** (42×) |
| 100k × 16 KiB values: first read-only open | 0.9 s, 2.6 GB | 17.8 s, 2.6 GB | **worse** |
| 100k × 16 KiB values: load, puts/s | 4,458 | 1,953 | **worse** (2.3×) |
| Local disk, 1M keys | none | 149 MB | cost |

¹ A read-only handle keeps its own cache under `readers/`; the first open on a machine builds it
from Lance (8.5 s for 1M keys, 57 s for 10M in this run), and the 10M read-only row is therefore
not significant under the rule even though four of five repetitions took 4 ms. Writable reopen adopts
the writer's file and has no such outlier.

**Where the time goes.** Every write is its own SQLite transaction (pending row and view row in
one commit). Moving SQLite's automatic checkpoints out of the write path (checkpointing after each
Lance flush instead) was the largest single fix, but a transaction still costs 7 µs at the median against 0.9 µs for a `BTreeMap` insert plus a log
append. Iterators read 256 rows per SQL query and copy every key and value; that is the scan gap.
Crash recovery and the first read-only open both rebuild or replay through SQLite row by row.
The large-value reader build holds about as much memory as the in-memory view because Lance's
scanner buffers ahead; streaming smaller batches into the tier should fix that.

**What it would take.** The tier suits processes that open large databases often or must run in
bounded memory (a tracker's server, evaluation records), where it turns a 3 GB, 6-second start
into 56 MB and milliseconds. Its throughput costs are real; the fixes, in order of payoff, would
be batched scans (read a page into a buffer with one statement
and no per-row `Bytes` copies), group commit for single puts (one transaction per short interval
instead of per call, durable on the same terms as `wal_sync="batch"`), and smaller Lance scan
batches when building a cache. Without them, the in-memory view (rust-lance, the design kv0 ships) stays
the faster choice for small databases that are written and read heavily in one process.


## Tables: in-memory view (rust-lance) against SQLite tier

| Workload | Size | in-memory median [min–max] | SQLite tier median [min–max] | Change | Verdict |
|---|---|---:|---:|---:|---|
| Tracker run: tracked puts | 5,000 | 505,452 [454,225–559,937] | 34,225 [18,590–57,999] | 14.8× worse | **worse** |
| Tracker run: put p50 | 5,000 | 0.9 µs [0.9 µs–0.9 µs] | 7.1 µs [7.0 µs–11.9 µs] | 7.7× worse | **worse** |
| Tracker run: put p99 | 5,000 | 1.9 µs [1.9 µs–2.3 µs] | 20.0 µs [18.8 µs–40.2 µs] | 10.5× worse | **worse** |
| Tracker run: close (final flush) | 5,000 | 36 ms [26 ms–45 ms] | 1.04 s [657 ms–2.82 s] | 28.7× worse | **worse** |
| Tracker run: writer peak RSS | 5,000 | 268 MB [263 MB–285 MB] | 215 MB [205 MB–234 MB] | 25% better | **better** |
| Tracker run: stored size | 5,000 | 22 MB [22 MB–22 MB] | 22 MB [22 MB–22 MB] | 0% worse | same |
| Tracker run: S3 requests while writing | 5,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Tracker reader: open both DBs (cold) | 5,000 | 160 ms [159 ms–233 ms] | 661 ms [602 ms–751 ms] | 4.1× worse | **worse** |
| Tracker reader: list run metadata | 5,000 | 0 ms [0 ms–8 ms] | 0 ms [0 ms–0 ms] | 2.4× worse | 2.4× worse (not significant) |
| Tracker reader: read one series (val+time) p50 | 5,000 | 1.3 ms [1.2 ms–1.4 ms] | 1.8 ms [1.8 ms–2.1 ms] | 46% worse | **worse** |
| Tracker reader: whole session | 5,000 | 187 ms [185 ms–267 ms] | 698 ms [639 ms–788 ms] | 3.7× worse | **worse** |
| Tracker reader: S3 requests | 5,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Small tracker run: tracked puts | 200 | 768,403 [510,465–794,285] | 106,167 [101,013–107,537] | 7.2× worse | **worse** |
| Small tracker run: put p99 | 200 | 1.9 µs [1.8 µs–3.7 µs] | 19.2 µs [18.7 µs–20.0 µs] | 10.2× worse | **worse** |
| Bulk load 1M (batches of 1000) | 1,000,000 | 638,143 [620,196–670,442] | 60,748 [54,840–86,333] | 10.5× worse | **worse** |
| Bulk load: batch p99 | 1,000,000 | 23.3 ms [20.2 ms–25.4 ms] | 518.6 ms [411.8 ms–600.8 ms] | 22.3× worse | **worse** |
| Bulk load: peak RSS | 1,000,000 | 420 MB [414 MB–448 MB] | 223 MB [220 MB–242 MB] | 88% better | **better** |
| Bulk load: stored size | 1,000,000 | 65 MB [65 MB–65 MB] | 65 MB [65 MB–65 MB] | 0% better | same |
| Bulk load: S3 requests | 1,000,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Point reads: open (cold cache) | 1,000,000 | 505 ms [499 ms–534 ms] | 4 ms [4 ms–8.51 s] | 137.0× better | 137.0× better (not significant) |
| Point reads cold: get p50 | 1,000,000 | 0.9 µs [0.9 µs–1.3 µs] | 62.2 µs [1.8 µs–63.3 µs] | 65.9× worse | **worse** |
| Point reads cold: get p99 | 1,000,000 | 1.7 µs [1.5 µs–3.2 µs] | 172.4 µs [2.9 µs–175.9 µs] | 102.2× worse | 102.2× worse (not significant) |
| Point reads hot: gets | 1,000,000 | 973,414 [895,565–996,654] | 480,515 [444,388–499,375] | 2.0× worse | **worse** |
| Point reads hot: get p99 | 1,000,000 | 1.6 µs [1.5 µs–1.8 µs] | 2.7 µs [2.4 µs–3.3 µs] | 74% worse | **worse** |
| Point reads: peak RSS | 1,000,000 | 522 MB [516 MB–566 MB] | 109 MB [109 MB–323 MB] | 4.8× better | **better** |
| Range scan: seek + 100 next p50 | 1,000,000 | 27.6 µs [26.8 µs–28.8 µs] | 605.9 µs [488.5 µs–791.5 µs] | 21.9× worse | **worse** |
| Range scan: seek + 100 next p99 | 1,000,000 | 205.4 µs [38.7 µs–221.5 µs] | 826.8 µs [727.1 µs–1.4 ms] | 4.0× worse | **worse** |
| Full scan | 1,000,000 | 5,489,418 [4,846,929–5,627,378] | 791,863 [718,136–1,100,977] | 6.9× worse | **worse** |
| Reopen + first get (cold cache) | 10,000 | 9 ms [9 ms–11 ms] | 9 ms [3 ms–83 ms] | 1% worse | same |
| Reopen + first get (cold cache) | 1,000,000 | 495 ms [482 ms–521 ms] | 4 ms [3 ms–10 ms] | 125.7× better | **better** |
| Reopen + first get (cold cache) | 10,000,000 | 5.59 s [5.57 s–5.60 s] | 4 ms [4 ms–57.24 s] | 1419.7× better | 1419.7× better (not significant) |
| Reopen: peak RSS | 10,000 | 77 MB [75 MB–78 MB] | 55 MB [53 MB–99 MB] | 40% better | 40% better (not significant) |
| Reopen: peak RSS | 1,000,000 | 526 MB [506 MB–562 MB] | 56 MB [54 MB–56 MB] | 9.4× better | **better** |
| Reopen: peak RSS | 10,000,000 | 3,029 MB [2,989 MB–3,099 MB] | 56 MB [53 MB–753 MB] | 54.3× better | **better** |
| Reopen: S3 requests | 10,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Reopen: S3 requests | 1,000,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Reopen: S3 requests | 10,000,000 | 0 [0–0] | 0 [0–0] |  | n/a |
| Large values (100k × 16 KiB): load | 100,000 | 4,458 [2,295–6,080] | 1,953 [1,666–2,020] | 2.3× worse | **worse** |
| Large values: reopen | 100,000 | 929 ms [886 ms–2.53 s] | 17.76 s [14.78 s–21.23 s] | 19.1× worse | **worse** |
| Large values: reopen peak RSS | 100,000 | 2,632 MB [2,091 MB–2,803 MB] | 2,635 MB [2,376 MB–2,690 MB] | 0% worse | same |
| Large values: stored size | 100,000 | 4,852 MB [4,852 MB–4,852 MB] | 4,852 MB [4,852 MB–4,852 MB] | 0% worse | same |
| Two writers: combined keys | 200,000 | 1,046,207 [1,008,826–1,067,346] | 203,940 [195,933–372,238] | 5.1× worse | **worse** |
| Two writers: keys visible after | 200,000 | 400,001 [400,001–400,001] | 400,001 [400,001–400,001] | 0% better | same |
| Crash recovery: reopen after SIGKILL | 200,000 | 180 ms [177 ms–181 ms] | 7.62 s [3.62 s–12.80 s] | 42.4× worse | **worse** |
| Crash recovery: keys recovered of 200k | 200,000 | 200,000 [200,000–200,000] | 200,000 [200,000–200,000] | 0% better | same |
| Writable reopen + first get (cold cache) | 1,000,000 | 450 ms [444 ms–478 ms] | 4 ms [3 ms–5 ms] | 100.5× better | **better** |
| Writable reopen + first get (cold cache) | 10,000,000 | 5.64 s [5.57 s–5.75 s] | 4 ms [4 ms–12 ms] | 1294.7× better | **better** |
| Writable reopen: peak RSS | 1,000,000 | 541 MB [533 MB–559 MB] | 56 MB [56 MB–56 MB] | 9.6× better | **better** |
| Writable reopen: peak RSS | 10,000,000 | 3,109 MB [3,051 MB–3,115 MB] | 56 MB [54 MB–56 MB] | 55.4× better | **better** |
| Local tier file size | 1,000,000 | 0.0 MB [0.0 MB–0.0 MB] | 149 MB [76 MB–149 MB] | not comparable | n/a (one side is ~0) |
