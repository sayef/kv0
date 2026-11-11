# Benchmarks

Two reports, both produced by the harness in `bench/eval/` on one machine (Intel i5-13500, NVMe,
Linux), five repetitions per cell. A difference counts as significant only when the ranges do not
overlap and the medians differ by more than 20%; everything else is reported as "same" or "not
significant", and regressions are in the same tables as wins.

- [**Memory and reopen**](https://github.com/sayef/kv0/blob/main/bench/MEMORY-REPORT.md): the
  current build against the build before compact entries, lazy values and snapshots.
- [**kv0 against a SQLite-cache baseline**](https://github.com/sayef/kv0/blob/main/bench/REPORT.md):
  throughput, latency, memory, disk, S3 requests and crash recovery for the tracker workload, bulk
  loads, point reads, scans, reopen, large values and concurrent writers.

## The workloads

- **Tracker run:** an experiment tracker logging one training run: 20 series × 5000 steps, about
  520 000 single puts into two databases (values, epochs, timestamps and running summaries). The
  **tracker reader** opens both databases in a new process, lists the run's metadata and reads
  every series by seek and scan.
- **Bulk load:** 1M keys in batches of 1000. **Point reads:** 20 000 random gets on 1M keys, cold
  and hot. **Range scans:** seek plus 100 `next()`, and a full scan.
- **Reopen:** open an existing database in a new process and read one key, at 10 000, 1M and 10M
  keys, read-only and writable, with and without a view snapshot.
- **Large values:** 100 000 values of 16 KiB. **Crash recovery:** 200 000 single puts, `SIGKILL`,
  reopen. **Two writers:** two processes writing 200 000 keys each into one database.

## Memory and reopen

| | before | now |
|---|---:|---:|
| Reopen 1M / 10M keys | 451 ms / 5.57 s | 74 ms / 705 ms |
| Memory after reopen, 1M / 10M keys | 547 MB / 3.1 GB | 151 MB / 1.0 GB |
| First open without a snapshot, 10M keys | 5.57 s, 3.1 GB | 5.41 s, 1.4 GB |
| 100k × 16 KiB values: reopen | 886 ms, 2.6 GB | 20 ms, 90 MB |
| Seek + 100 × next, p50 | 25 µs | 11 µs |
| Hot point reads | 1.15M gets/s | 1.63M gets/s |
| Crash recovery, 200k puts | 173 ms | 218 ms (worse) |

## Against the SQLite-cache baseline

| | baseline | kv0 |
|---|---:|---:|
| Tracker run, puts/s | 10.4k | 533k |
| Tracker reader session (cold) | 373 ms | 192 ms |
| Bulk load 1M keys, keys/s | 20k | 716k |
| Seek + 100 × next, p50 | 498 µs | 24 µs |
| Two concurrent writers, keys/s | 395k | 1.0M |
| Disk for 100k × 16 KiB values | 1.7 GB | 4.9 GB (worse) |
| `wal_sync="always"`, small tracker run, puts/s | 15.3k (no fsync) | 163 |

This comparison predates the memory work above; the reopen and memory rows of that report are
superseded by the first table.

## The tracker at scale

2 000 runs and 4.8M points logged with one commit per run, then a cold start that reads every
run's parameters and series:

| | before background compaction | now |
|---|---:|---:|
| Logging | 267 s | 92 s |
| Per run, last tenth of the runs | 0.30 s | 0.06 s |
| Start-up | 216 s, 4.8 GB | 20 s, 851 MB |

## Reproduce

```sh
python bench/eval/run_all.py                    # every cell; appends to bench/eval/results.jsonl
python bench/eval/report.py                     # bench/REPORT.md and its charts
python bench/eval/run_all.py --plan compare --out bench/eval/results-compare.jsonl
python bench/eval/report.py --compare           # bench/MEMORY-REPORT.md
python bench/eval/scale.py --repo /tmp/scale    # the tracker at scale
```
