# Benchmarks

The evaluation of kv0 against the pure-Python prototype it replaced (workloads, method, statistics,
verdict) is
[REPORT.md](https://github.com/sayef/kv0/blob/main/bench/REPORT.md), produced by `bench/eval/`:

```sh
python bench/eval/run_all.py          # every cell, 5 repetitions, appends to bench/eval/results.jsonl
python bench/eval/report.py           # bench/REPORT.md and bench/eval/charts/*.svg
```

[MEMORY-REPORT.md](https://github.com/sayef/kv0/blob/main/bench/MEMORY-REPORT.md) compares this
build with a baseline build of kv0 on memory and reopen:

```sh
KV0_EVAL_BASE_PY=.venv-base/bin/python python bench/eval/run_all.py --plan compare \
    --out bench/eval/results-compare.jsonl
python bench/eval/report.py --compare  # bench/MEMORY-REPORT.md
```

`bench/eval/scale.py` runs the tracker's scale scenario (2,000 runs, 4.8M points); REPORT.md
shows it before and after automatic compaction in the tracking layer.

The older quick comparison below is a single run per number:

Two benchmarks, both on synthetic data:

- `bench_kv.py`: the key-value API (`put`, batched `write`, `get`, `seek` plus 100 `next()`, full scan,
  flush, reopen and size on disk) with tracker-shaped keys (`seqs/chunks/run0007/metric03/val/<step>`)
  and 8-byte values. The same script runs against this release and against the pure-Python prototype
  (on a SQLite cache), each in its own environment; set
  `KV0_BASELINE_MODULE` to the prototype's import name.
- `bench_tracking.py`: the tracking tables. Many runs track many metrics with an explicit step and a
  context, then whole series and 100-step windows are read before and after `optimize()`.

Run both with `bench/run.sh` (needs uv; set `N` for the number of keys). It writes JSON to
`bench/results/`, a summary to `bench/results/results.md`, and the tables in the top-level README.

Both run on the local file system. Object stores add latency to every commit and to every read that
refreshes; buffer sizes and intervals matter more there (see the object-store guide).

The Rust micro-benchmarks (`cargo bench -p kv0 --bench kv`) run in CI on every pull request and are
report-only.

`bench/eval/records.py PATH N` measures records tables: N rows in one table next to N rows in
another, half of the keys written twice, then `optimize()`; lookups by item id and a full read.
One run each on the evaluation machine, before (one shared dataset, no index, no folding) and
after (a dataset per table, folded and indexed):

| Rows | lookup p50 before | after | full read before | after |
|---:|---:|---:|---:|---:|
| 100 000 | 5.0 ms | 0.6 ms | 0.37 s (150 000 rows) | 0.27 s (100 000 rows) |
| 1 000 000 | 35.2 ms | 1.0 ms | 4.37 s (1.5M rows) | 3.45 s (1M rows) |

`optimize()` takes 0.13 s and 1.4 s at these sizes (it did nothing for records before).

`bench/eval/records_runs.py PATH`: records of 31 runs out of 2,000 in a 1M-row table (indexed by
`optimize()`), median of five: one `run_ids` query 37.6 ms, 31 `run_id` queries 49.1 ms, a full
read filtered in Python 3,277 ms (15,500 rows each way).

`bench/eval/run_rows.py PATH`: 50 runs that each wrote 400 heartbeat rows (20,050 rows):
`runs()` takes 10.0 ms before `optimize()` folds them and 0.8 ms after.

`optimize()` memory on the 2,000-run, 4.8M-point scenario (`bench/eval/scale.py`), first
optimize of the repository, one process, RSS from `/proc/self/status`:

| | main before | now |
|---|---:|---:|
| Peak | 1.9 GB | 0.96–0.99 GB |
| Resident afterwards | 1.8 GB | 0.66–0.73 GB |
| Time | 2.7 s | 3.1–3.6 s |

An optimize with nothing new takes 0.02 s; after one more run of 2,000 points, 0.2 s.
