# Results

Key-value, 100,000 keys:

| Operation | prototype | kv0 | Change |
|---|---:|---:|---:|
| put (one key per call) | 10,588 ops/s | 522,730 ops/s | 49.4× faster |
| put with sync=True (durable per call) | 13,467 ops/s | 117 ops/s | 115× slower |
| write (batches of 1000) | 39,855 ops/s | 966,397 ops/s | 24.2× faster |
| get (random existing keys) | 215,787 ops/s | 1,614,808 ops/s | 7.5× faster |
| seek + 100 × next | 12,573 seeks/s | 53,294 seeks/s | 4.2× faster |
| full scan | 552,087 items/s | 8,482,700 items/s | 15.4× faster |
| flush after puts | 12 ms | 32 ms | 3× slower |
| reopen | 0 ms | 54 ms | 187× slower |
| size on disk | 14.6 MB | 7.0 MB | 0.48× the size |

Write-ahead log modes, with a buffer that never fills (no Lance commits during the run), so the rows isolate the cost of the log:

| put (one key per call), kv0 | ops/s |
|---|---:|
| WAL off | 1,286,234 |
| wal_sync="none" (no fsync) | 948,169 |
| wal_sync="batch" (fsync every 100 ms, default) | 972,307 |
| wal_sync="always" (fsync per write) | 165 |

Tracking: 20 runs × 10 metrics × 5000 steps = 1,000,000 points.

| Operation | Result |
|---|---:|
| `track` + flush | 440,189 points/s |
| read a whole series (5000 points), no index | 12.2 ms |
| read 100 steps of a series, no index | 11.0 ms |
| `optimize()` (compaction and index) | 0.1 s |
| read a whole series, indexed | 9.3 ms |
| read 100 steps of a series, indexed | 8.6 ms |
