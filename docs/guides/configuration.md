# Configuration

Options are the same in Python and Rust: a keyword of `kv0.Options` or `kv0.DB(...)`, a field of
`kv0::kv::Options`; for tracking, a keyword of `kv0.tracking.Repo(...)`, a field of
`kv0::tracking::RepoOptions`. Durations are seconds in Python and `Duration` in Rust; `None` turns
a periodic task off.

## Key-value store

| Option | Default | Meaning |
|---|---|---|
| `create_if_missing` | `True` | Create the database if it does not exist; otherwise raise `errors.NotFound` |
| `storage_options` | `{}` | Object-store options (S3 configuration keys); see [Storage backends](storage.md) |
| `write_buffer_size` | 4 MiB | Commit once this many bytes of writes are buffered |
| `flush_interval` | 5 | Seconds between background commits of buffered writes; `None` disables |
| `refresh_interval` | 1 | Seconds between checks for other writers' commits; `None` disables |
| `max_fragments` | 32 | Compact automatically above this many data files; `0` disables |
| `lazy_value_threshold` | 4096 | Values longer than this many bytes leave memory once committed and are read from storage when needed; `0` keeps every value in memory |
| `value_cache_size` | 64 MiB | Bytes of values read from storage cached per handle |
| `snapshot_min_entries` | 100 000 | On close, save a snapshot of the view next to the WAL when it holds at least this many entries, so the next open reads only newer data; `None` disables |
| `version_retention` | 7 days | Every 20 commits, collect the files that only versions older than this many seconds used (the latest and tagged versions stay); `None` leaves them to moraine's vacuum |
| `orphan_grace` | 1 h | Accepted and ignored: moraine deletes a data file no version references (a crashed writer's upload) only after its own grace period |
| `wal` | `True` | Log every write to a local write-ahead log before acknowledging it |
| `wal_sync` | `"batch"` | `"none"` (never fsync), `"batch"` (fsync about every 100 ms) or `"always"` (fsync per write) |
| `wal_dir` | cache dir | WAL root; else `KV0_WAL_DIR`, else `~/.cache/kv0/wal` (macOS `~/Library/Caches/kv0/wal`) |
| `mark_ttl` | 30 days | A WAL left by a writer that never committed, holding writes older than this, is ambiguous (see `replay_stale_wal`). Writers' marks themselves never expire |
| `replay_stale_wal` | `False` | Replay such an ambiguous WAL (writes may be stored twice); otherwise it fails the open |
| `version` | `None` | Open this version read-only: the latest or a tagged one |
| `engine` | `None` | Storage engine of a database created here; only `"moraine"` (see [Storage engine](storage.md#storage-engine)). `None` uses `KV0_ENGINE`, else moraine |

Rust also has `read_only`, `tombstone_grace` (1 h: how long compaction keeps delete markers, so a
slower writer's older write to the same key stays deleted) and `WriteOptions { sync, disable_wal }`
per write.

## Tracking repository

| Option | Default | Meaning |
|---|---|---|
| `create_if_missing` | `True` | Create the tables if they do not exist |
| `storage_options` | `{}` | Object-store options, as above |
| `flush_rows` | 50 000 | Commit once this many metric points are buffered |
| `flush_interval` | 5 | Seconds between background commits |
| `wal`, `wal_sync`, `wal_dir` | as above | The tracking writes' write-ahead log |
| `replay_stale_wal` | `False` | As above |
| `version_retention` | 7 days | As above; each writing handle's background maintenance collects every 20 flushes |
| `orphan_grace` | 1 h | As above |
| `engine` | `None` | As above, for a repository created here |
| `max_fragments` | 32 | Rust: compact a table in the background after a flush above this many small files |
| `compaction_target_rows` | 1 000 000 | Rust: only files below this many rows count towards `max_fragments`; moraine's compaction uses its own target file size |
| `mark_prune_interval` | 1 h | Rust: accepted and ignored (writer marks are moraine transaction ids, not rows) |
| `mark_ttl` | 30 days | Rust: tombstones of deleted runs older than this are removed by `optimize`; also bounds the stale-WAL check as above |

## Environment

| Variable | Meaning |
|---|---|
| `KV0_WAL_DIR` | Root of write-ahead logs and view snapshots when `wal_dir` is not given |
| `KV0_ENGINE` | Storage engine of new databases and repositories when `engine` is not given: only `moraine` (the default) |
| `AWS_*` | S3 credentials, region and endpoint, read when not in `storage_options` |

## Choosing values

- **Write-heavy, on an object store:** raise `write_buffer_size` (or `flush_rows`) and
  `flush_interval`; each commit is a few requests.
- **Readers that must see writes quickly:** lower `refresh_interval`, or call `refresh()`.
- **Values of a few KiB that you read often:** raise `lazy_value_threshold` above their size, or
  raise `value_cache_size`.
- **Short-lived processes on large databases:** keep snapshots on; the first open on a machine
  pays a full scan, later opens do not.
