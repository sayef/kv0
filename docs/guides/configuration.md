# Configuration

Options are the same in Python and Rust: a keyword of `kv0.Options` or `kv0.DB(...)`, a field of
`kv0::kv::Options`; for tracking, a keyword of `kv0.tracking.Repo(...)`, a field of
`kv0::tracking::RepoOptions`. Durations are seconds in Python and `Duration` in Rust; `None` turns
a periodic task off.

## Key-value store

| Option | Default | Meaning |
|---|---|---|
| `create_if_missing` | `True` | Create the database if it does not exist; otherwise raise `errors.NotFound` |
| `storage_options` | `{}` | Object-store options passed to Lance; see [Storage backends](storage.md) |
| `write_buffer_size` | 4 MiB | Commit once this many bytes of writes are buffered |
| `flush_interval` | 5 | Seconds between background commits of buffered writes; `None` disables |
| `refresh_interval` | 1 | Seconds between checks for other writers' commits; `None` disables |
| `max_fragments` | 32 | Compact automatically above this many data files; `0` disables |
| `lazy_value_threshold` | 4096 | Values longer than this many bytes leave memory once committed and are read from storage when needed; `0` keeps every value in memory |
| `value_cache_size` | 64 MiB | Bytes of values read from storage cached per handle |
| `snapshot_min_entries` | 100 000 | On close, save a snapshot of the view next to the WAL when it holds at least this many entries, so the next open reads only newer data; `None` disables |
| `version_retention` | 7 days | On creation, enable automatic deletion of versions older than this many seconds; `None` keeps all |
| `wal` | `True` | Log every write to a local write-ahead log before acknowledging it |
| `wal_sync` | `"batch"` | `"none"` (never fsync), `"batch"` (fsync about every 100 ms) or `"always"` (fsync per write) |
| `wal_dir` | cache dir | WAL root; else `KV0_WAL_DIR`, else `~/.cache/kv0/wal` (macOS `~/Library/Caches/kv0/wal`) |
| `mark_ttl` | 30 days | How long compaction keeps an inactive writer's WAL mark |
| `replay_stale_wal` | `False` | Replay a WAL older than `mark_ttl` whose mark is gone (writes may be stored twice); otherwise such a WAL fails the open |
| `version` | `None` | Open this version read-only (time travel) |

Rust also has `read_only`, `tombstone_grace` (1 h: how long compaction keeps delete markers, so a
slower writer's older write to the same key stays deleted) and `WriteOptions { sync, disable_wal }`
per write.

## Tracking repository

| Option | Default | Meaning |
|---|---|---|
| `create_if_missing` | `True` | Create the tables if they do not exist |
| `storage_options` | `{}` | Object-store options passed to Lance |
| `flush_rows` | 50 000 | Commit once this many metric points are buffered |
| `flush_interval` | 5 | Seconds between background commits |
| `wal`, `wal_sync`, `wal_dir` | as above | The tracking writes' write-ahead log |
| `replay_stale_wal` | `False` | As above |
| `version_retention` | 7 days | On creation, automatic deletion of older versions |
| `max_fragments` | 32 | Rust: compact a table in the background after a flush above this many small files |
| `compaction_target_rows` | 1 000 000 | Rust: files of at least this many rows are settled and never recompacted |
| `mark_prune_interval` | 1 h | Rust: how often background maintenance prunes WAL mark rows |
| `mark_ttl` | 30 days | Rust: how long a writer's WAL mark rows are kept |

## Environment

| Variable | Meaning |
|---|---|
| `KV0_WAL_DIR` | Root of write-ahead logs and view snapshots when `wal_dir` is not given |
| `AWS_*`, `GOOGLE_APPLICATION_CREDENTIALS`, `AZURE_*` | Credentials, read by the object store when not in `storage_options` |

## Choosing values

- **Write-heavy, on an object store:** raise `write_buffer_size` (or `flush_rows`) and
  `flush_interval`; each commit is several requests.
- **Readers that must see writes quickly:** lower `refresh_interval`, or call `refresh()`.
- **Values of a few KiB that you read often:** raise `lazy_value_threshold` above their size, or
  raise `value_cache_size`.
- **Short-lived processes on large databases:** keep snapshots on; the first open on a machine
  pays a full scan, later opens do not.
