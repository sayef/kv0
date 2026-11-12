# kv0 design

kv0 is an embedded storage engine on [Lance](https://lance.org) datasets, on any object store
Lance supports. It has a Rust core (`crates/kv0`) and Python bindings (`crates/kv0-py`), and two
layers:

1. **Key-value layer** (`kv0::kv`, Python `kv0.DB`): an ordered key-value store. Keys sort in
   byte-wise (memcmp) order; writes are single puts and deletes, range deletes and atomic
   `WriteBatch`es; reads are `get`, `multi_get` and iterators with `seek`, `seek_for_prev`,
   `seek_to_first` and `seek_to_last`.
2. **Tracking layer** (`kv0::tracking`, Python `kv0.tracking`): typed, columnar tables
   for runs, parameters and metric series, the storage of an experiment tracker.

This document records my decisions, the guarantees kv0 gives and the alternatives I rejected.
`docs/architecture.md` is the shorter tour of the code.

## Goals and non-goals

Goals:

- One storage engine for every backend: a local directory, `memory://`, S3 and S3-compatible
  stores (MinIO, RustFS, R2), Google Cloud Storage and Azure Blob Storage. A location is a path
  or URI given to Lance, and credentials are Lance storage options.
- No storage protocol of our own: no segment files, no manifest, no direct S3 calls. Lance owns
  files, versions and atomic commits.
- A familiar ordered key-value API (`get`, `put`, `delete`, `delete_range`, `WriteBatch`,
  iterators with `seek` and `seek_for_prev`), in Rust and Python.
- A tracker-shaped schema for new work: high-rate metric appends, step-range reads, many runs
  times many metrics.
- Honest, documented consistency guarantees.

Non-goals (for now): merge operators, column families, snapshots of the
key-value store other than storage versions, SQL over the key-value store.

## Why Lance, and what it costs

Lance gives atomic, versioned commits on object stores (conditional writes on S3, rename or
conditional put elsewhere), concurrent appenders that do not conflict, time travel by version,
compaction, scalar indices, and fast random access. kv0 needs no storage protocol of its own: no
segment files, no manifest of its own, no compare-and-swap on object keys.

The cost is commit granularity. Every Lance commit writes at least one data file and one
manifest; on S3 that is several requests and tens to hundreds of milliseconds. A per-`put`
commit is out of the question. Both layers therefore **buffer writes in memory and commit in
batches**, and the guarantees below follow from that.

## Key-value layer

### Storage format

One Lance dataset per database (the path or URI given to `DB`). It is an operation log:

| column  | type      | meaning |
|---------|-----------|---------|
| `seq`   | `uint64`  | sequence number; newest wins |
| `op`    | `uint8`   | 0 = put, 1 = delete, 2 = delete range |
| `key`   | `binary`  | the key, or the inclusive start of a range |
| `value` | `binary?` | the value; the exclusive end of a range; null for a delete |

`seq` is a hybrid logical clock in nanoseconds since the Unix epoch: each new number is greater
than the wall clock and than every number the handle has seen. Local writes therefore always win
over anything loaded, and concurrent writers order by wall-clock time.

### Reads: an in-memory ordered view

On open, the handle reads the dataset and applies the rows into an ordered set of entries plus a
list of range tombstones. Every read (`get`, `multi_get`, iterators) is served from this set, so
keys come back in byte-wise order without I/O. Applying is idempotent and order-independent: a row
is ignored when the key already holds a newer `seq`, when a newer point tombstone exists, or when a
newer range tombstone covers it. Rows are applied batch by batch as Lance decodes them, so an open
never holds the dataset twice.

Three measures keep memory and reopen time in check without a second storage engine:

- **One allocation per entry.** An entry is a single byte string (sequence number, flags, key
  length, key, value) in a `BTreeSet` ordered by key, instead of a map entry with separate key and
  value buffers.
- **Large values stay in Lance.** A committed value longer than `lazy_value_threshold` (4 KiB)
  is replaced in the view by its row address and length. Reads take such values from Lance in one
  request per lookup, `multi_get` or iterator chunk, through a byte-bounded LRU cache
  (`value_cache_size`, 64 MiB). Unflushed values stay in memory until committed. Every commit that
  moves rows (this handle's appends and compactions, and other writers' compactions picked up by a
  refresh) reports the new addresses, and the view records them together with the dataset version
  they refer to, so a reader always resolves an address against the version it came from. If
  that version's files are gone (another writer compacted and then ran `cleanup_old_versions`
  with a short age), the read refreshes the handle and resolves the key again at the new
  version; only if that also fails does it return a storage error, never "not found". Concurrent
  misses on one address share a single read. A full
  rewrite streams stored values batch by batch instead of loading them all.
- **A snapshot for reopen.** On close, a handle whose view holds at least `snapshot_min_entries`
  entries (100 000) writes the view, the dataset version and the identity of each fragment
  (its first data file, which Lance names uniquely, its row count and its deletion file)
  to `view.snap` in the dataset's WAL directory, with a checksum. The file is fsynced before it
  is renamed into place, and the directory after. The next open uses it only if
  every fragment it covers is still present and unchanged; it then reads just the fragments
  committed since. A compaction (which may drop tombstones), a recreated dataset, a different
  `lazy_value_threshold` or a damaged file sends the open back to a full scan. Snapshots hold only
  committed state: a handle with unflushed writes does not save one.

A tracker's databases are typically per run (one for metadata, one for sequences) plus an
index, which fits comfortably.

Alternatives I rejected for the local view:

- **RocksDB, LMDB, redb, fjall** as a local tier. I don't want a second storage engine next to
  Lance: it doubles the formats to operate, and the point of kv0 is one engine on every backend.
- **SQLite local tier**: I built and benchmarked it, then dropped it. It fixed memory and reopen
  (10M keys: 56 MB, 4 ms) but cost 7–22× throughput and slower recovery. The measurements are
  in [docs/design/sqlite-tier-experiment.md](docs/design/sqlite-tier-experiment.md).

Rejected: serving reads straight from Lance with filtered scans. A seek is a range scan that
Lance cannot answer in key order without sorting, and every `get` would plan a query; both are
orders of magnitude slower than a tracker's access pattern (many small puts, seeks and short
scans) needs.

### Writes: buffer, then append

A write is applied to the view at once (read-your-writes) and appended to a pending buffer. The
buffer is committed as **one Lance append** (one fragment, one commit):

- when it exceeds `write_buffer_size` (default 4 MiB),
- every `flush_interval` (default 5 s) from one shared background thread,
- on `flush()`, `flush_wal()`, `compact_range()`, a `sync=True` write and `close()`,
- when the last handle is dropped.

A `WriteBatch` is applied under one lock and never split across commits, so it is atomic for
local readers and in storage. If a commit fails, the operations go back to the front of the
buffer and the next flush retries them.

In Python, handles still open at interpreter exit are flushed by an `atexit` hook, so a script
that never calls `close()` keeps its writes.

### Write-ahead log

Every `put`, `delete`, `delete_range` and `write(batch)` is appended to a **local** write-ahead
log before it is applied or acknowledged; Lance commits stay batched as above.

- **Location:** always local disk, also when the dataset is on S3, GCS or Azure:
  `<root>/<xxh3 of the dataset URI>/<writer id>/`, where the root is `wal_dir`, else
  `KV0_WAL_DIR`, else the platform cache directory (`~/.cache/kv0/wal`,
  `~/Library/Caches/kv0/wal`). Each open handle is one writer with its own directory,
  held by an advisory file lock while it runs.
- **Format:** segment files of records `len: u32 | xxh3-64: u64 | payload`; one record per write
  call, so a batch is one record and replays all or nothing.
- **Durability modes** (`wal_sync`): `none` writes to the OS on every call and never fsyncs
  (survives a process crash, not a power loss or kernel crash); `batch` (default) also fsyncs
  about every 100 ms from the background thread (a power loss can lose the last ~100 ms);
  `always` fsyncs before every write returns. `sync=True` still means committed to Lance before
  the call returns.
- **Truncation:** a flush seals the current segment and opens a new one under the same lock that
  takes the buffered operations, so the sealed segments hold exactly what the commit covers;
  after the Lance commit succeeds they are deleted. A clean close removes the writer's directory.
- **Replay:** a writable open claims every writer directory under the dataset's WAL directory
  whose lock it can take (writers that are no longer running, including earlier runs of this
  program), reads their segments and applies the operations that are not committed yet to the
  view and the buffer, writes them into its own WAL, deletes the claimed directories and
  commits. Operations keep their original sequence numbers, so replay never reorders writes.
- **Idempotency:** each commit that covers WAL operations also writes a *mark row* in the same
  fragment: an operation of kind 3 holding the writer id and its highest committed sequence
  number. The mark is atomic with the data it covers, compaction carries the latest mark of every
  writer forward, and version cleanup never removes it, so replay skips every operation at or
  below the writer's mark however old the WAL is. Marks are one small row per writer that ever
  committed. (Lance transaction properties were the first design; they disappear with cleaned-up
  versions, so a WAL older than the retention period could have been replayed twice.)
- **Per-write opt-out:** `disable_wal=True` (`WriteOptions::disable_wal` in Rust) skips the log for that write. It is committed with the next flush like any other, and is
  lost if the process crashes before then.
- **Failed appends:** a write that fails part-way (a full disk, an I/O error, a failed fsync in
  `always` mode) is cut back out of the segment before the error is returned, so a reported
  failure is never replayed and a partial record never sits in front of later acknowledged ones
  (replay would take it for a torn tail and drop them). If the cut fails, the WAL moves to a new
  segment; if that fails too, it refuses writes until a flush rotates it.
- **Writer ids are reused:** a clean close keeps the writer's (empty) directory, and the next
  open on the machine adopts an idle directory and its writer id. Each id leaves one mark row, so
  marks stay at the number of writers that ran at the same time, not one per open. Compaction drops
  marks older than `mark_ttl` (default 30 days). A WAL whose writer's mark is gone is ambiguous
  only if it holds an operation older than `mark_ttl` (a committed operation's mark is at least
  its sequence number, so newer operations are certainly uncommitted, however old the segment
  file): then the open fails with an error naming the directory, and `replay_stale_wal=True`
  replays it anyway (writes may then be stored twice).
- **Integrity:** a record cut off by the end of the last segment is a torn write from a crash and
  is dropped (and cut from the file). A checksum mismatch anywhere else fails the open with
  `Corruption`, naming the file; move it away to open the database without it.
- **Read-only handles and `memory://`** neither create nor replay a WAL. A WAL is only
  recoverable on the machine that wrote it: if the machine is lost before the next flush, the
  writes since the last commit are lost with it.
- Compaction never writes operations that are still buffered (it runs only when the buffer is
  empty right after a flush), so no operation is committed twice through compaction either.

The tracking layer uses the same WAL (directory keyed by `repo:<uri>`, record kinds 20 to 23 for
run changes, parameters, series and points). A flush writes up to four commits (runs, params,
series, metrics); each carries its own mark row (a reserved `run_id` prefix `__kv0_wal__/`
in `runs`, `params` and `series`, `series_id = 0` in `metrics`), so a crash between two of them
replays exactly the tables that were not committed. Readers skip mark rows. After a flush, a table with more than `max_fragments`
(default 32) fragments is compacted automatically, and mark rows are pruned to each writer's newest
(and dropped after `mark_ttl`) at the same time; `optimize()` does both as well. Finding the marks
scans the four tables, which happens only when an abandoned WAL is found.

Rejected: putting the log in the object store. Each logged write would be an object-store request
(tens of milliseconds), and the log would need its own commit protocol next to Lance's.

### Compaction and write amplification

Each flush adds a fragment, so the dataset accumulates small files and shadowed rows. After a
flush, when the dataset has more than `max_fragments` (default 32) fragments:

- if most stored rows are shadowed (stored rows > 2 × live keys + 1024), the handle
  **materialises** the view: it writes the live rows (sorted by key) and recent tombstones as new
  fragments, and commits one Lance `Update` that removes every fragment it read and adds the new
  ones;
- otherwise it runs Lance's `compact_files` (a `Rewrite`), which merges small fragments without
  changing rows.

`compact_range()` always materialises. Lance's conflict rules let an `Update` or `Rewrite` commit
alongside concurrent appends (the appended fragments are untouched), so compaction does not
block or lose other writers' data. Two concurrent compactions of the same fragments conflict;
the loser gives up and tries again after a later flush, and its flush still succeeds.

Write amplification is therefore bounded by one full rewrite per `max_fragments` flushes when
the workload overwrites keys, and by Lance's file merging otherwise. A size-tiered scheme that
rewrites only recent fragments is a possible refinement.

**Tombstones** (point and range) are kept by compaction for `tombstone_grace` (default 1 h), and
removed from the in-memory view at the same time, so the view stays equal to what a reopen loads; so a
slower writer's older write to a deleted key stays deleted. After the grace period they are
dropped, and such a very late write could reappear. This is the usual tombstone-GC trade-off.

### Concurrency and consistency

- **One process, many threads:** a handle is thread-safe. Reads see every write made through the
  handle, including unflushed ones.
- **Several handles or processes on one database:** appends from different writers never
  conflict; Lance retries the commit on a newer version. Each handle sees other writers'
  committed data when it **refreshes**: reads check for a newer version at most every
  `refresh_interval` (default 1 s), and `refresh()` checks now. A refresh reads only the
  fragments the handle has not seen, so it costs one listing plus the new data.
- **Conflicting writes to one key** from different writers resolve last-writer-wins by `seq`
  (wall-clock time). There are no transactions across handles.
- **Iterators** are not snapshots. They read the view in chunks of 256 entries, so they observe
  writes beyond their position made before the next chunk is fetched, and never return a key
  twice or out of order.
- **Read-only handles** (`read_only=True`, or opening a `version`) reject writes with
  `NotSupported`.
- **Time travel:** every commit is a Lance version. `DB(path, version=n)` opens version `n`
  read-only, `versions()` lists them and `cleanup_old_versions(seconds)` deletes old ones.
  Lance 14 keeps every version by default, which with a commit every few seconds would keep
  every superseded file forever; kv0 creates datasets with Lance's automatic cleanup
  (every 20 commits, versions older than `version_retention`, default 7 days). Time travel
  therefore reaches back 7 days unless configured otherwise.

An application that serialises its writers per database (with its own lock files, say) gets
stronger guarantees than the above.

### Python API

`kv0.DB` subclasses the native `_DB`: `DB(path, opts,
column_families, read_only, **kwargs)`, `get(key=...)`, `put/delete/delete_range/merge` with the
`sync`/`disable_wal`/`column_family` keywords, `write(WriteBatch)`, `multi_get`,
`key_may_exist`, `iterkeys/itervalues/iteritems`, iterator `seek`, `seek_for_prev`,
`seek_to_first`, `seek_to_last`, `get()` (peek, `ValueError` past the end), `next()` (return and
advance, `None` past the end), `skip()` and the iterator protocol, `flush`, `flush_wal`,
`compact_range`, `close`, and the column-family and property shims. `Options` is a permissive
bag: unknown engine-tuning options are ignored. `kv0.errors` has `NotFound` (also a
`FileNotFoundError`) and `StoreIOError` (also an `OSError`), among others.

A database on S3 is `DB("s3://bucket/prefix/db", storage_options={...})`; an application with a
repository of many databases maps its root to a URI prefix.

## Tracking layer

### Tables

A repository is a location with four Lance datasets, plus `records.lance` (below):

`runs.lance`: one row per change of a run. Each row says which fields it sets (`fields`, a
bitmask; null in rows written before it existed means all of them), and a read merges a run's rows
field by field, newest `seq` first. Two writers changing different fields of one run therefore
never undo each other; a later change of the same field still wins. `Run::update` and
`Repo::update_run` write only the fields they changed. Tables written before `fields`, `kind` and
`target` existed gain them as null columns on open (a metadata-only commit).

`optimize` folds `runs`: each live run's rows become one row per sequence number at which some
field was last set, each holding the merged record and the mask of those fields (usually two or
three rows), in one commit that keeps rows appended meanwhile. Every field keeps its own `seq`,
so a row committed later with an older `seq` (buffered in another handle, clock skew, a replayed
WAL) wins or loses each field exactly as against the unfolded rows; fields only the run's first
row gave (taken whole, outside its mask) stay tied to that row. Tombstones, rows of deleted runs and WAL mark rows
are kept as they are.

Two limits of the per-field merge: `tags` is one field, so two writers adding different tags at
once keep only the later list; and with clock skew between machines a change can sort before the
run's creation row and lose to it, as with any last-writer-wins by `seq`.

A **tombstone** is a row whose `fields` is exactly the `DELETED` bit. `delete_runs` commits
tombstones first, then deletes the runs' points, statistics, series, parameters, records and
other rows. Reads ignore every row of a tombstoned run whatever its `seq`. A flush reads the
`runs` fragments it has not read before for tombstones (its own previous commits excluded),
checking for a newer `runs` version at most once a second, so a writer can commit writes to a
run deleted elsewhere for up to a second plus one flush; those rows stay hidden and `optimize`
removes them. It drops buffered writes to deleted runs
(a writer that keeps going, or a WAL replayed later), and the handle's next `track`, `update` or
`finish` of such a run fails with `NotFound`. A point whose series is in neither the buffer, the
handle's cache nor the series table belongs to a deleted run and is dropped too. A writer racing
the delete can still commit one flush; its rows stay hidden, and `optimize` removes them (points
of series that no live run names are dropped by the sorted rewrite). Tombstones older than
`mark_ttl` are removed by `optimize`, with any rows of their runs: no WAL can be replayed after
that without `replay_stale_wal`.

| column | type |
|---|---|
| `seq` | `uint64` |
| `run_id` | `utf8` (UUIDv7 hex unless given) |
| `name`, `experiment`, `description` | `utf8?` |
| `tags` | `list<utf8>` |
| `status` | `utf8`: running, finished, failed, aborted |
| `archived` | `bool` |
| `created_at`, `updated_at`, `ended_at?` | `timestamp[us, UTC]` |
| `fields` | `uint32?`: the fields this row sets |
| `kind` | `utf8?`: training, eval, ... |
| `target` | `utf8?` (JSON): what the run is about |

`params.lance`: flattened parameters, one row per (run, dotted key) change; highest `seq` wins.

| column | type |
|---|---|
| `seq` | `uint64` |
| `run_id`, `key` | `utf8` (`optimizer.lr`) |
| `value` | `utf8` (JSON) |
| `num`, `text`, `flag` | `float64?`, `utf8?`, `bool?`: the scalar repeated for filtering |

Nested objects flatten to dotted keys; arrays and scalars are leaves. Setting a key merges; it
does not delete siblings.

`series.lance`: one row per metric series.

| column | type |
|---|---|
| `series_id` | `uint64`: xxh3 of run id, name and canonical context |
| `run_id`, `name` | `utf8` |
| `context` | `utf8`: JSON with sorted keys, `{}` when empty |
| `created_at` | `timestamp[us, UTC]` |

`metrics.lance`: append-only points.

| column | type |
|---|---|
| `series_id` | `uint64` |
| `step` | `int64` |
| `epoch` | `int64?` |
| `timestamp` | `timestamp[us, UTC]` |
| `value` | `float64` |

`metrics` also holds **statistics deltas**: reserved rows (`series_id` 0, the series id in
`step`, the encoded delta in the nullable `stats` column, added to older tables on open). A
delta holds the count, first and last step, last value (ordered by step, then sequence number),
minimum and maximum with their steps, non-finite count and last timestamp of some points of one
series. Each flush appends one delta per series it touched in the same commit as the points, so
statistics are atomic with their points, cost no extra commit and need no read at flush, and
merging deltas is associative and commutative, so concurrent writers to one series both count.
Reading statistics merges the deltas (a filtered scan of the reserved rows). A delta is *dirty*
when its points may overwrite a step counted elsewhere (a writer wrote a step at or below one it
had written to that series, a WAL was replayed, or `recompute_series_stats` asked); reads compute
dirty series from their points, deduplicated by step as reads see them. `optimize` replaces each
series' deltas by one exact delta computed during its sorted rewrite.

`optimize` rewrites only the metrics fragments it has not sorted before (recorded in the
dataset metadata as `kv0.sorted_fragments`), and does nothing when there are none. When a new
point is at or below the last step a sorted fragment holds for its series (a step written again,
or out of order), it rewrites every fragment, so overwritten points are dropped and statistics
stay exact. It also rewrites everything when a sorted fragment has disappeared (merged by background
compaction) or the fragments to rewrite hold statistics an earlier `optimize` wrote, so sorted
points and their exact statistics are never split between the kept and rewritten parts. Time-range reads first
find the steps with a point in the range, then read only those steps of each series (with any
later points at them) and apply the range after choosing the latest point of each step.

`records/<table>.lance`: one dataset per records table, rows `(seq, table_name, run_id?, item_id,
data, created_at)` keyed by `(run_id, item_id)`; the newest `seq` of a key wins, at read time
always and physically once `optimize` folds superseded rows. `optimize` also keeps B-tree indices
on `item_id` and `run_id`. A dataset per table costs a manifest per table but scopes versions and
tags: a tag on an evaluation dataset keeps that dataset only (one shared dataset made every tag
keep every table's rows at that version, so storage grew with versions times records).

**Purge.** `purge` rewrites every fragment holding deleted rows (Lance compaction with deletions
materialised; the key-value store rewrites its live view) and removes every untagged version but
the latest. Before that it checks each tag on an older version: for tracking tables and records
tables, whether that version holds rows of a deleted run; for the key-value store, whether it
holds a put of a key that is deleted now. Such a tag fails the purge with its name, or is deleted
with `force`. See the operations guide for what purge does not reach (other machines' WALs and
snapshots, delete markers within their grace period).

Rejected alternatives:

- **A table per run or per series.** Thousands of tiny datasets, each with its own manifest and
  versions, make cross-run queries and listing expensive on object stores.
- **Metric name and context as columns of `metrics`.** Repeating strings per point inflates
  files and makes the filter a string comparison; a 64-bit series id is compact and indexable,
  and `series` resolves names and contexts.
- **Wide tables (one column per metric).** Metrics appear and disappear per run; schema
  evolution per new metric would be a commit storm.

### Writes

`Run.track(value, name, step, epoch, context)` and `set_params` only append to an in-memory
buffer shared by the `Repo` handle. A flush writes each non-empty table as one append, in the
order runs, params, series, metrics, so a reader never sees points of a series it cannot name.
Flushes happen at `flush_rows` buffered points (default 50 000), every `flush_interval`, on
`flush()`, `Run.finish()` and close. On failure the unwritten rows return to the buffer.

A `step` defaults to one past the series' last step in this handle; `resume_run` loads the last
steps of an existing run first. Two points at the same step: the later one wins on read.

Runs and parameters are change logs (newest `seq` wins) rather than updated in place: an
in-place Lance update rewrites fragments and conflicts with concurrent updates, while appends
never conflict.

### Reads

Query methods flush the handle's buffer, move to the latest version of the table, then scan
with a filter: `run_id = …` for runs, params and series; `series_id IN (…) AND step >= … AND
step < …` for metrics. `optimize()` compacts every table and builds (or updates) a BTree index on
`metrics.series_id`; Lance uses it for the filter and scans fragments added since the index was
built. Results are ordered by step.

Planned (see `TODO.md`): sorting `metrics` by `(series_id, step)` during compaction so a series
is contiguous on disk; downsampled reads for charts; Arrow results instead of Python lists;
artifact/blob references; per-table time travel.

### Concurrency

Many processes can write different runs to one repository at once: all writes are appends.
Updates to one run's metadata from two processes resolve last-writer-wins per whole row.

## Object stores and configuration

Locations are normalised (relative paths become absolute) and handed to Lance. Storage options
are passed through unchanged (`aws_endpoint`, `aws_region`, `aws_access_key_id`, `allow_http`,
`google_service_account`, `azure_storage_account_name`, …), and Lance falls back to the usual
environment variables. The `aws`, `gcp` and `azure` features are on by default; the local file
system and `memory://` are always available. A `memory://` store is private to one handle.

S3 needs conditional writes for safe concurrent commits. AWS S3, MinIO and RustFS support them;
the tests and CI run against RustFS because MinIO no longer publishes public container images.

## Runtime and threading

Lance is async. kv0 runs every Lance future on its own multi-threaded Tokio runtime:

- the blocking APIs (key-value layer, Python) spawn onto it and wait, so they never panic inside
  a caller's runtime;
- the async tracking API also spawns onto it and awaits the handle, so locks held across I/O are
  only held by kv0's tasks, never by tasks of a caller's single-threaded runtime.

One background thread drives every open handle's timers: each tick it fsyncs every due WAL, then
starts the due flushes on the runtime's blocking pool, at most one at a time per handle, so a slow
commit (a hung object store) never delays another handle's fsync or flush. It holds weak
references, so it never keeps a database alive. There is no commit timeout yet (see `TODO.md`).

The Python bindings release the GIL around every call that can wait on a kv0 lock or on
the runtime: Lance logs from runtime threads through `pyo3-log`, which needs the GIL, so holding it
while waiting would deadlock. `DB.version` reads an atomic and takes no lock. On Linux the extension uses mimalloc with `local_dynamic_tls`, because a `dlopen`ed module
with initial-exec TLS can exhaust the static TLS block.

## What a tracker needs from kv0

A tracker with a Rust server should depend on the `kv0` crate directly:

- `tracking::Repo` with async methods, usable from an axum/Tokio server: `runs()`, `run(id)`,
  `all_params()`, `all_series()`, `series_stats()`, `latest_versions()` for cheap change detection,
  `read_many` / `read_many_in` for step and time ranges, `update_run`, `delete_runs`, the records
  table with tags, `optimize()`.
- `Run` handles for the SDK side: `track`, `set_params`, `update`, `finish` / `finish_at`;
  `resume_run`.
- Serialisable types (`RunInfo`, `SeriesInfo`, `MetricData`, `SeriesStats`) for the HTTP API.
- Not yet: Arrow record batches or streaming reads for very large charts, run filtering by
  parameter values in storage (the typed `num`, `text`, `flag` columns exist for it), and artifact
  references.

## Settled decisions

These are fixed for the first release, 0.1.0:

- Read-only handles now reject writes (`NotSupported`) instead of keeping them locally.
- Opening a database that does not exist read-only raises `NotFound`.
- Locations are paths or URIs; credentials and endpoints are `storage_options`.
- Writes since the last commit survive a process crash through a local WAL (fsync every 100 ms by
  default), but only on the same machine.
- Versions older than 7 days are cleaned up automatically (`version_retention`).
- Python 3.11 or newer (abi3 wheel).
- Versioned from 0.1.0, with betas (0.1.0-beta.N) first; the pure-Python prototype before it was
  never published. Licence unchanged (Apache-2.0).
