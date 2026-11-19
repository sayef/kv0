# kv0 design

kv0 is an embedded storage engine on [moraine](https://github.com/sayef/moraine) (the `morainedb`
crate), a client-only database of Delta Lake tables on object storage. It has a Rust core (`crates/kv0`) and Python bindings (`crates/kv0-py`), and two
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

- One storage engine for every backend: a local directory, `memory://`, and S3 and
  S3-compatible stores (MinIO, RustFS, R2). Google Cloud Storage and Azure Blob Storage are not
  supported yet. A location is a path or URI, and credentials are storage options.
- No commit protocol of kv0's own: moraine owns data files, versions and atomic commits, and
  kv0 builds on its tables.
- A familiar ordered key-value API (`get`, `put`, `delete`, `delete_range`, `WriteBatch`,
  iterators with `seek` and `seek_for_prev`), in Rust and Python.
- A tracker-shaped schema for new work: high-rate metric appends, step-range reads, many runs
  times many metrics.
- Honest, documented consistency guarantees.

Non-goals (for now): merge operators, column families, snapshots of the
key-value store other than tagged versions, SQL over the key-value store.

## Why moraine, and what it costs

kv0 started on Lance datasets. I moved it to moraine because moraine is built for exactly this
shape of work: several writers appending to one table from many processes, with no server and
no coordinator. A moraine table is a Delta Lake table; a commit is one PUT per data file plus one
conditional PUT of the log entry, and moraine resolves concurrent commits itself. It also gives
kv0 what it used to build or work around: transaction ids for exactly-once replay of a
write-ahead log, named pins for tags, files sorted by a cluster key, tiered compaction, and a
purge that collects removed files at once and erases their values from the log. Delta Lake is
also a format many other tools read.

I removed the Lance engine rather than keep both. kv0 is in beta with no compatibility promise,
and two engines meant two formats to test, document and operate; there is no migration path
while kv0 is in beta (see [Storage backends](docs/guides/storage.md#earlier-formats)).

The cost is still commit granularity. Every commit writes a data file and a log entry; on S3
that is a few requests and tens to hundreds of milliseconds. A per-`put` commit is out of the
question. Both layers therefore **buffer writes in memory and commit in batches**, and the
guarantees below follow from that. moraine keeps only the latest version and pinned (tagged)
versions readable, so time travel reaches tagged versions only.

## Key-value layer

### Storage format

One moraine database per key-value database (the path or URI given to `DB`), with one table,
`ops`. It is an operation log, keyless and unclustered, so moraine stores each append's rows in
the order given:

| column  | type      | meaning |
|---------|-----------|---------|
| `seq`   | `int64`   | sequence number (unsigned in the API, bit-cast); newest wins |
| `op`    | `int8`    | 0 = put, 1 = delete, 2 = delete range |
| `key`   | `binary`  | the key, or the inclusive start of a range |
| `value` | `binary?` | the value; the exclusive end of a range; null for a delete |
| `vlen`  | `int32?`  | the value's length |

Delta Lake has no unsigned integers, so kv0's tables use signed columns and the APIs bit-cast
their unsigned values; every `u64` round-trips.

`seq` is a hybrid logical clock in nanoseconds since the Unix epoch: each new number is greater
than the wall clock and than every number the handle has seen. Local writes therefore always win
over anything loaded, and concurrent writers order by wall-clock time.

### Reads: an in-memory ordered view

On open, the handle reads the log and applies the rows into an ordered set of entries plus a
list of range tombstones. Every read (`get`, `multi_get`, iterators) is served from this set, so
keys come back in byte-wise order without I/O. Applying is idempotent and order-independent: a row
is ignored when the key already holds a newer `seq`, when a newer point tombstone exists, or when a
newer range tombstone covers it. Rows are applied batch by batch as the Parquet files are decoded,
so an open never holds the log twice.

Three measures keep memory and reopen time in check without a second storage engine:

- **One allocation per entry.** An entry is a single byte string (sequence number, flags, key
  length, key, value) in a `BTreeSet` ordered by key, instead of a map entry with separate key and
  value buffers.
- **Large values stay in storage.** A committed value longer than `lazy_value_threshold` (4 KiB)
  is replaced in the view by its row address (a file ordinal and the row's offset in that file)
  and length. Reads fetch such values by offset through moraine, which reads only the pages that
  hold them, in one request per file per lookup, `multi_get` or iterator chunk, through a
  byte-bounded LRU cache
  (`value_cache_size`, 64 MiB). Unflushed values stay in memory until committed. Every commit that
  moves rows (this handle's appends and compactions, and other writers' compactions picked up by a
  refresh) reports the new addresses, and the view records them together with the version they
  refer to, so a reader always resolves an address against the version it came from. If that
  version's files are gone (another writer compacted and then ran `cleanup_old_versions` with a
  short age, or purged), the read refreshes the handle and resolves the key again at the new
  version; only if that also fails does it return a storage error, never "not found". Concurrent
  misses on one address share a single read. A full
  rewrite streams stored values batch by batch instead of loading them all.
- **A snapshot for reopen.** On close, a handle whose view holds at least `snapshot_min_entries`
  entries (100 000) writes the view, the version, the table's id (new when a database is
  created again at the same location), the identity of each data file (its path, which moraine
  never reuses, and its size) and the table of file ordinals behind row addresses to `view.snap`
  in the database's WAL directory, with a checksum. The file is fsynced before it
  is renamed into place, and the directory after. The next open uses it only if
  every file it covers is still present; it then reads just the files committed since. A
  compaction (which may drop tombstones), a recreated database, a different
  `lazy_value_threshold` or a damaged file sends the open back to a full scan. Snapshots hold only
  committed state: a handle with unflushed writes does not save one.

A tracker's databases are typically per run (one for metadata, one for sequences) plus an
index, which fits comfortably.

Alternatives I rejected for the local view:

- **RocksDB, LMDB, redb, fjall** as a local tier. I don't want a second storage engine next to
  the object store's: it doubles the formats to operate, and the point of kv0 is one engine on every backend.
- **SQLite local tier**: I built and benchmarked it, then dropped it. It fixed memory and reopen
  (10M keys: 56 MB, 4 ms) but cost 7–22× throughput and slower recovery. The measurements are
  in [docs/design/sqlite-tier-experiment.md](docs/design/sqlite-tier-experiment.md).

Rejected: serving reads straight from storage with filtered scans. A seek is a range scan that
the log cannot answer in key order without sorting, and every `get` would plan a query; both are
orders of magnitude slower than a tracker's access pattern (many small puts, seeks and short
scans) needs.

### Writes: buffer, then append

A write is applied to the view at once (read-your-writes) and appended to a pending buffer. The
buffer is committed as **one moraine append** (one data file, one commit):

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
log before it is applied or acknowledged; commits stay batched as above.

- **Location:** always local disk, also when the database is on S3:
  `<root>/<xxh3 of the database URI>/<writer id>/`, where the root is `wal_dir`, else
  `KV0_WAL_DIR`, else the platform cache directory (`~/.cache/kv0/wal`,
  `~/Library/Caches/kv0/wal`). Each open handle is one writer with its own directory,
  held by an advisory file lock while it runs.
- **Format:** segment files of records `len: u32 | xxh3-64: u64 | payload`; one record per write
  call, so a batch is one record and replays all or nothing.
- **Durability modes** (`wal_sync`): `none` writes to the OS on every call and never fsyncs
  (survives a process crash, not a power loss or kernel crash); `batch` (default) also fsyncs
  about every 100 ms from the background thread (a power loss can lose the last ~100 ms);
  `always` fsyncs before every write returns. `sync=True` still means committed to storage
  before the call returns.
- **Truncation:** a flush seals the current segment and opens a new one under the same lock that
  takes the buffered operations, so the sealed segments hold exactly what the commit covers;
  after the commit succeeds they are deleted. A clean close removes the writer's directory.
- **Replay:** a writable open claims every writer directory under the database's WAL directory
  whose lock it can take (writers that are no longer running, including earlier runs of this
  program), reads their segments and applies the operations that are not committed yet to the
  view and the buffer, writes them into its own WAL, deletes the claimed directories and
  commits. Operations keep their original sequence numbers, so replay never reorders writes.
- **Idempotency:** each commit that covers WAL operations carries a moraine transaction id,
  `kv0-wal/<writer id>` at the writer's highest committed sequence number, in the same log entry
  as the data it covers. moraine keeps every application's newest transaction version in the
  table's state, through compaction, checkpoints and log cleanup, and a write whose version is
  not above it is not applied again. Replay therefore skips every operation at or below the
  writer's mark however old the WAL is, and a write whose commit response was lost lands once.
  (The Lance engine wrote marks as rows of the log and pruned them; I first tried Lance
  transaction properties, which disappeared with cleaned-up versions.)
- **Per-write opt-out:** `disable_wal=True` (`WriteOptions::disable_wal` in Rust) skips the log for that write. It is committed with the next flush like any other, and is
  lost if the process crashes before then.
- **Failed appends:** a write that fails part-way (a full disk, an I/O error, a failed fsync in
  `always` mode) is cut back out of the segment before the error is returned, so a reported
  failure is never replayed and a partial record never sits in front of later acknowledged ones
  (replay would take it for a torn tail and drop them). If the cut fails, the WAL moves to a new
  segment; if that fails too, it refuses writes until a flush rotates it.
- **Writer ids are reused:** a clean close keeps the writer's (empty) directory, and the next
  open on the machine adopts an idle directory and its writer id, so the transaction ids stay at
  the number of writers that ran at the same time, not one per open. moraine never expires
  them, so a WAL left behind is ambiguous only when its writer never committed at all and it
  holds an operation older than `mark_ttl` (default 30 days): then the open fails with an error
  naming the directory, and `replay_stale_wal=True` replays it anyway (writes may then be
  stored twice).
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
series, metrics), each with its own transaction id, so a crash between two of them replays
exactly the tables that were not committed. moraine has no commit across tables, so a reader can
see a flush partly applied (points before their series row, for example); reads tolerate that,
and I leave the order of visibility undefined rather than serialise for an ordering nothing
relies on. After a flush, a table with more than `max_fragments` (default 32) small files is
compacted automatically; `optimize()` compacts as well.

Rejected: putting the log in the object store. Each logged write would be an object-store request
(tens of milliseconds), and the log would need its own commit protocol next to moraine's.

### Compaction and write amplification

Each flush adds a data file, so the log accumulates small files and shadowed rows. After a
flush, when the log has more than `max_fragments` (default 32) files below moraine's compaction
target:

- if most stored rows are shadowed (stored rows > 2 × live keys + 1024), the handle
  **materialises** the view: it writes the live rows (sorted by key) and recent tombstones as new
  files, in one moraine replace that removes every file it read and keeps files other writers
  appended meanwhile;
- otherwise it runs moraine's compaction, which merges runs of adjacent small files without
  changing rows or their order.

`compact_range()` always materialises. A replace or a compaction commits alongside concurrent
appends (the appended files are untouched), so compaction does not block or lose other writers'
data. Two concurrent compactions of the same files conflict; the loser gives up and tries again
after a later flush, and its flush still succeeds.

Write amplification is therefore bounded by one full rewrite per `max_fragments` flushes when
the workload overwrites keys, and by moraine's tiered merging otherwise.

**Tombstones** (point and range) are kept by compaction for `tombstone_grace` (default 1 h), and
removed from the in-memory view at the same time, so the view stays equal to what a reopen loads; so a
slower writer's older write to a deleted key stays deleted. After the grace period they are
dropped, and such a very late write could reappear. This is the usual tombstone-GC trade-off.

### Concurrency and consistency

- **One process, many threads:** a handle is thread-safe. Reads see every write made through the
  handle, including unflushed ones.
- **Several handles or processes on one database:** appends from different writers never
  conflict; moraine puts the log entry again at the next version when another writer took it.
  Each handle sees other writers' committed data when it **refreshes**: reads check for a newer
  version at most every `refresh_interval` (default 1 s), and `refresh()` checks now. A refresh
  reads only the log entries and files the handle has not seen.
- **Conflicting writes to one key** from different writers resolve last-writer-wins by `seq`
  (wall-clock time). There are no transactions across handles.
- **Iterators** are not snapshots. They read the view in chunks of 256 entries, so they observe
  writes beyond their position made before the next chunk is fetched, and never return a key
  twice or out of order.
- **Read-only handles** (`read_only=True`, or opening a `version`) reject writes with
  `NotSupported`.
- **Versions and tags:** every commit is a version, but moraine keeps only the latest one and
  pinned ones readable: a kv0 tag is a moraine pin, which keeps its version's files through
  vacuum. `DB(path, version=n)` opens version `n` read-only when it is the latest or a tagged
  one, and fails with `NotFound` otherwise; `versions()` lists the latest and tagged versions.
  Removed files are collected by moraine's vacuum after its grace period; a handle with
  `version_retention` (default 7 days) also runs a moraine purge every 20 of its commits, which
  collects at once the files removed by commits older than that. `cleanup_old_versions(age)`
  does the same on demand.

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

A repository is one moraine database with four tables, `runs`, `params`, `series` and
`metrics`, plus one table per records table (below). Every table is keyless and clustered by
`run_id` where it has one; moraine sorts every file it writes by the cluster key, stably, so the
rows of one run sit together and keep their append order.

`runs`: one row per change of a run. Each row says which fields it sets (`fields`, a
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
row gave (taken whole, outside its mask) stay tied to that row. Tombstones and rows of deleted runs
are kept as they are.

Two limits of the per-field merge: `tags` is one field, so two writers adding different tags at
once keep only the later list; and with clock skew between machines a change can sort before the
run's creation row and lose to it, as with any last-writer-wins by `seq`.

A **tombstone** is a row whose `fields` is exactly the `DELETED` bit. `delete_runs` commits
tombstones first, then deletes the runs' points, statistics, series, parameters, records and
other rows. Reads ignore every row of a tombstoned run whatever its `seq`. A flush reads the
`runs` files it has not read before for tombstones (its own previous commits excluded),
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
| `seq` | `int64` |
| `run_id` | `utf8` (UUIDv7 hex unless given) |
| `name`, `experiment`, `description` | `utf8?` |
| `tags` | `list<utf8>` |
| `status` | `utf8`: running, finished, failed, aborted |
| `archived` | `bool` |
| `created_at`, `updated_at`, `ended_at?` | `timestamp[us, UTC]` |
| `fields` | `int32?`: the fields this row sets |
| `kind` | `utf8?`: training, eval, ... |
| `target` | `utf8?` (JSON): what the run is about |

`params`: flattened parameters, one row per (run, dotted key) change; highest `seq` wins.

| column | type |
|---|---|
| `seq` | `int64` |
| `run_id`, `key` | `utf8` (`optimizer.lr`) |
| `value` | `utf8` (JSON) |
| `num`, `text`, `flag` | `float64?`, `utf8?`, `bool?`: the scalar repeated for filtering |

Nested objects flatten to dotted keys; arrays and scalars are leaves. Setting a key merges; it
does not delete siblings.

`series`: one row per metric series.

| column | type |
|---|---|
| `series_id` | `int64`: xxh3 of run id, name and canonical context, bit-cast |
| `run_id`, `name` | `utf8` |
| `context` | `utf8`: JSON with sorted keys, `{}` when empty |
| `created_at` | `timestamp[us, UTC]` |

`metrics`: append-only points, clustered by (`run_id`, `series_id`), so a series is a few pages
of a few files and deleting a run needs no join.

| column | type |
|---|---|
| `run_id` | `utf8`: the run of the series |
| `series_id` | `int64` |
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

`optimize` rewrites only the metrics files it has not sorted before (moraine files tagged
`kv0.sorted`), and does nothing when there are none. When a new point is at or below the last
step a sorted file holds for its series (a step written again, or out of order), it rewrites
every file, so overwritten points are dropped and statistics stay exact. It also rewrites
everything when the files to rewrite hold statistics an earlier `optimize` wrote (a sorted file
rewritten by a delete loses its tag), so sorted points and their exact statistics are never
split between the kept and rewritten parts. moraine's compaction merges only files with equal
tags and keeps the tag on the merged file. Time-range reads first
find the steps with a point in the range, then read only those steps of each series (with any
later points at them) and apply the range after choosing the latest point of each step.

Records tables: one moraine table per records table, rows `(seq, table_name, run_id?, item_id,
data, created_at)` keyed by `(run_id, item_id)` and clustered by them; the newest `seq` of a key
wins, at read time always and physically once `optimize` folds superseded rows. moraine names
are `[a-z0-9][a-z0-9_-]{0,63}`, so a records table is `rec-` and the base32 of its name (cut
with a hash when long), and its real name is kept under `_kv0/records/`. A table per records
table costs a log per table but scopes versions and tags: a tag on an evaluation table keeps
that table only (one shared table made every tag keep every table's rows at that version, so
storage grew with versions times records).

**Purge.** `purge` rewrites every file holding deleted rows (moraine deletes are
copy-on-write; the key-value store rewrites its live view), then runs a moraine purge with
erasure on every table: the files the rewrite removed, and every other file only untagged older
versions hold, are deleted at once instead of after moraine's grace period, and the minimum and
maximum statistics of those files are taken out of every log entry, older checkpoints and stale
pin manifests. Before that it checks each tag on an older version: for tracking tables and
records tables, whether that version holds rows of a deleted run; for the key-value store,
whether it holds a put of a key that is deleted now. Such a tag fails the purge with its name,
or is deleted with `force`. See the operations guide for what purge does not reach (other
machines' WALs and snapshots, other processes' caches, delete markers within their grace
period).

Rejected alternatives:

- **A table per run or per series.** Thousands of tiny tables, each with its own log and
  versions, make cross-run queries and listing expensive on object stores.
- **Metric name and context as columns of `metrics`.** Repeating strings per point inflates
  files and makes the filter a string comparison; a 64-bit series id is compact and prunes well,
  and `series` resolves names and contexts.
- **Wide tables (one column per metric).** Metrics appear and disappear per run; schema
  evolution per new metric would be a commit storm.

### Writes

`Run.track(value, name, step, epoch, context)` and `set_params` only append to an in-memory
buffer shared by the `Repo` handle. A flush writes each non-empty table as one append, in the
order runs, params, series, metrics; reads skip points whose series they cannot name yet.
Flushes happen at `flush_rows` buffered points (default 50 000), every `flush_interval`, on
`flush()`, `Run.finish()` and close. On failure the unwritten rows return to the buffer.

A `step` defaults to one past the series' last step in this handle; `resume_run` loads the last
steps of an existing run first. Two points at the same step: the later one wins on read.

Runs and parameters are change logs (newest `seq` wins) rather than updated in place: an
in-place update rewrites files and conflicts with concurrent updates, while appends never
conflict.

### Reads

Query methods flush the handle's buffer, move to the latest version of the table, then scan
with a filter: `run_id = …` for runs, params and series; `series_id IN (…) AND step >= … AND
step < …` for metrics. moraine prunes files by their key ranges and pages by the Parquet page
index, so a filter on the cluster key reads little more than the rows it returns. Results are
ordered by step.

Planned (see `TODO.md`): downsampled reads for charts; Arrow results instead of Python lists;
artifact/blob references.

### Concurrency

Many processes can write different runs to one repository at once: all writes are appends.
Updates to one run's metadata from two processes resolve last-writer-wins per whole row.

## Object stores and configuration

Locations are normalised (relative paths become absolute) and handed to moraine. Storage options
are the object store's S3 configuration (`aws_endpoint`, `aws_region`, `aws_access_key_id`,
`aws_secret_access_key`, `allow_http`, …), and fall back to the `AWS_*` environment variables.
The local file system and `memory://` are always available; a `memory://` store is private to
one handle. `gs://` and `az://` locations fail with `NotSupported` until moraine supports them;
the `gcp` and `azure` features stay so that builds that name them keep compiling.

S3 needs conditional writes for safe concurrent commits. AWS S3, MinIO and RustFS support them;
the tests and CI run against RustFS because MinIO no longer publishes public container images.

A location carries a marker, `_kv0/format.json` (`{"engine":"moraine","version":1,"kind":...}`),
written before its first table. Locations written by earlier kv0 releases (Lance datasets) or by
the Delta Lake engine kv0 had for a while are recognised by their layout and refused with
`NotSupported`; there is no migration while kv0 is in beta.

## Runtime and threading

moraine is async. kv0 runs every storage future on its own multi-threaded Tokio runtime:

- the blocking APIs (key-value layer, Python) spawn onto it and wait, so they never panic inside
  a caller's runtime;
- the async tracking API also spawns onto it and awaits the handle, so locks held across I/O are
  only held by kv0's tasks, never by tasks of a caller's single-threaded runtime.

One background thread drives every open handle's timers: each tick it fsyncs every due WAL, then
starts the due flushes on the runtime's blocking pool, at most one at a time per handle, so a slow
commit (a hung object store) never delays another handle's fsync or flush. It holds weak
references, so it never keeps a database alive. There is no commit timeout yet (see `TODO.md`).

The Python bindings release the GIL around every call that can wait on a kv0 lock or on
the runtime: kv0 and moraine log from runtime threads through `pyo3-log`, which needs the GIL, so holding it
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
- Removed files older than 7 days are collected automatically (`version_retention`); only the
  latest and tagged versions are readable.
- One storage engine, moraine; locations written by earlier betas are refused, not migrated.
- Python 3.11 or newer (abi3 wheel).
- Versioned from 0.1.0, with betas (0.1.0-beta.N) first; the pure-Python prototype before it was
  never published. Licence unchanged (Apache-2.0).
