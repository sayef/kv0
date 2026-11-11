# Operations and cleanup

What to watch and what to run when kv0 holds data you care about.

## Where data lives

| What | Where | Lost when |
|---|---|---|
| Committed data and every version | The dataset (path or URI) | You delete it, or cleanup removes old versions |
| Writes not yet committed | The write-ahead log under `wal_dir` (`~/.cache/kv0/wal`) | The machine is lost |
| View snapshots (`view.snap`) | Next to the write-ahead log | Never needed: a missing or stale snapshot means a full scan on open |

In containers, point `wal_dir` (or `KV0_WAL_DIR`) at a persistent volume, or accept that a crash
of the container loses the writes of its last few seconds.

## Flush and close

`close()` commits buffered writes, removes the handle's write-ahead log and saves a snapshot. A
process that never calls `close()` still commits: Python flushes open handles at exit, Rust on the
last `drop`. A killed process loses nothing: the next writable open on the machine replays its log.

## Compaction

Each commit adds a file. kv0 compacts automatically above `max_fragments` small files. Run it on
demand with `compact_range()` (Python) / `compact()` (Rust) for the key-value store and
`optimize()` for tracking, which also sorts metric points by series and step and builds the
series index.

## Cleanup safety

Compaction rewrites rows into new files; the old ones stay until their versions are cleaned up,
automatically after `version_retention` (7 days) or on `cleanup_old_versions(age)`.

Other handles read values over `lazy_value_threshold` from the version they last loaded. If a
cleanup deletes that version's files, the handle's next read refreshes and reads the value again
from the new version: correct, but it costs a refresh, and it fails if the handle cannot refresh
(a handle opened at a fixed `version`). Keep the cleanup age above the longest a reader goes
without refreshing: its `refresh_interval`, or the life of an iterator. Time-travel opens of a
cleaned-up version fail.

Tagged versions are never cleaned up:

```python
db.create_tag('release-3')                 # this handle's version
db.tags()                                  # [('release-3', 42)]
old = kv0.DB(path, version=db.tag_version('release-3'))
```

## Monitoring

`db.stats()` returns the version, live keys, buffered operations and bytes, the number of files
and stored rows, and view entries. Stored rows far above live keys means compaction will rewrite
soon; many files means commits are frequent relative to `max_fragments`.

## Several machines

Writers on different machines need no coordination: Lance commits with conditional writes, which
AWS S3 and current S3-compatible servers support. Each machine has its own write-ahead log. A
machine that dies with uncommitted writes keeps them in its log until it opens the database again.

## Deletion and erasure

Deleting and erasing are different steps.

**Delete** (`delete_runs`, `DB.delete`, `delete_range`) makes data invisible at once. The bytes
stay: deleted rows are marked in deletion files, older versions still reference the files that hold
them, and time travel (`DB(path, version=n)`, `At::Version`, tags) can read them back.

**Purge** removes them:

```python
report = repo.purge()          # tracking: every run deleted so far
report = db.purge()            # key-value: deleted and overwritten values
# {'runs': 1, 'fragments_rewritten': 4, 'versions_removed': 37, 'bytes_removed': 1834562,
#  'tags_deleted': [], 'snapshots_removed': 1, 'tags_left': []}
```

It rewrites every data file that holds deleted rows (the key-value store rewrites the database as
its live view), then removes every version but the latest, the way `cleanup_old_versions(0)` would.
For tracking, it first deletes any rows a writer racing the delete committed afterwards.

**Tags.** A tag keeps its version through cleanup. If a tag keeps a version that still holds data
purge would remove (rows of a deleted run; in the key-value store, any value that is not the live
one now, deleted or overwritten), `purge` stops with an error naming the tag and changes nothing. Delete the tag, or call `purge(force=True)`, which deletes such tags and
lists them in `tags_deleted`. Records tables are tagged one by one, so a tag on an evaluation
dataset's table does not block erasing a run's scores. A write that overwrites a tagged key while a purge runs can
leave such a tag behind; the report lists it in `tags_left`, and a second purge handles it.

**What purge does not reach.**

- **Write-ahead logs.** A write is in the local WAL until its batch is committed; committed
  segments are deleted. A WAL left by a process that crashed before committing still holds its
  writes until the next open on that machine replays and removes it; replay drops writes to
  deleted runs. Purge only sees the machine and handle it runs on: open (and close) the database
  on each machine that ran a crashed writer, or delete that machine's WAL directory.
- **View snapshots.** `view.snap` holds a key-value database's live view as of the last close.
  `db.purge()` removes this machine's snapshot of the database; other machines' snapshots are
  rejected on their next open (the data files changed) and replaced on close, or can be deleted
  by hand (`<wal_dir>/<hash>/view.snap`).
- **Delete markers.** The key-value store keeps delete markers younger than `tombstone_grace`
  (1 h) so a slower writer cannot bring a key back; they hold the key, not its value. Tracking
  keeps a tombstone row per deleted run (its id only) until `mark_ttl`.
- **Copies elsewhere:** object-store versioning or replication, backups, caches outside kv0.
- **Disk blocks.** Purge deletes files; it does not overwrite them. On a local disk or SSD the
  blocks may stay readable to someone with access to the raw device until reused. Where that
  matters, use disk-level encryption (and destroy the key with the disk) or secure deletion at
  the storage layer; on object stores, deletion is the provider's.
- **Memory of other handles.** Other open handles keep values they have read or cached until they
  refresh or close.

**Concurrency.** Writers can keep writing during a purge: compaction and cleanup commit like any
other writer, and Lance's cleanup never removes a file that no version references until it is
7 days old, so a file a concurrent writer is about to commit is safe. (Such an unreferenced file,
left by a writer that crashed before committing, is removed only by a cleanup after those 7 days.) Readers that hold an
older version lose it: a key-value handle reading a value whose file is gone refreshes and reads it
again; a tracking query in flight may fail and should be retried. Run `purge` when a short burst of
reader refreshes is acceptable.
