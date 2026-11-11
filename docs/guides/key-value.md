# Key-value API

`kv0.DB` is an ordered key-value store. Keys and values are `bytes`; keys are ordered byte-wise
(`memcmp`).

## Opening

```python
import kv0

db = kv0.DB(path, kv0.Options(create_if_missing=True), read_only=False)
```

`path` is a local directory (a `str` or `os.PathLike`) or a URI such as `s3://bucket/prefix/db`.
`Options` accepts any keyword; unknown engine-tuning options are ignored. The options kv0 uses are
listed in [Configuration](configuration.md). Keyword arguments to `DB(...)` override the same
options: `kv0.DB(path, storage_options={...})`.

## Reading and writing

```python
db.put(b'a', b'1')
db.get(b'a')                    # b'1'
db.get(key=b'missing')          # None
db.multi_get([b'a', b'b'])      # {b'a': b'1', b'b': None}
db.key_may_exist(b'a', fetch=True)   # (True, b'1')
db.delete(b'a')
db.delete_range(b'a', b'b')     # deletes keys in [b'a', b'b')

batch = kv0.WriteBatch()
batch.put(b'k', b'v1')
batch.delete(b'k')
batch.put(b'k', b'v2')
db.write(batch)                 # atomic: readers see all of it or none of it
```

`merge` is accepted and behaves like `put` (merge operators are not supported). Column-family arguments
are accepted and ignored; there is one keyspace.

## Iterators

`iterkeys()`, `itervalues()` and `iteritems()` return an unpositioned iterator. Position it with
`seek(key)` (first key `>= key`), `seek_for_prev(key)` (last key `<= key`), `seek_to_first()` or
`seek_to_last()`, then:

- `get()` returns the current entry without moving, and raises `ValueError` past the end;
- `next()` returns the current entry and advances, and returns `None` past the end;
- `for item in it` and `next(it)` use the iterator protocol (`StopIteration` past the end).

Iterators only move forward. They read in chunks of 256 entries and are not snapshots: writes beyond the
cursor made before the next chunk is read are seen.

## Durability and visibility

- Writes are visible to the handle immediately.
- They are **committed** (one Lance version per commit) when the buffer is full, every `flush_interval`,
  on `flush()` / `flush_wal()` / `compact_range()`, on `sync=True` writes and on `close()`.
- Before a write returns it is appended to a local write-ahead log. If the process crashes, the next
  writable open of the same database **on the same machine** replays the writes that were not
  committed, then commits them. With `wal_sync="batch"` (default) a power loss can lose the last
  ~100 ms; use `"always"` to fsync every write, or `sync=True` to commit it to Lance.
- If the machine itself is lost, writes since the last commit are lost with it.
- `disable_wal=True` on `put`, `delete`, `delete_range` or `write` skips the log for that write:
  it is committed with the next flush, and lost if the process crashes before then.
- Other handles, in this or another process, see committed writes after their next refresh: reads check
  at most every `refresh_interval`, and `db.refresh()` checks now.
- Read-only handles reject writes with `errors.NotSupported`.

## Versions

```python
db.version                         # the version this handle has read
db.versions()                      # [(version, commit time in µs since the epoch, UTC), ...]
kv0.DB(path, version=3)       # read-only view of version 3
db.cleanup_old_versions(86400)     # delete versions older than a day; returns bytes removed
db.stats()                         # live keys, buffered writes, files, stored rows
```

Values over `lazy_value_threshold` are read from the version a handle last loaded. If another
process compacts and then cleans up that version, the handle's next read of such a value refreshes
and reads it again from the new version: the result is correct but costs a refresh. Keep the age
given to `cleanup_old_versions` above the longest a reader goes without refreshing
(`refresh_interval`, or the life of an iterator). The default retention is 7 days.

## Errors

`kv0.errors` has `NotFound` (also a `FileNotFoundError`), `StoreIOError` (also an `OSError`),
`Corruption`, `NotSupported`, `InvalidArgument` (bad arguments or a closed handle), `Conflict`,
`MergeInProgress` and `Incomplete` (the last two are never raised).
