# Concepts

kv0 has two layers on the same storage: an ordered key-value store and tracking tables. This page
explains the key-value model; the tracking layer reuses the same machinery (buffering, a
write-ahead log, Lance versions) for typed tables. The [design](design.md) has the full
guarantees and the alternatives that were rejected.

## An ordered key-value store

Keys and values are byte strings. Keys are ordered byte-wise, the way `memcmp` compares them:
`b"a" < b"ab" < b"b"`, and a big-endian integer suffix sorts numerically. Order is what makes a
key layout useful: put the things you read together next to each other
(`run/0007/loss/<step>`), and a range read is one seek and a short scan.

| Operation | Meaning |
|---|---|
| `put(key, value)`, `delete(key)` | Set or remove one key |
| `delete_range(begin, end)` | Remove every key in `[begin, end)` |
| `write(batch)` | Apply a `WriteBatch` atomically |
| `get(key)`, `multi_get(keys)` | Read the latest value |
| iterators | `seek`, `seek_for_prev`, `seek_to_first`, `seek_to_last`, then forward |

## WriteBatch

A `WriteBatch` groups puts, deletes and range deletes. It is applied under one lock and committed in
one Lance version, never split: other handles see all of it or none of it, and so does storage after
a crash. Within a batch, later operations win (`put` then `delete` of one key leaves it deleted).

## Iterators

An iterator is a cursor over keys in order. `seek(k)` positions at the first key `>= k`;
`seek_for_prev(k)` at the last key `<= k`. Iterators read the in-memory view in chunks of 256
entries, so they are cheap to create and never return a key twice or out of order. They are not
snapshots: a write past the cursor made before the next chunk is read is seen.

## Durability: the write-ahead log

A write returns after it is in a **local write-ahead log**, applied to the in-memory view and
buffered. When the process dies before the buffer is committed, the next writable open on the same
machine replays the log and commits what was missing. Replay is idempotent: every commit carries
the writer's highest committed sequence number, so nothing is stored twice.

| `wal_sync` | A write survives | Cost |
|---|---|---|
| `"none"` | a killed process (the OS still has it) | none |
| `"batch"` (default) | a killed process; a power loss loses at most about 100 ms | one fsync per 100 ms |
| `"always"` | a power loss | one fsync per write |

The log is on the machine that wrote. Losing the machine loses writes since the last commit;
`sync=True` on a write, or `flush()`, commits before returning.

## Flush and compaction

The buffer is committed as one Lance append (one data file, one version) when it exceeds
`write_buffer_size` (4 MiB), every `flush_interval` (5 s), on `flush()`, on `sync=True` writes and
on `close()`. Each commit adds a file, so kv0 **compacts** once there are more than `max_fragments`
small files: it merges files, or, when most stored rows are overwritten or deleted, rewrites the
dataset as the live view in one commit. Compaction runs concurrently with other writers' appends.

## Versions and time travel

Every commit is a Lance version. `DB(path, version=n)` opens version `n` read-only; `versions()`
lists them; tags name a version and keep it. Old versions are cleaned up automatically after
`version_retention` (7 days) or on `cleanup_old_versions(age)`. See
[operations](guides/operations.md#cleanup-safety) before cleaning up aggressively.

## Several writers

Any number of handles, in any number of processes and machines, can write to one database. Lance
commits are atomic, and appends from different writers do not conflict. Each operation carries a
sequence number from a hybrid logical clock; when two writers write one key, the higher sequence
number wins on every reader. Other handles see committed writes within `refresh_interval` (1 s), or
at once after `refresh()`. There are no transactions across handles.

## Lazy values and snapshots

The view of keys lives in memory, one compact allocation per entry. Two measures keep memory and
open time bounded:

- **Lazy values.** A committed value longer than `lazy_value_threshold` (4 KiB) is not kept in
  memory: the view holds its row address and reads it from Lance when needed, through a
  `value_cache_size` (64 MiB) cache. 100 000 values of 16 KiB reopen in 90 MB instead of 2.6 GB.
- **Snapshots.** On close, a handle saves its view (when it has at least 100 000 entries) next to
  the write-ahead log. The next open loads it and reads only data committed since: 10M keys open
  in 0.7 s instead of 5.4 s. A snapshot that no longer matches the dataset is ignored.

## Tracking tables

`kv0.tracking` stores runs, parameters and metric series in four Lance tables, written through
the same buffering and write-ahead log. Points are appended; reads ask for a series and a step or
time range. See the [tracking guide](guides/tracking.md).
