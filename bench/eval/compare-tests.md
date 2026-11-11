## What the tests cover

Each test of these changes and what it pins down, followed by known gaps.

**View entries** (`crates/kv0/src/kv/view.rs`, unit tests)

- `newest_wins_regardless_of_order`, `tombstone_blocks_older_put`,
  `late_older_put_stays_hidden_by_range`: the newest-wins rules on the new entry layout.
- `slots_hold_long_keys_and_empty_values`: key lengths above 127 bytes (multi-byte length
  prefix) and empty values.
- `any_order_gives_the_same_view`: 400 random puts, deletes and range deletes applied forwards
  and backwards give the same entries and live count. Loading no longer sorts rows by sequence
  number, so this property is what makes streaming the load correct.

**Value cache** (`crates/kv0/src/kv/cache.rs`)

- `evicts_least_recently_used_beyond_capacity`: least-recently-used eviction, a hit refreshes
  recency, a value larger than the whole capacity is not cached, and the byte count stays within
  capacity.

**Lazy values** (`crates/kv0/tests/lazy.rs`, threshold 16 bytes)

- `large_values_leave_memory_and_read_back`:
  - Unflushed values stay in memory; after a flush, exactly the large ones are stored in Lance.
  - `get`, `multi_get`, forward iteration and `seek_for_prev` return the right bytes.
  - Overwrites read back correctly before and after their flush.
  - A full rewrite (`compact`) moves every row, and the view follows the new addresses.
  - Reopen, and a handle with `lazy_value_threshold=0`, read identical data.
- `file_merging_compaction_moves_stored_values`: automatic compaction through Lance's file
  merge (the path that keeps superseded rows), checked after every flush, then at the end.
- `another_writers_compaction_is_followed_on_refresh`:
  - Handle B reads stored values that handle A committed.
  - A compacts. Before B refreshes, B still reads correctly from the version its addresses refer
    to; after refresh, B reads from the new addresses.
  - A read-only handle with `value_cache_size=0` reads correctly.
  - The refresh reads A's rewritten fragments without copying large values: rows over the
    threshold are applied as addresses (`Row::stored_len`).

- `values_moved_and_cleaned_up_by_another_writer_still_read` (review M1): a reader with
  `refresh_interval` None and then 1 s, a cache of 0 bytes and no snapshot reads a value; another
  writer compacts and runs `cleanup_old_versions(0)`, deleting the files the reader's addresses
  point at. `get`, then `multi_get` and a full iteration after further compactions and cleanups,
  return the right values: the read refreshes once and resolves the keys again.
- `concurrent_misses_on_one_value_read_correctly` (review M2): 8 threads × 20 reads of one
  uncached 100 kB value; misses on one address share a single read.

**Snapshots** (`crates/kv0/tests/snapshot.rs`, minimum 10 entries)

- `reopen_loads_the_snapshot_and_catches_up`:
  - A close saves a snapshot and the next open uses it, with stored values and a range tombstone
    included.
  - Later commits (new keys, a delete, an overwrite) by a handle that does not save a snapshot
    are applied on the next open from the old snapshot.
  - The result equals a full scan.
- `compaction_invalidates_the_snapshot`: another handle deletes a key and compacts with no
  tombstone grace, so the tombstone is gone. The next open must not use the snapshot, which
  still holds the key, and must not show the key.
- `a_read_only_handle_saves_a_snapshot_when_dropped`: a read-only handle (no writes, no flush)
  saves a snapshot when dropped, and the next read-only open uses it.
- `a_damaged_or_foreign_snapshot_is_ignored`:
  - A flipped byte fails the checksum and causes a full scan; the next close writes a good
    snapshot.
  - A dataset deleted and created again at the same path is not matched to the old snapshot.

**Known gaps** (not covered by a test yet)

- Iterators that span a compaction by another writer are covered by the M1 test only after the
  compaction, not with a chunk in flight during it.
- Fsyncing the snapshot (review M3) and the deletion-file identity (review M4) are not exercised by
  a test: the first needs a power cut, the second a deletion that kv0 itself never makes.
- Lazy values and snapshots on S3: covered only by local tests; `tests/s3.rs` runs the basic
  key-value suite.
- The cache bound counts value bytes only, not per-entry bookkeeping (about 60 bytes each).
