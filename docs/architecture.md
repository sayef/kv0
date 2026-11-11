# Architecture

A short tour of the code. [Design](design.md) explains the decisions and guarantees.

```
crates/kv0            the library
  src/storage.rs           locations, storage options, open-or-create a Lance dataset
  src/rt.rs                the Tokio runtime behind the blocking APIs
  src/background.rs        one thread that flushes every open handle on its interval
  src/clock.rs             sequence numbers (a hybrid logical clock)
  src/kv/view.rs           the in-memory ordered view: newest-wins application of operations
  src/kv/wal.rs            the local write-ahead log: segments, records, replay of crashed writers
  src/kv/store.rs          the Lance operation log: read fragments, append, replace (compaction)
  src/kv/mod.rs            Db, Options, WriteBatch, DbIterator: buffering, flush, refresh, compaction
  src/tracking/schema.rs   Arrow schemas of runs, params, series and metrics
  src/tracking/mod.rs      Repo and Run: buffers, flush, queries, optimize
crates/kv0-py         PyO3 bindings and the pure-Python package kv0
```

## Key-value write path

```
put / delete / write(batch)
  -> assign sequence numbers, append one record to the local WAL (fsync per wal_sync)
  -> apply to the view (visible now), append to the pending buffer
  -> buffer full, timer, flush(), sync=True or close():
       seal the WAL segment; append the buffer as one Lance fragment (one commit, retried by
       Lance on conflict) carrying the writer id and highest sequence number; delete the sealed segments
       read any fragments other writers committed meanwhile into the view
       more than max_fragments files? compact (materialise the view, or merge files)
```

## Key-value read path

```
get / multi_get / iterator seek
  -> refresh_interval elapsed? check the latest version; read only fragments not yet applied
  -> answer from the in-memory ordered view (values over the lazy threshold are read from Lance)
```

## Tracking write and read paths

```
Run.track / set_params / update -> the Repo buffer
flush -> one append per non-empty table: runs, params, series, metrics (in that order)
query -> flush, check out the latest table version, filtered scan, resolve newest-wins / order by step
```
