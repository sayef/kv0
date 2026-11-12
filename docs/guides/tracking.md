# Tracking tables

`kv0.tracking.Repo` stores runs, parameters and metric series in four Lance tables under one
location. It is the storage of an experiment tracker; the Rust crate exposes the same API
(`kv0::tracking`) with async methods.

## Runs

```python
from kv0.tracking import Repo

repo = Repo('/data/tracking')       # or s3://..., with storage_options={...}
run = repo.create_run(name='baseline', experiment='resnet', description='first try', tags=['synthetic'])
run.update(description='first try, fixed seed')
run.finish('finished')              # or 'failed', 'aborted'
```

A run used as a context manager finishes as `finished`, or `failed` when the block raises. Run ids are
time-ordered UUIDv7 strings unless you pass `run_id=`.

## Parameters

```python
run['hparams'] = {'lr': 3e-4, 'optimizer': {'name': 'adamw', 'betas': [0.9, 0.999]}}
run['seed'] = 7
run.set_params({'hparams': {'lr': 1e-4}})     # merges by dotted key: hparams.lr changes, the rest stays
repo.params(run.id)                           # the nested dict
```

Parameters are stored flattened (`hparams.optimizer.name`) with the JSON value and, for scalars, typed
copies for filtering.

## Metrics

```python
run.track(0.25, name='loss')                                    # step 0, 1, 2, ... per series
run.track(0.91, name='accuracy', step=100, epoch=3, context={'subset': 'val'})

repo.series(run.id)        # [{'series_id', 'name', 'context', ...}]
repo.metric(run.id, 'accuracy', context={'subset': 'val'}, start=0, stop=1000)
# {'steps': [...], 'values': [...], 'epochs': [...], 'timestamps': [...]}
```

A series is a metric name under a context in a run. Without `step`, a point gets one past the series' last
step; `repo.resume_run(run_id)` continues numbering after the stored steps. When a step is tracked twice,
the later point wins.

`track_many` writes many points of one series at once, with the same result as calling `track` for each
in order but with one lock and one write-ahead-log append for the batch, which suits a server that
receives points in batches. `steps`, `epochs` and `timestamps` are lists as long as `values` (with `None`
entries for defaults), or omitted; a batch is buffered completely or, on an error, not at all:

```python
run.track_many('loss', [0.9, 0.7, 0.5])                         # the next three steps
run.track_many('accuracy', [0.8, 0.85], steps=[10, 20], context={'subset': 'val'})
```

In Rust it is `Run::track_many(name, context, &[TrackPoint])`.

## Buffering and reading

`track` and parameter writes are buffered in the `Repo` handle and committed in batches: at `flush_rows`
points (default 50 000), every `flush_interval` seconds (default 5), on `flush()`, `finish()` and
`close()`. Queries commit the handle's buffer first, then read the latest version of each table, so they
include this handle's writes and every other writer's committed writes.

Each flush can add a file per table; kv0 compacts small files in the background. `repo.optimize()`
also folds each run's change rows into a few (a run updated every few seconds otherwise leaves
thousands of rows that every listing merges again), sorts metric points by series and step (keeping only the latest point of a step written twice)
and builds an index on the series id. It only rewrites points added since the last `optimize()`
(everything when a step was written again), and returns at once when nothing was added, so
running it every few minutes is cheap.

Every `track`, parameter write and run change is also appended to a local write-ahead log before
it returns (options `wal`, `wal_sync` and `wal_dir`, as for the
[key-value store](key-value.md#durability-and-visibility)). If the process crashes, the next
`Repo` opened on the same machine replays and commits what was not committed yet.

Many processes can write different runs to one repository at the same time.

## Changing runs from elsewhere

A run's record is stored as change rows, and each row records which fields it sets. Reads merge
them field by field, newest first. Two processes that change different fields of one run therefore
never undo each other: a note saved from a web app while the training process finishes the run
keeps the run finished.

```python
repo.update_run(run_id, description='looks good', tags=['keep'])    # no points are read
repo.update_run(run_id, status='aborted')
```

Runs can carry a `kind` (`'training'`, `'eval'`, ...) and a JSON `target`, and imported runs their
original times:

```python
run = repo.create_run(kind='eval', target={'model': 'model-a'}, created_at=1_700_000_000_000_000)
run.finish('finished', ended_at=1_700_000_100_000_000)            # microseconds, UTC
```

## Reading everything at once

A server that indexes a whole repository reads each table once:

```python
repo.runs()                    # every run, newest first
repo.all_params()              # {run_id: params}
repo.all_series()              # every series of every run
repo.series_stats()            # {series_id: {'count', 'last', 'min', 'max', ...}}, no points read
repo.latest_versions()         # (runs, params, series, metrics): compare to skip a refresh
```

Series statistics are stored with the points, in the same commit, so they never lag them and
concurrent writers to one series both count. They agree with reads: a step written again counts
once, with its last value. (A handle that writes a step of a series it never wrote or resumed
cannot tell that the step existed; until the next `optimize()` such a step counts twice.)
`recompute_series_stats()` makes reads compute the given series from their points until
`optimize()` stores them exactly.

Points can also be read by time:

```python
repo.metric(run_id, 'score', start_time=t0, stop_time=t1)          # microseconds, UTC
```

## Records

Per-item data that is not a series (evaluation datasets, one output and several scores per item
per run, application state) goes in records tables: JSON rows under a table name, each with an
item id and optionally a run id. A row's key is `(run_id, item_id)`: appending a row whose key is
already present supersedes the older one, and reads return the newest row per key. Each table is
its own Lance dataset with its own versions and tags; `optimize()` drops superseded rows and keeps
indices on `item_id` and `run_id`, so a lookup reads only the matching rows. Nothing is kept in
memory.

```python
v = repo.append_records('dataset:qa', [{'item_id': 'q1', 'data': {'input': '...', 'expected': '...'}}])
repo.tag_records('dataset:qa', 'qa-v1', v)        # keeps this table's version, and only this table
repo.append_records('scores', [{'item_id': 'q1', 'run_id': run.id, 'data': {'exact_match': 1.0}}])

repo.records('scores', run_id=run.id)
repo.records('scores', run_ids=[a.id, b.id])       # several runs in one query
repo.records('dataset:qa', tag='qa-v1')           # exactly as it was when tagged
```

A run can record the version of a table it read (`records_version(table)`), and read it back later
by version or tag. `record_tables()` lists the tables. Give data with its own lifecycle (an
evaluation dataset) its own table, so tagging it does not keep other tables' history alive.

### Records from earlier builds

Builds before per-table records kept every table in one `records.lance`. Opening such a
repository fails with a message, so the rows are not silently ignored. Migrate once:

```python
repo = Repo(path, legacy_records='allow')        # repo.has_legacy_records() checks cheaply
for table in {r['table'] for r in repo.legacy_records()}:
    rows = repo.legacy_records(table)               # every row, in append order
    repo.append_records(table, [{'item_id': r['item_id'], 'run_id': r['run_id'], 'data': r['data']}
                                for r in rows])     # later rows of a key supersede earlier ones
repo.drop_legacy_records()
repo.close()
repo = Repo(path)                                    # the default check passes now
```

Tags on the old table are not carried over; tag the new tables again where needed.

## Deleting runs

```python
repo.delete_runs([run_id])     # its runs, params, series, statistics, points and records
repo.cleanup_old_versions(0)   # reclaim the space, once no reader needs the old versions
```

A deleted run stays deleted: a process still writing to it notices within about a second, drops
its later writes at its next flush, and its `track`, `update` and `finish` then raise `NotFound`
(writes committed in that second stay hidden and `optimize()` removes them); and a write-ahead log replayed
after the delete does not bring it back. Its id cannot be reused until the deletion record
expires (`mark_ttl`, 30 days, at the next `optimize()`).
