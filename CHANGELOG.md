# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- kv0 now stores everything with [moraine](https://github.com/sayef/moraine) (the `morainedb`
  crate), a client-only database of Delta Lake tables on object storage, and it is the only
  storage engine. A key-value database is a moraine database with one table; a tracking
  repository is one moraine database with a table per kv0 table. Each commit is one conditional
  write of a log entry, and a write-ahead log replays exactly once through moraine transaction
  ids.
- Locations written by kv0 0.1.0-beta.2 and earlier (Lance datasets) are recognised and refused
  with `NotSupported`, with a message to recreate them. I chose not to ship a migration while kv0
  is in beta: read the data with the release that wrote it and write it into a new location.
- Only the latest version and tagged versions are readable. Tags are moraine pins;
  `versions()` lists the latest and tagged versions, and opening any other version fails with
  `NotFound`.
- `cleanup_old_versions` and the automatic cleanup after `version_retention` run a moraine purge:
  files removed before the cutoff are deleted at once. `purge` also erases the statistics of the
  files it removes from the log; its report's `versions_removed` now counts the data files erased.
- The `engine` option and `KV0_ENGINE` accept only `moraine`, which is also the default.
- Locations are local paths, `memory://` and `s3://` (including S3-compatible servers). `gs://`
  and `az://` fail with `NotSupported` until moraine supports them; the `gcp` and `azure` cargo
  features remain but enable nothing usable yet.
- A tracking flush is four commits, one per table; a reader can see a flush partly applied.
- Minimum Rust version 1.95. Arrow and Parquet 59. Building no longer needs `protoc`.
- `orphan_grace` and `mark_prune_interval` are accepted and ignored: moraine keeps unreferenced
  files for its own grace period, and writer marks are no longer rows.

### Removed

- The Lance engine and the Delta Lake (delta-rs) engine.
- `kv0 migrate`, the `lance-migrate` cargo feature, the `kv0` binary that carried it, the
  `kv0-migrate` Python package and `kv0.migrate()`.
- Reading the shared `records.lance` of builds before per-table records: no moraine repository
  has one, so `legacy_records()` is always empty and `drop_legacy_records()` returns `False`.

## [0.1.0-beta.2](https://github.com/sayef/kv0/compare/v0.1.0-beta.1...v0.1.0-beta.2) - 2026-11-12

### Added

- `Run::track_many` (Python `Run.track_many`) writes a batch of points of one series with one lock
  and one write-ahead-log append, with the same result as repeated `track` calls: steps are
  numbered in order, the latest point of a step wins and non-finite values are stored as given. A
  batch is buffered completely or not at all.

## [0.1.0-beta.1](https://github.com/sayef/kv0/releases/tag/v0.1.0-beta.1) - 2026-11-12

The first release, published as betas (0.1.0-beta.N) before 0.1.0. Before it, kv0 was a pure-Python
prototype (an ordered key-value store on a local SQLite cache with optional S3 sync) that was never
published; the entries below describe the changes from that prototype.

### Changed

- kv0 is now a Rust library with Python bindings, and stores data in Lance datasets. A
  database is a local path or any URI Lance supports: `s3://` (and S3-compatible stores),
  `gs://`, `az://` or `memory://`. Credentials and endpoints are `storage_options`.
- Writes are buffered and committed in batches (by size, on a timer, on `flush()`, `sync=True`
  and `close()`), with automatic compaction.
- Read-only handles reject writes with `kv0.errors.NotSupported`.
- `kv0.errors.NotFound` is also a `FileNotFoundError`, and `StoreIOError` an `OSError`.
- Python 3.11 or newer.

### Added

- A local write-ahead log (`wal`, `wal_sync`, `wal_dir`, `KV0_WAL_DIR`): writes survive a
  process crash and are replayed, idempotently, on the next open on the same machine. Covers the
  key-value store and the tracking tables; `disable_wal=True` skips it per write.
- Version history and time travel: `DB(path, version=n)`, `DB.versions()`,
  `DB.cleanup_old_versions()`, `DB.refresh()`, `DB.stats()`.
- `kv0.tracking`: runs, parameters and metric series in columnar tables
  (`Repo`, `Run.track`, `Repo.metric`), also available from Rust as `kv0::tracking`.
- The `kv0` Rust crate (`kv0::kv`, `kv0::tracking`).
- Values longer than `lazy_value_threshold` (4 KiB) stay in Lance once committed and are read on
  demand through a cache of `value_cache_size` (64 MiB).
- A view snapshot saved on close (`snapshot_min_entries`, 100 000) lets the next open read only
  data committed since.
- Automatic background compaction of the tracking tables.
- Tracking: per-field run updates (concurrent changes to different fields are kept),
  `update_run`, run `kind` and `target`, `created_at` and `ended_at` on create and finish,
  `all_params`, `all_series`, `latest_versions`, `series_stats` stored with the points,
  time-range metric reads, `delete_runs` with tombstones (writes to a deleted run are dropped and
  rejected), a versioned `records` table with tags, and metric points sorted by series and step in
  `optimize()`. The `runs` and `metrics` tables gain columns on open; upgrade every writer of a
  repository together, as older builds cannot append to the upgraded tables.
- `purge` (key-value and tracking): physically removes deleted data from data files and old
  versions, refusing (or with `force`, deleting) tags that keep it; returns a report.
- `optimize()` on a large repository needs about half the memory: the sorted rewrite works in
  buckets of 500 000 points and folds statistics as it goes, tracking scans bound Lance's
  read-ahead, dataset handles bound Lance's metadata and index caches (64 MiB and 128 MiB), and
  the Python bindings return freed memory to the OS after `optimize()`, `compact_range()` and
  `purge()`. 4.8M points: peak 1.9 GB to about 1.0 GB, resident afterwards 1.8 GB to about
  0.7 GB.
- Records tables whose names need escaping are stored as `name=XX.lance`; the earlier `~XX`
  names made Lance file their tags under a percent-encoded twin directory, where version cleanup
  and purge did not see them, so tagged versions were not protected. Opening a local repository
  moves such tables and their tags; on object stores it reports them.
- `optimize()` folds each run's change rows into a few rows (one per sequence number a field was
  last set at, so later rows still merge field by field): 20,000 heartbeat rows of 50 runs
  list in 0.8 ms instead of 10 ms.
- `Repo::has_legacy_records` (Python `has_legacy_records()`) checks for the shared
  `records.lance` without reading it.
- `RecordQuery.run_ids` (Python `run_ids=`) reads records of several runs in one query.
- Opening a repository that still has the shared `records.lance` of earlier builds fails unless
  `legacy_records` is `Allow`; `legacy_records()` reads its rows and `drop_legacy_records()`
  removes it after a migration.
- Records tables are separate datasets, keyed by `(run_id, item_id)` with the newest row winning,
  folded and indexed by `optimize()`; `tag_records`, `record_tags`, `records_version` and
  `delete_record_tag` take the table name, and `record_tables` lists tables. See above for records written
  in the shared `records.lance` of earlier builds.
- Key-value: Lance tags (`create_tag`, `tags`, `tag_version`, `delete_tag`); version cleanup keeps
  tagged versions.

### Removed

- The SQLite cache and the S3 segment and manifest sync, with the `KV0_S3_*` environment
  variables and the `boto3` dependency.
