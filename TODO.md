# To do

Open items. Remove an item once it is done.

## Key-value layer

- **Time-range reads across many series.** The second read builds one `(series_id, step range)`
  clause per series; with thousands of series, batch them or use one `series_id IN` list with the
  overall step range.
- **Two-pass first open.** Without a snapshot, opening a large-value database peaks at 1.1–1.6 GB
  because the value pages are decoded before the view drops them (measured on the Lance engine;
  to be measured on moraine). Read key, sequence number, op, row
  address and value length first, then fetch only the values at or below the threshold by address.
- **Shared key prefixes.** Keys in one view share long prefixes (`seqs/chunks/<run>/...`); front
  coding in blocks would cut key memory further.
- **Snapshots after compaction:** a compaction invalidates the saved snapshot until the next close.
- **Expire WAL marks** of writers that are long gone (today one transaction id per writer is kept
  in each table's state forever).
- **Rewrite only recent files** when materialising the view, instead of the whole log once it is
  mostly shadowed rows.
- **Commit timeout** for flushes on a hung object store.

## Tracking layer

- **Queries for the tracker UI:** filter runs by experiment, tags and parameter values; list metric
  names across runs; downsampled series for charts; Arrow results (Rust `RecordBatch`, Python
  `__arrow_c_stream__`) instead of Python lists.
- **Artifacts and blobs:** a table of references (run, name, path or URI, size, checksum, media type),
  and optionally small blobs in moraine's blob columns.
- **Time travel** across the four tables (open a repository as of a tag set at one moment).
- **Retention:** `cleanup_old_versions` for repositories; deleting runs.

## Storage

- **GCS and Azure** locations, once moraine supports them.
- **An atomic tracking flush:** today a flush is four commits, and a reader can see it partly
  applied.

## Measurements

- Benchmarks against S3 latency (a real bucket or a delayed proxy), not only local disk.
- Memory of a key-value handle per million keys.
- Every benchmark again on moraine: the published numbers were measured on the Lance engine.
