# Storage backends

A kv0 location is a local path, `memory://` or an `s3://` URI. Options are passed to the object
store as `storage_options` (Python) or `StorageOptions` (Rust); anything not given falls back to
the usual `AWS_*` environment variables.

| Store | Location | Common options |
|---|---|---|
| Local disk | `/data/db`, `relative/db`, `file:///data/db` | none |
| Memory | `memory://name` | none; private to one handle, gone when it closes |
| Amazon S3 | `s3://bucket/prefix/db` | `aws_region`, or the `AWS_*` environment |
| S3-compatible (MinIO, RustFS, R2, …) | `s3://bucket/prefix/db` | `aws_endpoint`, `aws_access_key_id`, `aws_secret_access_key`, `allow_http` for plain HTTP |

```python
db = kv0.DB('s3://bucket/kv', storage_options={
    'aws_endpoint': 'http://localhost:9000',
    'aws_access_key_id': 'local-key',
    'aws_secret_access_key': 'local-secret',
    'aws_region': 'us-east-1',
    'allow_http': 'true',
})
```

Do not put credentials in code you commit. Prefer the environment, instance roles or short-lived
credentials.

Google Cloud Storage (`gs://`) and Azure Blob Storage (`az://`) are not supported for now:
opening such a location fails with `errors.NotSupported`. The `aws`, `gcp` and `azure` cargo
features still exist, so builds that enable them keep compiling, but they enable nothing: S3 is
always built in, and the other two have no backend yet.

## Storage engine

kv0 stores everything with [moraine](https://github.com/sayef/moraine), a client-only database of
Delta Lake tables on object storage. A key-value database is a moraine database with one table,
`ops`; a tracking repository is one moraine database with the tables `runs`, `params`, `series`
and `metrics`, and one table per records table (`rec-` and an encoding of its name; the real
names are kept under `_kv0/records/`).

The `engine` option and the `KV0_ENGINE` environment variable remain, but the only engine is
`"moraine"`, which is also what `None` means. Naming `"lance"` or `"delta"` fails with
`errors.NotSupported`. A database location opened as a tracking repository (or the reverse)
fails as well.

Locations carry a marker, `_kv0/format.json`, written when they are created:

```json
{"engine": "moraine", "version": 1, "kind": "database", "created_by": "kv0 0.1.0-beta.3"}
```

A marker from a newer format version is refused rather than misread.
`kv0::storage::detect(location, options)` reports what a location holds without opening it:
empty, a database or a repository with its engine, or unknown.

## Earlier formats

kv0 0.1.0-beta.2 and earlier stored data in Lance datasets, and for a while kv0 also had a Delta
Lake engine of its own. Both are gone. kv0 still recognises their layouts, but only to refuse
them: opening such a location fails with `errors.NotSupported` and a message saying to recreate
it.

While kv0 is in beta there is no migration. I chose this over carrying the Lance reader and a
converter in every build: the betas come with no compatibility promise, and keeping two engines
meant two formats to test and document. To keep data from such a location, read it with the kv0
release that wrote it (for example `pip install kv0==0.1.0b2`), and write it into a new location
with the current kv0. Do not open one location with both releases: kv0 0.1.0-beta.2 does not
know the moraine format and would create a Lance dataset next to it.

## The write-ahead log stays local

Key-value writes are logged to a write-ahead log on the local disk (see the
[key-value guide](key-value.md#durability-and-visibility)) even when the database is on S3. A
crash is recoverable by reopening on the same machine; writes since the last commit do not
survive the loss of the machine. Point `wal_dir` (or `KV0_WAL_DIR`) at persistent local storage
in containers.

## Concurrent writers on S3

A moraine commit is one conditional write of a log entry, which AWS S3 and current S3-compatible
servers support. Several writers can append to one database or repository at once; see
[Design](../design.md#concurrency-and-consistency).

## Costs and latency

Each commit is one object-store request per data file plus one for the log entry, so on S3 a
commit takes tens to hundreds of milliseconds. kv0 buffers writes to keep commits rare; raise
`write_buffer_size` / `flush_rows` and `flush_interval` for write-heavy workloads on remote
stores, and call `compact_range()` / `optimize()` and `cleanup_old_versions()` to keep the
number of files small.
