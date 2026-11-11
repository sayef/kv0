# Storage backends

A kv0 location is a local path or a URI that Lance understands. Options are passed to Lance's object
store unchanged, as `storage_options` (Python) or `StorageOptions` (Rust); anything not given falls back
to the store's usual environment variables.

| Store | Location | Common options |
|---|---|---|
| Local disk | `/data/db`, `relative/db`, `file:///data/db` | none |
| Memory | `memory://name` | none; private to one handle, gone when it closes |
| Amazon S3 | `s3://bucket/prefix/db` | `aws_region`, or the `AWS_*` environment and profiles |
| S3-compatible (MinIO, RustFS, R2, …) | `s3://bucket/prefix/db` | `aws_endpoint`, `aws_access_key_id`, `aws_secret_access_key`, `allow_http` for plain HTTP |
| Google Cloud Storage | `gs://bucket/prefix/db` | `google_service_account`, or `GOOGLE_APPLICATION_CREDENTIALS` |
| Azure Blob Storage | `az://container/prefix/db` | `azure_storage_account_name`, `azure_storage_account_key`, or the `AZURE_*` environment |

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

## The write-ahead log stays local

Key-value writes are logged to a write-ahead log on the local disk (see the
[key-value guide](key-value.md#durability-and-visibility)) even when the database is on S3, GCS or
Azure. A crash is recoverable by reopening on the same machine; writes since the last commit do
not survive the loss of the machine. Point `wal_dir` (or `KV0_WAL_DIR`) at persistent local
storage in containers.

## Concurrent writers on S3

Lance commits with conditional writes, which AWS S3 and current S3-compatible servers support. Several
writers can append to one database or repository at once; see [Design](../design.md#concurrency-and-consistency).

## Costs and latency

Each commit is a few object-store requests, so on S3 a commit takes tens to hundreds of milliseconds.
kv0 buffers writes to keep commits rare; raise `write_buffer_size` / `flush_rows` and
`flush_interval` for write-heavy workloads on remote stores, and call `compact_range()` /
`optimize()` and `cleanup_old_versions()` to keep the number of files and versions small.
