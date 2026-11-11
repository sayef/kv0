# Getting started

kv0 is a library: there is no server to run. You open a database by path or URI and use it from
your process; several processes, on one machine or many, can open the same database.

## Install

=== "Python"

    ```sh
    pip install kv0        # Python 3.11 or newer
    ```

    Until the first release, build from a checkout with [maturin](https://www.maturin.rs/):
    `maturin develop --release`.

=== "Rust"

    ```sh
    cargo add kv0
    ```

    Building needs `protoc` with the well-known `.proto` includes, which Lance compiles against:
    `apt install protobuf-compiler libprotobuf-dev` or `brew install protobuf`.

## A key-value database

=== "Python"

    ```python
    --8<-- "crates/kv0-py/examples/quickstart.py"
    ```

=== "Rust"

    ```rust
    --8<-- "crates/kv0/examples/quickstart.rs"
    ```

What happened:

1. `DB(path)` created a Lance dataset at `path` (it would be `s3://bucket/prefix` on S3).
2. The batch was logged to a local write-ahead log, applied to the in-memory view (so `get` sees it
   at once), and buffered.
3. `flush()` committed the buffer as one Lance version; `close()` would have done the same.

## Open the same database elsewhere

```python
reader = kv0.DB(path, read_only=True)       # another process, or another machine on S3
reader.get(b'key_1')                        # sees every committed write; refreshes every second
old = kv0.DB(path, version=1)               # a read-only view of an earlier version
```

## A tracking repository

```python
--8<-- "crates/kv0-py/examples/tracking.py"
```

## Next

- [Concepts](concepts.md): ordering, batches, iterators, durability, versions and writers.
- [Key-value API](guides/key-value.md) and [tracking tables](guides/tracking.md).
- [Storage backends](guides/storage.md): S3, GCS, Azure and `storage_options`.
- [Operations and cleanup](guides/operations.md) before you run it in production.
