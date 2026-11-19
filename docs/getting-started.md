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

    Building needs Rust 1.95 or newer; no system libraries or code generators.

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

1. `DB(path)` created a database at `path` (it would be `s3://bucket/prefix` on S3): a
   moraine database with one Delta Lake table of operations.
2. The batch was logged to a local write-ahead log, applied to the in-memory view (so `get` sees it
   at once), and buffered.
3. `flush()` committed the buffer as one version; `close()` would have done the same.

## Open the same database elsewhere

```python
reader = kv0.DB(path, read_only=True)       # another process, or another machine on S3
reader.get(b'key_1')                        # sees every committed write; refreshes every second
db.create_tag('v1')                         # keep this handle's version readable
old = kv0.DB(path, version=db.tag_version('v1'))   # read-only, at the tagged version
```

## A tracking repository

```python
--8<-- "crates/kv0-py/examples/tracking.py"
```

## Next

- [Concepts](concepts.md): ordering, batches, iterators, durability, versions and writers.
- [Key-value API](guides/key-value.md) and [tracking tables](guides/tracking.md).
- [Storage backends](guides/storage.md): S3 and S3-compatible stores, and `storage_options`.
- [Operations and cleanup](guides/operations.md) before you run it in production.
