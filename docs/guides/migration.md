# Migrating from Lance to Delta Lake

kv0 0.1.0-beta.2 and earlier store data in Lance datasets. Later releases store it in Delta Lake
tables, and a location written with Lance has to be converted once. `kv0 migrate` converts a
key-value database or a tracking repository: it copies the data, carries over tags and writer
marks, verifies every copied version, and leaves a report.

## Get the tool

=== "Command line"

    ```sh
    cargo install kv0 --features lance-migrate
    kv0 migrate <old location> <new location>
    ```

=== "Python"

    ```sh
    pip install kv0-migrate
    kv0-migrate <old location> <new location>
    ```

    ```python
    import kv0_migrate

    report = kv0_migrate.migrate("s3://bucket/repo", "s3://bucket/repo-delta")
    ```

    `kv0.migrate(...)` calls the same function when `kv0-migrate` is installed. The converter is
    a separate package so the `kv0` wheel does not carry the Lance reader.

## Steps

1. **Stop every writer.** Every process that writes to the location, on every machine, must be
   stopped, and should close cleanly so its buffered writes are committed. A writer that crashed
   leaves its write-ahead log behind; the migration carries over each writer's mark, so a new
   kv0 replays such a log correctly, but only on a location with the same name (an in-place
   migration).
2. **Upgrade every client** that will open the location to a kv0 with the Delta Lake engine.
   kv0 0.1.0-beta.2 does not know the new format: opening a migrated location, it would create
   new Lance datasets next to the Delta tables and write there.
3. **Migrate**, preferably to a new location:

    ```sh
    kv0 migrate s3://bucket/repo s3://bucket/repo-delta --storage-option aws_region=eu-central-1
    ```

    A tracking repository can also be migrated in place (`kv0 migrate <repo> <repo>`): the Delta
    tables (`runs/`, `params/`, …) do not clash with the Lance ones (`runs.lance/`, …). A
    key-value database needs `--force-in-place` for that, since both formats then share its
    directory.
4. **Check the report** that the command prints, also saved as `<new>/_kv0/migration.json`.
5. **Start the new version** on the new location. Once satisfied, delete the Lance data (or run
   the migration with `--remove-source`, which deletes it after verifying).

## What is carried over

- **Key-value databases:** the entries of the latest version, and every tagged version: for
  each tag, oldest first, the entries of that version are written as one commit replacing the
  previous one, and the tag is created on the resulting version. Values stored outside memory
  are copied as they are. Delete markers younger than an hour are kept, for slower writers.
- **Tracking repositories:** `runs`, `params`, `series`, `metrics`, every records table and the
  shared `records.lance` of older builds (as `_legacy_records`, so `legacy_records()` keeps
  working), each with its tags in the same way. Statistics rows are kept; `optimize` runs at the
  end (`--no-optimize` skips it).
- **Writer marks** (the high-water marks of write-ahead logs) become Delta `txn` actions. A
  metrics mark records only a hash of its writer; marks of writers that wrote nothing but
  points cannot be named and are counted as `unmapped_marks` in the report.
- **Version numbers restart.** The Delta versions are the commits the migration made, so tags
  name new numbers; the report maps every tag's old version to its new one. Untagged history is
  not carried over.

## Checks and failures

- Every copied version is verified: the number of entries (rows of a table, without WAL marks)
  and a checksum of their content must match the source. A mismatch fails the migration.
- A source written to in the last 10 seconds is refused (`--quiet-period` changes the period),
  and a source that changes while the migration runs fails it at the end.
- The format marker `_kv0/format.json` is written last: until then, the target is not a Delta
  location, and kv0 does not open a half-migrated one as one.
- A migration that stopped (a crash, a lost connection, a failed check) is continued with
  `--resume`: tables already verified are checked again and kept, the rest is written again. A
  key-value database is written again from the start.
- `--dry-run` reports what would be migrated (tables, rows, tags) and writes nothing.

## Options

| Option | Meaning |
|---|---|
| `--storage-option KEY=VALUE` | Object-store option of the source, and of the target unless the next is given; repeatable |
| `--target-storage-option KEY=VALUE` | Object-store option of the target, when it is on another store |
| `--no-tags` | Carry over the latest version only |
| `--dry-run` | Report, write nothing |
| `--resume` | Continue a migration that stopped |
| `--remove-source` | Delete the Lance data once everything is verified |
| `--force-in-place` | Allow a key-value database to be migrated in place |
| `--no-optimize` | Skip `optimize` on a migrated repository |
| `--quiet-period SECONDS` | Refuse a source written to more recently (default 10) |
| `--json` | Print the full report as JSON |

The Python function takes the same options as keyword arguments (`storage_options`,
`target_storage_options`, `keep_tags`, `dry_run`, `resume`, `remove_source`, `force_in_place`,
`optimize`, `quiet_period`) and returns the report as a dict.

## For vigily operators

1. Stop the vigily server and every SDK process that writes to the repository.
2. Migrate the repository, in place or to a new location:
   `kv0 migrate <repo> <repo>` or `kv0 migrate <repo> <new repo>`.
3. Start the vigily version built on the Delta Lake engine, pointing at the migrated repository.

Dataset versions that vigily stored as storage version numbers change at the migration, and the
tags behind them are recreated on the new versions. A forthcoming vigily release heals the
stored versions from the tags on its first start.
