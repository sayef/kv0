# kv0-migrate

Converts kv0 key-value databases and tracking repositories written with the Lance engine (kv0
0.1.0-beta.2 and earlier) to Delta Lake.

```sh
pip install kv0-migrate
kv0-migrate <old location> <new location>
```

```python
import kv0_migrate

report = kv0_migrate.migrate("s3://bucket/repo", "s3://bucket/repo-delta")
```

Stop every writer before migrating. The steps, options and what is carried over are in the
[migration guide](https://github.com/sayef/kv0/blob/main/docs/guides/migration.md).

This package is separate from `kv0` so the main wheel does not carry the Lance reader.
