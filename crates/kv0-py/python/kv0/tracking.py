"""Experiment tracking tables: runs, parameters and metric series.

>>> from kv0.tracking import Repo
>>> repo = Repo('/tmp/tracking-example')
>>> with repo.create_run(name='baseline', experiment='demo') as run:
...     run['hparams'] = {'lr': 0.001, 'batch_size': 32}
...     for step in range(10):
...         run.track(1.0 / (step + 1), name='loss', context={'subset': 'train'})
>>> repo.metric(run.id, 'loss', context={'subset': 'train'}, start=5)['steps']
[5, 6, 7, 8, 9]

Writes are buffered per ``Repo`` and appended in batches. Reads flush the handle's buffer first
and see every other writer's flushed data.
"""

from .core import _open_handles
from .kv0 import Repo as _Repo
from .kv0 import Run

__all__ = ["Repo", "Run"]


class Repo(_Repo):
    """A tracking repository at a local path or object-store URI.

    Args:
        uri: Local directory or URI (``s3://``, ``gs://``, ``az://``, ``memory://``).
        storage_options: Object-store options passed to Lance.
        create_if_missing: Create the tables when they do not exist.
        flush_rows: Commit once this many metric points are buffered.
        flush_interval: Seconds between background commits; ``None`` disables.
        version_retention: When creating the tables, delete versions older than this many seconds
            automatically; ``None`` keeps every version.
        wal: Log every write to a local write-ahead log before acknowledging it, so buffered
            points, parameters and run changes survive a process crash.
        wal_sync: ``"none"``, ``"batch"`` (default, fsync about every 100 ms) or ``"always"``.
        wal_dir: WAL root; default ``KV0_WAL_DIR``, else the platform cache directory.
        replay_stale_wal: Replay a write-ahead log older than 30 days whose writer's marks have
            expired; its writes may then be stored twice.
        legacy_records: ``"error"`` (default) refuses a repository that still has the shared
            ``records.lance`` of earlier builds; ``"allow"`` opens it, so ``legacy_records()``
            can read the old rows for a migration and ``drop_legacy_records()`` remove them.
    """

    def __init__(
        self,
        uri,
        storage_options=None,
        create_if_missing=True,
        flush_rows=50_000,
        flush_interval=5.0,
        version_retention=7 * 24 * 3600.0,
        wal=True,
        wal_sync="batch",
        wal_dir=None,
        replay_stale_wal=False,
        legacy_records="error",
    ):
        _open_handles.add(self)
