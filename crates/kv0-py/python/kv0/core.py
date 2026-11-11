"""kv0 key-value API: ``DB``, ``Options``, ``WriteBatch`` and the iterator types.

``DB`` is a thin subclass of the native ``kv0.kv0._DB``; reads, writes and iteration
run in Rust. Writes are visible to this handle immediately and are committed to storage in
batches: when ``write_buffer_size`` bytes are buffered, every ``flush_interval`` seconds, on
``flush()``, on ``sync=True`` writes and on ``close()``.
"""

import atexit
import logging
import weakref

from .kv0 import _DB, ItemsIterator, KeysIterator, ValuesIterator

__all__ = [
    "DB",
    "BaseIterator",
    "ItemsIterator",
    "Iterator",
    "KeysIterator",
    "Options",
    "ValuesIterator",
    "WriteBatch",
]


class Iterator:
    """Base class for iterator implementations outside kv0 (for example a union of DBs).

    It defines the sentinel-``None`` ``next()`` protocol. The iterators ``DB`` returns are the
    native ``KeysIterator``, ``ValuesIterator`` and ``ItemsIterator``.
    """

    def __iter__(self):
        return self

    def __next__(self):
        item = self.next()
        if item is None:
            raise StopIteration
        return item

    def next(self):
        return None

    def get(self):
        raise ValueError()

    def skip(self):
        pass


class BaseIterator(Iterator):
    pass


class Options:
    """Permissive options bag. Unknown engine-tuning kwargs are accepted and ignored.

    Recognised options:

    * ``create_if_missing`` (default ``True``)
    * ``storage_options``: dict of object-store options passed to Lance, such as
      ``aws_endpoint``, ``aws_region`` or ``allow_http``
    * ``write_buffer_size``: flush after this many buffered bytes (default 4 MiB)
    * ``flush_interval``: seconds between background flushes; ``None`` disables (default 5)
    * ``refresh_interval``: seconds between checks for other writers' commits; ``None``
      disables automatic checks (default 1)
    * ``max_fragments``: compact automatically above this many files (default 32)
    * ``lazy_value_threshold``: values longer than this many bytes are not kept in memory once
      committed and are read from storage when needed; ``0`` keeps every value in memory
      (default 4096)
    * ``value_cache_size``: bytes of such values cached per handle (default 64 MiB)
    * ``snapshot_min_entries``: on close, save a snapshot of the view next to the WAL when it
      holds at least this many entries, so the next open reads only newer data; ``None``
      disables snapshots (default 100 000)
    * ``version_retention``: when creating the database, delete versions older than this many
      seconds automatically (default 7 days); ``None`` keeps every version
    * ``wal``: log every write to a local write-ahead log before acknowledging it, so buffered
      writes survive a process crash (default ``True``)
    * ``wal_sync``: ``"none"`` (never fsync), ``"batch"`` (fsync about every 100 ms, default) or
      ``"always"`` (fsync per write)
    * ``wal_dir``: WAL root directory (default ``KV0_WAL_DIR``, else the platform cache
      directory)
    * ``mark_ttl``: seconds compaction keeps another writer's WAL mark (default 30 days)
    * ``replay_stale_wal``: replay a write-ahead log older than ``mark_ttl`` whose writer's mark
      has expired; its writes may then be stored twice (default ``False``: the open fails)
    * ``version``: open this storage version read-only (time travel)
    """

    def __init__(self, create_if_missing: bool = True, **kwargs):
        self.create_if_missing = create_if_missing
        # Some callers check and set ``in_use``; kept as a plain attribute.
        self.in_use = False
        for key, value in kwargs.items():
            setattr(self, key, value)


class WriteBatch:
    """Accumulates write operations for an atomic :meth:`DB.write`."""

    def __init__(self, data: bytes | None = None):
        self.operations: list[tuple] = []

    def put(self, key: bytes, value: bytes, column_family=None):
        self.operations.append(("PUT", key, value, column_family))

    def merge(self, key: bytes, value: bytes, column_family=None):
        # Merge operators are unsupported; treated as last-write-wins.
        self.operations.append(("MERGE", key, value, column_family))

    def delete(self, key: bytes, column_family=None):
        self.operations.append(("DELETE", key, None, column_family))

    def delete_range(self, begin_key: bytes, end_key: bytes, column_family=None):
        self.operations.append(("DELETE_RANGE", begin_key, end_key, column_family))

    def clear(self):
        self.operations.clear()

    def count(self) -> int:
        return len(self.operations)

    def __iter__(self):
        return iter(self.operations)


class DB(_DB):
    """Ordered key-value database stored in Lance.

    ``db_path`` is a local directory or an object-store URI. Extra keyword arguments override
    the matching ``Options`` attributes (for example ``storage_options={...}``).
    """

    def __init__(
        self,
        db_path,
        opts: Options | None = None,
        column_families: dict | None = None,
        read_only: bool = False,
        **kwargs,
    ):
        self.db_path = db_path
        self.opts = opts if opts is not None else Options()
        _open_handles.add(self)

    # ----- compatibility shims ------------------------------------------

    def get_column_family(self, name: bytes):
        return name.decode("utf-8") if isinstance(name, (bytes, bytearray)) else name

    @property
    def column_families(self):
        return ["default"]

    def get_property(self, prop: bytes, column_family=None):
        return None

    def get_live_files_metadata(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


_log = logging.getLogger("kv0")

# Handles still open at interpreter exit are flushed, so a script that never calls close() keeps
# its writes.
_open_handles: "weakref.WeakSet" = weakref.WeakSet()


@atexit.register
def _flush_open_handles():
    for handle in list(_open_handles):
        try:
            handle.close()
        except Exception as exc:  # noqa: BLE001 - exit must not fail; report and go on
            _log.warning("kv0: closing %r at exit failed: %s", handle, exc)
