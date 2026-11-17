"""kv0: an ordered key-value store and experiment-tracking tables on Lance.

The key-value API is ``DB``, ``Options``, ``WriteBatch`` and the iterators: an ordered store with
byte-wise key order, atomic batches and seekable iterators. Data lives in a Lance dataset at a local path or
any object-store URI Lance supports (``s3://``, ``gs://``, ``az://``, ``memory://``).
"""

from . import errors
from .core import *
from .core import __all__ as _core_all
from .kv0 import __version__

__all__ = list(_core_all) + ["errors", "migrate", "__version__"]


def migrate(old, new, **options):
    """Converts a database or repository written with the Lance engine (kv0 0.1.0-beta.2 and
    earlier) to Delta Lake.

    The converter is the separate ``kv0-migrate`` package (``pip install kv0-migrate``), so this
    wheel does not carry the Lance reader; this calls its ``kv0_migrate.migrate`` with the same
    arguments. Raises ``errors.NotSupported`` when it is not installed.
    """
    try:
        import kv0_migrate
    except ImportError:
        raise errors.NotSupported(
            "converting Lance data needs the kv0-migrate package: pip install kv0-migrate, then "
            "kv0-migrate <old> <new> (see docs/guides/migration.md)"
        ) from None
    return kv0_migrate.migrate(old, new, **options)
