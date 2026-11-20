"""kv0: an ordered key-value store and experiment-tracking tables on object storage.

The key-value API is ``DB``, ``Options``, ``WriteBatch`` and the iterators: an ordered store with
byte-wise key order, atomic batches and seekable iterators. Data lives in a moraine database (Delta Lake
tables) at a local path, ``s3://`` (and S3-compatible stores) or ``memory://``.
"""

from . import errors
from .core import *
from .core import __all__ as _core_all
from .kv0 import __version__

__all__ = list(_core_all) + ["errors", "__version__"]
