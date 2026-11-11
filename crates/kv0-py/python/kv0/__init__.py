"""kv0: an ordered key-value store and experiment-tracking tables on Lance.

The key-value API is ``DB``, ``Options``, ``WriteBatch`` and the iterators: an ordered store with
byte-wise key order, atomic batches and seekable iterators. Data lives in a Lance dataset at a local path or
any object-store URI Lance supports (``s3://``, ``gs://``, ``az://``, ``memory://``).
"""

from . import errors
from .core import *
from .core import __all__ as _core_all
from .kv0 import __version__

__all__ = list(_core_all) + ["errors", "__version__"]
