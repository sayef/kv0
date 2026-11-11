"""Exceptions raised by kv0.

``NotFound`` is also a ``FileNotFoundError`` and ``StoreIOError`` an ``OSError``; the others
derive from ``Exception`` as before.
"""

from .kv0 import (
    Conflict,
    Corruption,
    Incomplete,
    InvalidArgument,
    MergeInProgress,
    NotFound,
    NotSupported,
    StoreIOError,
)

__all__ = [
    "Conflict",
    "Corruption",
    "Incomplete",
    "InvalidArgument",
    "MergeInProgress",
    "NotFound",
    "NotSupported",
    "StoreIOError",
]
