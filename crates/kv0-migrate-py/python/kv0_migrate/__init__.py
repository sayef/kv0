"""kv0-migrate: converts kv0 data written with the Lance engine (kv0 0.1.0-beta.2 and earlier) to
Delta Lake.

Stop every writer first. See the migration guide in kv0's documentation.
"""

import json
import os
import sys

from ._native import MigrationError, __version__, cli, migrate_json

__all__ = ["MigrationError", "__version__", "main", "migrate"]


def migrate(
    old,
    new,
    storage_options=None,
    target_storage_options=None,
    keep_tags=True,
    dry_run=False,
    resume=False,
    remove_source=False,
    force_in_place=False,
    optimize=True,
    quiet_period=10.0,
):
    """Converts the database or repository at ``old`` to Delta Lake at ``new``.

    ``new`` may equal ``old`` for a repository (and for a database with ``force_in_place``).
    Every tag is carried over with the data of its version unless ``keep_tags`` is false. Each
    copied version is verified (row counts and checksums); ``remove_source`` then deletes the Lance
    data. ``dry_run`` only reports. ``resume`` continues a migration that stopped. A source written
    to less than ``quiet_period`` seconds ago is refused.

    Returns the report (as written to ``<new>/_kv0/migration.json``) as a dict. Raises
    ``MigrationError`` when the migration fails or is refused.
    """
    report = migrate_json(
        os.fspath(old),
        os.fspath(new),
        storage_options,
        target_storage_options,
        keep_tags,
        dry_run,
        resume,
        remove_source,
        force_in_place,
        optimize,
        quiet_period,
    )
    return json.loads(report)


def main(argv=None):
    """The ``kv0-migrate`` command: ``kv0-migrate <from> <to> [options]``."""
    args = list(sys.argv[1:] if argv is None else argv)
    if args[:1] != ["migrate"]:
        args = ["migrate", *args]
    return cli(args)
