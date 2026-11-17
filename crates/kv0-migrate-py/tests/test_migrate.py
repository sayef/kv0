"""kv0-migrate converts a Lance database and repository written with the kv0 package."""

import json
import os

import kv0
import kv0_migrate
import pytest
from kv0.tracking import Repo


def _lance_db(path):
    db = kv0.DB(str(path), kv0.Options(engine="lance", flush_interval=None))
    for i in range(200):
        db.put(b"k%03d" % i, b"v%d" % i)
    db.delete(b"k007")
    db.flush()
    db.close()


def test_migrate_a_database_and_read_it(tmp_path):
    src, dst = tmp_path / "db", tmp_path / "db-delta"
    _lance_db(src)
    report = kv0_migrate.migrate(src, dst, quiet_period=0)
    assert report["status"] == "complete"
    assert report["kind"] == "database"
    assert report["tables"][0]["rows"] == 199
    with open(dst / "_kv0" / "migration.json") as f:
        assert json.load(f) == report
    with open(dst / "_kv0" / "format.json") as f:
        assert json.load(f)["engine"] == "delta"
    try:
        db = kv0.DB(str(dst), kv0.Options(create_if_missing=False))
    except kv0.errors.NotSupported:
        pytest.skip("this kv0 build has no Delta Lake engine")
    assert db.get(b"k001") == b"v1"
    assert db.get(b"k007") is None
    db.close()


def test_migrate_a_repository_in_place_and_refusals(tmp_path):
    path = tmp_path / "repo"
    repo = Repo(str(path), engine="lance", flush_interval=None)
    run = repo.create_run(run_id="r1", name="first")
    for step in range(50):
        run.track(1.0 / (step + 1), "loss", step=step)
    repo.flush()
    repo.close()
    with pytest.raises(kv0_migrate.MigrationError, match="stop every writer"):
        kv0_migrate.migrate(path, path)
    report = kv0_migrate.migrate(path, path, quiet_period=0, dry_run=True)
    assert report["status"] == "dry-run"
    report = kv0_migrate.migrate(path, path, quiet_period=0, remove_source=True)
    assert report["in_place"] and report["source_removed"]
    assert not os.path.exists(path / "runs.lance")
    with pytest.raises(kv0_migrate.MigrationError, match="already"):
        kv0_migrate.migrate(path, path, quiet_period=0)


def test_the_command_line(tmp_path, capfd):
    src, dst = tmp_path / "db", tmp_path / "db-delta"
    _lance_db(src)
    assert kv0_migrate.main([str(src), str(dst), "--quiet-period", "0"]) == 0
    assert "migrated the database" in capfd.readouterr().out
    assert kv0_migrate.main([str(src)]) == 2


def test_the_kv0_stub_points_here(tmp_path):
    src, dst = tmp_path / "db", tmp_path / "db-delta"
    _lance_db(src)
    report = kv0.migrate(str(src), str(dst), quiet_period=0)
    assert report["status"] == "complete"
