"""Storage behaviour new in the Lance engine: persistence, refresh, time travel, errors."""

import os
import pathlib
import uuid

import kv0
import pytest
from kv0 import errors


def _opts(**kw):
    kw.setdefault("flush_interval", None)
    return kv0.Options(create_if_missing=True, **kw)


def test_persists_across_reopen(tmp_path):
    path = tmp_path / "db"
    db = kv0.DB(path, _opts())  # os.PathLike is accepted
    batch = kv0.WriteBatch()
    for i in range(1000):
        batch.put(b"k%04d" % i, b"v%d" % i)
    db.write(batch)
    db.delete_range(b"k0000", b"k0100")
    db.close()

    db = kv0.DB(str(path), _opts())
    assert db.get(b"k0050") is None
    assert db.get(b"k0500") == b"v500"
    it = db.iterkeys()
    it.seek_to_first()
    assert next(it) == b"k0100"
    assert db.stats()["live_keys"] == 900
    db.compact_range()
    assert db.stats()["fragments"] == 1
    db.close()


def test_unflushed_writes_are_visible_and_flushed_on_close(tmp_path):
    path = str(tmp_path / "db")
    db = kv0.DB(path, _opts())
    db.put(b"a", b"1")
    assert db.get(b"a") == b"1"
    assert db.stats()["pending_ops"] == 1
    db.close()
    with pytest.raises(errors.InvalidArgument):
        db.get(b"a")
    assert kv0.DB(path, _opts()).get(b"a") == b"1"


def test_sync_write_commits_immediately(tmp_path):
    path = str(tmp_path / "db")
    db = kv0.DB(path, _opts())
    reader = kv0.DB(path, _opts(refresh_interval=None), read_only=True)
    db.put(b"a", b"1", sync=True)
    assert db.stats()["pending_ops"] == 0
    assert reader.get(b"a") is None
    assert reader.refresh() is True
    assert reader.get(b"a") == b"1"


def test_read_only_rejects_writes(tmp_path):
    path = str(tmp_path / "db")
    kv0.DB(path, _opts()).close()
    ro = kv0.DB(path, _opts(), read_only=True)
    assert ro.read_only
    with pytest.raises(errors.NotSupported):
        ro.put(b"a", b"1")


def test_missing_database(tmp_path):
    with pytest.raises(errors.NotFound) as exc:
        kv0.DB(str(tmp_path / "nope"), kv0.Options(create_if_missing=False))
    assert isinstance(exc.value, FileNotFoundError)


def test_time_travel(tmp_path):
    path = str(tmp_path / "db")
    db = kv0.DB(path, _opts())
    db.put(b"k", b"old", sync=True)
    v = db.version
    db.put(b"k", b"new", sync=True)
    old = kv0.DB(path, _opts(version=v))
    assert old.get(b"k") == b"old"
    assert old.read_only
    assert [n for n, _ in db.versions()][-1] == db.version


def test_memory_uri():
    db = kv0.DB("memory://" + uuid.uuid4().hex, _opts())
    db.put(b"a", b"1", sync=True)
    assert db.get(b"a") == b"1"


def test_context_manager(tmp_path):
    path = pathlib.Path(tmp_path) / "db"
    with kv0.DB(path, _opts()) as db:
        db.put(b"a", b"1")
    assert kv0.DB(path, _opts()).get(b"a") == b"1"


@pytest.mark.skipif(
    not os.environ.get("KV0_TEST_S3_ENDPOINT"),
    reason="set KV0_TEST_S3_ENDPOINT (and a bucket) to run against MinIO",
)
def test_s3_compatible_store():
    endpoint = os.environ["KV0_TEST_S3_ENDPOINT"]
    bucket = os.environ.get("KV0_TEST_S3_BUCKET", "kv0-test")
    storage = {
        "aws_endpoint": endpoint,
        "aws_access_key_id": os.environ.get("KV0_TEST_S3_KEY", "minioadmin"),
        "aws_secret_access_key": os.environ.get("KV0_TEST_S3_SECRET", "minioadmin"),
        "aws_region": "us-east-1",
        "allow_http": "true",
    }
    uri = f"s3://{bucket}/py-{uuid.uuid4().hex}/db"
    db = kv0.DB(uri, _opts(storage_options=storage))
    for i in range(100):
        db.put(b"%03d" % i, b"x")
    db.flush()
    db.delete(b"050", sync=True)
    other = kv0.DB(uri, _opts(storage_options=storage), read_only=True)
    assert other.get(b"049") == b"x"
    assert other.get(b"050") is None
    db.compact_range()
    assert other.refresh()
    it = other.iterkeys()
    it.seek_to_first()
    assert len(list(it)) == 99


def test_unclosed_handles_flush_at_exit(tmp_path):
    import subprocess
    import sys
    import textwrap

    code = textwrap.dedent(f"""
        import kv0
        from kv0.tracking import Repo
        db = kv0.DB({str(tmp_path / "db")!r}, flush_interval=None)
        db.put(b'k', b'v')
        repo = Repo({str(tmp_path / "repo")!r}, flush_interval=None)
        run = repo.create_run(run_id='r1')
        run.track(1.0, name='m')
        # No close(), no flush(): the interpreter exits with buffered writes.
    """)
    subprocess.run([sys.executable, "-c", code], check=True)
    assert kv0.DB(str(tmp_path / "db"), read_only=True).get(b"k") == b"v"
    from kv0.tracking import Repo

    assert Repo(str(tmp_path / "repo")).metric("r1", "m")["values"] == [1.0]


def test_no_deadlock_when_lance_logs_during_flush(tmp_path):
    # Lance logs from its runtime threads through pyo3-log, which needs the GIL. Writes that flush
    # inline, reads that refresh and dropping the last handle must not hold it.
    import subprocess
    import sys
    import textwrap

    code = textwrap.dedent(f"""
        import logging
        logging.basicConfig(level=logging.DEBUG, stream=open('/dev/null', 'w'))
        import kv0
        from kv0.tracking import Repo
        db = kv0.DB({str(tmp_path / "db")!r}, write_buffer_size=1024, refresh_interval=0.001)
        for i in range(3000):
            db.put(b'%05d' % i, b'x' * 16)
            db.get(b'00001')
        del db
        repo = Repo({str(tmp_path / "repo")!r}, flush_rows=100)
        run = repo.create_run()
        for i in range(1000):
            run.track(float(i), name='m')
        del run, repo
    """)
    subprocess.run([sys.executable, "-c", code], check=True, timeout=120)


# ---- write-ahead log ------------------------------------------------------------------------


def _wal_files(root):
    return sorted(p for p in pathlib.Path(root).rglob("*.wal"))


def _child(code, env_extra, **kw):
    import subprocess
    import sys

    env = dict(os.environ, **env_extra)
    return subprocess.Popen([sys.executable, "-c", code], env=env, **kw)


def test_wal_survives_sigkill(tmp_path):
    import signal
    import subprocess
    import textwrap

    code = textwrap.dedent(f"""
        import sys, kv0
        db = kv0.DB({str(tmp_path / "db")!r}, flush_interval=None, write_buffer_size=1 << 40)
        i = 0
        while True:
            if i % 10 == 0:
                b = kv0.WriteBatch()
                b.put(b'b%08d' % i, b'x'); b.put(b'c%08d' % i, b'x')
                db.write(b)
            else:
                db.put(b'k%08d' % i, b'v')
            print(i, flush=True)
            i += 1
    """)
    child = _child(code, {"KV0_WAL_DIR": str(tmp_path / "wal")}, stdout=subprocess.PIPE, text=True)
    acked = -1
    for line in child.stdout:
        acked = int(line)
        if acked >= 2000:
            break
    child.send_signal(signal.SIGKILL)
    child.wait()
    db = kv0.DB(str(tmp_path / "db"), _opts(wal_dir=str(tmp_path / "wal")))
    for i in range(acked + 1):
        keys = [b"b%08d" % i, b"c%08d" % i] if i % 10 == 0 else [b"k%08d" % i]
        for k in keys:
            assert db.get(k) is not None, (i, k)
    assert db.stats()["pending_ops"] == 0
    db.close()
    assert _wal_files(tmp_path / "wal") == []


def _abandon(code_db, tmp_path, body):
    """Runs ``body`` against a DB in a child that then dies without flushing."""
    import textwrap

    code = (
        textwrap.dedent(f"""
        import os, signal, kv0
        db = kv0.DB({code_db!r}, flush_interval=None, write_buffer_size=1 << 40,
                         wal_dir={str(tmp_path / "wal")!r})
    """)
        + textwrap.dedent(body)
        + "\nos.kill(os.getpid(), signal.SIGKILL)\n"
    )
    _child(code, {}).wait()


def test_wal_torn_tail_and_corruption(tmp_path):
    db_path = str(tmp_path / "db")
    _abandon(db_path, tmp_path, "db.put(b'a', b'1')\ndb.put(b'b', b'2')\n")
    seg = _wal_files(tmp_path / "wal")[-1]
    clean = seg.read_bytes()
    seg.write_bytes(clean + b"\x28\x00\x00\x00\x01\x02")  # a record cut off mid-write
    db = kv0.DB(db_path, _opts(wal_dir=str(tmp_path / "wal")))
    assert (db.get(b"a"), db.get(b"b")) == (b"1", b"2")
    db.close()

    db_path = str(tmp_path / "db2")
    _abandon(db_path, tmp_path, "db.put(b'a', b'1')\ndb.put(b'b', b'2')\n")
    seg = _wal_files(tmp_path / "wal")[-1]
    bad = bytearray(seg.read_bytes())
    bad[8 + 12 + 6] ^= 0xFF
    seg.write_bytes(bytes(bad))
    with pytest.raises(errors.Corruption):
        kv0.DB(db_path, _opts(wal_dir=str(tmp_path / "wal")))


def test_wal_replay_after_commit_is_idempotent(tmp_path):
    db_path = str(tmp_path / "db")
    # Copy the WAL aside, commit (which deletes it), write once more, then die and put the
    # stale WAL back: a crash between the commit and the WAL truncation.
    _abandon(
        db_path,
        tmp_path,
        """
        import pathlib, shutil
        for i in range(100):
            db.put(b'%03d' % i, b'v1')
        saved = [(p, p.read_bytes()) for p in pathlib.Path(WAL).rglob('*.wal')]
        db.flush()
        db.put(b'050', b'v2', sync=True)
        stored = db.stats()['stored_rows']
        for p, b in saved:
            p.write_bytes(b)
        open(STORED, 'w').write(str(stored))
    """.replace("WAL", repr(str(tmp_path / "wal"))).replace("STORED", repr(str(tmp_path / "stored"))),
    )
    stored = int((tmp_path / "stored").read_text())
    db = kv0.DB(db_path, _opts(wal_dir=str(tmp_path / "wal")))
    assert db.stats()["stored_rows"] == stored
    assert db.get(b"050") == b"v2"
    assert db.get(b"099") == b"v1"


def test_wal_separate_writers(tmp_path):
    db_path = str(tmp_path / "db")
    _abandon(db_path, tmp_path, "db.put(b'from-a', b'1')\n")
    _abandon(db_path, tmp_path, "db.put(b'from-b', b'2')\n")
    db = kv0.DB(db_path, _opts(wal_dir=str(tmp_path / "wal")))
    assert (db.get(b"from-a"), db.get(b"from-b")) == (b"1", b"2")


def test_wal_sync_modes_and_validation(tmp_path):
    for mode in ("none", "batch", "always"):
        db = kv0.DB(str(tmp_path / mode), _opts(wal_sync=mode, wal_dir=str(tmp_path / "wal")))
        db.put(b"k", b"v")
        db.close()
    with pytest.raises(errors.InvalidArgument):
        kv0.DB(str(tmp_path / "x"), _opts(wal_sync="sometimes"))


def test_disable_wal_write_is_lost_in_a_crash(tmp_path):
    db_path = str(tmp_path / "db")
    _abandon(
        db_path,
        tmp_path,
        """
        db.put(b'logged', b'1')
        db.put(b'unlogged', b'2', disable_wal=True)
        b = kv0.WriteBatch()
        b.put(b'unlogged-batch', b'3')
        db.write(b, disable_wal=True)
        db.put(b'synced', b'4', disable_wal=True, sync=True)
    """,
    )
    db = kv0.DB(db_path, _opts(wal_dir=str(tmp_path / "wal")))
    assert db.get(b"logged") == b"1"
    assert db.get(b"synced") == b"4"
    # The sync=True write committed everything buffered before it, unlogged writes included.
    assert db.get(b"unlogged") == b"2"
    assert db.get(b"unlogged-batch") == b"3"


def test_disable_wal_write_after_last_flush_is_lost(tmp_path):
    db_path = str(tmp_path / "db")
    _abandon(
        db_path,
        tmp_path,
        """
        db.put(b'logged', b'1')
        db.put(b'unlogged', b'2', disable_wal=True)
    """,
    )
    db = kv0.DB(db_path, _opts(wal_dir=str(tmp_path / "wal")))
    assert db.get(b"logged") == b"1"
    assert db.get(b"unlogged") is None


def test_kv_replay_after_cleanup_to_zero_retention(tmp_path):
    db_path = str(tmp_path / "db")
    _abandon(
        db_path,
        tmp_path,
        """
        import pathlib, time
        for i in range(20):
            db.put(b'%03d' % i, b'v1')
        saved = [(p, p.read_bytes()) for p in pathlib.Path(WAL).rglob('*.wal')]
        db.flush()
        db.put(b'005', b'v2')
        db.delete(b'006')
        db.compact_range()
        time.sleep(0.02)
        db.cleanup_old_versions(0)
        assert len(db.versions()) == 1
        open(STORED, 'w').write(str(db.stats()['stored_rows']))
        for p, b in saved:
            p.write_bytes(b)
    """.replace("WAL", repr(str(tmp_path / "wal"))).replace("STORED", repr(str(tmp_path / "stored"))),
    )
    db = kv0.DB(db_path, _opts(wal_dir=str(tmp_path / "wal")))
    assert db.stats()["stored_rows"] == int((tmp_path / "stored").read_text())
    assert db.get(b"005") == b"v2"
    assert db.get(b"006") is None


def test_getters_do_not_deadlock_with_a_flush(tmp_path):
    # db.version and db.stats() used to wait for the flush lock while holding the GIL, and a
    # flush holding that lock logs through pyo3-log, which needs the GIL (review L1).
    import subprocess
    import sys
    import textwrap

    code = textwrap.dedent(f"""
        import logging, os, threading, time
        logging.basicConfig(level=logging.DEBUG, stream=open(os.devnull, 'w'))
        import kv0
        db = kv0.DB({str(tmp_path / "db")!r}, wal_dir={str(tmp_path / "wal")!r},
                         flush_interval=None)
        stop = False
        def writer():
            i = 0
            while not stop:
                for j in range(2000):
                    db.put(b'k%08d' % (i * 2000 + j), b'v' * 100)
                db.flush()
                i += 1
        t = threading.Thread(target=writer, daemon=True)
        t.start()
        t0 = time.time()
        while time.time() - t0 < 5:
            _ = db.version
            _ = db.stats()
        stop = True
        t.join(30)
        assert not t.is_alive()
    """)
    subprocess.run([sys.executable, "-c", code], check=True, timeout=120)


def test_failed_wal_append_does_not_drop_later_writes(tmp_path):
    # A write that fails part-way (file-size limit standing in for a full disk) must not leave a
    # partial record that hides later acknowledged writes on replay (review L2).
    import subprocess
    import sys
    import textwrap

    code = textwrap.dedent(f"""
        import glob, os, resource, signal
        import kv0
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        d = {str(tmp_path)!r}
        db = kv0.DB(os.path.join(d, 'db'), wal_dir=os.path.join(d, 'wal'), flush_interval=None)
        db.put(b'a', b'1')
        seg = sorted(glob.glob(os.path.join(d, 'wal', '*', '*', '*.wal')))[-1]
        soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
        resource.setrlimit(resource.RLIMIT_FSIZE, (os.path.getsize(seg) + 100, hard))
        try:
            db.put(b'b', b'x' * 10_000)
            raise SystemExit('the big put should have failed')
        except kv0.errors.StoreIOError:
            pass
        resource.setrlimit(resource.RLIMIT_FSIZE, (soft, hard))
        db.put(b'c', b'3')
        os._exit(0)  # crash before any flush
    """)
    subprocess.run([sys.executable, "-c", code], check=True, timeout=120)
    db = kv0.DB(str(tmp_path / "db"), _opts(wal_dir=str(tmp_path / "wal")))
    assert db.get(b"a") == b"1"
    assert db.get(b"b") is None  # reported as failed, so not replayed either
    assert db.get(b"c") == b"3"


def test_long_idle_writer_wal_is_not_mistaken_for_stale(tmp_path):
    # A writer idle for longer than mark_ttl, whose mark another handle's compaction pruned, then
    # writes and crashes: its WAL holds only uncommitted writes and must replay (review N1).
    import subprocess
    import sys
    import textwrap
    import time

    code = textwrap.dedent(f"""
        import os, sys, kv0
        d = {str(tmp_path)!r}
        a = kv0.DB(os.path.join(d, 'db'), wal_dir=os.path.join(d, 'wal'), flush_interval=None,
                        mark_ttl=1)
        a.put(b'k1', b'1')
        a.flush()
        print('flushed', flush=True)
        sys.stdin.readline()
        a.put(b'k2', b'2')
        os._exit(0)
    """)
    child = subprocess.Popen(
        [sys.executable, "-c", code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    child.stdout.readline()
    time.sleep(1.5)
    b = kv0.DB(str(tmp_path / "db"), _opts(wal_dir=str(tmp_path / "wal"), mark_ttl=1))
    b.put(b"x", b"y")
    b.compact_range()
    b.close()
    child.stdin.write("go\n")
    child.stdin.flush()
    child.wait(timeout=60)
    r = kv0.DB(str(tmp_path / "db"), _opts(wal_dir=str(tmp_path / "wal"), mark_ttl=1))
    assert r.get(b"k2") == b"2"
    assert r.get(b"k1") == b"1"
