"""Tracking layer: runs, parameters and metric series."""

import kv0
import pytest
from kv0.tracking import Repo


def _repo(path, **kw):
    kw.setdefault("flush_interval", None)
    return Repo(str(path), **kw)


def test_track_and_read(tmp_path):
    repo = _repo(tmp_path / "repo")
    with repo.create_run(name="baseline", experiment="exp-a", tags=["synthetic"]) as run:
        run["hparams"] = {"lr": 0.001, "optimizer": {"name": "adam"}}
        run["seed"] = 7
        for step in range(50):
            run.track(1.0 / (step + 1), name="loss")
            run.track(step / 50, name="acc", step=step, epoch=step // 10, context={"subset": "val"})

    runs = repo.runs()
    assert [r["name"] for r in runs] == ["baseline"]
    assert runs[0]["status"] == "finished"
    assert runs[0]["tags"] == ["synthetic"]
    assert repo.params(run.id) == {"hparams": {"lr": 0.001, "optimizer": {"name": "adam"}}, "seed": 7}

    series = sorted(repo.series(run.id), key=lambda s: s["name"])
    assert [(s["name"], s["context"]) for s in series] == [("acc", {"subset": "val"}), ("loss", {})]

    loss = repo.metric(run.id, "loss")
    assert loss["steps"] == list(range(50))
    assert loss["values"][0] == 1.0
    acc = repo.metric(run.id, "acc", context={"subset": "val"}, start=10, stop=15)
    assert acc["steps"] == [10, 11, 12, 13, 14]
    assert acc["epochs"] == [1, 1, 1, 1, 1]
    assert repo.metric(run.id, "acc")["steps"] == []  # different context

    repo.optimize()
    assert repo.metric(run.id, "loss") == loss


def test_track_many(tmp_path):
    repo = _repo(tmp_path / "repo")
    with repo.create_run() as one, repo.create_run() as many:
        values = [0.5, float("nan"), float("inf"), 0.25, 0.125]
        steps = [None, None, 7, 1, None]
        for v, s in zip(values, steps, strict=True):
            one.track(v, name="loss", step=s, context={"subset": "train"})
        many.track_many("loss", values, steps=steps, context={"subset": "train"})
        many.track_many("loss", [])
        with pytest.raises(ValueError, match="steps has 1 entries"):
            many.track_many("loss", [1.0, 2.0], steps=[3])

    a = repo.metric(one.id, "loss", context={"subset": "train"})
    b = repo.metric(many.id, "loss", context={"subset": "train"})
    assert a["steps"] == b["steps"] == [0, 1, 7, 8]
    assert [repr(v) for v in a["values"]] == [repr(v) for v in b["values"]]
    assert b["values"][1] == 0.25  # the later point at step 1 wins


def test_failed_run_and_resume(tmp_path):
    repo = _repo(tmp_path / "repo")
    with pytest.raises(RuntimeError), repo.create_run(name="crash") as run:
        run.track(1.0, name="x")
        raise RuntimeError("synthetic failure")
    assert repo.run(run.id)["status"] == "failed"
    repo.close()

    repo = _repo(tmp_path / "repo")
    resumed = repo.resume_run(run.id)
    resumed.track(2.0, name="x")
    resumed.update(description="resumed", archived=True)
    resumed.finish()
    assert repo.metric(run.id, "x")["steps"] == [0, 1]
    info = repo.run(run.id)
    assert info["description"] == "resumed" and info["archived"] and info["status"] == "finished"


def test_buffer_flushes_by_size(tmp_path):
    repo = _repo(tmp_path / "repo", flush_rows=100)
    run = repo.create_run()
    for i in range(1000):
        run.track(float(i), name="m")
    other = _repo(tmp_path / "repo")
    # At least nine full buffers were committed without an explicit flush.
    assert len(other.metric(run.id, "m")["steps"]) >= 900


# ---- write-ahead log ------------------------------------------------------------------------


def _child(code, **kw):
    import subprocess
    import sys

    return subprocess.Popen([sys.executable, "-c", code], **kw)


def _wal_files(root):
    import pathlib

    return sorted(pathlib.Path(root).rglob("*.wal"))


def test_tracking_wal_survives_sigkill(tmp_path):
    import signal
    import subprocess
    import textwrap

    code = textwrap.dedent(f"""
        from kv0.tracking import Repo
        repo = Repo({str(tmp_path / "repo")!r}, flush_interval=None, flush_rows=1 << 40,
                    wal_dir={str(tmp_path / "wal")!r})
        run = repo.create_run(run_id='r1', name='crash')
        run['hparams'] = {{'lr': 0.1}}
        i = 0
        while True:
            run.track(float(i), name='loss', context={{'subset': 'train'}})
            print(i, flush=True)
            i += 1
    """)
    child = _child(code, stdout=subprocess.PIPE, text=True)
    acked = -1
    for line in child.stdout:
        acked = int(line)
        if acked >= 3000:
            break
    child.send_signal(signal.SIGKILL)
    child.wait()
    repo = _repo(tmp_path / "repo", wal_dir=str(tmp_path / "wal"))
    m = repo.metric("r1", "loss", context={"subset": "train"})
    assert m["steps"][: acked + 1] == list(range(acked + 1))
    assert repo.params("r1") == {"hparams": {"lr": 0.1}}
    assert [r["name"] for r in repo.runs()] == ["crash"]


def _crash(tmp_path, body):
    import textwrap

    code = (
        textwrap.dedent(f"""
        import os, signal
        from kv0.tracking import Repo
        repo = Repo({str(tmp_path / "repo")!r}, flush_interval=None, flush_rows=1 << 40,
                    wal_dir={str(tmp_path / "wal")!r})
    """)
        + textwrap.dedent(body)
        + "\nos.kill(os.getpid(), signal.SIGKILL)\n"
    )
    _child(code).wait()


def test_tracking_wal_torn_tail(tmp_path):
    _crash(
        tmp_path,
        """
        run = repo.create_run(run_id='r1')
        for i in range(10):
            run.track(float(i), name='m')
    """,
    )
    seg = _wal_files(tmp_path / "wal")[-1]
    seg.write_bytes(seg.read_bytes() + b"\x63\x00\x00\x00\x07")
    repo = _repo(tmp_path / "repo", wal_dir=str(tmp_path / "wal"))
    assert len(repo.metric("r1", "m")["steps"]) == 10


@pytest.mark.parametrize("cleanup", [False, True])
def test_tracking_replay_is_idempotent(tmp_path, cleanup):
    _crash(
        tmp_path,
        """
        import pathlib, time
        run = repo.create_run(run_id='r1')
        run['lr'] = 0.1
        for i in range(100):
            run.track(float(i), name='m')
        saved = [(p, p.read_bytes()) for p in pathlib.Path(WAL).rglob('*.wal')]
        repo.flush()
        run['lr'] = 0.2
        repo.flush()
        if CLEANUP:
            repo.optimize()
            time.sleep(0.02)
            repo.cleanup_old_versions(0)
        for p, b in saved:
            p.write_bytes(b)
    """.replace("WAL", repr(str(tmp_path / "wal"))).replace("CLEANUP", repr(cleanup)),
    )
    repo = _repo(tmp_path / "repo", wal_dir=str(tmp_path / "wal"))
    m = repo.metric("r1", "m")
    assert m["steps"] == list(range(100)), "no point twice"
    assert repo.params("r1") == {"lr": 0.2}


def test_tracker_requests(tmp_path):
    repo = _repo(tmp_path / "repo")
    run = repo.create_run(run_id="e1", kind="eval", target={"model": "model-a"}, created_at=1_000_000)
    run["lr"] = 0.1
    run.track(0.5, name="loss")
    run.update(description="first")
    repo.flush()

    info = repo.run("e1")
    assert (info["kind"], info["target"], info["created_at"]) == ("eval", {"model": "model-a"}, 1_000_000)
    assert repo.all_params() == {"e1": {"lr": 0.1}}
    assert [s["name"] for s in repo.all_series()] == ["loss"]

    before = repo.latest_versions()
    written = repo.update_run("e1", tags=["x"], status="failed")
    assert written["tags"] == ["x"] and written["description"] == "first"
    repo.flush()
    assert repo.latest_versions()[0] > before[0]
    assert repo.run("e1")["status"] == "failed"
    with pytest.raises(kv0.errors.NotFound):
        repo.update_run("missing", name="x")

    run.finish("finished", ended_at=2_000_000)
    assert repo.run("e1")["ended_at"] == 2_000_000

    v1 = repo.append_records(
        "items", [{"item_id": "q1", "data": {"input": "a"}}, {"item_id": "q2", "data": {"input": "b"}}]
    )
    repo.tag_records("items", "items-v1")
    repo.append_records("items", [{"item_id": "q3", "data": {"input": "c"}}])
    repo.append_records("scores", [{"item_id": "q1", "run_id": "e1", "data": {"score": 1.0}}])
    assert [r["item_id"] for r in repo.records("items")] == ["q1", "q2", "q3"]
    assert [r["item_id"] for r in repo.records("items", version=v1)] == ["q1", "q2"]
    assert repo.records("items", tag="items-v1") == repo.records("items", version=v1)
    assert repo.record_tags("items") == [("items-v1", v1)]
    assert repo.record_tables() == ["items", "scores"]
    repo.append_records("items", [{"item_id": "q1", "data": {"input": "a2"}}])  # supersedes q1
    assert [r["data"]["input"] for r in repo.records("items", item_id="q1")] == ["a2"]
    assert repo.records("scores", run_id="e1")[0]["data"] == {"score": 1.0}

    assert repo.delete_runs(["e1"]) == 1
    assert repo.runs() == []
    report = repo.purge()
    assert report["runs"] == 1 and report["versions_removed"] > 0
    assert repo.records("items", tag="items-v1")  # a dataset tag is not in the way
    assert repo.records("scores") == []


def test_series_stats(tmp_path):
    repo = _repo(tmp_path / "repo")
    run = repo.create_run(run_id="r")
    for step, v in enumerate([3.0, 1.0, 5.0]):
        run.track(v, name="loss", step=step)
    repo.flush()
    ((sid, st),) = repo.series_stats().items()
    assert (st["count"], st["last"], st["min"], st["max"]) == (3, 5.0, 1.0, 5.0)
    assert repo.series_stats([sid]) == {sid: st}
    assert repo.recompute_series_stats() == 1


def test_records_of_several_runs(tmp_path):
    repo = _repo(tmp_path / "repo")
    repo.append_records("scores", [{"item_id": "q", "run_id": f"r{i}", "data": {"v": i}} for i in range(5)])
    rows = repo.records("scores", run_ids=["r1", "r3", "nope"])
    assert sorted(r["run_id"] for r in rows) == ["r1", "r3"]
    assert repo.records("scores", run_ids=[]) == []
    assert repo.legacy_records() == []
    assert repo.has_legacy_records() is False
    assert repo.drop_legacy_records() is False
