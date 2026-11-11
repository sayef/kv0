"""Runs the 0.1-vs-0.2 evaluation: every cell, REPS times, each in a fresh process.

    python bench/eval/run_all.py [--reps 5] [--only tracker,bulk] [--out bench/eval/results.jsonl]

Needs: bench/.venv-old with the 0.1 baseline (bench/run.sh makes it; set KV0_BASELINE_MODULE), .venv with this release built
in release mode, Docker (RustFS for the S3 cells), the aws CLI, and sudo for dropping the page
cache before cold cells (skipped, and recorded as warm, without it). Results are appended as
JSON lines; cells already in the output file are skipped, so a run can be resumed.
"""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CELL = Path(__file__).with_name("cell.py")
PY = {
    "v01": ROOT / "bench/.venv-old/bin/python",
    "v02": Path(os.environ.get("KV0_EVAL_V02_PY", ROOT / ".venv/bin/python")),
    # A baseline build of 0.2 to compare this one against (`--plan compare`).
    "v02b": Path(os.environ.get("KV0_EVAL_BASE_PY", ROOT / ".venv-base/bin/python")),
}
BUCKET = "kv0-bench"
S3 = "http://127.0.0.1:9000"
PROXY = "http://127.0.0.1:9100"
CREDS = {
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "AWS_REGION": "us-east-1",
    "AWS_DEFAULT_REGION": "us-east-1",
}
MIN_FREE = 30 << 30
WORK = Path(os.environ.get("KV0_EVAL_DIR", tempfile.gettempdir())) / "kv0-eval"

LOCAL = ["v01", "v02"]
ALL = ["v01", "v02", "v02-always", "v01-s3", "v02-s3"]


def aws_env():
    env = {k: v for k, v in os.environ.items() if k != "AWS_PROFILE"}
    env.update(CREDS)
    return env


DROP = hasattr(os, "posix_fadvise")


def drop_caches(*dirs):
    """Evicts the files under `dirs` from the page cache (posix_fadvise DONTNEED, no root needed:
    /proc/sys/vm/drop_caches is read-only in containers)."""
    if not DROP:
        return False
    subprocess.run(["sync"], check=False)
    for d in dirs:
        for dp, _, files in os.walk(d):
            for f in files:
                try:
                    fd = os.open(os.path.join(dp, f), os.O_RDONLY)
                except OSError:
                    continue
                try:
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                finally:
                    os.close(fd)
    return True


def env_for(engine, data, prefix):
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("KV0_", "AWS_")) or k in ("KV0_EVAL_DIR", "KV0_BASELINE_MODULE")
    }
    if engine == "v01-s3":
        env.update(
            CREDS,
            KV0_S3_BUCKET=BUCKET,
            KV0_S3_ENDPOINT=PROXY,
            KV0_S3_REGION="us-east-1",
            KV0_S3_PREFIX=prefix,
            KV0_LOCAL_ROOT=str(data),
            KV0_FLUSH_INTERVAL_SEC="5",
        )
    return env


def spec_for(engine, workload, data, prefix, **params):
    spec = {
        "engine": engine,
        "workload": workload,
        "data": str(data),
        "wal_dir": str(data / "_wal"),
        **params,
    }
    if engine == "v02-s3":
        spec["s3_prefix"] = f"s3://{BUCKET}/{prefix.rstrip('/')}"
        spec["storage_options"] = {
            "aws_endpoint": PROXY,
            "allow_http": "true",
            "aws_access_key_id": "minioadmin",
            "aws_secret_access_key": "minioadmin",
            "aws_region": "us-east-1",
        }
    return spec


def proxy(path, method="GET"):
    req = urllib.request.Request(PROXY + path, method=method, data=b"" if method == "POST" else None)
    return urllib.request.urlopen(req, timeout=10).read()


def run_cell(engine, workload, data, prefix, tag=None, background=False, **params):
    data.mkdir(parents=True, exist_ok=True)
    py = PY["v01" if engine.startswith("v01") else "v02b" if engine == "v02b" else "v02"]
    spec = spec_for(engine, workload, data, prefix, **params)
    if tag and engine.endswith("-s3"):
        proxy("/__tag/" + tag, "POST")
    args = [str(py), str(CELL), json.dumps(spec)]
    env = env_for(engine, data, prefix)
    if background:
        return subprocess.Popen(args, env=env, stdout=subprocess.PIPE, text=True)
    r = subprocess.run(args, env=env, check=False, capture_output=True, text=True, timeout=7200)
    lines = [ln for ln in r.stdout.splitlines() if ln.startswith("{")]
    if workload == "crash-child":
        return json.loads(lines[-1]) if lines else {"error": r.stderr[-500:]}
    if r.returncode != 0 or not lines:
        return {"error": (r.stderr or r.stdout)[-800:]}
    return json.loads(lines[-1])


def s3_stats(tag):
    try:
        return json.loads(proxy("/__stats")).get(tag, {})
    except Exception:  # noqa: BLE001
        return {}


def disk(engine, data, prefix):
    if engine.endswith("-s3"):
        r = subprocess.run(
            [
                "aws",
                "--endpoint-url",
                S3,
                "s3",
                "ls",
                f"s3://{BUCKET}/{prefix}",
                "--recursive",
                "--summarize",
            ],
            check=False,
            capture_output=True,
            text=True,
            env=aws_env(),
        )
        for ln in r.stdout.splitlines():
            if "Total Size" in ln:
                return int(ln.split(":")[1])
        return None
    total = 0
    for dp, dirs, files in os.walk(data):
        dirs[:] = [d for d in dirs if d != "_wal"]
        total += sum(os.path.getsize(os.path.join(dp, f)) for f in files)
    return total


# ---- the plan --------------------------------------------------------------------------------


def plan():
    cells = []
    for e in ["v01", "v02", "v01-s3", "v02-s3"]:
        cells.append(("tracker", e, {"steps": 5000}))
    for e in ALL:
        cells.append(("tracker-small", e, {"steps": 200}))
    for e in ALL:
        cells.append(("bulk", e, {"n": 1_000_000}))
    for e in ["v01", "v02", "v01-s3", "v02-s3"]:
        cells.append(("point", e, {"n": 1_000_000}))
        cells.append(("scan", e, {"n": 1_000_000}))
    for e in LOCAL:
        for n in (10_000, 1_000_000, 10_000_000):
            cells.append(("reopen", e, {"n": n}))
    for e in ["v01-s3", "v02-s3"]:
        for n in (10_000, 1_000_000):
            cells.append(("reopen", e, {"n": n}))
    for e in LOCAL:
        cells.append(("large", e, {"n": 100_000, "value_size": 16384}))
    for e in ["v01", "v02", "v01-s3", "v02-s3"]:
        cells.append(("concurrent", e, {"n": 200_000}))
    for e in ["v01", "v02"]:
        cells.append(("crash", e, {"n": 200_000}))
    # Every put fsynced (about 6 ms each here): 20 000 puts keeps a repetition near two minutes.
    cells.append(("crash", "v02-always", {"n": 20_000}))
    return cells


def plan_compare():
    """This build (v02) against a baseline build of 0.2 (v02b), local cells, both in this run."""
    cells = []
    for e in ("v02b", "v02"):
        cells += [
            ("tracker", e, {"steps": 5000}),
            ("tracker-small", e, {"steps": 200}),
            ("bulk", e, {"n": 1_000_000}),
            ("point", e, {"n": 1_000_000}),
            ("scan", e, {"n": 1_000_000}),
        ]
        for n in (10_000, 1_000_000, 10_000_000):
            cells.append(("reopen", e, {"n": n}))
        for n in (1_000_000, 10_000_000):
            cells.append(("reopen-rw", e, {"n": n}))
        cells += [
            ("large", e, {"n": 100_000, "value_size": 16384}),
            ("concurrent", e, {"n": 200_000}),
            ("crash", e, {"n": 200_000}),
        ]
    # This build opening without a view snapshot (a first open on a machine): a full scan.
    for n in (1_000_000, 10_000_000):
        cells.append(("reopen", "v02ns", {"n": n}))
    cells.append(("large", "v02ns", {"n": 100_000, "value_size": 16384}))
    return cells


def drop_snapshots(engine, data):
    if engine == "v02ns":
        for p in Path(data).glob("_wal/*/view.snap"):
            p.unlink()


def dataset(engine, n, value_size=8):
    """A database of n keys built once per engine, for the read cells."""
    name = f"ds-{engine}-{n}-{value_size}"
    data = WORK / name
    marker = data / "_built.json"
    if marker.exists():
        return data, name + "/", json.loads(marker.read_text())
    shutil.rmtree(data, ignore_errors=True)
    r = run_cell(engine, "bulk", data, name + "/", n=n, value_size=value_size)
    if "error" in r:
        raise RuntimeError(f"building {name}: {r['error']}")
    r["disk_bytes"] = disk(engine, data, name + "/")
    marker.write_text(json.dumps(r))
    return data, name + "/", r


def fresh(cell_id):
    d = WORK / "cells" / cell_id
    shutil.rmtree(d, ignore_errors=True)
    return d


def run_one(kind, engine, p, rep):
    cid = f"{kind}-{engine}-{p.get('n', p.get('steps'))}-r{rep}"
    res = {"kind": kind, "engine": engine, "rep": rep, **p}
    if kind in ("tracker", "tracker-small"):
        d = fresh(cid)
        w = run_cell(engine, "tracker-write", d, cid + "/", tag=cid + "-w", steps=p["steps"])
        res["write"] = w
        res["write"]["s3"] = s3_stats(cid + "-w")
        res["disk_bytes"] = disk(engine, d, cid + "/")
        if kind == "tracker":
            ui_dir = d if engine != "v01-s3" else fresh(cid + "-ui")
            res["cold"] = drop_caches(d)
            u = run_cell(engine, "tracker-read", ui_dir, cid + "/", tag=cid + "-u", steps=p["steps"])
            u["s3"] = s3_stats(cid + "-u")
            res["ui"] = u
    elif kind == "large" and shutil.disk_usage(WORK).free < MIN_FREE:
        # The host disk is shared: a 4.9 GB cell needs 30 GB of headroom.
        res["skipped"] = f"less than {MIN_FREE >> 30} GiB free"
    elif kind in ("bulk", "large"):
        d = fresh(cid)
        r = run_cell(engine, "bulk", d, cid + "/", tag=cid, **p)
        r["s3"] = s3_stats(cid)
        r["disk_bytes"] = disk(engine, d, cid + "/")
        res.update(r)
        if kind == "large":
            drop_snapshots(engine, d)
            res["cold"] = drop_caches(d)
            res["reopen"] = run_cell(engine, "reopen", d, cid + "/", **p)
        shutil.rmtree(d, ignore_errors=True)  # 100k × 16 KiB is 4.9 GB per repetition
    elif kind in ("point", "scan", "reopen", "reopen-rw"):
        data, prefix, built = dataset(engine, p["n"])
        res["dataset"] = built
        if engine == "v01-s3":  # a cold cache on a new machine: nothing local yet
            data = fresh(cid)
        drop_snapshots(engine, data)
        res["cold"] = drop_caches(data)
        r = run_cell(
            engine,
            "reopen" if kind == "reopen-rw" else kind,
            data,
            prefix,
            tag=cid,
            rep=rep,
            writable=kind == "reopen-rw",
            **p,
        )
        r["s3"] = s3_stats(cid)
        res.update(r)
    elif kind == "concurrent":
        d = fresh(cid)
        if engine.startswith("v02"):  # create the dataset first, so both writers append
            run_cell(engine, "bulk", d, cid + "/", n=1)
        t0 = time.perf_counter()
        # 0.1 on S3 is one local cache per machine: two writers are two machines on one prefix.
        dirs = [d / f"w{i}" if engine == "v01-s3" else d for i in range(2)]
        procs = [
            run_cell(engine, "write-part", dirs[i], cid + "/", background=True, part=i, n=p["n"])
            for i in range(2)
        ]
        outs = [
            json.loads(pr.communicate()[0].splitlines()[-1]) if pr.wait() == 0 else {"error": "writer failed"}
            for pr in procs
        ]
        wall = time.perf_counter() - t0
        res["writers"] = outs
        res["keys_per_s"] = 2 * p["n"] / wall
        if engine == "v01-s3":  # what a third machine sees: only what both pushed to S3
            res["visible"] = run_cell(engine, "count", fresh(cid + "-v"), cid + "/")
        else:
            res["visible"] = run_cell(engine, "count", d, cid + "/")
    elif kind == "crash":
        d = fresh(cid)
        res["child"] = run_cell(engine, "crash-child", d, cid + "/", **p)
        r = run_cell(engine, "crash-recover", d, cid + "/", **p)
        res.update(r)
    return cid, res


def start_s3():
    # Reuse a running server: recreating it would empty the bucket under datasets already built.
    running = (
        subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", "kv0-eval-s3"],
            check=False,
            capture_output=True,
            text=True,
        ).stdout.strip()
        == "true"
    )
    if not running:
        subprocess.run(["docker", "rm", "-f", "kv0-eval-s3"], check=False, capture_output=True)
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                "kv0-eval-s3",
                "-p",
                "9000:9000",
                "-e",
                "RUSTFS_ACCESS_KEY=minioadmin",
                "-e",
                "RUSTFS_SECRET_KEY=minioadmin",
                "rustfs/rustfs:latest",
            ],
            check=True,
            capture_output=True,
        )
        for d in WORK.glob("ds-*-s3-*"):  # their objects went with the old server
            shutil.rmtree(d, ignore_errors=True)
    for _ in range(60):
        try:
            urllib.request.urlopen(S3 + "/health", timeout=2)
            break
        except Exception:  # noqa: BLE001
            time.sleep(1)
    r = subprocess.run(
        ["aws", "--endpoint-url", S3, "s3", "mb", f"s3://{BUCKET}"],
        check=False,
        capture_output=True,
        text=True,
        env=aws_env(),
    )
    if r.returncode != 0 and "BucketAlreadyOwnedByYou" not in r.stderr:
        raise RuntimeError(f"creating the bucket failed: {r.stderr}")
    p = subprocess.Popen(
        [sys.executable, str(Path(__file__).with_name("s3proxy.py")), "9100", "127.0.0.1:9000"]
    )
    time.sleep(1)
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--only")
    ap.add_argument("--plan", choices=["main", "compare"], default="main")
    ap.add_argument("--out", default=str(ROOT / "bench/eval/results.jsonl"))
    a = ap.parse_args()
    out = Path(a.out)
    done = set()
    if out.exists():
        done = {json.loads(ln)["id"] for ln in out.read_text().splitlines() if ln.strip()}
    cells = plan_compare() if a.plan == "compare" else plan()
    if a.only:
        cells = [c for c in cells if c[0] in a.only.split(",")]
    WORK.mkdir(parents=True, exist_ok=True)
    px = start_s3() if any(e.endswith("-s3") for _, e, _ in cells) else None
    host = {
        "cpu": platform.processor() or platform.machine(),
        "python": platform.python_version(),
        "drop_caches": DROP,
    }
    try:
        for kind, engine, p in cells:
            for rep in range(a.reps):
                cid = f"{kind}-{engine}-{p.get('n', p.get('steps'))}-r{rep}"
                if cid in done:
                    continue
                t = time.time()
                cid, res = run_one(kind, engine, p, rep)
                res.update(id=cid, host=host, wall_s=time.time() - t)
                with out.open("a") as f:
                    f.write(json.dumps(res) + "\n")
                print(f"{cid}: {res['wall_s']:.1f}s", flush=True)
                shutil.rmtree(WORK / "cells", ignore_errors=True)
    finally:
        if px:
            px.terminate()


if __name__ == "__main__":
    main()
