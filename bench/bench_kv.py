"""Key-value benchmark: put, batch write, get, seek + scan and full scan.

Runs against whichever ``kv0`` is importable, so the same script measures the Lance-backed
release and the pure-Python 0.1 release (installed in another environment). Synthetic keys
shaped like a tracker's (a run prefix, a metric path and a big-endian step).

    python bench/bench_kv.py --n 100000 --out bench/results/kv-new.json
"""

import argparse
import json
import os
import platform
import random
import shutil
import struct
import sys
import tempfile
import time

try:
    import kv0
except ImportError:  # the 0.1 baseline (pure Python), under the import name it was released with
    import importlib

    name = os.environ.get("KV0_BASELINE_MODULE")
    if not name:
        sys.exit(
            "kv0 is not installed here; for the 0.1 baseline, set KV0_BASELINE_MODULE to its import name"
        )
    kv0 = importlib.import_module(name)


def keys(n):
    out = []
    for i in range(n):
        run = i % 50
        metric = (i // 50) % 20
        step = i // 1000
        out.append(
            b"seqs/chunks/run%04d/metric%02d/val/" % (run, metric) + struct.pack(">Q", step) + b"%d" % i
        )
    return out


def timed(fn):
    t = time.perf_counter()
    r = fn()
    return time.perf_counter() - t, r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=100_000)
    ap.add_argument("--out")
    args = ap.parse_args()
    n = args.n
    ks = keys(n)
    vals = [struct.pack("<d", random.random()) for _ in range(n)]
    lookups = random.Random(1).sample(ks, min(n, 20_000))
    root = tempfile.mkdtemp(prefix="kv0-bench-")
    res = {
        "engine": getattr(kv0, "__version__", "0.1 (pure Python + SQLite)"),
        "n": n,
        "python": platform.python_version(),
        "machine": platform.machine(),
    }
    try:
        opts = kv0.Options(create_if_missing=True, flush_interval=None)

        db = kv0.DB(os.path.join(root, "put"), opts)
        t, _ = timed(lambda: [db.put(k, v) for k, v in zip(ks, vals)])
        res["put_ops_s"] = n / t
        t, _ = timed(db.flush)
        res["flush_after_put_s"] = t

        db2 = kv0.DB(os.path.join(root, "batch"), opts)

        def batch_write():
            for start in range(0, n, 1000):
                b = kv0.WriteBatch()
                for k, v in zip(ks[start : start + 1000], vals[start : start + 1000]):
                    b.put(k, v)
                db2.write(b)
            db2.flush()

        t, _ = timed(batch_write)
        res["batch_write_ops_s"] = n / t

        t, _ = timed(lambda: [db.get(k) for k in lookups])
        res["get_ops_s"] = len(lookups) / t

        def seek_scan():
            total = 0
            it = db.iteritems()
            for run in range(50):
                it.seek(b"seqs/chunks/run%04d/metric03/" % run)
                for _ in range(100):
                    if it.next() is None:
                        break
                    total += 1
            return total

        t, cnt = timed(seek_scan)
        res["seek_plus_100_next_per_s"] = 50 / t

        def full_scan():
            it = db.iteritems()
            it.seek_to_first()
            return sum(1 for _ in it)

        t, cnt = timed(full_scan)
        assert cnt == n, cnt
        res["full_scan_items_s"] = n / t
        db.close()
        db2.close()

        # Write-ahead log modes (0.2 only): plain puts with each wal_sync setting, and with the
        # WAL off. The default (batch) is the put_ops_s row above.
        if not res["engine"].startswith("0.1"):
            for mode, m in (("none", n), ("batch", n), ("always", 2000), ("off", n)):
                o = kv0.Options(
                    create_if_missing=True,
                    flush_interval=None,
                    write_buffer_size=1 << 40,
                    wal=mode != "off",
                    wal_sync="batch" if mode == "off" else mode,
                )
                dbw = kv0.DB(os.path.join(root, "wal-" + mode), o)
                t, _ = timed(lambda dbw=dbw, m=m: [dbw.put(k, v) for k, v in zip(ks[:m], vals[:m])])
                res[f"put_wal_{mode}_ops_s"] = m / t
                dbw.close()

        # Durable per write: 0.1 commits every put to SQLite anyway (and ignores sync); 0.2 makes
        # each sync=True write one Lance commit.
        db3 = kv0.DB(os.path.join(root, "sync"), opts)
        m = 500
        t, _ = timed(lambda: [db3.put(k, v, sync=True) for k, v in zip(ks[:m], vals[:m])])
        res["sync_put_ops_s"] = m / t
        db3.close()

        t, db = timed(lambda: kv0.DB(os.path.join(root, "put"), opts, read_only=True))
        res["reopen_s"] = t
        res["disk_bytes"] = sum(
            os.path.getsize(os.path.join(dp, f))
            for dp, _, fs in os.walk(os.path.join(root, "put"))
            for f in fs
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print(json.dumps(res, indent=2))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(res, f, indent=2)
            f.write("\n")


if __name__ == "__main__":
    main()
