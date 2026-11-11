"""One benchmark cell: a workload against one engine, in a fresh process.

    python cell.py '<json spec>'

The spec names the engine (`v01`, `v01-s3`, `v02`, `v02-always`, `v02-s3`), the workload and its
parameters, and the directories to use. The result is one JSON object on stdout. The same file
runs under the 0.1 baseline (pure Python on a SQLite cache) and 0.2 (Rust), so it uses only the API
both share. Set `KV0_BASELINE_MODULE` to the baseline's import name for the 0.1 engines.

Workloads, all on synthetic data:

- tracker-write / tracker-read: an experiment tracker's key-value traffic for one training run:
  many small puts of metric points and their running summaries into two databases, then a reader
  that opens both, lists run metadata and reads series by range (see bench/REPORT.md).
- bulk: N keys in `WriteBatch`es of 1000, then close.
- point: random `get`s on an existing database (`cold`: first pass after open; `hot`: second pass).
- scan: `seek` + 100 `next()` at random prefixes, and one full scan.
- reopen: open an existing database and read one key.
- write-part: one of two concurrent writers.
- crash-child: N single puts, then SIGKILL itself; crash-recover: reopen and count.
"""

import json
import os
import random
import resource
import signal
import struct
import sys
import time
from array import array

SPEC = json.loads(sys.argv[1])
ENGINE = SPEC["engine"]
V02 = ENGINE.startswith("v02")

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

SEP = b"\xfe"


def opts(**kw):
    o = dict(create_if_missing=True, **kw)
    if V02:
        o["wal_dir"] = SPEC["wal_dir"]
        o["wal_sync"] = "always" if ENGINE == "v02-always" else "batch"
        if ENGINE == "v02-s3":
            o["storage_options"] = SPEC["storage_options"]
    return kv0.Options(**o)


def location(name):
    if ENGINE == "v02-s3":
        return f"{SPEC['s3_prefix']}/{name}"
    return os.path.join(SPEC["data"], name)


def open_db(name, read_only=False, **kw):
    return kv0.DB(location(name), opts(**kw), read_only=read_only)


def pct(lat_ns, q):
    if not lat_ns:
        return None
    s = sorted(lat_ns)
    return s[min(len(s) - 1, int(q * len(s)))] / 1000.0  # µs


def lat_summary(prefix, lat):
    return {f"{prefix}_p50_us": pct(lat, 0.50), f"{prefix}_p99_us": pct(lat, 0.99)}


def key(i):
    run = i % 50
    metric = (i // 50) % 20
    return b"seqs/chunks/run%04d/metric%02d/val/" % (run, metric) + struct.pack(">Q", i)


VALUE = struct.pack("<d", 0.5)


# ---- tracker workload -------------------------------------------------------------------------------


def series():
    return [(b"%d" % ctx, b"metric%02d" % m) for ctx in range(2) for m in range(10)]


def tracker_write():
    steps, run = SPEC["steps"], b"3f2a9c"
    meta, seqs = open_db("meta"), open_db("seqs")
    rnd = random.Random(1)
    lat = array("q")
    puts = 0
    state = {s: [0.5, 0.5, 0.5] for s in series()}  # value, min, max
    t0 = time.perf_counter()
    for ctx, name in series():  # first track of a series: dtype, version, first...
        base = b"meta" + SEP + run + SEP + b"traces" + SEP + ctx + SEP + name + SEP
        for f in (b"dtype", b"version", b"first", b"first_step"):
            meta.put(base + f, b"\x01float")
            puts += 1
    for step in range(steps):
        h = struct.pack(">Q", step)
        for s in series():
            ctx, name = s
            st = state[s]
            st[0] = max(0.0, st[0] + rnd.uniform(-0.01, 0.01))
            v = struct.pack("<d", st[0])
            sp = b"seqs" + SEP + run + SEP + ctx + SEP + name + SEP
            mp = b"meta" + SEP + run + SEP + b"traces" + SEP + ctx + SEP + name + SEP
            ops = [
                (seqs, sp + b"val" + SEP + h, v),
                (seqs, sp + b"epoch" + SEP + h, b"\x00"),
                (seqs, sp + b"time" + SEP + h, struct.pack("<d", time.time())),
                (meta, mp + b"last", v),
                (meta, mp + b"last_step", h),
            ]
            if st[0] < st[1]:
                st[1] = st[0]
                ops.append((meta, mp + b"min", v))
            if st[0] > st[2]:
                st[2] = st[0]
                ops.append((meta, mp + b"max", v))
            for db, k, val in ops:
                a = time.perf_counter_ns()
                db.put(k, val)
                lat.append(time.perf_counter_ns() - a)
            puts += len(ops)
    t_track = time.perf_counter() - t0
    a = time.perf_counter()
    meta.close()
    seqs.close()
    t_close = time.perf_counter() - a
    return {
        "puts": puts,
        "puts_per_s": puts / t_track,
        "close_s": t_close,
        "total_s": time.perf_counter() - t0,
        **lat_summary("put", lat),
    }


def tracker_read():
    run = b"3f2a9c"
    t0 = time.perf_counter()
    meta = open_db("meta", read_only=True)
    seqs = open_db("seqs", read_only=True)
    t_open = time.perf_counter() - t0
    a = time.perf_counter()
    it = meta.iteritems()
    it.seek_to_first()
    n_meta = sum(1 for _ in it)
    t_list = time.perf_counter() - a
    lat = array("q")
    points = 0
    for ctx, name in series():
        a = time.perf_counter_ns()
        for arr in (b"val", b"time"):
            prefix = b"seqs" + SEP + run + SEP + ctx + SEP + name + SEP + arr + SEP
            it = seqs.iteritems()
            it.seek(prefix)
            for k, _ in it:
                if not k.startswith(prefix):
                    break
                points += 1
        lat.append(time.perf_counter_ns() - a)
    glat = array("q")
    for ctx, name in series():
        k = b"meta" + SEP + run + SEP + b"traces" + SEP + ctx + SEP + name + SEP + b"last"
        a = time.perf_counter_ns()
        assert meta.get(k) is not None
        glat.append(time.perf_counter_ns() - a)
    return {
        "open_s": t_open,
        "list_s": t_list,
        "meta_keys": n_meta,
        "points_read": points,
        "total_s": time.perf_counter() - t0,
        **lat_summary("series_read", lat),
        **lat_summary("last_get", glat),
    }


# ---- generic workloads -----------------------------------------------------------------------


def bulk():
    n, vsize = SPEC["n"], SPEC.get("value_size", 8)
    # Pseudo-random (incompressible) values, seeded so every engine stores the same bytes; a
    # constant value would measure the storage format's compression, not the store.
    rnd = random.Random(42)
    db = open_db("db", write_buffer_size=SPEC.get("write_buffer_size", 4 << 20))
    lat = array("q")
    t0 = time.perf_counter()
    for start in range(0, n, 1000):
        b = kv0.WriteBatch()
        for i in range(start, min(n, start + 1000)):
            b.put(key(i), rnd.randbytes(vsize))
        a = time.perf_counter_ns()
        db.write(b)
        lat.append(time.perf_counter_ns() - a)
    a = time.perf_counter()
    db.close()
    t_close = time.perf_counter() - a
    total = time.perf_counter() - t0
    return {
        "n": n,
        "keys_per_s": n / total,
        "close_s": t_close,
        "total_s": total,
        **lat_summary("batch", lat),
    }


def point():
    n = SPEC["n"]
    t0 = time.perf_counter()
    db = open_db("db", read_only=True)
    t_open = time.perf_counter() - t0
    rnd = random.Random(SPEC.get("rep", 0))
    keys = [key(rnd.randrange(n)) for _ in range(20000)]
    out = {"open_s": t_open}
    for phase in ("cold", "hot"):
        lat = array("q")
        a = time.perf_counter()
        for k in keys:
            b = time.perf_counter_ns()
            assert db.get(k) is not None
            lat.append(time.perf_counter_ns() - b)
        out[f"{phase}_gets_per_s"] = len(keys) / (time.perf_counter() - a)
        out.update(lat_summary(f"{phase}_get", lat))
    return out


def scan():
    n = SPEC["n"]
    db = open_db("db", read_only=True)
    rnd = random.Random(SPEC.get("rep", 0))
    lat = array("q")
    for _ in range(200):
        start = key(rnd.randrange(n))[:-8]
        a = time.perf_counter_ns()
        it = db.iteritems()
        it.seek(start)
        for _ in range(100):
            if it.next() is None:
                break
        lat.append(time.perf_counter_ns() - a)
    a = time.perf_counter()
    it = db.iteritems()
    it.seek_to_first()
    count = sum(1 for _ in it)
    t_full = time.perf_counter() - a
    assert count == n, (count, n)
    return {"full_scan_items_per_s": n / t_full, **lat_summary("seek100", lat)}


def reopen():
    t0 = time.perf_counter()
    db = open_db("db", read_only=not SPEC.get("writable"))
    assert db.get(key(SPEC["n"] - 1)) is not None
    return {"open_s": time.perf_counter() - t0}


def write_part():
    part, n = SPEC["part"], SPEC["n"]
    db = open_db("db")
    errors = 0
    t0 = time.perf_counter()
    for start in range(0, n, 100):
        b = kv0.WriteBatch()
        for i in range(start, min(n, start + 100)):
            b.put(b"w%d/" % part + struct.pack(">Q", i), VALUE)
        for attempt in range(20):
            try:
                db.write(b)
                break
            except Exception:  # noqa: BLE001 - 0.1 on one SQLite file raises "database is locked"
                errors += 1
                time.sleep(0.05)
    db.close()
    return {"elapsed_s": time.perf_counter() - t0, "errors": errors, "start": t0}


def count_keys():
    db = open_db("db", read_only=True)
    it = db.iterkeys()
    it.seek_to_first()
    return {"keys": sum(1 for _ in it)}


def crash_child():
    n = SPEC["n"]
    db = open_db("db", flush_interval=None, write_buffer_size=1 << 40)
    for i in range(n):
        db.put(key(i), VALUE)
    sys.stdout.write(json.dumps({"written": n}) + "\n")
    sys.stdout.flush()
    os.kill(os.getpid(), signal.SIGKILL)


def crash_recover():
    t0 = time.perf_counter()
    db = open_db("db")
    t_open = time.perf_counter() - t0
    it = db.iterkeys()
    it.seek_to_first()
    n = sum(1 for _ in it)
    return {"recover_s": t_open, "keys": n}


WORKLOADS = {
    "tracker-write": tracker_write,
    "tracker-read": tracker_read,
    "bulk": bulk,
    "point": point,
    "scan": scan,
    "reopen": reopen,
    "write-part": write_part,
    "count": count_keys,
    "crash-child": crash_child,
    "crash-recover": crash_recover,
}

if __name__ == "__main__":
    out = WORKLOADS[SPEC["workload"]]()
    out["peak_rss_mb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print(json.dumps(out))
