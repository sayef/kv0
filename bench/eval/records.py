"""Records get and scan at N rows: 'evals' (N rows, half of the keys written twice) next to
'other' (N rows), then optimize. Prints JSON."""

import json
import random
import shutil
import statistics
import sys
import time

from kv0.tracking import Repo

path, n = sys.argv[1], int(sys.argv[2])
shutil.rmtree(path, ignore_errors=True)
repo = Repo(path, flush_interval=None)
pad = "x" * 160
t0 = time.perf_counter()
for table in ("other", "evals"):
    for lo in range(0, n, 20000):
        repo.append_records(
            table, [{"item_id": f"i{k}", "data": {"v": k, "pad": pad}} for k in range(lo, min(n, lo + 20000))]
        )
for lo in range(0, n // 2, 20000):  # newer rows for half of the keys
    repo.append_records(
        "evals",
        [{"item_id": f"i{k}", "data": {"v": -k, "pad": pad}} for k in range(lo, min(n // 2, lo + 20000))],
    )
write_s = time.perf_counter() - t0
t0 = time.perf_counter()
repo.optimize()
opt_s = time.perf_counter() - t0
repo.close()
repo = Repo(path, flush_interval=None)
rnd = random.Random(1)
gets = []
for _ in range(50):
    k = rnd.randrange(n)
    t0 = time.perf_counter()
    rows = repo.records("evals", item_id=f"i{k}")
    gets.append(time.perf_counter() - t0)
t0 = time.perf_counter()
rows = repo.records("evals")
scan_s = time.perf_counter() - t0
print(
    json.dumps(
        {
            "n": n,
            "write_s": round(write_s, 1),
            "optimize_s": round(opt_s, 2),
            "get_p50_ms": round(statistics.median(gets) * 1000, 1),
            "get_max_ms": round(max(gets) * 1000, 1),
            "scan_s": round(scan_s, 2),
            "scan_rows": len(rows),
        }
    ),
    flush=True,
)
repo.close()
shutil.rmtree(path, ignore_errors=True)
