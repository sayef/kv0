"""runs() on a repository where 50 runs each wrote 400 heartbeat rows (20,000 rows), before and
after optimize() folds them. Prints JSON.

    python bench/eval/run_rows.py PATH
"""

import json
import shutil
import statistics
import sys
import time

from kv0.tracking import Repo

path = sys.argv[1]
shutil.rmtree(path, ignore_errors=True)
repo = Repo(path, flush_interval=None)
runs = [repo.create_run(name=f"r{i}") for i in range(50)]
for beat in range(400):
    for r in runs:
        r.update(description=f"beat {beat}")
    repo.flush()


def timed():
    out = []
    for _ in range(5):
        t0 = time.perf_counter()
        repo.runs()
        out.append(time.perf_counter() - t0)
    return round(statistics.median(out) * 1000, 1)


before = timed()
t0 = time.perf_counter()
repo.optimize()
opt = round(time.perf_counter() - t0, 2)
print(
    json.dumps({"run_rows": 50 * 401, "runs_ms_before": before, "optimize_s": opt, "runs_ms_after": timed()})
)
repo.close()
shutil.rmtree(path, ignore_errors=True)
