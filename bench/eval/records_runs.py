"""Records of 31 runs out of 2,000 at 1M rows: one `run_ids` query, 31 `run_id` queries, and a
full read filtered in Python. Prints JSON.

    python bench/eval/records_runs.py PATH
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
runs, items = 2000, 500
for r0 in range(0, runs, 40):
    repo.append_records(
        "scores",
        [
            {"item_id": f"q{i}", "run_id": f"r{r}", "data": {"s": i}}
            for r in range(r0, r0 + 40)
            for i in range(items)
        ],
    )
repo.optimize()
want = [f"r{r}" for r in range(0, runs, 65)][:31]


def timed(f, n=5):
    out = []
    for _ in range(n):
        t0 = time.perf_counter()
        rows = f()
        out.append(time.perf_counter() - t0)
    return round(statistics.median(out) * 1000, 1), len(rows)


one, n1 = timed(lambda: repo.records("scores", run_ids=want))
many, n2 = timed(lambda: [x for r in want for x in repo.records("scores", run_id=r)])
full, n3 = timed(lambda: [x for x in repo.records("scores") if x["run_id"] in set(want)], 1)
print(
    json.dumps(
        {
            "rows": runs * items,
            "runs_wanted": len(want),
            "run_ids_ms": one,
            "per_run_queries_ms": many,
            "full_read_ms": full,
            "rows_returned": [n1, n2, n3],
        }
    )
)
repo.close()
shutil.rmtree(path, ignore_errors=True)
