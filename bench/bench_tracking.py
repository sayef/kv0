"""Tracking benchmark: metric append throughput and step-range reads.

python bench/bench_tracking.py --runs 20 --metrics 10 --steps 5000 --out bench/results/tracking.json
"""

import argparse
import json
import platform
import random
import shutil
import tempfile
import time

from kv0.tracking import Repo


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--metrics", type=int, default=10)
    ap.add_argument("--steps", type=int, default=5000)
    ap.add_argument("--out")
    a = ap.parse_args()
    root = tempfile.mkdtemp(prefix="kv0-track-")
    points = a.runs * a.metrics * a.steps
    res = {
        "runs": a.runs,
        "metrics": a.metrics,
        "steps": a.steps,
        "points": points,
        "python": platform.python_version(),
        "machine": platform.machine(),
    }
    try:
        repo = Repo(root, flush_interval=None)
        runs = [repo.create_run(name=f"run{r}", experiment="bench") for r in range(a.runs)]
        t = time.perf_counter()
        for step in range(a.steps):
            for run in runs:
                for m in range(a.metrics):
                    run.track(random.random(), name=f"metric{m}", step=step, context={"subset": "train"})
        repo.flush()
        el = time.perf_counter() - t
        res["track_points_s"] = points / el

        ids = [r.id for r in runs]
        rnd = random.Random(0)

        def reads(label, full):
            t = time.perf_counter()
            k = 50
            got = 0
            for _ in range(k):
                rid = rnd.choice(ids)
                if full:
                    d = repo.metric(rid, "metric3", context={"subset": "train"})
                else:
                    s = rnd.randrange(0, a.steps - 100)
                    d = repo.metric(rid, "metric3", context={"subset": "train"}, start=s, stop=s + 100)
                got += len(d["steps"])
            el = time.perf_counter() - t
            res[f"{label}_ms"] = el / k * 1000
            return got

        assert reads("read_full_series_noindex", True) == 50 * a.steps
        reads("read_100_steps_noindex", False)
        t = time.perf_counter()
        repo.optimize()
        res["optimize_s"] = time.perf_counter() - t
        reads("read_full_series_indexed", True)
        reads("read_100_steps_indexed", False)
        repo.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print(json.dumps(res, indent=2))
    if a.out:
        with open(a.out, "w") as f:
            json.dump(res, f, indent=2)
            f.write("\n")


if __name__ == "__main__":
    main()
