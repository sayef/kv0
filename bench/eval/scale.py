"""The tracker's scale scenario: many runs logged through one handle, then a cold catalog refresh.

    python bench/eval/scale.py --repo /path/to/empty/dir --runs 2000 --points 2000 --out result.json

Mirrors a tracker's scale test: runs in 20 experiments, three series each (`loss[train]` with
`--points` points, `loss[val]` and `accuracy[val]` every 10th step), params set once, and one
commit per run (`finish` then `flush`). Start-up is a fresh handle listing the runs and reading
each run's params and series, as the tracker's first catalog refresh does.
"""

import argparse
import json
import math
import random
import resource
import time

try:
    from kv0.tracking import Repo
except ImportError:  # older builds, under the import name they were built with
    import importlib
    import os
    import sys

    name = os.environ.get("KV0_BASELINE_MODULE")
    if not name:
        sys.exit("kv0 is not installed here; for an older build, set KV0_BASELINE_MODULE to its import name")
    Repo = importlib.import_module(name + ".tracking").Repo


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repo", required=True)
    p.add_argument("--runs", type=int, default=2000)
    p.add_argument("--points", type=int, default=2000)
    p.add_argument("--phase", choices=["log", "start", "both"], default="both")
    p.add_argument("--out")
    a = p.parse_args()
    res = {"runs": a.runs, "points_per_run": a.points, "points": int(a.runs * a.points * 1.2)}
    if a.phase in ("log", "both"):
        rng = random.Random(1)
        repo = Repo(a.repo)
        began = time.perf_counter()
        last, per_run = began, []
        for i in range(a.runs):
            run = repo.create_run(name=f"run-{i}", experiment=f"sweep-{i % 20}")
            lr = 10 ** rng.uniform(-4, -1)
            run.set_params({"hparams": {"lr": lr, "batch_size": rng.choice([32, 64, 128]), "seed": i}})
            for s in range(a.points):
                loss = 0.2 + 2 * math.exp(-s / (300 + 2000 * lr)) + rng.gauss(0, 0.02)
                run.track(loss, name="loss", step=s, context={"subset": "train"})
                if s % 10 == 0:
                    run.track(loss * 1.1, name="loss", step=s, context={"subset": "val"})
                    run.track(1 - loss / 3, name="accuracy", step=s, context={"subset": "val"})
            run.finish()
            repo.flush()
            now = time.perf_counter()
            per_run.append(now - last)
            last = now
        repo.close()
        res["log_s"] = time.perf_counter() - began
        q = len(per_run) // 10 or 1
        res["first_tenth_run_s"] = sum(per_run[:q]) / q
        res["last_tenth_run_s"] = sum(per_run[-q:]) / q
    if a.phase in ("start", "both"):
        began = time.perf_counter()
        repo = Repo(a.repo, flush_interval=None)
        runs = repo.runs()
        for r in runs:
            repo.params(r["run_id"] if "run_id" in r else r["id"])
            repo.series(r["run_id"] if "run_id" in r else r["id"])
        res["start_s"] = time.perf_counter() - began
        res["start_runs"] = len(runs)
        res["start_rss_mb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        repo.close()
        if hasattr(repo, "all_params"):  # builds with bulk reads: one scan per table
            began = time.perf_counter()
            repo = Repo(a.repo, flush_interval=None)
            runs = repo.runs()
            params, series = repo.all_params(), repo.all_series()
            stats = repo.series_stats() if hasattr(repo, "series_stats") else {}
            res["start_bulk_s"] = time.perf_counter() - began
            res["start_bulk_counts"] = [len(runs), len(params), len(series), len(stats)]
            repo.close()
    print(json.dumps(res), flush=True)
    if a.out:
        with open(a.out, "a") as f:
            f.write(json.dumps(res) + "\n")


if __name__ == "__main__":
    main()
