"""Tracking quickstart: a run with parameters and two metric series."""

import math
import tempfile

from kv0.tracking import Repo

repo = Repo(tempfile.mkdtemp() + "/tracking")

with repo.create_run(name="baseline", experiment="demo", tags=["synthetic"]) as run:
    run["hparams"] = {"lr": 3e-4, "optimizer": {"name": "adamw", "weight_decay": 0.01}}
    for step in range(200):
        run.track(math.exp(-step / 50), name="loss", context={"subset": "train"})
        if step % 20 == 0:
            run.track(1 - math.exp(-step / 80), name="accuracy", step=step, context={"subset": "val"})

print(repo.runs()[0]["status"])  # finished
print(repo.params(run.id)["hparams"]["optimizer"])
loss = repo.metric(run.id, "loss", context={"subset": "train"}, start=190)
print(list(zip(loss["steps"], (round(v, 4) for v in loss["values"]))))
repo.optimize()  # compact files and index metric series
