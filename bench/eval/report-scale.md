An experiment tracker built on kv0 reported that logging 2,000 runs took 27 minutes, because each commit
slowed down as fragments accumulated, and that its server took 250–386 s to start against an
uncompacted repository. `bench/eval/scale.py` reproduces the shape of that test through the
tracking API alone:

- 2,000 runs in 20 experiments, each with parameters and three series (`loss[train]` at every
  step, `loss[val]` and `accuracy[val]` at every tenth step);
- one commit per run (`finish`, then `flush`), as the tracker's SDK does;
- a cold start that lists the runs and reads each run's parameters and series, as the tracker's
  first catalog refresh does.

"Before" is `b498409`, the build the tracker measured: no automatic compaction in the tracking
layer. "After" is `main`, which compacts small fragments in the background after a flush.

The per-run cost is the clearest signal. Before, a commit's cost grows with the number of
fragments: the last tenth of the runs took about 8× as long as the first. After, background
compaction keeps the fragment count bounded, and the last tenth took about 2× as long as the
first. Start-up gains for the same reason: each per-run scan reads a few compacted fragments
instead of 2,000 small ones.

Start-up still makes two filtered scans per run. With the bulk reads (`runs`, `all_params`,
`all_series` and `series_stats`, one scan each) the same start-up takes 0.07 s instead of 10–25 s
(`bench/eval/results-scale-v2.jsonl`, one repetition). Series statistics are stored as delta rows
in the same commit as the points, and a flush checks for deleted runs at most once a second:
logging the 2,000 runs takes 63 s (92 s on main, measured on another day on this shared machine),
and the last tenth of the runs 0.04 s per run. On the resulting 4.8M points, `optimize` takes
2.1 s the first time (a full sorted rewrite, which every call did before), 0.02 s when nothing was
added, and 0.3 s after one more run of 2,000 points.

This machine logs faster than the tracker's run (4.5 minutes before against their 27), because
the tracker's SDK adds its own work per point. The ratios are what carries over.
