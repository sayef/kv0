"""Writes bench/results/results.md from the JSON results of bench/run.sh."""

import json
import pathlib

root = pathlib.Path(__file__).resolve().parent.parent
res = root / "bench" / "results"
new = json.loads((res / "kv-kv0.json").read_text())
old = json.loads((res / "kv-prototype.json").read_text())
trk = json.loads((res / "tracking.json").read_text())

rows = [
    ("put (one key per call)", "put_ops_s", "ops/s", True),
    ("put with sync=True (durable per call)", "sync_put_ops_s", "ops/s", True),
    ("write (batches of 1000)", "batch_write_ops_s", "ops/s", True),
    ("get (random existing keys)", "get_ops_s", "ops/s", True),
    ("seek + 100 × next", "seek_plus_100_next_per_s", "seeks/s", True),
    ("full scan", "full_scan_items_s", "items/s", True),
    ("flush after puts", "flush_after_put_s", "s", False),
    ("reopen", "reopen_s", "s", False),
    ("size on disk", "disk_bytes", "MB", False),
]


def fmt(v, unit):
    if unit == "MB":
        return f"{v / 1e6:.1f} MB"
    if unit == "s":
        return f"{v * 1000:.0f} ms" if v < 10 else f"{v:.1f} s"
    return f"{v:,.0f} {unit}"


kv = ["| Operation | prototype | kv0 | Change |", "|---|---:|---:|---:|"]
for label, key, unit, higher in rows:
    a, b = old[key], new[key]
    if unit == "MB":
        change = f"{b / a:.2f}× the size"
    else:
        ratio = (b / a) if higher else (a / b)
        change = f"{ratio:.1f}× faster" if ratio >= 1 else f"{1 / ratio:.0f}× slower"
    kv.append(f"| {label} | {fmt(a, unit)} | {fmt(b, unit)} | {change} |")
wal = ["| put (one key per call), kv0 | ops/s |", "|---|---:|"]
for mode, label in (
    ("off", "WAL off"),
    ("none", 'wal_sync="none" (no fsync)'),
    ("batch", 'wal_sync="batch" (fsync every 100 ms, default)'),
    ("always", 'wal_sync="always" (fsync per write)'),
):
    if f"put_wal_{mode}_ops_s" in new:
        wal.append(f"| {label} | {new[f'put_wal_{mode}_ops_s']:,.0f} |")
kv_md = (
    "\n".join(kv) + "\n\nWrite-ahead log modes, with a buffer that never fills (no Lance commits "
    "during the run), so the rows isolate the cost of the log:\n\n" + "\n".join(wal)
)

tr = [
    f"Tracking: {trk['runs']} runs × {trk['metrics']} metrics × {trk['steps']} steps = {trk['points']:,} points.",
    "",
    "| Operation | Result |",
    "|---|---:|",
    f"| `track` + flush | {trk['track_points_s']:,.0f} points/s |",
    f"| read a whole series ({trk['steps']} points), no index | {trk['read_full_series_noindex_ms']:.1f} ms |",
    f"| read 100 steps of a series, no index | {trk['read_100_steps_noindex_ms']:.1f} ms |",
    f"| `optimize()` (compaction and index) | {trk['optimize_s']:.1f} s |",
    f"| read a whole series, indexed | {trk['read_full_series_indexed_ms']:.1f} ms |",
    f"| read 100 steps of a series, indexed | {trk['read_100_steps_indexed_ms']:.1f} ms |",
]
tr_md = "\n".join(tr)

(res / "results.md").write_text(f"# Results\n\nKey-value, {new['n']:,} keys:\n\n{kv_md}\n\n{tr_md}\n")

print((res / "results.md").read_text())
