"""Builds bench/REPORT.md and bench/eval/charts/*.svg from bench/eval/results.jsonl.

    python bench/eval/report.py

The hand-written parts (method notes and verdicts) live in bench/eval/report-intro.md and
bench/eval/report-verdict.md and are copied in; everything numeric is generated.

A difference is "significant" when the ranges (min to max over the repetitions) of the two
engines do not overlap, which for 5 repetitions each is the most extreme Mann-Whitney U (two-sided
p = 0.008), and the medians differ by more than 20%.
"""

import json
import statistics
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
CHARTS = HERE / "charts"

ACCENT = "#1A6E70"  # kv0 0.2
NEUTRAL = "#ADB5BA"  # the 0.1 baseline
INK, MUTED, GRID = "#1B1F22", "#5E676D", "#D9DEE1"

# (kind, label, path into the result, unit, higher_is_better)
METRICS = [
    ("tracker", "Tracker run: puts", "write.puts_per_s", "ops/s", True),
    ("tracker", "Tracker run: put p50", "write.put_p50_us", "µs", False),
    ("tracker", "Tracker run: put p99", "write.put_p99_us", "µs", False),
    ("tracker", "Tracker run: close (final flush)", "write.close_s", "s", False),
    ("tracker", "Tracker run: writer peak RSS", "write.peak_rss_mb", "MB", False),
    ("tracker", "Tracker run: stored size", "disk_bytes", "MB", False),
    ("tracker", "Tracker run: S3 requests while writing", "write.s3", "req", False),
    ("tracker", "Tracker reader: open both DBs (cold)", "ui.open_s", "s", False),
    ("tracker", "Tracker reader: list run metadata", "ui.list_s", "s", False),
    ("tracker", "Tracker reader: read one series (val+time) p50", "ui.series_read_p50_us", "µs", False),
    ("tracker", "Tracker reader: whole session", "ui.total_s", "s", False),
    ("tracker", "Tracker reader: S3 requests", "ui.s3", "req", False),
    ("tracker-small", "Small tracker run: puts", "write.puts_per_s", "ops/s", True),
    ("tracker-small", "Small tracker run: put p99", "write.put_p99_us", "µs", False),
    ("bulk", "Bulk load 1M (batches of 1000)", "keys_per_s", "keys/s", True),
    ("bulk", "Bulk load: batch p99", "batch_p99_us", "µs", False),
    ("bulk", "Bulk load: peak RSS", "peak_rss_mb", "MB", False),
    ("bulk", "Bulk load: stored size", "disk_bytes", "MB", False),
    ("bulk", "Bulk load: S3 requests", "s3", "req", False),
    ("point", "Point reads: open (cold cache)", "open_s", "s", False),
    ("point", "Point reads cold: get p50", "cold_get_p50_us", "µs", False),
    ("point", "Point reads cold: get p99", "cold_get_p99_us", "µs", False),
    ("point", "Point reads hot: gets", "hot_gets_per_s", "ops/s", True),
    ("point", "Point reads hot: get p99", "hot_get_p99_us", "µs", False),
    ("point", "Point reads: peak RSS", "peak_rss_mb", "MB", False),
    ("scan", "Range scan: seek + 100 next p50", "seek100_p50_us", "µs", False),
    ("scan", "Range scan: seek + 100 next p99", "seek100_p99_us", "µs", False),
    ("scan", "Full scan", "full_scan_items_per_s", "items/s", True),
    ("reopen", "Reopen + first get (cold cache)", "open_s", "s", False),
    ("reopen", "Reopen: peak RSS", "peak_rss_mb", "MB", False),
    ("reopen", "Reopen: S3 requests", "s3", "req", False),
    ("large", "Large values (100k × 16 KiB): load", "keys_per_s", "keys/s", True),
    ("large", "Large values: reopen", "reopen.open_s", "s", False),
    ("large", "Large values: reopen peak RSS", "reopen.peak_rss_mb", "MB", False),
    ("large", "Large values: stored size", "disk_bytes", "MB", False),
    ("concurrent", "Two writers: combined keys", "keys_per_s", "keys/s", True),
    ("concurrent", "Two writers: keys visible after", "visible.keys", "keys", True),
    ("crash", "Crash recovery: reopen after SIGKILL", "recover_s", "s", False),
    ("crash", "Crash recovery: keys recovered of 200k", "keys", "keys", True),
    ("reopen-rw", "Writable reopen + first get (cold cache)", "open_s", "s", False),
    ("reopen-rw", "Writable reopen: peak RSS", "peak_rss_mb", "MB", False),
]

PAIRS = [
    ("v01", "v02", 'local, 0.2 default (wal_sync="batch")'),
    ("v01", "v02-always", 'local, 0.2 wal_sync="always"'),
    ("v01-s3", "v02-s3", "S3 (RustFS)"),
]


def get(res, path):
    v = res
    for p in path.split("."):
        if not isinstance(v, dict) or p not in v:
            return None
        v = v[p]
    if isinstance(v, dict):  # S3 request counts
        v = sum(n for k, n in v.items() if not k.startswith("bytes"))
    if isinstance(v, (int, float)) and path.endswith("disk_bytes"):
        v = v / 1e6
    return v


def load(*files):
    rows = []
    for f in files or ("results.jsonl",):
        if (HERE / f).exists():
            rows += [json.loads(ln) for ln in (HERE / f).read_text().splitlines() if ln.strip()]
    cells = defaultdict(list)
    for r in rows:
        size = r.get("n", r.get("steps"))
        cells[(r["kind"], r["engine"], size)].append(r)
    return rows, cells


def stats(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return {"med": statistics.median(vals), "lo": min(vals), "hi": max(vals), "n": len(vals)}


def fmt(v, unit):
    if v is None:
        return "n/a"
    if unit in ("s",):
        return f"{v * 1000:.0f} ms" if v < 1 else f"{v:.2f} s"
    if unit == "µs":
        return f"{v / 1000:.1f} ms" if v >= 1000 else f"{v:.1f} µs"
    if unit == "MB":
        return f"{v:,.0f} MB" if v >= 10 else f"{v:.1f} MB"
    if v >= 100:
        return f"{v:,.0f}"
    return f"{v:.3g}"


def compare(a, b, higher):
    """(ratio as 'x better/worse', verdict) of b against a."""
    if not a or not b or a["med"] == 0 or b["med"] == 0:
        return "", "n/a"
    better = b["med"] / a["med"] if higher else a["med"] / b["med"]
    overlap = not (b["lo"] > a["hi"] or b["hi"] < a["lo"])
    meaningful = abs(better - 1) > 0.2
    if better >= 1:
        text = f"{better:.1f}× better" if better >= 1.95 else f"{(better - 1) * 100:.0f}% better"
    else:
        inv = 1 / better
        text = f"{inv:.1f}× worse" if inv >= 1.95 else f"{(inv - 1) * 100:.0f}% worse"
    if overlap or not meaningful or min(a["n"], b["n"]) < 3:
        return text, "same" if not meaningful else f"{text} (not significant)"
    return text, ("**better**" if better > 1 else "**worse**")


def table(cells, kinds, pair, names=("baseline 0.1", "kv0 0.2")):
    a_eng, b_eng, _ = pair
    out = [
        f"| Workload | Size | {names[0]} median [min–max] | {names[1]} median [min–max] | Change | Verdict |",
        "|---|---|---:|---:|---:|---|",
    ]
    sizes = sorted({s for (k, e, s) in cells if k in kinds})
    for kind, label, path, unit, higher in METRICS:
        if kind not in kinds:
            continue
        for size in sizes:
            ra, rb = cells.get((kind, a_eng, size)), cells.get((kind, b_eng, size))
            if not ra or not rb:
                continue
            sa, sb = stats([get(r, path) for r in ra]), stats([get(r, path) for r in rb])
            if not sa or not sb:
                continue
            change, verdict = compare(sa, sb, higher)

            def cell(s, unit=unit):
                return f"{fmt(s['med'], unit)} [{fmt(s['lo'], unit)}–{fmt(s['hi'], unit)}]"

            out.append(f"| {label} | {size:,} | {cell(sa)} | {cell(sb)} | {change} | {verdict} |")
    return "\n".join(out)


# ---- charts: grouped bars (median) with min-max whiskers, one accent + one neutral --------------


def bar_chart(name, title, groups, unit, log=False):
    """groups: [(label, {series: stats})]; series 'baseline 0.1' and 'kv0 0.2'."""
    import math

    w, row, top, left = 720, 46, 56, 250
    h = top + row * len(groups) + 40
    vals = [s[k] for _, d in groups for s in d.values() if s for k in ("hi",)]
    vmax = max(vals) if vals else 1
    vmin = min(s["lo"] for _, d in groups for s in d.values() if s) if vals else 1

    def x(v):
        span = w - left - 70
        if log:
            lo = 10 ** math.floor(math.log10(max(vmin, 1e-9)))
            return left + span * (math.log10(max(v, lo)) - math.log10(lo)) / (
                math.log10(vmax * 1.2) - math.log10(lo)
            )
        return left + span * v / (vmax * 1.1)

    parts = [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
            f'font-family="system-ui, -apple-system, Segoe UI, sans-serif" font-size="12">'
        ),
        f'<rect width="{w}" height="{h}" fill="#FFFFFF"/>',
        f'<text x="16" y="24" font-size="14" font-weight="600" fill="{INK}">{title}</text>',
        (
            f'<text x="16" y="42" fill="{MUTED}">median of runs, whiskers min–max; {unit}'
            f"{', log scale' if log else ''}</text>"
        ),
    ]
    for i, (label, series) in enumerate(groups):
        y = top + i * row
        parts.append(f'<text x="{left - 10}" y="{y + 20}" text-anchor="end" fill="{INK}">{label}</text>')
        for j, (sname, color) in enumerate((("baseline 0.1", NEUTRAL), ("kv0 0.2", ACCENT))):
            s = series.get(sname)
            if not s:
                continue
            by = y + 4 + j * 18
            x0 = x(10 ** math.floor(math.log10(max(vmin, 1e-9)))) if log else left
            parts.append(
                f'<rect x="{x0:.1f}" y="{by}" width="{max(2, x(s["med"]) - x0):.1f}" height="14" '
                f'rx="3" fill="{color}"/>'
            )
            parts.append(
                f'<line x1="{x(s["lo"]):.1f}" x2="{x(s["hi"]):.1f}" y1="{by + 7}" y2="{by + 7}" '
                f'stroke="{INK}" stroke-width="1.5"/>'
            )
            parts.append(
                f'<text x="{x(s["hi"]) + 6:.1f}" y="{by + 11}" fill="{MUTED}">{fmt(s["med"], unit)}</text>'
            )
    ly = h - 16
    parts.append(
        f'<rect x="{left}" y="{ly - 10}" width="12" height="12" rx="3" fill="{NEUTRAL}"/>'
        f'<text x="{left + 18}" y="{ly}" fill="{INK}">baseline 0.1 (SQLite cache)</text>'
        f'<rect x="{left + 110}" y="{ly - 10}" width="12" height="12" rx="3" fill="{ACCENT}"/>'
        f'<text x="{left + 128}" y="{ly}" fill="{INK}">kv0 0.2</text>'
    )
    parts.append("</svg>")
    CHARTS.mkdir(exist_ok=True)
    (CHARTS / f"{name}.svg").write_text("\n".join(parts) + "\n")
    return f"![{title}](eval/charts/{name}.svg)"


def charts(cells):
    out = []

    def g(kind, engines, size, path):
        a, b = engines
        return {
            "baseline 0.1": stats([get(r, path) for r in cells.get((kind, a, size), [])]),
            "kv0 0.2": stats([get(r, path) for r in cells.get((kind, b, size), [])]),
        }

    out.append(
        bar_chart(
            "throughput",
            "Write and read throughput, local disk",
            [
                ("Tracker run, puts/s", g("tracker", ("v01", "v02"), 5000, "write.puts_per_s")),
                ("Bulk load 1M, keys/s", g("bulk", ("v01", "v02"), 1_000_000, "keys_per_s")),
                ("Hot point reads, gets/s", g("point", ("v01", "v02"), 1_000_000, "hot_gets_per_s")),
                ("Full scan, items/s", g("scan", ("v01", "v02"), 1_000_000, "full_scan_items_per_s")),
            ],
            "operations per second",
            log=True,
        )
    )
    out.append(
        bar_chart(
            "reopen",
            "Reopen + first get, cold page cache",
            [
                (f"{n:,} keys", g("reopen", ("v01", "v02"), n, "open_s"))
                for n in (10_000, 1_000_000, 10_000_000)
            ]
            + [("100k × 16 KiB values", g("large", ("v01", "v02"), 100_000, "reopen.open_s"))],
            "seconds",
            log=True,
        )
    )
    out.append(
        bar_chart(
            "memory",
            "Peak RSS after reopen",
            [
                (f"{n:,} keys", g("reopen", ("v01", "v02"), n, "peak_rss_mb"))
                for n in (10_000, 1_000_000, 10_000_000)
            ]
            + [("100k × 16 KiB values", g("large", ("v01", "v02"), 100_000, "reopen.peak_rss_mb"))],
            "MB",
            log=True,
        )
    )
    out.append(
        bar_chart(
            "s3",
            "S3 (RustFS): requests",
            [
                ("Tracker run, writing", g("tracker", ("v01-s3", "v02-s3"), 5000, "write.s3")),
                ("Tracker reader, cold start", g("tracker", ("v01-s3", "v02-s3"), 5000, "ui.s3")),
                ("Bulk load 1M", g("bulk", ("v01-s3", "v02-s3"), 1_000_000, "s3")),
                ("Reopen 1M", g("reopen", ("v01-s3", "v02-s3"), 1_000_000, "s3")),
            ],
            "requests",
            log=True,
        )
    )
    return out


def scale_table():
    """The tracker's scale scenario (bench/eval/scale.py), one run per build."""
    res = {}
    for v in ("before", "after"):
        f = HERE / f"results-scale-{v}.jsonl"
        if f.exists():
            res[v] = json.loads(f.read_text().splitlines()[-1])
    if len(res) < 2:
        return ""
    b, a = res["before"], res["after"]
    rows = [
        ("Logging 2,000 runs (one commit per run)", "log_s", "s"),
        ("Per run, first tenth of the runs", "first_tenth_run_s", "s"),
        ("Per run, last tenth of the runs", "last_tenth_run_s", "s"),
        ("Start-up: open and read every run's params and series", "start_s", "s"),
        ("Start-up: peak RSS", "start_rss_mb", "MB"),
    ]
    out = [
        (
            f"{b['runs']:,} runs of {b['points_per_run']:,} steps, {b['points'] / 1e6:.1f} M points, one "
            "repetition per build."
        ),
        "",
        "| | before (b498409) | after (main) | Change |",
        "|---|---:|---:|---:|",
    ]
    for label, k, unit in rows:
        x, y = b[k], a[k]
        fmt = (
            (lambda v: f"{v:,.0f} MB")
            if unit == "MB"
            else (lambda v: f"{v:.2f} s" if v < 10 else f"{v:,.0f} s")
        )
        out.append(
            f"| {label} | {fmt(x)} | {fmt(y)} | {x / y:.1f}× better |"
            if y < x
            else f"| {label} | {fmt(x)} | {fmt(y)} | {y / x:.1f}× worse |"
        )
    return "\n".join(out)


def main():
    rows, cells = load()
    host = rows[0]["host"] if rows else {}
    intro = (HERE / "report-intro.md").read_text() if (HERE / "report-intro.md").exists() else ""
    verdict = (HERE / "report-verdict.md").read_text() if (HERE / "report-verdict.md").exists() else ""
    reps = sorted({len(v) for v in cells.values()})
    parts = [
        "# kv0 0.2 against 0.1: evaluation",
        "",
        intro,
        (
            f"Repetitions per cell: {', '.join(map(str, reps))}. Dataset files evicted from the page cache before cold cells: "
            f"{'yes' if host.get('drop_caches') else 'no'}. Python {host.get('python', '?')}."
        ),
        "",
        verdict,
        "",
        "## The tracker's scale scenario",
        "",
        (HERE / "report-scale.md").read_text() if (HERE / "report-scale.md").exists() else "",
        scale_table(),
        "",
        "## Charts",
        "",
    ]
    parts += [c + "\n" for c in charts(cells)]
    for pair in PAIRS:
        parts += [f"## Tables: {pair[2]}", "", table(cells, {m[0] for m in METRICS}, pair), ""]
    (ROOT / "bench" / "REPORT.md").write_text("\n".join(parts).rstrip("\n") + "\n")
    print("wrote bench/REPORT.md")


def main_compare():
    # Both builds come from the same run (results-compare.jsonl), so they are comparable.
    _rows, cells = load("results-compare.jsonl")
    text = {
        n: (HERE / f"compare-{n}.md").read_text() if (HERE / f"compare-{n}.md").exists() else ""
        for n in ("intro", "verdict", "tests")
    }
    kinds = {m[0] for m in METRICS}
    parts = [
        "# Memory and reopen: native improvements",
        "",
        text["intro"],
        text["verdict"],
        "",
        "## Tables: before against after",
        "",
        table(cells, kinds, ("v02b", "v02", ""), ("before", "after")),
        "",
        "## First open without a snapshot: before against after",
        "",
        (
            'The same build as "after" with the snapshot file removed before every open, as on a '
            "machine that has not opened the database before: a full scan into the compact view."
        ),
        "",
        table(cells, {"reopen", "large"}, ("v02b", "v02ns", ""), ("before", "after, no snapshot")),
        "",
        text["tests"],
    ]
    (ROOT / "bench" / "MEMORY-REPORT.md").write_text("\n".join(parts).rstrip("\n") + "\n")
    print("wrote bench/MEMORY-REPORT.md")


if __name__ == "__main__":
    import sys

    main_compare() if "--compare" in sys.argv else main()
