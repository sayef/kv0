#!/usr/bin/env bash
# Runs the key-value comparison (this release against the 0.1 baseline, pure Python on a SQLite
# cache) and the tracking benchmark. Needs uv. Builds the release extension into .venv and
# installs the baseline into bench/.venv-old. Set KV0_BASELINE_SPEC to a pip requirement for the 0.1
# baseline (a path to a 0.1 checkout, for example) and KV0_BASELINE_MODULE to its import name.
set -euo pipefail
cd "$(dirname "$0")/.."
N="${N:-100000}"

uv venv -q --allow-existing .venv
uv pip install -q maturin
uv run --no-project maturin develop -q --release

uv venv -q --allow-existing bench/.venv-old
VIRTUAL_ENV=bench/.venv-old uv pip install -q "${KV0_BASELINE_SPEC:?a pip requirement for the 0.1 baseline}"

mkdir -p bench/results
uv run --no-project python bench/bench_kv.py --n "$N" --out bench/results/kv-kv0-0.2.json
KV0_BASELINE_MODULE="${KV0_BASELINE_MODULE:?the import name of the 0.1 baseline}" \
    bench/.venv-old/bin/python bench/bench_kv.py --n "$N" --out bench/results/kv-baseline-0.1.json
uv run --no-project python bench/bench_tracking.py --out bench/results/tracking.json
uv run --no-project python bench/report.py
