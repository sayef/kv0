# Contributing to kv0

Thanks for your interest in kv0. Bug reports, benchmarks, documentation fixes and code are all welcome.

## Before you start

- For anything beyond a small fix, open an issue first so we can agree on the approach.
- Security issues go through [SECURITY.md](https://github.com/sayef/kv0/blob/main/SECURITY.md), not public issues.
- By contributing, you agree that your contributions are licensed under the [Apache License 2.0](https://github.com/sayef/kv0/blob/main/LICENSE).
- Use synthetic data in tests, examples and issues.

## Development setup

You need Rust 1.94.1 or newer (the minimum supported version, checked in CI), `protoc` with the
well-known `.proto` includes (Lance needs it at build time: `apt install protobuf-compiler libprotobuf-dev`
or `brew install protobuf`) and, for the Python bindings, Python 3.11+ with [uv](https://docs.astral.sh/uv/)
and [maturin](https://www.maturin.rs/).

```sh
git clone https://github.com/sayef/kv0 && cd kv0
cargo test -p kv0                     # or: cargo nextest run -p kv0

uv sync --no-install-project          # pytest, maturin and ruff, at the versions in uv.lock
uv run --no-sync maturin develop --release
uv run --no-sync pytest
```

The key-value and tracking layers also run on a Delta Lake engine (delta-rs without DataFusion),
behind the `delta` feature, which needs Rust 1.95 or newer. New databases and repositories use it
when `KV0_TEST_BACKEND=delta` is set; existing ones keep the engine they were created with. CI runs
the Rust and Python suites both ways:

```sh
KV0_TEST_BACKEND=delta cargo test -p kv0 --features delta
uv run --no-sync maturin develop --features delta && KV0_TEST_BACKEND=delta uv run --no-sync pytest
```

The migration from Lance (`kv0 migrate`, `crates/kv0/src/migrate.rs`) is behind the
`lance-migrate` feature, which includes `delta`; its Python package `kv0-migrate` lives in
`crates/kv0-migrate-py` (built and tested in CI, not published yet):

```sh
cargo test -p kv0 --features lance-migrate --test migrate
uv run --no-sync maturin develop -m crates/kv0-migrate-py/Cargo.toml
uv run --no-sync pytest crates/kv0-migrate-py/tests
```

`tests/chaos.rs` is the Delta engine's concurrency and crash suite: processes writing, compacting,
deleting, optimizing, cleaning up and purging at once, object-store faults (`kv0::testing`), and
crashes at every write of a flush, each checked against a model of the acknowledged writes. Every
test prints its seed; `KV0_CHAOS_SEED=<seed>` repeats the same random choices (up to thread
timing) and `KV0_CHAOS_SCALE=<n>` multiplies the work. Pull requests run it at scale 1; the
`Chaos` workflow runs it nightly with larger scales and new seeds (report-only).

Object-store tests run against any S3-compatible server. CI starts RustFS in Docker:

```sh
KV0_TEST_S3_BUCKET=kv0-test .github/scripts/s3-server.sh
KV0_TEST_S3_ENDPOINT=http://127.0.0.1:9000 cargo test -p kv0 --test s3
KV0_TEST_S3_ENDPOINT=http://127.0.0.1:9000 uv run pytest -k s3
.github/scripts/s3-server.sh stop   # removes the container and its data volume
```

The documentation site is MkDocs with the Material theme:

```sh
uv pip install -r docs/requirements.txt
uv run mkdocs serve               # preview at http://127.0.0.1:8000
uv run mkdocs build --strict      # what CI runs: no warnings allowed
```

## Project layout

- `crates/kv0`: the library. `src/kv` is the key-value layer (`view.rs` the in-memory ordered view,
  `store.rs` reading the operation log), `src/tracking` the tracking tables, `src/storage` locations,
  object-store options and the storage engines (`oplog.rs` and `table.rs` the interfaces, `pred.rs` filters, `lance/` the Lance engine, `delta/` the Delta Lake engine: `kv.rs` the key-value log, `rows.rs` the tracking and records tables, `filter.rs` predicates without a query engine), `src/rt.rs` the Tokio runtime behind the blocking APIs.
- `crates/kv0-py`: the Python bindings (PyO3), the pure-Python package in `python/kv0`, typed stubs
  and the Python tests.
- `bench/`: the evaluation harness (`bench/eval/`), the key-value comparison with the SQLite-cache
  baseline and the tracking benchmark.
- `docs/` and `mkdocs.yml`: the documentation site. `assets/`: brand files.
- [DESIGN.md](https://github.com/sayef/kv0/blob/main/DESIGN.md): decisions, guarantees and rejected alternatives. Update it when behaviour changes.

## Checks

CI runs the checks below on every pull request, and the pre-commit hooks run most of them before each
commit. Install the hooks once with [pre-commit](https://pre-commit.com/) (or
[prek](https://github.com/j178/prek), which reads the same file):

```sh
uvx pre-commit install                # or: pipx install pre-commit && pre-commit install
uvx pre-commit run --all-files        # everything at once, as CI does
```

pre-commit installs its own copies of ruff, typos, actionlint and zizmor, at the versions pinned in
`.pre-commit-config.yaml`; the Rust hooks use your toolchain.

| Check | What it covers | Run it locally |
| --- | --- | --- |
| Whitespace, YAML, TOML, merge markers | trailing whitespace, final newlines, line endings | pre-commit |
| gitleaks | secrets in the tree (CI scans the whole history) | pre-commit |
| `cargo fmt`, `cargo clippy` | Rust formatting and lints, warnings as errors | pre-commit, or `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` (CI also with `--features kv0/delta`) |
| ruff | Python lints and formatting | pre-commit, or `uv run --no-sync ruff check` and `uv run --no-sync ruff format --check` |
| typos | spelling in code and docs; exclusions and allowed words in `_typos.toml` | pre-commit, or `uvx typos` |
| actionlint, zizmor | GitHub Actions workflows: correctness and security | pre-commit, or `uvx zizmor --offline .github` |
| `cargo machete` | unused Rust dependencies (CI only) | `cargo install cargo-machete && cargo machete` |
| `cargo deny check` | licences, advisories and sources (CI only) | `cargo install cargo-deny && cargo deny check` |
| Tests, MSRV, docs | Rust and Python tests, Rust 1.94.1 (1.95.0 with `delta`), `mkdocs build --strict` | see the sections above |

The benchmark outputs (`bench/results/*.json`, `bench/eval/charts/*.svg`, `bench/REPORT.md`) are generated,
but they pass the same whitespace hooks: regenerate them rather than editing them by hand. If ruff and
the pre-commit hook disagree, the ruff pin in `pyproject.toml` and the `rev` in `.pre-commit-config.yaml`
have drifted apart; I keep them on the same version.

## Releasing

Releases are off until the repository variable `RELEASE_ENABLED` is `true`; nothing has been published
yet. With it on, every push to `main` updates a release pull request (version and `CHANGELOG.md`).
Merging that pull request publishes `kv0` to crates.io, tags `v<version>` and creates the GitHub
release (release-plz), and `release.yml` builds the wheels and publishes them to PyPI.

The only version is `version` in `[workspace.package]` of `Cargo.toml`; maturin turns it into the
PEP 440 form for the wheels (`0.1.0-beta.1` is `0.1.0b1` on PyPI). The first versions are betas
toward 0.1.0:

- **The first beta.** Nothing is published, so the first release pull request releases the current
  version, `0.1.0-beta.1`, as it is.
- **The next beta.** From a pre-release, release-plz bumps the last number (`0.1.0-beta.1` to
  `0.1.0-beta.2`); merging the release pull request cuts it. A version with a pre-release part
  becomes a GitHub pre-release (`git_release_type = "auto"` in `release-plz.toml`), and pip installs
  it only when asked for (`pip install --pre kv0`, or `kv0==0.1.0b2`).
- **0.1.0.** release-plz never leaves a pre-release on its own, so I set the version explicitly: a
  pull request that sets `version = "0.1.0"` (or runs `release-plz set-version kv0@0.1.0`) and runs
  `cargo update -w`. Once it is merged, release-plz sees a version higher than the published one,
  keeps it, and opens the release pull request for 0.1.0. The same works for `0.1.0-rc.1` or any
  other version.

The notes under `## [Unreleased]` become the release's entry: release-plz only adds the version
heading.

## Checklist for a pull request

- `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings` are clean.
- `cargo deny check` passes (licences, advisories and sources).
- `pre-commit run --all-files` passes and leaves no changes.
- New behaviour has Rust and, where it is exposed, Python tests.
- Performance changes include numbers from `bench/` or `cargo bench -p kv0 --bench kv`.
