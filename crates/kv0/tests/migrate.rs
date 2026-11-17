//! `kv0 migrate`: Lance databases and repositories (the beta.2 fixtures, and freshly written
//! ones with tags, deletes, lazy values, WAL marks and legacy records) read the same after the
//! conversion to Delta Lake, at the latest version and at every tag.
#![cfg(feature = "lance-migrate")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use kv0::kv::{Db, Options};
use kv0::migrate::{migrate, MigrateOptions, Report};
use kv0::rt::block_on;
use kv0::storage::{detect, Engine, Format};
use kv0::tracking::{
    LegacyRecords, NewRecord, NewRun, RecordQuery, Repo, RepoOptions, StepRange, Track,
};
use serde_json::json;

fn mopts() -> MigrateOptions {
    MigrateOptions {
        quiet_period: Duration::ZERO,
        ..MigrateOptions::default()
    }
}

fn kv_opts(wal: &Path, engine: Option<Engine>) -> Options {
    Options {
        flush_interval: None,
        refresh_interval: None,
        snapshot_min_entries: usize::MAX,
        version_retention: None,
        lazy_value_threshold: 64,
        wal_dir: Some(wal.to_path_buf()),
        engine,
        ..Options::default()
    }
}

fn repo_opts(wal: &Path, engine: Option<Engine>) -> RepoOptions {
    RepoOptions {
        flush_interval: None,
        version_retention: None,
        wal_dir: Some(wal.to_path_buf()),
        legacy_records: LegacyRecords::Allow,
        engine,
        ..RepoOptions::default()
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dst = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), dst).unwrap();
        }
    }
}

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lance-unsigned")
}

fn s(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// Every entry of a database (at `version` when given), in key order.
fn kv_dump(path: &str, wal: &Path, version: Option<u64>) -> Entries {
    let db = Db::open(
        path,
        Options {
            read_only: true,
            version,
            ..kv_opts(wal, None)
        },
    )
    .unwrap();
    let mut it = db.iter();
    it.seek_to_first();
    let out = it.map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
    db.close().unwrap();
    out
}

/// The entries at the latest version and at every tag (by name).
fn kv_reads(path: &str, wal: &Path) -> (Entries, Vec<(String, Entries)>) {
    let db = Db::open(
        path,
        Options {
            read_only: true,
            ..kv_opts(wal, None)
        },
    )
    .unwrap();
    let mut tags = db.tags().unwrap();
    db.close().unwrap();
    tags.sort();
    let at = tags
        .into_iter()
        .map(|(t, v)| (t, kv_dump(path, wal, Some(v))))
        .collect();
    (kv_dump(path, wal, None), at)
}

fn dump_repo(path: &str, wal: &Path) -> Vec<String> {
    let (p, w) = (path.to_string(), wal.to_path_buf());
    block_on(async move { repo_dump(&p, &w).await })
}

/// Everything the public API reads from a repository, as text, with tag versions left out
/// (they are renumbered).
async fn repo_dump(path: &str, wal: &Path) -> Vec<String> {
    let repo = Repo::open(path, repo_opts(wal, None)).await.unwrap();
    let mut out = Vec::new();
    for r in repo.runs().await.unwrap() {
        out.push(format!("run {r:?}"));
    }
    let mut params: Vec<_> = repo.all_params().await.unwrap().into_iter().collect();
    params.sort_by(|a, b| a.0.cmp(&b.0));
    out.push(format!("params {params:?}"));
    let mut series = repo.all_series().await.unwrap();
    series.sort_by_key(|s| s.series_id);
    let ids: Vec<u64> = series.iter().map(|s| s.series_id).collect();
    for s in &series {
        out.push(format!("series {s:?}"));
    }
    let mut points: Vec<_> = repo
        .read_many(&ids, StepRange::default())
        .await
        .unwrap()
        .into_iter()
        .collect();
    points.sort_by_key(|p| p.0);
    for (id, d) in points {
        out.push(format!("points {id} {d:?}"));
    }
    let mut stats: Vec<_> = repo.all_series_stats().await.unwrap().into_iter().collect();
    stats.sort_by_key(|s| s.0);
    for (id, s) in stats {
        out.push(format!("stats {id} {s:?}"));
    }
    let mut tables = repo.record_tables().await.unwrap();
    tables.sort();
    for t in tables {
        let mut rows = repo.records(&t, RecordQuery::default()).await.unwrap();
        rows.sort_by(|a, b| (&a.item_id, a.seq).cmp(&(&b.item_id, b.seq)));
        for r in rows {
            out.push(format!("record {t} {r:?}"));
        }
        let mut tags = repo.record_tags(&t).await.unwrap();
        tags.sort();
        for (tag, v) in tags {
            let mut rows = repo
                .records(
                    &t,
                    RecordQuery {
                        at: kv0::tracking::At::Version(v),
                        ..RecordQuery::default()
                    },
                )
                .await
                .unwrap();
            rows.sort_by(|a, b| (&a.item_id, a.seq).cmp(&(&b.item_id, b.seq)));
            out.push(format!("record tag {t} {tag}: {rows:?}"));
        }
    }
    if repo.has_legacy_records().await.unwrap() {
        let mut rows = repo.legacy_records(None).await.unwrap();
        rows.sort_by(|a, b| (&a.item_id, a.seq).cmp(&(&b.item_id, b.seq)));
        out.push(format!("legacy {rows:?}"));
    }
    repo.close().await.unwrap();
    out
}

fn table<'a>(r: &'a Report, name: &str) -> &'a kv0::migrate::TableReport {
    r.tables
        .iter()
        .find(|t| t.name == name)
        .unwrap_or_else(|| panic!("no table {name} in {r:?}"))
}

#[test]
fn the_beta2_fixture_reads_the_same_after_migration() {
    let d = tempfile::tempdir().unwrap();
    copy_dir(&fixture(), d.path());
    let wal = d.path().join("wal");
    let kv = s(&d.path().join("kv"));
    let repo = s(&d.path().join("repo"));
    let kv_new = s(&d.path().join("kv-delta"));
    let repo_new = s(&d.path().join("repo-delta"));

    let kv_before = kv_reads(&kv, &wal);
    let repo_before = dump_repo(&repo, &wal);

    let t0 = std::time::Instant::now();
    let r = migrate(&kv, &kv_new, &mopts()).unwrap();
    assert_eq!(
        (r.kind.as_str(), r.status.as_str()),
        ("database", "complete")
    );
    let t = table(&r, ".");
    assert_eq!(t.rows as usize, kv_before.0.len());
    assert_eq!(t.tags.len(), 1);
    assert_eq!(t.tags[0].name, "first");
    let r = migrate(&repo, &repo_new, &mopts()).unwrap();
    eprintln!("fixture migrated in {:?}", t0.elapsed());
    assert_eq!(r.kind, "repository");
    let evals = table(&r, "records/evals");
    assert_eq!(evals.tags.len(), 1);

    assert_eq!(
        detect(&kv_new, &HashMap::new()).unwrap(),
        Format::Database(Engine::Delta)
    );
    assert_eq!(
        detect(&repo_new, &HashMap::new()).unwrap(),
        Format::Repository(Engine::Delta)
    );
    assert_eq!(kv_reads(&kv_new, &wal), kv_before);
    assert_eq!(dump_repo(&repo_new, &wal), repo_before);
    // Tags name the migrated versions.
    let db = Db::open(&kv_new, kv_opts(&wal, None)).unwrap();
    assert_eq!(
        db.tag_version("first").unwrap(),
        table(&r_kv(&kv_new), ".").tags[0].target_version
    );
    db.close().unwrap();
    let (u, w) = (repo_new.clone(), wal.clone());
    let tags = block_on(async move {
        let repo = Repo::open(&u, repo_opts(&w, None)).await.unwrap();
        let t = repo.record_tags("evals").await.unwrap();
        repo.close().await.unwrap();
        t
    });
    assert_eq!(tags, vec![("v1".to_string(), evals.tags[0].target_version)]);
    // The source is untouched (no remove_source).
    assert_eq!(
        detect(&kv, &HashMap::new()).unwrap(),
        Format::Database(Engine::Lance)
    );
    assert_eq!(kv_reads(&kv, &wal), kv_before);
}

/// The report a migration left at `uri`.
fn r_kv(uri: &str) -> Report {
    serde_json::from_slice(&std::fs::read(Path::new(uri).join("_kv0/migration.json")).unwrap())
        .unwrap()
}

/// A database with lazy values, overwrites, deletes, range deletes and several tags (two on
/// one version) reads the same at every tag and at the latest version.
#[test]
fn a_database_with_tags_deletes_and_lazy_values_round_trips() {
    let d = tempfile::tempdir().unwrap();
    let wal = d.path().join("wal");
    let src = s(&d.path().join("src"));
    let db = Db::open(&src, kv_opts(&wal, Some(Engine::Lance))).unwrap();
    for round in 0..4u32 {
        for i in 0..500u32 {
            let v = if i % 37 == 0 {
                vec![(round + i) as u8; 5000] // lazy, and above the migration's in-memory limit
            } else {
                format!("v{round}-{i}").into_bytes()
            };
            db.put(format!("k{i:04}").as_bytes(), &v).unwrap();
        }
        db.delete(format!("k{:04}", round * 7).as_bytes()).unwrap();
        db.delete_range(
            format!("k{:04}", 100 + round * 50).as_bytes(),
            format!("k{:04}", 110 + round * 50).as_bytes(),
        )
        .unwrap();
        db.flush().unwrap();
        db.create_tag(&format!("round-{round}"), None).unwrap();
        if round == 1 {
            db.create_tag("same-as-round-1", None).unwrap();
        }
    }
    db.put(b"after-tags", b"x").unwrap();
    db.flush().unwrap();
    db.close().unwrap();
    let before = kv_reads(&src, &wal);
    let dst = s(&d.path().join("dst"));
    let r = migrate(&src, &dst, &mopts()).unwrap();
    let t = table(&r, ".");
    assert_eq!(t.tags.len(), 5);
    assert_eq!(t.rows as usize, before.0.len());
    assert!(t.marks >= 1, "the writer's WAL mark was carried over");
    assert_eq!(kv_reads(&dst, &wal), before);
    // The migrated database takes writes.
    let db = Db::open(&dst, kv_opts(&wal, None)).unwrap();
    db.put(b"new", b"1").unwrap();
    db.flush().unwrap();
    db.compact().unwrap();
    assert_eq!(db.get(b"new").unwrap().as_deref(), Some(&b"1"[..]));
    db.close().unwrap();
}

/// A key-value database migrated in place keeps its WAL: writes a crashed writer committed are
/// not replayed twice (its mark is a `txn` action now), uncommitted ones are replayed once.
#[test]
fn a_database_migrated_in_place_replays_its_wal_once() {
    let d = tempfile::tempdir().unwrap();
    let wal = d.path().join("wal");
    let path = s(&d.path().join("db"));
    let db = Db::open(&path, kv_opts(&wal, Some(Engine::Lance))).unwrap();
    for i in 0..100u32 {
        db.put(&i.to_be_bytes(), b"v1").unwrap();
    }
    db.flush().unwrap();
    db.put(b"late", b"in the WAL only").unwrap();
    db.abandon();
    let e = migrate(&path, &path, &mopts()).unwrap_err();
    assert!(e.to_string().contains("force_in_place"), "{e}");
    let r = migrate(
        &path,
        &path,
        &MigrateOptions {
            force_in_place: true,
            ..mopts()
        },
    )
    .unwrap();
    assert!(r.in_place);
    assert_eq!(table(&r, ".").rows, 100);
    assert_eq!(
        detect(&path, &HashMap::new()).unwrap(),
        Format::Database(Engine::Delta)
    );
    let db = Db::open(&path, kv_opts(&wal, None)).unwrap();
    assert_eq!(
        db.get(b"late").unwrap().as_deref(),
        Some(&b"in the WAL only"[..])
    );
    assert_eq!(
        db.stats().stored_rows,
        101,
        "committed writes were not replayed again"
    );
    db.close().unwrap();
}

/// What a fresh repository with every kind of data reads like, migrated in place: runs (one
/// deleted), params, series with statistics, records with tags (and a delete between them), a
/// legacy shared records table and WAL marks.
#[test]
fn a_repository_migrated_in_place_reads_the_same() {
    let d = tempfile::tempdir().unwrap();
    let wal = d.path().join("wal");
    let path = s(&d.path().join("repo"));
    let (u, w) = (path.clone(), wal.clone());
    block_on(async move {
        let repo = Repo::open(&u, repo_opts(&w, Some(Engine::Lance)))
            .await
            .unwrap();
        for n in 0..6 {
            let run = repo
                .create_run(NewRun {
                    run_id: Some(format!("run-{n}")),
                    name: Some(format!("name {n}")),
                    tags: vec!["t".into()],
                    ..NewRun::default()
                })
                .unwrap();
            run.set_params(&json!({"lr": 0.1 * n as f64, "deep": {"x": n}}))
                .unwrap();
            for step in 0..300i64 {
                for m in ["loss", "acc"] {
                    run.track(
                        m,
                        step as f64 / (n + 1) as f64,
                        Track {
                            step: Some(step),
                            context: Some(json!({"subset": "train"})),
                            ..Track::default()
                        },
                    )
                    .unwrap();
                }
            }
            repo.flush().await.unwrap();
        }
        repo.delete_runs(&["run-5".to_string()]).await.unwrap();
        let rows = |k: usize| -> Vec<NewRecord> {
            (0..k)
                .map(|i| NewRecord {
                    item_id: format!("item{i}"),
                    run_id: Some(format!("run-{}", i % 3)),
                    data: json!({"score": i}),
                })
                .collect()
        };
        repo.append_records("evals", rows(10)).await.unwrap();
        repo.tag_records("evals", "v1", None).await.unwrap();
        repo.append_records("evals", rows(4)).await.unwrap();
        repo.delete_runs(&["run-4".to_string()]).await.unwrap();
        repo.tag_records("evals", "v2", None).await.unwrap();
        repo.append_records("other table/x", rows(3)).await.unwrap();
        repo.close().await.unwrap();
    });
    write_legacy_records(&d.path().join("repo"));
    let before = dump_repo(&path, &wal);
    assert!(before.iter().any(|l| l.starts_with("legacy ")));
    let r = migrate(
        &path,
        &path,
        &MigrateOptions {
            remove_source: true,
            ..mopts()
        },
    )
    .unwrap();
    assert!(r.in_place && r.source_removed && r.optimized);
    assert_eq!(table(&r, "records/evals").tags.len(), 2);
    assert!(table(&r, "runs").marks >= 1, "WAL marks carried over");
    assert_eq!(table(&r, "_legacy_records").rows, 2);
    assert!(!d.path().join("repo/runs.lance").exists());
    assert!(!d.path().join("repo/records.lance").exists());
    assert_eq!(
        detect(&path, &HashMap::new()).unwrap(),
        Format::Repository(Engine::Delta)
    );
    assert_eq!(dump_repo(&path, &wal), before);
    // Running it again finds nothing to migrate.
    assert!(migrate(&path, &path, &mopts()).is_err());
}

/// What builds before per-table records wrote: every table in `records.lance`.
fn write_legacy_records(root: &Path) {
    use arrow_array::{
        RecordBatch, RecordBatchIterator, StringArray, TimestampMicrosecondArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, true),
        Field::new("item_id", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            std::sync::Arc::new(UInt64Array::from(vec![1, 2])),
            std::sync::Arc::new(StringArray::from(vec!["dataset", "scores"])),
            std::sync::Arc::new(StringArray::from(vec![None, Some("run-1")])),
            std::sync::Arc::new(StringArray::from(vec!["a", "a"])),
            std::sync::Arc::new(StringArray::from(vec!["{\"x\":1}", "{\"s\":2}"])),
            std::sync::Arc::new(TimestampMicrosecondArray::from(vec![10, 20]).with_timezone("UTC")),
        ],
    )
    .unwrap();
    let legacy = s(&root.join("records.lance"));
    block_on(async move {
        lance::Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            &legacy,
            None,
        )
        .await
    })
    .unwrap();
}

/// A dry run writes nothing; a source written to moments ago is refused; a stopped migration
/// needs `resume`, which keeps the verified tables and finishes the rest.
#[test]
fn dry_runs_recent_writes_and_resume() {
    let d = tempfile::tempdir().unwrap();
    copy_dir(&fixture().join("repo"), &d.path().join("repo"));
    let wal = d.path().join("wal");
    let src = s(&d.path().join("repo"));
    let dst = s(&d.path().join("new"));
    let before = dump_repo(&src, &wal);

    let r = migrate(
        &src,
        &dst,
        &MigrateOptions {
            dry_run: true,
            ..mopts()
        },
    )
    .unwrap();
    assert_eq!(r.status, "dry-run");
    assert!(table(&r, "metrics").rows > 0);
    assert!(!d.path().join("new").exists(), "a dry run writes nothing");

    // A writer that just committed.
    let fresh = s(&d.path().join("fresh"));
    let db = Db::open(&fresh, kv_opts(&wal, Some(Engine::Lance))).unwrap();
    db.put(b"k", b"v").unwrap();
    db.close().unwrap();
    let e = migrate(
        &fresh,
        &s(&d.path().join("fresh-new")),
        &MigrateOptions::default(),
    )
    .unwrap_err();
    assert!(e.to_string().contains("stop every writer"), "{e}");
    assert!(
        !d.path().join("fresh-new").exists(),
        "a refused migration writes nothing"
    );

    // An interrupted migration: the report says running, one table is missing, no marker.
    let r = migrate(&src, &dst, &mopts()).unwrap();
    let mut stopped = r.clone();
    stopped.status = "running".into();
    stopped.finished_at = None;
    for t in &mut stopped.tables {
        if t.name == "metrics" || t.name == "records/evals" {
            t.done = false;
        }
    }
    let new = d.path().join("new");
    std::fs::write(
        new.join("_kv0/migration.json"),
        serde_json::to_vec(&stopped).unwrap(),
    )
    .unwrap();
    std::fs::remove_file(new.join("_kv0/format.json")).unwrap();
    std::fs::remove_dir_all(new.join("metrics")).unwrap();
    let e = migrate(&src, &dst, &mopts()).unwrap_err();
    assert!(e.to_string().contains("resume"), "{e}");
    let r = migrate(
        &src,
        &dst,
        &MigrateOptions {
            resume: true,
            ..mopts()
        },
    )
    .unwrap();
    assert_eq!(r.status, "complete");
    assert!(r.tables.iter().all(|t| t.done));
    assert_eq!(dump_repo(&dst, &wal), before);
    let e = migrate(&src, &dst, &mopts()).unwrap_err();
    assert!(e.to_string().contains("migrated already"), "{e}");
}

/// The command line: `kv0 migrate <from> <to>` (the binary runs `kv0::migrate::cli`).
#[test]
fn the_command_line_migrates() {
    let d = tempfile::tempdir().unwrap();
    copy_dir(&fixture().join("kv"), &d.path().join("kv"));
    let args = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let from = s(&d.path().join("kv"));
    let to = s(&d.path().join("kv-new"));
    assert_eq!(kv0::migrate::cli(&args(&["migrate", &from])), 2);
    assert_eq!(
        kv0::migrate::cli(&args(&["migrate", &from, &to, "--quiet-period", "0"])),
        0
    );
    assert_eq!(
        detect(&to, &HashMap::new()).unwrap(),
        Format::Database(Engine::Delta)
    );
    assert_eq!(
        kv0::migrate::cli(&args(&["migrate", &from, &to, "--quiet-period", "0"])),
        1,
        "a second run fails"
    );
}

/// Size and time of a realistic repository's migration; run by hand:
/// `KV0_MIGRATE_RUNS=200 cargo test -p kv0 --features lance-migrate --test migrate -- --ignored --nocapture`.
#[test]
#[ignore = "a measurement, run by hand"]
fn a_realistic_repository_migration() {
    let runs: usize = std::env::var("KV0_MIGRATE_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let d = tempfile::tempdir().unwrap();
    let wal = d.path().join("wal");
    let src = s(&d.path().join("repo"));
    let t0 = std::time::Instant::now();
    let (u, w) = (src.clone(), wal.clone());
    block_on(async move {
        let repo = Repo::open(&u, repo_opts(&w, Some(Engine::Lance)))
            .await
            .unwrap();
        for n in 0..runs {
            let run = repo
                .create_run(NewRun {
                    run_id: Some(format!("run-{n}")),
                    ..NewRun::default()
                })
                .unwrap();
            run.set_params(&json!({"lr": 0.001, "batch": 64, "model": {"layers": 12}}))
                .unwrap();
            for m in ["loss", "acc", "lr", "grad_norm", "val_loss"] {
                for step in 0..1000i64 {
                    run.track(
                        m,
                        step as f64,
                        Track {
                            step: Some(step),
                            ..Track::default()
                        },
                    )
                    .unwrap();
                }
            }
            if n % 10 == 9 {
                repo.flush().await.unwrap();
            }
        }
        let rows: Vec<NewRecord> = (0..2000)
            .map(|i| NewRecord {
                item_id: format!("item{i}"),
                run_id: Some(format!("run-{}", i % runs)),
                data: json!({"prompt": "x".repeat(200), "score": i}),
            })
            .collect();
        repo.append_records("evals", rows).await.unwrap();
        repo.tag_records("evals", "v1", None).await.unwrap();
        repo.close().await.unwrap();
    });
    let written = t0.elapsed();
    let size = |p: &Path| -> u64 {
        fn walk(p: &Path) -> u64 {
            std::fs::read_dir(p)
                .map(|rd| {
                    rd.flatten()
                        .map(|e| {
                            if e.path().is_dir() {
                                walk(&e.path())
                            } else {
                                e.metadata().map(|m| m.len()).unwrap_or(0)
                            }
                        })
                        .sum()
                })
                .unwrap_or(0)
        }
        walk(p)
    };
    let src_size = size(&d.path().join("repo"));
    let dst = s(&d.path().join("new"));
    let t1 = std::time::Instant::now();
    let r = migrate(&src, &dst, &mopts()).unwrap();
    let took = t1.elapsed();
    eprintln!(
        "{runs} runs written in {written:?}; Lance {:.1} MB -> Delta {:.1} MB; migrated in {took:?}",
        src_size as f64 / 1e6,
        size(&d.path().join("new")) as f64 / 1e6
    );
    for t in &r.tables {
        eprintln!("  {:<16} {:>9} rows", t.name, t.rows);
    }
    assert!(table(&r, "metrics").rows as usize >= runs * 5 * 1000);
}
