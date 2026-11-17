//! The Delta Lake engine of the tracking layer under concurrency: several processes writing
//! runs, parameters, metrics and records to one repository, alone and racing `delete_runs` and
//! `optimize`.
//!
//! Every repository here is a Delta one (`KV0_TEST_BACKEND=delta` for this process and its
//! children). The S3 variants run when `KV0_TEST_S3_ENDPOINT` is set (CI starts an
//! S3-compatible server).
#![cfg(feature = "delta")]

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;

use kv0::rt::block_on;
use kv0::tracking::{NewRecord, NewRun, RecordQuery, Repo, RepoOptions, StepRange, TrackPoint};
use serde_json::json;

/// Points each run logs, and runs each writer creates (one flush, one commit per table, each).
const STEPS: i64 = 50;

fn delta() {
    // Every test of this binary sets the same value, so the races between them are harmless.
    std::env::set_var("KV0_TEST_BACKEND", "delta");
}

fn opts(wal: &Path, storage: &HashMap<String, String>) -> RepoOptions {
    RepoOptions {
        flush_interval: None,
        wal_dir: Some(wal.to_path_buf()),
        storage_options: storage.clone(),
        ..RepoOptions::default()
    }
}

fn open(uri: &str, wal: &Path, storage: &HashMap<String, String>) -> Repo {
    let (uri, o) = (uri.to_string(), opts(wal, storage));
    block_on(async move { Repo::open(&uri, o).await }).unwrap()
}

fn s3() -> Option<(String, HashMap<String, String>)> {
    let endpoint = std::env::var("KV0_TEST_S3_ENDPOINT")
        .ok()
        .filter(|e| !e.is_empty())?;
    let bucket = std::env::var("KV0_TEST_S3_BUCKET").unwrap_or_else(|_| "kv0-test".into());
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.into());
    let opts = HashMap::from([
        ("aws_endpoint".to_string(), endpoint),
        (
            "aws_access_key_id".to_string(),
            env("KV0_TEST_S3_KEY", "minioadmin"),
        ),
        (
            "aws_secret_access_key".to_string(),
            env("KV0_TEST_S3_SECRET", "minioadmin"),
        ),
        ("aws_region".to_string(), "us-east-1".to_string()),
        ("allow_http".to_string(), "true".to_string()),
    ]);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Some((format!("s3://{bucket}/delta-repo-{nanos:x}"), opts))
}

fn points(n: i64) -> Vec<TrackPoint> {
    (0..n)
        .map(|i| TrackPoint {
            value: i as f64,
            step: Some(i),
            epoch: None,
            timestamp: Some(1_000 + i),
        })
        .collect()
}

/// Writes one run with a parameter, `STEPS` points and a record, and flushes.
fn write_run(repo: &Repo, run_id: &str) {
    let run = repo
        .create_run(NewRun {
            run_id: Some(run_id.to_string()),
            ..NewRun::default()
        })
        .unwrap();
    run.set_param("lr", json!(run_id)).unwrap();
    run.track_many("loss", None, &points(STEPS)).unwrap();
    let (r, id) = (repo.clone(), run_id.to_string());
    block_on(async move {
        r.flush().await?;
        r.append_records(
            "evals",
            vec![NewRecord {
                item_id: id.clone(),
                run_id: Some(id),
                data: json!({ "ok": true }),
            }],
        )
        .await
    })
    .unwrap();
}

/// Child side: `KV0_TRK_MODE` is `write` (create `KV0_TRK_RUNS` runs `<id>-<k>`), `delete`
/// (delete the runs named in `KV0_TRK_RUNS`, comma separated, one call each) or `optimize`
/// (`KV0_TRK_RUNS` rounds).
#[test]
fn trk_child() {
    let (Ok(uri), Ok(wal), Ok(id), Ok(mode), Ok(arg)) = (
        std::env::var("KV0_TRK_REPO"),
        std::env::var("KV0_TRK_WAL"),
        std::env::var("KV0_TRK_ID"),
        std::env::var("KV0_TRK_MODE"),
        std::env::var("KV0_TRK_RUNS"),
    ) else {
        return;
    };
    let storage: HashMap<String, String> = std::env::var("KV0_TRK_OPTS")
        .ok()
        .map(|s| serde_json::from_str(&s).unwrap())
        .unwrap_or_default();
    let repo = open(&uri, Path::new(&wal), &storage);
    match mode.as_str() {
        "write" => {
            for k in 0..arg.parse::<u32>().unwrap() {
                write_run(&repo, &format!("{id}-{k}"));
            }
        }
        "delete" => {
            for victim in arg.split(',') {
                // Racing rewrites may make a delete give up; running it again finishes it.
                let mut tries = 0;
                loop {
                    let r = repo.clone();
                    let v = vec![victim.to_string()];
                    match block_on(async move { r.delete_runs(&v).await }) {
                        Ok(_) => break,
                        Err(kv0::Error::Conflict(m)) if tries < 20 => {
                            tries += 1;
                            eprintln!("delete {victim} conflicted ({m}); again");
                        }
                        Err(e) => panic!("delete {victim}: {e}"),
                    }
                }
            }
        }
        "optimize" => {
            // Optimize plans each rewrite again when another rewrite (a delete, a background
            // merge, the other optimizer) took some of the same files first: it never fails.
            for round in 0..arg.parse::<u32>().unwrap() {
                let r = repo.clone();
                if let Err(e) = block_on(async move { r.optimize().await }) {
                    panic!("optimize round {round}: {e}");
                }
            }
        }
        other => panic!("unknown mode {other}"),
    }
    let r = repo.clone();
    block_on(async move { r.close().await }).unwrap();
}

/// One child process: `(mode, id, argument)`.
type Child = (&'static str, String, String);

fn run_children(uri: &str, storage: &HashMap<String, String>, wal: &Path, children: &[Child]) {
    let exe = std::env::current_exe().unwrap();
    let procs: Vec<_> = children
        .iter()
        .map(|(mode, id, arg)| {
            Command::new(&exe)
                .args(["--exact", "trk_child", "--nocapture", "--test-threads=1"])
                .env("KV0_TEST_BACKEND", "delta")
                .env("KV0_TRK_REPO", uri)
                .env("KV0_TRK_WAL", wal.join(id))
                .env("KV0_TRK_ID", id)
                .env("KV0_TRK_MODE", mode)
                .env("KV0_TRK_RUNS", arg)
                .env("KV0_TRK_OPTS", serde_json::to_string(storage).unwrap())
                .spawn()
                .unwrap()
        })
        .collect();
    for mut c in procs {
        assert!(c.wait().unwrap().success(), "a child process failed");
    }
}

/// Every run of `live` is complete and stored once; every run of `gone` left nothing.
fn check(
    uri: &str,
    storage: &HashMap<String, String>,
    wal: &Path,
    live: &[String],
    gone: &[String],
) {
    let repo = open(uri, &wal.join("check"), storage);
    let r = repo.clone();
    let runs: Vec<String> = block_on(async move { r.runs().await })
        .unwrap()
        .into_iter()
        .map(|i| i.run_id)
        .collect();
    let mut want = live.to_vec();
    want.sort();
    let mut have = runs.clone();
    have.sort();
    assert_eq!(have, want, "runs");
    let r = repo.clone();
    let params = block_on(async move { r.all_params().await }).unwrap();
    let r = repo.clone();
    let records =
        block_on(async move { r.records("evals", RecordQuery::default()).await }).unwrap();
    let mut rec: Vec<String> = records.iter().map(|x| x.item_id.clone()).collect();
    rec.sort();
    assert_eq!(rec, want, "records");
    for id in live {
        assert_eq!(params[id]["lr"], json!(id), "{id}");
        let (r, i) = (repo.clone(), id.clone());
        let d = block_on(async move {
            r.read_metric(&i, "loss", &json!({}), StepRange::default())
                .await
        })
        .unwrap();
        assert_eq!(d.steps, (0..STEPS).collect::<Vec<_>>(), "{id}");
    }
    for id in gone {
        assert!(!params.contains_key(id), "{id}");
    }
    // Stored once, nothing of the deleted runs left in any table.
    let r = repo.clone();
    let rows = block_on(async move {
        Ok::<_, kv0::Error>((
            r.metric_rows().await?,
            r.table_rows("params").await?,
            r.table_rows("series").await?,
        ))
    })
    .unwrap();
    let n = live.len();
    assert_eq!(rows, (n * STEPS as usize, n, n), "points, params, series");
    let r = repo.clone();
    block_on(async move { r.close().await }).unwrap();
}

fn writers(procs: u32, runs: u32) -> (Vec<Child>, Vec<String>) {
    let children: Vec<Child> = (0..procs)
        .map(|p| ("write", format!("p{p}"), runs.to_string()))
        .collect();
    let live = (0..procs)
        .flat_map(|p| (0..runs).map(move |k| format!("p{p}-{k}")))
        .collect();
    (children, live)
}

fn several_writers(uri: &str, storage: &HashMap<String, String>, procs: u32, runs: u32) {
    delta();
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    // Created first, so the children race on appends only.
    let r = open(uri, &wal.join("setup"), storage);
    block_on(async move { r.close().await }).unwrap();
    let (children, live) = writers(procs, runs);
    run_children(uri, storage, &wal, &children);
    check(uri, storage, &wal, &live, &[]);
}

#[test]
fn concurrent_writers_from_several_processes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    several_writers(&uri, &HashMap::new(), 8, 5);
}

#[test]
fn concurrent_writers_from_several_processes_on_s3() {
    let Some((uri, storage)) = s3() else {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    };
    several_writers(&uri, &storage, 4, 3);
}

/// Writers append while one process deletes runs written before and another optimizes:
/// no append is lost, and the deleted runs are gone from every table.
fn deletes_and_optimize_racing_writers(
    uri: &str,
    storage: &HashMap<String, String>,
    procs: u32,
    runs: u32,
    victims: u32,
) {
    delta();
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    let repo = open(uri, &wal.join("setup"), storage);
    let gone: Vec<String> = (0..victims).map(|k| format!("victim-{k}")).collect();
    for v in &gone {
        write_run(&repo, v);
    }
    let r = repo.clone();
    block_on(async move { r.close().await }).unwrap();
    let (mut children, live) = writers(procs, runs);
    children.push(("delete", "deleter".into(), gone.join(",")));
    children.push(("optimize", "optimizer-a".into(), "6".into()));
    children.push(("optimize", "optimizer-b".into(), "6".into()));
    run_children(uri, storage, &wal, &children);
    // A last optimize, so the sorted rewrite also drops whatever the racing one kept.
    let repo = open(uri, &wal.join("final"), storage);
    let r = repo.clone();
    block_on(async move { r.optimize().await }).unwrap();
    let r = repo.clone();
    block_on(async move { r.close().await }).unwrap();
    check(uri, storage, &wal, &live, &gone);
}

#[test]
fn deletes_and_optimize_racing_appends_lose_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("repo").to_string_lossy().into_owned();
    deletes_and_optimize_racing_writers(&uri, &HashMap::new(), 6, 8, 6);
}

#[test]
fn deletes_and_optimize_racing_appends_lose_nothing_on_s3() {
    let Some((uri, storage)) = s3() else {
        eprintln!("skipped: KV0_TEST_S3_ENDPOINT is not set");
        return;
    };
    deletes_and_optimize_racing_writers(&uri, &storage, 3, 3, 3);
}

/// Repositories under one `memory://` root: every table of a handle shares one store, so the
/// records tables can be listed; another handle sees nothing.
#[test]
fn a_memory_repository_shares_one_store_across_its_tables() {
    delta();
    let uri = format!("memory://trk-{}", std::process::id());
    let o = RepoOptions {
        flush_interval: None,
        wal: false,
        ..RepoOptions::default()
    };
    let (u, oo) = (uri.clone(), o.clone());
    let repo = block_on(async move { Repo::open(&u, oo).await }).unwrap();
    let r = repo.clone();
    let tables = block_on(async move {
        for t in ["a", "b:c"] {
            r.append_records(
                t,
                vec![NewRecord {
                    item_id: "x".into(),
                    run_id: None,
                    data: json!(1),
                }],
            )
            .await?;
        }
        r.record_tables().await
    })
    .unwrap();
    assert_eq!(tables, ["a", "b:c"]);
    let other = block_on(async move { Repo::open(&uri, o).await }).unwrap();
    let tables = block_on(async move { other.record_tables().await }).unwrap();
    assert!(tables.is_empty());
}
