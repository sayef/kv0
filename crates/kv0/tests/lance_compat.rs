//! Lance data written before the logical schema became signed reads back identically.
//!
//! `tests/fixtures/lance-unsigned/` holds a key-value database and a tracking repository that
//! the ignored `write_fixture` test wrote with the build before that change, and `expected.txt`
//! what that build read from them. Regenerate both (only when the fixture must change) with
//! `KV0_FIXTURE_OUT=<dir> cargo test -p kv0 --test lance_compat -- --ignored write_fixture`.

use std::path::Path;

use kv0::kv::{Db, Options};
use kv0::tracking::{NewRecord, NewRun, RecordQuery, Repo, RepoOptions, StepRange, Track};
use serde_json::json;

fn kv_opts() -> Options {
    Options {
        flush_interval: None,
        refresh_interval: None,
        wal: false,
        snapshot_min_entries: usize::MAX,
        version_retention: None,
        lazy_value_threshold: 64,
        ..Options::default()
    }
}

fn repo_opts() -> RepoOptions {
    RepoOptions {
        flush_interval: None,
        wal: false,
        version_retention: None,
        ..RepoOptions::default()
    }
}

const RUNS: [&str; 3] = ["run-a", "run-b", "run-c"];
const NAMES: [&str; 4] = ["loss", "acc", "lr", "grad_norm"];

fn write_kv(path: &str) {
    let db = Db::open(path, kv_opts()).unwrap();
    for i in 0..40u32 {
        db.put(
            format!("key{i:02}").as_bytes(),
            format!("value{i}").as_bytes(),
        )
        .unwrap();
    }
    db.put(b"big", &[7u8; 300]).unwrap();
    db.delete(b"key03").unwrap();
    db.delete_range(b"key10", b"key15").unwrap();
    db.flush().unwrap();
    db.create_tag("first", None).unwrap();
    db.put(b"key00", b"overwritten").unwrap();
    db.put(b"big2", &[9u8; 500]).unwrap();
    db.flush().unwrap();
    db.close().unwrap();
}

async fn write_repo(path: &str) {
    let repo = Repo::open(path, repo_opts()).await.unwrap();
    for (n, id) in RUNS.iter().enumerate() {
        let run = repo
            .create_run(NewRun {
                run_id: Some(id.to_string()),
                name: Some(format!("name {n}")),
                tags: vec!["fixture".into()],
                created_at: Some(1_700_000_000_000_000 + n as i64),
                ..NewRun::default()
            })
            .unwrap();
        run.set_params(&json!({"lr": 0.001 * (n + 1) as f64, "opt": {"name": "adam"}}))
            .unwrap();
        for name in NAMES {
            for step in 0..25i64 {
                run.track(
                    name,
                    step as f64 * 0.5 + n as f64,
                    Track {
                        step: Some(step),
                        epoch: Some(step / 10),
                        context: Some(
                            json!({"subset": if step % 2 == 0 { "train" } else { "val" }}),
                        ),
                        timestamp: Some(1_700_000_000_000_000 + step),
                    },
                )
                .unwrap();
            }
        }
        repo.flush().await.unwrap();
    }
    repo.delete_runs(&["run-c".to_string()]).await.unwrap();
    let rows: Vec<NewRecord> = (0..6)
        .map(|i| NewRecord {
            item_id: format!("item{i}"),
            run_id: Some(RUNS[i % 2].to_string()),
            data: json!({"score": i, "label": format!("l{i}")}),
        })
        .collect();
    repo.append_records("evals", rows).await.unwrap();
    repo.tag_records("evals", "v1", None).await.unwrap();
    repo.close().await.unwrap();
}

/// Everything the public API reads from the database and the repository, as text.
async fn dump(kv: &str, repo: &str) -> String {
    let mut out = Vec::new();
    {
        let db = Db::open(
            kv,
            Options {
                read_only: true,
                ..kv_opts()
            },
        )
        .unwrap();
        let mut it = db.iter();
        it.seek_to_first();
        for (k, v) in it {
            out.push(format!("kv {:?} = {:?}", k, v));
        }
        out.push(format!("kv tags {:?}", db.tags().unwrap()));
        let first = db.tag_version("first").unwrap();
        let old = Db::open(
            kv,
            Options {
                version: Some(first),
                ..kv_opts()
            },
        )
        .unwrap();
        let mut it = old.iter();
        it.seek_to_first();
        for (k, v) in it {
            out.push(format!("kv@first {:?} = {:?}", k, v));
        }
    }
    let repo = Repo::open(repo, repo_opts()).await.unwrap();
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
        .read_many(
            &ids,
            StepRange {
                start: Some(3),
                stop: Some(20),
            },
        )
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
    for r in repo.records("evals", RecordQuery::default()).await.unwrap() {
        out.push(format!("record {r:?}"));
    }
    out.push(format!(
        "record tags {:?}",
        repo.record_tags("evals").await.unwrap()
    ));
    repo.close().await.unwrap();
    out.join("\n") + "\n"
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

fn fixture() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/lance-unsigned")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "writes the fixture; run by hand with KV0_FIXTURE_OUT"]
async fn write_fixture() {
    let out = std::path::PathBuf::from(std::env::var("KV0_FIXTURE_OUT").unwrap());
    let kv = out.join("kv").to_string_lossy().into_owned();
    let repo = out.join("repo").to_string_lossy().into_owned();
    tokio::task::spawn_blocking({
        let kv = kv.clone();
        move || write_kv(&kv)
    })
    .await
    .unwrap();
    write_repo(&repo).await;
    let text = dump(&kv, &repo).await;
    std::fs::write(out.join("expected.txt"), text).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn lance_data_written_unsigned_reads_back_identically() {
    let dir = tempfile::tempdir().unwrap();
    copy_dir(&fixture(), dir.path());
    let kv = dir.path().join("kv").to_string_lossy().into_owned();
    let repo = dir.path().join("repo").to_string_lossy().into_owned();
    let expected = std::fs::read_to_string(dir.path().join("expected.txt")).unwrap();
    // The fixture covers series ids whose top bit is set, which the signed schema stores as
    // negative numbers.
    assert!(expected
        .lines()
        .filter_map(|l| l.strip_prefix("points "))
        .filter_map(|l| l.split(' ').next()?.parse::<u64>().ok())
        .any(|id| id > i64::MAX as u64));
    assert_eq!(dump(&kv, &repo).await, expected);

    // Writing, compacting and optimizing with this build keeps the stored types, and the old
    // rows read back as before next to the new ones.
    {
        let db = Db::open(&kv, kv_opts()).unwrap();
        db.put(b"zz", b"new").unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        assert_eq!(db.get(b"key00").unwrap().unwrap(), &b"overwritten"[..]);
        assert_eq!(db.get(b"big").unwrap().unwrap(), &[7u8; 300][..]);
        assert_eq!(db.get(b"zz").unwrap().unwrap(), &b"new"[..]);
        assert!(db.get(b"key12").unwrap().is_none());
        db.close().unwrap();
    }
    {
        let r = Repo::open(&repo, repo_opts()).await.unwrap();
        let run = r.resume_run("run-a").await.unwrap();
        run.track(
            "loss",
            42.0,
            Track {
                step: Some(100),
                ..Track::default()
            },
        )
        .unwrap();
        r.flush().await.unwrap();
        r.optimize().await.unwrap();
        let id = kv0::tracking::series_id("run-a", "loss", &json!({}));
        let got = r
            .read_many(
                &[id],
                StepRange {
                    start: Some(100),
                    stop: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(got[&id].values, vec![42.0]);
        r.close().await.unwrap();
    }
    for (table, column, want) in [
        ("kv", "seq", "UInt64"),
        ("kv", "op", "UInt8"),
        ("repo/runs.lance", "seq", "UInt64"),
        ("repo/runs.lance", "fields", "UInt32"),
        ("repo/series.lance", "series_id", "UInt64"),
        ("repo/metrics.lance", "series_id", "UInt64"),
        ("repo/params.lance", "seq", "UInt64"),
        ("repo/records/evals.lance", "seq", "UInt64"),
    ] {
        let uri = dir.path().join(table).to_string_lossy().into_owned();
        let ds = lance::Dataset::open(&uri).await.unwrap();
        let schema = arrow_schema::Schema::from(ds.schema());
        let got = schema
            .field_with_name(column)
            .unwrap()
            .data_type()
            .to_string();
        assert_eq!(got, want, "{table}.{column}");
    }
}
