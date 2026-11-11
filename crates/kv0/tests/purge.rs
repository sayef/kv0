//! Erasure: `purge` removes deleted data from the current version and from every old version
//! that is not tagged, and refuses (or with `force`, removes) tags that keep deleted data.

use std::path::Path;

use kv0::kv::{Db, Options};
use kv0::rt::block_on;
use kv0::tracking::{At, NewRecord, NewRun, RecordQuery, Repo, RepoOptions, StepRange, Track};
use serde_json::json;

const MARKER: &str = "erase-me-7f3a9c1e";
/// Data that stays: purge must not touch it, and finding it shows the search works.
const LIVE: &str = "keep-me-41b8d2";

/// Whether any file under `dir` contains `needle`.
fn contains(dir: &Path, needle: &[u8]) -> bool {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return false;
    };
    rd.flatten().any(|e| {
        let p = e.path();
        if p.is_dir() {
            contains(&p, needle)
        } else {
            std::fs::read(&p).is_ok_and(|b| b.windows(needle.len()).any(|w| w == needle))
        }
    })
}

fn repo(uri: &str, wal: &Path) -> Repo {
    let o = RepoOptions {
        flush_interval: None,
        wal_dir: Some(wal.to_path_buf()),
        ..RepoOptions::default()
    };
    let uri = uri.to_string();
    block_on(async move { Repo::open(&uri, o).await }).unwrap()
}

#[test]
fn purge_makes_a_deleted_run_unrecoverable() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("repo");
    let uri = root.to_string_lossy().into_owned();
    let repo = repo(&uri, &d.path().join("wal"));
    let run = repo
        .create_run(NewRun {
            run_id: Some("r1".into()),
            name: Some(MARKER.into()),
            ..NewRun::default()
        })
        .unwrap();
    run.set_params(&json!({ "note": MARKER })).unwrap();
    for i in 0..20 {
        run.track("loss", i as f64, Track::default()).unwrap();
    }
    let keep = repo
        .create_run(NewRun {
            name: Some(LIVE.into()),
            ..NewRun::default()
        })
        .unwrap();
    keep.track("loss", 1.0, Track::default()).unwrap();
    let r = repo.clone();
    let (old_scores, old_dataset) = block_on(async move {
        r.flush().await?;
        let s = r
            .append_records(
                "scores",
                vec![NewRecord {
                    item_id: "q1".into(),
                    run_id: Some("r1".into()),
                    data: json!({ "out": MARKER }),
                }],
            )
            .await?;
        let ds = r
            .append_records(
                "dataset:qa",
                vec![NewRecord {
                    item_id: "q1".into(),
                    run_id: None,
                    data: json!({ "input": "kept" }),
                }],
            )
            .await?;
        // A dataset tag keeps its own table only, so it does not stand in the way.
        r.tag_records("dataset:qa", "qa-v1", None).await?;
        r.tag_records("scores", "scores-v1", None).await?;
        Ok::<_, kv0::Error>((s, ds))
    })
    .unwrap();
    assert!(
        contains(&root, MARKER.as_bytes()),
        "the marker is stored verbatim"
    );

    let r = repo.clone();
    assert_eq!(
        block_on(async move { r.delete_runs(&["r1".into()]).await }).unwrap(),
        1
    );
    assert!(contains(&root, MARKER.as_bytes()), "deleted is not erased");

    // A tag keeping a version with the run's records stops the purge, naming it.
    let r = repo.clone();
    let e = block_on(async move { r.purge(false).await }).unwrap_err();
    assert!(e.to_string().contains("scores-v1"), "{e}");
    let r = repo.clone();
    let report = block_on(async move { r.purge(true).await }).unwrap();
    assert_eq!(report.runs, 1);
    assert_eq!(report.tags_deleted.len(), 1, "{report:?}");
    assert!(
        report.versions_removed > 0 && report.bytes_removed > 0,
        "{report:?}"
    );

    assert!(
        !contains(&root, MARKER.as_bytes()),
        "no file holds the run's data"
    );
    assert!(
        !contains(&d.path().join("wal"), MARKER.as_bytes()),
        "nor the WAL"
    );
    assert!(
        contains(&root, LIVE.as_bytes()),
        "live data stays (and the search works)"
    );
    let r = repo.clone();
    let (scores_then, dataset_then, run, kept) = block_on(async move {
        let s = r
            .records(
                "scores",
                RecordQuery {
                    at: At::Version(old_scores),
                    ..RecordQuery::default()
                },
            )
            .await;
        let d = r
            .records(
                "dataset:qa",
                RecordQuery {
                    at: At::Tag("qa-v1".into()),
                    ..RecordQuery::default()
                },
            )
            .await;
        let run = r.run("r1").await.unwrap();
        let kept = r
            .read_metric(&keep.id(), "loss", &json!({}), StepRange::default())
            .await
            .unwrap();
        (s, d, run, kept)
    });
    assert!(scores_then.is_err(), "the old version is gone");
    assert_eq!(
        dataset_then.unwrap().len(),
        1,
        "the dataset tag still works"
    );
    let _ = old_dataset;
    assert!(run.is_none());
    assert_eq!(kept.len(), 1, "other runs are untouched");
}

#[test]
fn purge_makes_a_deleted_key_unrecoverable() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("db");
    let path = root.to_string_lossy().into_owned();
    let wal = d.path().join("wal");
    let opts = || Options {
        flush_interval: None,
        wal_dir: Some(wal.clone()),
        snapshot_min_entries: 1,
        ..Options::default()
    };
    let db = Db::open(&path, opts()).unwrap();
    db.put(b"secret", MARKER.as_bytes()).unwrap();
    db.put(b"big", format!("{MARKER}{}", "x".repeat(10_000)).as_bytes())
        .unwrap();
    db.put(b"keep", LIVE.as_bytes()).unwrap();
    db.flush().unwrap();
    let old = db.version();
    db.create_tag("before", None).unwrap();
    db.delete(b"secret").unwrap();
    db.delete(b"big").unwrap();
    db.flush().unwrap();
    db.close().unwrap();
    let db = Db::open(&path, opts()).unwrap();
    assert!(contains(&root, MARKER.as_bytes()));

    let e = db.purge(false).unwrap_err();
    assert!(e.to_string().contains("before"), "{e}");
    let report = db.purge(true).unwrap();
    assert_eq!(report.tags_deleted, vec!["before".to_string()]);
    assert!(report.versions_removed > 0, "{report:?}");
    assert_eq!(report.snapshots_removed, 1, "{report:?}");
    assert!(
        !contains(&root, MARKER.as_bytes()),
        "no data file holds the values"
    );
    assert!(
        !contains(&wal, MARKER.as_bytes()),
        "nor the WAL or a snapshot"
    );
    assert_eq!(db.get(b"keep").unwrap().as_deref(), Some(LIVE.as_bytes()));
    assert!(
        contains(&root, LIVE.as_bytes()),
        "live data stays (and the search works)"
    );
    assert!(Db::open(
        &path,
        Options {
            version: Some(old),
            ..opts()
        }
    )
    .is_err());
}

#[test]
fn purge_refuses_a_tag_that_keeps_an_overwritten_value() {
    // Review P1: put, tag, overwrite, purge. The old value is neither deleted nor live.
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("db");
    let path = root.to_string_lossy().into_owned();
    let db = Db::open(
        &path,
        Options {
            flush_interval: None,
            wal_dir: Some(d.path().join("wal")),
            ..Options::default()
        },
    )
    .unwrap();
    db.put(b"k", MARKER.as_bytes()).unwrap();
    db.flush().unwrap();
    db.create_tag("old", None).unwrap();
    db.put(b"k", LIVE.as_bytes()).unwrap();
    db.flush().unwrap();
    let e = db.purge(false).unwrap_err();
    assert!(e.to_string().contains("old"), "{e}");
    assert!(
        contains(&root, MARKER.as_bytes()),
        "a refused purge keeps the tag's data"
    );
    let report = db.purge(true).unwrap();
    assert_eq!(report.tags_deleted, vec!["old".to_string()]);
    assert!(!contains(&root, MARKER.as_bytes()));
    assert!(contains(&root, LIVE.as_bytes()));
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(LIVE.as_bytes()));
}
