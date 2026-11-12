//! Write-ahead log of the tracking layer: crash recovery, torn tails, idempotent replay.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use kv0::rt::block_on;
use kv0::tracking::{NewRun, Repo, RepoOptions, StepRange, Track};
use serde_json::json;

fn opts(wal: &Path) -> RepoOptions {
    RepoOptions {
        flush_interval: None,
        flush_rows: usize::MAX,
        wal_dir: Some(wal.to_path_buf()),
        ..RepoOptions::default()
    }
}

fn open(uri: &str, wal: &Path) -> Repo {
    let (uri, o) = (uri.to_string(), opts(wal));
    block_on(async move { Repo::open(&uri, o).await }).unwrap()
}

fn points(repo: &Repo, run: &str, name: &str) -> Vec<(i64, f64)> {
    let (r, run, name) = (repo.clone(), run.to_string(), name.to_string());
    let d = block_on(async move {
        r.read_metric(&run, &name, &json!({}), StepRange::default())
            .await
    })
    .unwrap();
    d.steps.into_iter().zip(d.values).collect()
}

fn wal_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "wal") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

fn setup() -> (tempfile::TempDir, String, PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let uri = d.path().join("repo").to_string_lossy().into_owned();
    let wal = d.path().join("wal");
    (d, uri, wal)
}

/// Child side of `sigkill_mid_track`.
#[test]
fn track_child() {
    let (Ok(uri), Ok(wal)) = (
        std::env::var("KV0_TRACK_URI"),
        std::env::var("KV0_TRACK_WAL"),
    ) else {
        return;
    };
    let repo = open(&uri, Path::new(&wal));
    let run = repo
        .create_run(NewRun {
            run_id: Some("r1".into()),
            ..NewRun::default()
        })
        .unwrap();
    run.set_params(&json!({"lr": 0.1})).unwrap();
    let mut out = std::io::stdout().lock();
    for i in 0..i64::MAX {
        run.track("loss", i as f64, Track::default()).unwrap();
        writeln!(out, "{i}").unwrap();
        out.flush().unwrap();
    }
}

#[cfg(unix)]
#[test]
fn sigkill_mid_track() {
    let (_d, uri, wal) = setup();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "track_child",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env("KV0_TRACK_URI", &uri)
        .env("KV0_TRACK_WAL", &wal)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut acked = -1i64;
    for line in BufReader::new(child.stdout.take().unwrap()).lines() {
        if let Ok(n) = line.unwrap().trim().parse::<i64>() {
            acked = n;
            if n >= 3000 {
                break;
            }
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(acked >= 3000);

    let repo = open(&uri, &wal);
    let pts = points(&repo, "r1", "loss");
    assert!(
        pts.len() as i64 > acked,
        "{} points for {acked} acks",
        pts.len()
    );
    for (i, (step, value)) in pts.iter().enumerate().take(acked as usize + 1) {
        assert_eq!((*step, *value), (i as i64, i as f64));
    }
    let p = repo.clone();
    let params = block_on(async move { p.params("r1").await }).unwrap();
    assert_eq!(params["lr"], json!(0.1));
    let r = repo.clone();
    let runs = block_on(async move { r.runs().await }).unwrap();
    assert_eq!(runs.len(), 1, "WAL mark rows are not runs");
}

#[test]
fn torn_tail_and_corruption() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo
        .create_run(NewRun {
            run_id: Some("r1".into()),
            ..NewRun::default()
        })
        .unwrap();
    for i in 0..10 {
        run.track("m", i as f64, Track::default()).unwrap();
    }
    drop(run);
    repo.abandon();
    let seg = wal_files(&wal).pop().unwrap();
    let mut bytes = std::fs::read(&seg).unwrap();
    bytes.extend_from_slice(&[99, 0, 0, 0, 7]);
    std::fs::write(&seg, &bytes).unwrap();
    let repo = open(&uri, &wal);
    assert_eq!(points(&repo, "r1", "m").len(), 10);

    let (_d2, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo.create_run(NewRun::default()).unwrap();
    run.track("m", 1.0, Track::default()).unwrap();
    drop(run);
    repo.abandon();
    let seg = wal_files(&wal).pop().unwrap();
    let mut bytes = std::fs::read(&seg).unwrap();
    bytes[8 + 12 + 6] ^= 0xff;
    std::fs::write(&seg, &bytes).unwrap();
    let (u, o) = (uri.clone(), opts(&wal));
    let err = block_on(async move { Repo::open(&u, o).await }).unwrap_err();
    assert!(matches!(err, kv0::Error::Corruption(_)), "{err:?}");
}

/// A crash after the commits and before the WAL is truncated, optionally with every old version
/// cleaned up before the reopen.
fn crash_between_commit_and_truncate(cleanup: bool) {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo
        .create_run(NewRun {
            run_id: Some("r1".into()),
            ..NewRun::default()
        })
        .unwrap();
    run.set_param("lr", json!(0.1)).unwrap();
    for i in 0..100 {
        run.track("m", i as f64, Track::default()).unwrap();
    }
    let saved: Vec<(PathBuf, Vec<u8>)> = wal_files(&wal)
        .into_iter()
        .map(|p| {
            let b = std::fs::read(&p).unwrap();
            (p, b)
        })
        .collect();
    let r = repo.clone();
    block_on(async move { r.flush().await }).unwrap();
    // Newer writes the stale WAL must not undo.
    run.set_param("lr", json!(0.2)).unwrap();
    let r = repo.clone();
    block_on(async move { r.flush().await }).unwrap();
    if cleanup {
        let r = repo.clone();
        block_on(async move { r.optimize().await }).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let r = repo.clone();
        block_on(async move { r.cleanup_old_versions(std::time::Duration::ZERO).await }).unwrap();
    }
    drop(run);
    repo.abandon();
    for (p, b) in &saved {
        std::fs::write(p, b).unwrap();
    }

    let repo = open(&uri, &wal);
    assert_eq!(points(&repo, "r1", "m").len(), 100);
    let r = repo.clone();
    let rows = block_on(async move { r.metric_rows().await }).unwrap();
    assert_eq!(rows, 100 + 1, "100 points and one mark row, nothing twice");
    let r = repo.clone();
    let params = block_on(async move { r.params("r1").await }).unwrap();
    assert_eq!(params["lr"], json!(0.2));
}

#[test]
fn replay_after_commit_before_truncate_is_idempotent() {
    crash_between_commit_and_truncate(false);
}

#[test]
fn replay_is_idempotent_after_every_old_version_is_cleaned_up() {
    crash_between_commit_and_truncate(true);
}

#[test]
fn separate_writers() {
    let (_d, uri, wal) = setup();
    let a = open(&uri, &wal);
    let b = open(&uri, &wal);
    for (repo, id) in [(&a, "ra"), (&b, "rb")] {
        let run = repo
            .create_run(NewRun {
                run_id: Some(id.into()),
                ..NewRun::default()
            })
            .unwrap();
        run.track("m", 1.0, Track::default()).unwrap();
    }
    a.abandon();
    b.abandon();
    let c = open(&uri, &wal);
    assert_eq!(points(&c, "ra", "m").len(), 1);
    assert_eq!(points(&c, "rb", "m").len(), 1);
}

#[test]
fn automatic_step_past_the_largest_is_an_error() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo.create_run(NewRun::default()).unwrap();
    run.track(
        "m",
        1.0,
        Track {
            step: Some(i64::MAX),
            ..Track::default()
        },
    )
    .unwrap();
    let err = run.track("m", 2.0, Track::default()).unwrap_err();
    assert!(matches!(err, kv0::Error::InvalidArgument(_)), "{err:?}");
}

#[test]
fn optimize_keeps_one_mark_per_writer() {
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo
        .create_run(NewRun {
            run_id: Some("r1".into()),
            ..NewRun::default()
        })
        .unwrap();
    // The first maintenance after open prunes; the next four flushes each leave a mark.
    for i in 0..5 {
        run.track("m", i as f64, Track::default()).unwrap();
        flush_and_settle(&repo);
    }
    let r = repo.clone();
    assert_eq!(
        block_on(async move { r.metric_rows().await }).unwrap(),
        5 + 5
    );
    let r = repo.clone();
    block_on(async move { r.optimize().await }).unwrap();
    let r = repo.clone();
    assert_eq!(
        block_on(async move { r.metric_rows().await }).unwrap(),
        5 + 1
    );
    assert_eq!(points(&repo, "r1", "m").len(), 5);
}

fn open_with(uri: &str, o: RepoOptions) -> Repo {
    let u = uri.to_string();
    block_on(async move { Repo::open(&u, o).await }).unwrap()
}

fn flush_and_settle(repo: &Repo) {
    let r = repo.clone();
    block_on(async move {
        r.flush().await.unwrap();
        // Maintenance starts after the flush returns; let it run and finish.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        r.wait_for_maintenance().await;
    });
}

#[test]
fn automatic_compaction_bounds_fragments_and_marks() {
    let (_d, uri, wal) = setup();
    let repo = open_with(
        &uri,
        RepoOptions {
            max_fragments: 4,
            mark_prune_interval: std::time::Duration::ZERO,
            ..opts(&wal)
        },
    );
    let run = repo
        .create_run(NewRun {
            run_id: Some("r1".into()),
            ..NewRun::default()
        })
        .unwrap();
    for i in 0..20 {
        run.track("m", i as f64, Track::default()).unwrap();
        flush_and_settle(&repo);
    }
    let r = repo.clone();
    let rows = block_on(async move { r.metric_rows().await }).unwrap();
    // 20 points; marks pruned to one per writer by maintenance, plus the newest.
    assert!(rows <= 20 + 2, "{rows} rows");
    assert!(repo.background_compactions() > 0);
    assert_eq!(points(&repo, "r1", "m").len(), 20);
}

#[test]
fn settled_fragments_do_not_trigger_compaction() {
    // Review N5: fragments at the compaction target never merge further, so they must not
    // count toward max_fragments, or every flush would rescan and recompact.
    let (_d, uri, wal) = setup();
    let repo = open_with(
        &uri,
        RepoOptions {
            max_fragments: 2,
            compaction_target_rows: 100,
            ..opts(&wal)
        },
    );
    let run = repo.create_run(NewRun::default()).unwrap();
    let metrics_version = || {
        let r = repo.clone();
        block_on(async move { r.latest_versions().await })
            .unwrap()
            .3
    };
    let before = metrics_version();
    for round in 0..10 {
        for i in 0..150 {
            run.track(
                "m",
                i as f64,
                Track {
                    step: Some(round * 150 + i),
                    ..Track::default()
                },
            )
            .unwrap();
        }
        flush_and_settle(&repo);
    }
    // Every metrics fragment holds 150 points (at or above the target of 100): settled, so the
    // metrics table saw ten appends and no compaction. (The one-row statistics fragments are
    // small and do get merged.)
    assert_eq!(metrics_version(), before + 10);
    let merged = repo.background_compactions();
    // A run of small fragments is still merged.
    for i in 0..5 {
        run.track("small", i as f64, Track::default()).unwrap();
        flush_and_settle(&repo);
    }
    assert!(repo.background_compactions() > merged);
}

#[test]
fn a_panicking_maintenance_does_not_stop_later_maintenance() {
    // Review N8: a panic must clear the in-progress flag, or no maintenance runs again.
    let (_d, uri, wal) = setup();
    let repo = open_with(
        &uri,
        RepoOptions {
            max_fragments: 2,
            ..opts(&wal)
        },
    );
    let run = repo.create_run(NewRun::default()).unwrap();
    repo.inject_maintenance_panic();
    run.track("m", 0.0, Track::default()).unwrap();
    // Returns only because the panic cleared the flag.
    flush_and_settle(&repo);
    assert_eq!(repo.background_compactions(), 0);
    for i in 1..6 {
        run.track("m", i as f64, Track::default()).unwrap();
        flush_and_settle(&repo);
    }
    assert!(repo.background_compactions() > 0);
}

#[test]
fn marks_are_pruned_soon_after_open() {
    // Review N9: the prune interval starts unexpired, so a process that never runs for an
    // hour still prunes the marks earlier sessions left behind.
    let (_d, uri, wal) = setup();
    let o = RepoOptions {
        max_fragments: 0,
        ..opts(&wal)
    };
    let repo = open_with(&uri, o.clone());
    let run = repo
        .create_run(NewRun {
            run_id: Some("r1".into()),
            ..NewRun::default()
        })
        .unwrap();
    for i in 0..10 {
        run.track("m", i as f64, Track::default()).unwrap();
        flush_and_settle(&repo);
    }
    let r = repo.clone();
    let before = block_on(async move { r.metric_rows().await }).unwrap();
    // Only the first maintenance pruned; later flushes each left a mark.
    assert!(before >= 10 + 8, "{before} rows");
    drop(run);
    let r = repo.clone();
    block_on(async move { r.close().await }).unwrap();

    let repo = open_with(&uri, o);
    let r = repo.clone();
    let run = block_on(async move { r.resume_run("r1").await }).unwrap();
    run.track(
        "m",
        10.0,
        Track {
            step: Some(10),
            ..Track::default()
        },
    )
    .unwrap();
    flush_and_settle(&repo);
    let r = repo.clone();
    let after = block_on(async move { r.metric_rows().await }).unwrap();
    assert!(after <= 11 + 2, "{after} rows");
    assert_eq!(points(&repo, "r1", "m").len(), 11);
}

#[test]
fn close_waits_for_running_maintenance() {
    let (_d, uri, wal) = setup();
    let repo = open_with(
        &uri,
        RepoOptions {
            max_fragments: 4,
            ..opts(&wal)
        },
    );
    let run = repo.create_run(NewRun::default()).unwrap();
    for i in 0..4 {
        run.track("m", i as f64, Track::default()).unwrap();
        flush_and_settle(&repo);
    }
    assert_eq!(repo.background_compactions(), 0);
    run.track("m", 4.0, Track::default()).unwrap();
    // The flush in close makes a fifth small fragment in metrics (and in the statistics
    // table), which starts a compaction; close returns only after it.
    let r = repo.clone();
    block_on(async move { r.close().await }).unwrap();
    assert!(repo.background_compactions() >= 1);
}

#[test]
fn track_many_is_replayed_after_a_crash() {
    use kv0::tracking::TrackPoint;
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo
        .create_run(NewRun {
            run_id: Some("r1".into()),
            ..NewRun::default()
        })
        .unwrap();
    let batch: Vec<TrackPoint> = (0..1000)
        .map(|i| TrackPoint {
            value: i as f64,
            ..TrackPoint::default()
        })
        .collect();
    run.track_many("m", None, &batch).unwrap();
    // The second batch continues the numbering and rewrites step 3.
    run.track_many(
        "m",
        None,
        &[
            TrackPoint {
                value: -1.0,
                ..TrackPoint::default()
            },
            TrackPoint {
                value: 3.5,
                step: Some(3),
                ..TrackPoint::default()
            },
        ],
    )
    .unwrap();
    drop(run);
    repo.abandon();

    let repo = open(&uri, &wal);
    let got = points(&repo, "r1", "m");
    assert_eq!(got.len(), 1001);
    assert_eq!(got[3], (3, 3.5));
    assert_eq!(got[1000], (1000, -1.0));
}

#[test]
fn a_failed_track_many_buffers_nothing() {
    use kv0::tracking::TrackPoint;
    let (_d, uri, wal) = setup();
    let repo = open(&uri, &wal);
    let run = repo
        .create_run(NewRun {
            run_id: Some("r1".into()),
            ..NewRun::default()
        })
        .unwrap();
    run.track("m", 0.0, Track::default()).unwrap();
    // The third point has no step after i64::MAX: the whole batch is refused.
    let err = run
        .track_many(
            "m",
            None,
            &[
                TrackPoint {
                    value: 1.0,
                    ..TrackPoint::default()
                },
                TrackPoint {
                    value: 2.0,
                    step: Some(i64::MAX),
                    ..TrackPoint::default()
                },
                TrackPoint {
                    value: 3.0,
                    ..TrackPoint::default()
                },
            ],
        )
        .unwrap_err();
    assert!(matches!(err, kv0::Error::InvalidArgument(_)), "{err:?}");
    let err = run
        .track_many("m", Some(json!([1])), &[TrackPoint::default()])
        .unwrap_err();
    assert!(matches!(err, kv0::Error::InvalidArgument(_)), "{err:?}");
    // Numbering continues from the last point that was stored.
    run.track("m", 4.0, Track::default()).unwrap();
    let r = repo.clone();
    block_on(async move { r.flush().await }).unwrap();
    assert_eq!(points(&repo, "r1", "m"), vec![(0, 0.0), (1, 4.0)]);
}
