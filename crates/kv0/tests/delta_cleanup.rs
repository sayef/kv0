//! Tags, version cleanup and purge on the Delta Lake engine, beyond what the shared suites
//! (`kv.rs`, `tag_paths.rs`, `purge.rs`, ...) cover on both engines: tagged versions loading
//! from their checkpoint once older commit files are gone, the orphan grace period, erasure of
//! files whose removal the log no longer remembers, and the automatic cleanup cadence.
#![cfg(feature = "delta")]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use kv0::kv::{Db, Options};
use kv0::rt::block_on;
use kv0::tracking::{NewRun, Repo, RepoOptions, Track};

const MARKER: &str = "erase-me-5c0d1e";

fn delta() {
    // Every test of this binary sets the same value, so the races between them are harmless.
    std::env::set_var("KV0_TEST_BACKEND", "delta");
}

fn opts(wal: &Path) -> Options {
    Options {
        flush_interval: None,
        refresh_interval: None,
        write_buffer_size: usize::MAX,
        max_fragments: 0,
        wal_dir: Some(wal.to_path_buf()),
        ..Options::default()
    }
}

fn setup() -> (tempfile::TempDir, String, PathBuf) {
    delta();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db").to_string_lossy().into_owned();
    let wal = dir.path().join("wal");
    (dir, db, wal)
}

/// Names of the files in `<table>/_delta_log` ending in `suffix`.
fn log_files(table: &str, suffix: &str) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(Path::new(table).join("_delta_log"))
        .unwrap()
        .flatten()
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n.ends_with(suffix))
        .collect();
    out.sort();
    out
}

fn data_files(table: &str) -> Vec<PathBuf> {
    std::fs::read_dir(table)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect()
}

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

fn put_flush(db: &Db, key: &str, value: &str) {
    db.put(key.as_bytes(), value.as_bytes()).unwrap();
    db.flush().unwrap();
}

/// Polls `done` for up to 30 s.
fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    let start = Instant::now();
    while !done() {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "timed out: {what}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_tagged_version_loads_from_its_checkpoint_after_log_cleanup() {
    let (_d, path, wal) = setup();
    let db = Db::open(&path, opts(&wal)).unwrap();
    for i in 0..4 {
        put_flush(&db, &format!("k{i}"), "a");
    }
    let tagged = db.version();
    db.create_tag("release", None).unwrap();
    for i in 0..4 {
        put_flush(&db, &format!("k{i}"), "b");
    }
    db.delete(b"k0").unwrap();
    db.flush().unwrap();
    db.compact().unwrap();
    let latest = db.version();
    db.cleanup_old_versions(Duration::ZERO).unwrap();

    // The log keeps the latest commit and checkpoints at the latest and tagged versions: a
    // gap below the latest checkpoint, and nothing but a checkpoint for the tagged version.
    let commits = log_files(&path, ".json");
    assert_eq!(commits, vec![format!("{latest:020}.json")], "{commits:?}");
    let checkpoints = log_files(&path, ".checkpoint.parquet");
    assert_eq!(
        checkpoints,
        vec![
            format!("{tagged:020}.checkpoint.parquet"),
            format!("{latest:020}.checkpoint.parquet")
        ]
    );
    let versions: Vec<u64> = db.versions().unwrap().iter().map(|v| v.version).collect();
    assert_eq!(versions, vec![tagged, latest]);

    // Checkout by tag, and a fresh open of the latest version over the gap.
    let old = Db::open(
        &path,
        Options {
            version: Some(db.tag_version("release").unwrap()),
            ..opts(&wal)
        },
    )
    .unwrap();
    for i in 0..4 {
        assert_eq!(
            old.get(format!("k{i}").as_bytes()).unwrap().as_deref(),
            Some(&b"a"[..])
        );
    }
    drop(old);
    db.close().unwrap();
    let db = Db::open(&path, opts(&wal.join("again"))).unwrap();
    assert_eq!(db.version(), latest);
    assert_eq!(db.get(b"k0").unwrap(), None);
    assert_eq!(db.get(b"k1").unwrap().as_deref(), Some(&b"b"[..]));
    put_flush(&db, "k9", "c");
    assert_eq!(db.version(), latest + 1);

    // Without the tag, the next cleanup removes the tagged version too.
    db.delete_tag("release").unwrap();
    db.cleanup_old_versions(Duration::ZERO).unwrap();
    assert!(Db::open(
        &path,
        Options {
            version: Some(tagged),
            ..opts(&wal)
        }
    )
    .is_err());
    let versions: Vec<u64> = db.versions().unwrap().iter().map(|v| v.version).collect();
    assert_eq!(versions, vec![latest + 1]);
}

#[test]
fn tags_are_sidecar_files_listed_and_deleted() {
    let (_d, path, wal) = setup();
    let db = Db::open(&path, opts(&wal)).unwrap();
    put_flush(&db, "a", "1");
    let v1 = db.version();
    db.create_tag("first one", None).unwrap();
    put_flush(&db, "b", "2");
    db.create_tag("second:ü", None).unwrap();
    db.create_tag("old", Some(v1)).unwrap();
    let e = db.create_tag("old", None).unwrap_err();
    assert!(matches!(e, kv0::Error::InvalidArgument(_)), "{e}");
    assert!(db.create_tag("future", Some(999)).is_err());
    assert_eq!(
        db.tags().unwrap(),
        vec![
            ("first one".to_string(), v1),
            ("old".to_string(), v1),
            ("second:ü".to_string(), v1 + 1)
        ]
    );
    let mut files: Vec<String> = std::fs::read_dir(Path::new(&path).join("_kv0/tags"))
        .unwrap()
        .flatten()
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    files.sort();
    assert_eq!(files.len(), 3, "{files:?}");
    assert!(files
        .iter()
        .all(|f| f.ends_with(".json") && !f.contains('%')));
    db.delete_tag("old").unwrap();
    assert!(matches!(
        db.delete_tag("old").unwrap_err(),
        kv0::Error::NotFound(_)
    ));
    assert!(matches!(
        db.tag_version("old").unwrap_err(),
        kv0::Error::NotFound(_)
    ));
    assert_eq!(db.tags().unwrap().len(), 2);
}

/// Copies a data file to an unreferenced name, as a writer that crashed between writing its
/// file and committing it leaves behind; optionally back-dated.
fn orphan(table: &str, age: Duration) -> PathBuf {
    let src = &data_files(table)[0];
    let dst =
        Path::new(table).join("part-00000-0rphan00-0000-0000-0000-000000000000-c000.zstd.parquet");
    std::fs::copy(src, &dst).unwrap();
    let f = std::fs::File::options().write(true).open(&dst).unwrap();
    f.set_modified(SystemTime::now() - age).unwrap();
    dst
}

#[test]
fn unreferenced_files_wait_for_the_orphan_grace_period() {
    let (_d, path, wal) = setup();
    let db = Db::open(&path, opts(&wal)).unwrap();
    put_flush(&db, "a", "1");
    let young = orphan(&path, Duration::from_secs(60));
    db.cleanup_old_versions(Duration::ZERO).unwrap();
    db.purge(false).unwrap();
    assert!(young.exists(), "younger than the default grace (1 h)");
    std::fs::remove_file(&young).unwrap();

    let old = orphan(&path, Duration::from_secs(7200));
    db.cleanup_old_versions(Duration::ZERO).unwrap();
    assert!(!old.exists(), "older than the grace");

    // A shorter grace, from the options.
    db.close().unwrap();
    let db = Db::open(
        &path,
        Options {
            orphan_grace: Duration::from_secs(10),
            ..opts(&wal)
        },
    )
    .unwrap();
    let mid = orphan(&path, Duration::from_secs(60));
    db.cleanup_old_versions(Duration::ZERO).unwrap();
    assert!(!mid.exists());
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
}

#[test]
fn purge_erases_files_whose_removal_the_snapshot_no_longer_lists() {
    // With a short retention, the removal of a compacted file expires from the checkpoint the
    // snapshot loads, so delta-rs's vacuum no longer lists the file, and it is younger than the
    // orphan grace. Purge still finds it in the commit that added it.
    let (_d, path, wal) = setup();
    let db = Db::open(
        &path,
        Options {
            version_retention: Some(Duration::from_secs(1)),
            ..opts(&wal)
        },
    )
    .unwrap();
    put_flush(&db, "secret", MARKER);
    put_flush(&db, "keep", "live");
    db.delete(b"secret").unwrap();
    db.flush().unwrap();
    db.compact().unwrap();
    std::thread::sleep(Duration::from_millis(1500));
    // A checkpoint at the latest version, written without the expired removals.
    db.create_tag("checkpoint", None).unwrap();
    db.delete_tag("checkpoint").unwrap();
    assert!(contains(Path::new(&path), MARKER.as_bytes()));
    let report = db.purge(false).unwrap();
    assert!(report.bytes_removed > 0, "{report:?}");
    assert!(
        !contains(Path::new(&path), MARKER.as_bytes()),
        "no file holds the deleted value"
    );
    assert_eq!(db.get(b"keep").unwrap().as_deref(), Some(&b"live"[..]));
    assert_eq!(db.get(b"secret").unwrap(), None);
}

#[test]
fn writing_handles_clean_up_old_versions_every_twenty_commits() {
    let (_d, path, wal) = setup();
    let keep_all = Db::open(
        &path,
        Options {
            version_retention: None,
            ..opts(&wal.join("all"))
        },
    )
    .unwrap();
    for i in 0..25 {
        put_flush(&keep_all, &format!("k{i}"), "v");
    }
    assert_eq!(keep_all.versions().unwrap().len(), 26, "nothing removed");
    keep_all.close().unwrap();

    let db = Db::open(
        &path,
        Options {
            version_retention: Some(Duration::ZERO),
            ..opts(&wal)
        },
    )
    .unwrap();
    for i in 0..19 {
        put_flush(&db, &format!("j{i}"), "v");
    }
    assert!(db.versions().unwrap().len() > 40, "not due yet");
    put_flush(&db, "j19", "v");
    eventually("the background cleanup", || {
        db.versions().unwrap().len() <= 2
    });
    assert_eq!(db.get(b"k0").unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(db.get(b"j19").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn tracking_maintenance_cleans_up_old_versions_every_twenty_flushes() {
    delta();
    let d = tempfile::tempdir().unwrap();
    let uri = d.path().join("repo").to_string_lossy().into_owned();
    let o = RepoOptions {
        flush_interval: None,
        version_retention: Some(Duration::ZERO),
        max_fragments: 0,
        wal_dir: Some(d.path().join("wal")),
        ..RepoOptions::default()
    };
    let u = uri.clone();
    let repo = block_on(async move { Repo::open(&u, o).await }).unwrap();
    let run = repo
        .create_run(NewRun {
            run_id: Some("r".into()),
            ..NewRun::default()
        })
        .unwrap();
    let metrics = format!("{uri}/metrics");
    for i in 0..19 {
        run.track("loss", i as f64, Track::default()).unwrap();
        let r = repo.clone();
        block_on(async move { r.flush().await }).unwrap();
    }
    let r = repo.clone();
    block_on(async move { r.wait_for_maintenance().await });
    assert!(log_files(&metrics, ".json").len() >= 19, "not due yet");
    run.track("loss", 19.0, Track::default()).unwrap();
    let r = repo.clone();
    block_on(async move {
        r.flush().await?;
        r.wait_for_maintenance().await;
        Ok::<_, kv0::Error>(())
    })
    .unwrap();
    assert_eq!(
        log_files(&metrics, ".json").len(),
        1,
        "only the latest commit"
    );
    let r = repo.clone();
    let points = block_on(async move {
        r.read_metric("r", "loss", &serde_json::json!({}), Default::default())
            .await
    })
    .unwrap();
    assert_eq!(points.len(), 20);
}
