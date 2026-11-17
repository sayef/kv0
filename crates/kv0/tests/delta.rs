//! The Delta Lake engine of the key-value layer: concurrency, compaction, snapshots, lazy values
//! and WAL replay, beyond what the shared key-value suites cover.
//!
//! Every test here creates Delta databases (`KV0_TEST_BACKEND=delta` for this process). The
//! S3 variants run when `KV0_TEST_S3_ENDPOINT` is set (CI starts an S3-compatible server).
#![cfg(feature = "delta")]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use kv0::kv::{Db, Options};

fn delta() {
    // Every test of this binary sets the same value, so the races between them are harmless.
    std::env::set_var("KV0_TEST_BACKEND", "delta");
}

fn opts(wal: &Path) -> Options {
    Options {
        flush_interval: None,
        refresh_interval: None,
        write_buffer_size: usize::MAX,
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
    Some((format!("s3://{bucket}/delta-{nanos:x}"), opts))
}

fn all(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = db.iter();
    it.seek_to_first();
    it.map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
}

fn data_files(db: &str) -> Vec<PathBuf> {
    std::fs::read_dir(db)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect()
}

/// Child side of the multi-process tests: appends `KV0_CONC_BATCHES` batches of 10 keys, one
/// flush (one commit) each, under its own key prefix.
#[test]
fn conc_child() {
    let (Ok(db), Ok(wal), Ok(id), Ok(n)) = (
        std::env::var("KV0_CONC_DB"),
        std::env::var("KV0_CONC_WAL"),
        std::env::var("KV0_CONC_ID"),
        std::env::var("KV0_CONC_BATCHES"),
    ) else {
        return;
    };
    let storage: HashMap<String, String> = std::env::var("KV0_CONC_OPTS")
        .ok()
        .map(|s| serde_json::from_str(&s).unwrap())
        .unwrap_or_default();
    let db = Db::open(
        &db,
        Options {
            storage_options: storage,
            max_fragments: 0,
            ..opts(Path::new(&wal))
        },
    )
    .unwrap();
    for b in 0..n.parse::<u32>().unwrap() {
        for i in 0..10u32 {
            db.put(
                format!("{id}/{b:04}/{i}").as_bytes(),
                format!("{id}-{b}-{i}").as_bytes(),
            )
            .unwrap();
        }
        db.flush().unwrap();
    }
    db.close().unwrap();
}

/// Starts `procs` child processes appending `batches` commits each to `db`, and waits.
fn run_children(db: &str, storage: &HashMap<String, String>, wal: &Path, procs: u32, batches: u32) {
    let exe = std::env::current_exe().unwrap();
    let children: Vec<_> = (0..procs)
        .map(|p| {
            Command::new(&exe)
                .args(["--exact", "conc_child", "--nocapture", "--test-threads=1"])
                .env("KV0_TEST_BACKEND", "delta")
                .env("KV0_CONC_DB", db)
                .env("KV0_CONC_WAL", wal.join(format!("w{p}")))
                .env("KV0_CONC_ID", format!("p{p}"))
                .env("KV0_CONC_BATCHES", batches.to_string())
                .env("KV0_CONC_OPTS", serde_json::to_string(storage).unwrap())
                .spawn()
                .unwrap()
        })
        .collect();
    for mut c in children {
        assert!(c.wait().unwrap().success(), "a writer process failed");
    }
}

/// Every batch of every process is present exactly once, and the log holds one commit per
/// batch after the creating one.
fn check_children(
    db: &str,
    storage: &HashMap<String, String>,
    wal: &Path,
    procs: u32,
    batches: u32,
) {
    let d = Db::open(
        db,
        Options {
            read_only: true,
            storage_options: storage.clone(),
            snapshot_min_entries: usize::MAX,
            ..opts(wal)
        },
    )
    .unwrap();
    let got = all(&d);
    assert_eq!(got.len() as u32, procs * batches * 10);
    for p in 0..procs {
        for b in 0..batches {
            for i in 0..10 {
                let v = d.get(format!("p{p}/{b:04}/{i}").as_bytes()).unwrap();
                assert_eq!(v.as_deref(), Some(format!("p{p}-{b}-{i}").as_bytes()));
            }
        }
    }
    // No duplicates in storage either: one row per key.
    assert_eq!(d.stats().stored_rows as u32, procs * batches * 10);
    let versions: Vec<u64> = d.versions().unwrap().iter().map(|v| v.version).collect();
    assert_eq!(
        versions,
        (0..=u64::from(procs * batches)).collect::<Vec<_>>()
    );
}

#[test]
fn concurrent_appends_from_several_processes() {
    let (dir, db, wal) = setup();
    // Created first so the children race only on appends; creation races are tested below.
    Db::open(&db, opts(&wal)).unwrap().close().unwrap();
    run_children(&db, &HashMap::new(), &dir.path().join("wal"), 8, 25);
    check_children(&db, &HashMap::new(), &wal, 8, 25);
}

#[test]
fn concurrent_appends_from_several_processes_on_s3() {
    let Some((db, storage)) = s3() else {
        return;
    };
    delta();
    let dir = tempfile::tempdir().unwrap();
    let wal = dir.path().join("wal");
    Db::open(
        &db,
        Options {
            storage_options: storage.clone(),
            ..opts(&wal)
        },
    )
    .unwrap()
    .close()
    .unwrap();
    run_children(&db, &storage, &wal, 4, 10);
    check_children(&db, &storage, &wal, 4, 10);
}

#[test]
fn concurrent_creators_make_one_table() {
    let (_dir, db, wal) = setup();
    let handles: Vec<_> = (0..16)
        .map(|i| {
            let (db, wal) = (db.clone(), wal.join(format!("w{i}")));
            std::thread::spawn(move || {
                let d = Db::open(&db, opts(&wal)).unwrap();
                d.put(format!("k{i}").as_bytes(), b"v").unwrap();
                d.close().unwrap();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    // One creation: version 1 is somebody's append, not a second protocol and metadata.
    let v1 = std::fs::read_to_string(Path::new(&db).join("_delta_log/00000000000000000001.json"))
        .unwrap();
    assert!(!v1.contains("\"metaData\"") && !v1.contains("\"protocol\""));
    let d = Db::open(
        &db,
        Options {
            read_only: true,
            ..opts(&wal)
        },
    )
    .unwrap();
    assert_eq!(all(&d).len(), 16);
}

/// Appenders and compactors on one database at once: compactions commit concurrently with
/// appends or lose cleanly to each other, and no row is lost or stored twice.
#[test]
fn compaction_racing_appends_loses_and_duplicates_nothing() {
    let (_dir, db, wal) = setup();
    Db::open(&db, opts(&wal)).unwrap().close().unwrap();
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let (db, wal) = (db.clone(), wal.join(format!("a{w}")));
            std::thread::spawn(move || {
                let d = Db::open(
                    &db,
                    Options {
                        lazy_value_threshold: 64,
                        max_fragments: 0,
                        ..opts(&wal)
                    },
                )
                .unwrap();
                for b in 0..30u32 {
                    for i in 0..20u32 {
                        let v = if i.is_multiple_of(4) {
                            vec![w as u8; 200]
                        } else {
                            vec![w as u8; 8]
                        };
                        d.put(format!("w{w}/{b:03}/{i:02}").as_bytes(), &v).unwrap();
                    }
                    d.flush().unwrap();
                }
                d.close().unwrap();
            })
        })
        .collect();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let compactors: Vec<_> = (0..2)
        .map(|c| {
            let (db, wal, stop) = (db.clone(), wal.join(format!("c{c}")), stop.clone());
            std::thread::spawn(move || {
                let d = Db::open(
                    &db,
                    Options {
                        lazy_value_threshold: 64,
                        max_fragments: 0,
                        ..opts(&wal)
                    },
                )
                .unwrap();
                let (mut done, mut lost) = (0, 0);
                while !stop.load(std::sync::atomic::Ordering::Acquire) {
                    d.refresh().unwrap();
                    match d.compact() {
                        Ok(()) => done += 1,
                        Err(kv0::Error::Conflict(_)) => lost += 1,
                        Err(e) => panic!("compaction failed: {e}"),
                    }
                }
                d.close().unwrap();
                (done, lost)
            })
        })
        .collect();
    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Release);
    let mut compactions = 0;
    for c in compactors {
        compactions += c.join().unwrap().0;
    }
    assert!(compactions > 0);
    let d = Db::open(
        &db,
        Options {
            read_only: true,
            lazy_value_threshold: 64,
            snapshot_min_entries: usize::MAX,
            ..opts(&wal)
        },
    )
    .unwrap();
    let got = all(&d);
    assert_eq!(got.len(), 4 * 30 * 20);
    for (k, v) in &got {
        let k = String::from_utf8(k.clone()).unwrap();
        let w: u8 = k[1..2].parse().unwrap();
        let i: u32 = k[k.len() - 2..].parse().unwrap();
        let len = if i.is_multiple_of(4) { 200 } else { 8 };
        assert_eq!(v, &vec![w; len], "{k}");
    }
    assert_eq!(d.stats().stored_rows, 4 * 30 * 20, "rows stored twice");
}

/// A reopen starts from the view snapshot, with lazy values readable through the restored file
/// table, and then follows another writer's commits.
#[test]
fn reopen_from_snapshot_then_refresh() {
    let (_dir, db, wal) = setup();
    let o = Options {
        lazy_value_threshold: 32,
        snapshot_min_entries: 1,
        ..opts(&wal)
    };
    let a = Db::open(&db, o.clone()).unwrap();
    for f in 0..3u32 {
        for i in 0..50u32 {
            a.put(format!("k{f}-{i:02}").as_bytes(), &[f as u8; 100])
                .unwrap();
        }
        a.flush().unwrap();
    }
    a.close().unwrap();

    let b = Db::open(&db, o.clone()).unwrap();
    assert!(b.opened_from_snapshot());
    assert!(b.stored_values() > 0);
    assert_eq!(b.get(b"k1-07").unwrap().unwrap(), vec![1u8; 100]);

    let c = Db::open(
        &db,
        Options {
            wal_dir: Some(wal.join("other")),
            snapshot_min_entries: usize::MAX,
            ..o.clone()
        },
    )
    .unwrap();
    c.put(b"new", &[9u8; 100]).unwrap();
    c.delete(b"k0-00").unwrap();
    c.close().unwrap();

    assert!(b.refresh().unwrap());
    assert_eq!(b.get(b"new").unwrap().unwrap(), vec![9u8; 100]);
    assert_eq!(b.get(b"k0-00").unwrap(), None);
    assert_eq!(b.get(b"k2-49").unwrap().unwrap(), vec![2u8; 100]);
    assert_eq!(all(&b).len(), 150);
    b.close().unwrap();
}

/// Large values stay readable after the handle's own compaction, after another writer's
/// compaction and cleanup, and after a reopen.
#[test]
fn lazy_values_after_compaction() {
    let (_dir, db, wal) = setup();
    let o = Options {
        lazy_value_threshold: 32,
        value_cache_size: 0,
        snapshot_min_entries: usize::MAX,
        ..opts(&wal)
    };
    let a = Db::open(&db, o.clone()).unwrap();
    for f in 0..4u32 {
        for i in 0..25u32 {
            a.put(
                format!("k{i:02}").as_bytes(),
                &vec![(f * 25 + i) as u8; 4096],
            )
            .unwrap();
        }
        a.flush().unwrap();
    }
    let files_before = data_files(&db).len();
    a.compact().unwrap();
    assert!(a.stored_values() > 0);
    for i in 0..25u32 {
        assert_eq!(
            a.get(format!("k{i:02}").as_bytes()).unwrap().unwrap(),
            vec![(75 + i) as u8; 4096]
        );
    }
    let reader = Db::open(
        &db,
        Options {
            read_only: true,
            ..o.clone()
        },
    )
    .unwrap();
    assert_eq!(reader.get(b"k03").unwrap().unwrap(), vec![78u8; 4096]);

    // Another writer rewrites everything and removes the files the reader's addresses name.
    a.put(b"k00", &[1u8; 4096]).unwrap();
    a.compact().unwrap();
    assert!(a.cleanup_old_versions(Duration::ZERO).unwrap() > 0);
    assert!(data_files(&db).len() < files_before + 2);
    assert_eq!(reader.get(b"k04").unwrap().unwrap(), vec![79u8; 4096]);
    assert_eq!(reader.get(b"k00").unwrap().unwrap(), vec![1u8; 4096]);
    a.close().unwrap();

    let again = Db::open(&db, o).unwrap();
    assert_eq!(again.get(b"k24").unwrap().unwrap(), vec![99u8; 4096]);
    assert_eq!(all(&again).len(), 25);
}

/// A crash after a commit but before the WAL learned of it: the replay finds the writer's mark
/// in the log (a Delta `txn` action) and stores nothing twice.
#[test]
fn wal_replay_after_a_crash_skips_what_was_committed() {
    let (dir, db, wal) = setup();
    let a = Db::open(&db, opts(&wal)).unwrap();
    for i in 0..10u32 {
        a.put(format!("c{i}").as_bytes(), b"committed").unwrap();
    }
    // The WAL as it was before the commit.
    let saved = dir.path().join("wal-saved");
    copy_dir(&wal, &saved);
    a.flush().unwrap();
    for i in 0..5u32 {
        a.put(format!("u{i}").as_bytes(), b"uncommitted").unwrap();
    }
    a.abandon();
    // Put the pre-commit segments back next to the uncommitted ones.
    copy_dir(&saved, &wal);

    let b = Db::open(&db, opts(&wal)).unwrap();
    assert_eq!(all(&b).len(), 15);
    b.flush().unwrap();
    assert_eq!(b.stats().stored_rows, 15, "committed writes replayed twice");
    b.close().unwrap();
    let c = Db::open(&db, opts(&wal)).unwrap();
    assert_eq!(all(&c).len(), 15);
    assert_eq!(c.stats().stored_rows, 15);
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let p = e.path();
        let t = to.join(e.file_name());
        if p.is_dir() {
            copy_dir(&p, &t);
        } else if !t.exists() {
            std::fs::copy(&p, &t).unwrap();
        }
    }
}

/// `memory://` stays private to one handle, as with Lance: each handle builds its own store.
#[test]
fn memory_locations_are_private_to_a_handle() {
    delta();
    let a = Db::open("memory://delta-private", Options::default()).unwrap();
    a.put(b"k", b"v").unwrap();
    a.flush().unwrap();
    let b = Db::open("memory://delta-private", Options::default()).unwrap();
    assert_eq!(b.get(b"k").unwrap(), None);
    assert_eq!(a.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(a.versions().unwrap().len(), 2);
}

/// A Delta database stays Delta, and a Lance one Lance, whatever a new location would get.
#[test]
fn an_existing_location_keeps_its_engine() {
    let (_dir, db, wal) = setup();
    let d = Db::open(&db, opts(&wal)).unwrap();
    d.put(b"k", b"v").unwrap();
    d.close().unwrap();
    assert!(Path::new(&db).join("_delta_log").is_dir());
    let names: HashSet<String> = std::fs::read_dir(&db)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(!names.contains("_versions"), "{names:?}");
}
