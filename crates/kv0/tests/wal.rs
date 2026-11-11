//! Write-ahead log: crash recovery, torn tails, corruption, idempotent replay, several writers.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use kv0::kv::{Db, Options, WalSync, WriteBatch};

fn opts(wal_dir: &Path) -> Options {
    Options {
        flush_interval: None,
        write_buffer_size: usize::MAX,
        wal_dir: Some(wal_dir.to_path_buf()),
        ..Options::default()
    }
}

fn setup() -> (tempfile::TempDir, String, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("db").to_string_lossy().into_owned();
    let wal = dir.path().join("wal");
    (dir, db, wal)
}

/// All `.wal` files under `root`.
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

fn count(db: &Db) -> usize {
    let mut it = db.iter();
    it.seek_to_first();
    it.count()
}

/// Child side of `sigkill_mid_write`: writes keys and acknowledges each on stdout until killed.
#[test]
fn crash_child() {
    let (Ok(db), Ok(wal)) = (
        std::env::var("KV0_CRASH_DB"),
        std::env::var("KV0_CRASH_WAL"),
    ) else {
        return;
    };
    let db = Db::open(&db, opts(Path::new(&wal))).unwrap();
    let mut out = std::io::stdout().lock();
    for i in 0u32..u32::MAX {
        if i % 10 == 0 {
            let mut b = WriteBatch::new();
            b.put(format!("b{i:08}"), b"batch")
                .put(format!("c{i:08}"), b"batch");
            db.write(b).unwrap();
        } else {
            db.put(format!("k{i:08}").as_bytes(), b"v").unwrap();
        }
        writeln!(out, "{i}").unwrap();
        out.flush().unwrap();
    }
}

#[cfg(unix)]
#[test]
fn sigkill_mid_write() {
    let (_d, db, wal) = setup();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "crash_child",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env("KV0_CRASH_DB", &db)
        .env("KV0_CRASH_WAL", &wal)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut acked = None;
    for line in BufReader::new(child.stdout.take().unwrap()).lines() {
        let Ok(n) = line.unwrap().trim().parse::<u32>() else {
            continue;
        };
        acked = Some(n);
        if n >= 2000 {
            break;
        }
    }
    // SIGKILL: no destructors, no flush.
    child.kill().unwrap();
    child.wait().unwrap();
    let acked = acked.expect("the child acknowledged writes");

    let db = Db::open(&db, opts(&wal)).unwrap();
    for i in 0..=acked {
        if i % 10 == 0 {
            assert!(
                db.get(format!("b{i:08}").as_bytes()).unwrap().is_some(),
                "batch {i}"
            );
            assert!(
                db.get(format!("c{i:08}").as_bytes()).unwrap().is_some(),
                "batch {i}"
            );
        } else {
            assert!(
                db.get(format!("k{i:08}").as_bytes()).unwrap().is_some(),
                "put {i}"
            );
        }
    }
    // Replayed writes were committed to Lance on open.
    assert_eq!(db.stats().pending_ops, 0);
    db.close().unwrap();
    assert!(wal_files(&wal).is_empty(), "a clean close removes the WAL");
}

#[test]
fn torn_tail_is_dropped_and_middle_corruption_is_an_error() {
    let (_d, db_path, wal) = setup();
    let db = Db::open(&db_path, opts(&wal)).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.abandon();
    let seg = wal_files(&wal).pop().unwrap();
    let clean = std::fs::read(&seg).unwrap();

    // A record cut off mid-write.
    let mut torn = clean.clone();
    torn.extend_from_slice(&[40, 0, 0, 0, 1, 2, 3]);
    std::fs::write(&seg, &torn).unwrap();
    let db = Db::open(&db_path, opts(&wal)).unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
    db.close().unwrap();

    // A flipped byte in the first of two records.
    let (_d2, db_path, wal) = setup();
    let db = Db::open(&db_path, opts(&wal)).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.abandon();
    let seg = wal_files(&wal).pop().unwrap();
    let mut bad = std::fs::read(&seg).unwrap();
    bad[8 + 12 + 6] ^= 0xff;
    std::fs::write(&seg, &bad).unwrap();
    let err = Db::open(&db_path, opts(&wal)).unwrap_err();
    assert!(matches!(err, kv0::Error::Corruption(_)), "{err:?}");
}

#[test]
fn replay_after_commit_before_truncate_is_idempotent() {
    let (_d, db_path, wal) = setup();
    let db = Db::open(&db_path, opts(&wal)).unwrap();
    for i in 0..100u32 {
        db.put(&i.to_be_bytes(), b"v1").unwrap();
    }
    db.delete_range(&0u32.to_be_bytes(), &10u32.to_be_bytes())
        .unwrap();
    // Keep the segments as they are before the commit deletes them.
    let saved: Vec<(PathBuf, Vec<u8>)> = wal_files(&wal)
        .into_iter()
        .map(|p| {
            let b = std::fs::read(&p).unwrap();
            (p, b)
        })
        .collect();
    db.flush().unwrap();
    let stored = db.stats().stored_rows;
    // A newer write that the stale WAL must not undo.
    db.put(&50u32.to_be_bytes(), b"v2").unwrap();
    db.flush().unwrap();
    db.abandon();
    // The crash: the commit happened, the WAL was not truncated.
    for (p, b) in &saved {
        std::fs::write(p, b).unwrap();
    }

    let db = Db::open(&db_path, opts(&wal)).unwrap();
    // The later flush added one put and its mark row; the replay added nothing.
    assert_eq!(
        db.stats().stored_rows,
        stored + 2,
        "nothing was appended twice"
    );
    assert_eq!(count(&db), 90);
    assert_eq!(
        db.get(&50u32.to_be_bytes()).unwrap().as_deref(),
        Some(&b"v2"[..])
    );
    assert_eq!(db.get(&5u32.to_be_bytes()).unwrap(), None);
}

#[test]
fn replay_is_idempotent_after_every_old_version_is_cleaned_up() {
    let (_d, db_path, wal) = setup();
    let db = Db::open(&db_path, opts(&wal)).unwrap();
    for i in 0..20u32 {
        db.put(&i.to_be_bytes(), b"v1").unwrap();
    }
    let saved: Vec<(PathBuf, Vec<u8>)> = wal_files(&wal)
        .into_iter()
        .map(|p| {
            let b = std::fs::read(&p).unwrap();
            (p, b)
        })
        .collect();
    db.flush().unwrap();
    // Later commits by this writer, a compaction, then cleanup of every old version: the commit
    // that covered the saved WAL is gone, only its mark row (carried by compaction) remains.
    db.put(&5u32.to_be_bytes(), b"v2").unwrap();
    db.delete(&6u32.to_be_bytes()).unwrap();
    db.compact().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    db.cleanup_old_versions(std::time::Duration::ZERO).unwrap();
    assert_eq!(db.versions().unwrap().len(), 1);
    let stored = db.stats().stored_rows;
    db.abandon();
    for (p, b) in &saved {
        std::fs::write(p, b).unwrap();
    }

    let db = Db::open(&db_path, opts(&wal)).unwrap();
    assert_eq!(db.stats().stored_rows, stored, "nothing was replayed");
    assert_eq!(
        db.get(&5u32.to_be_bytes()).unwrap().as_deref(),
        Some(&b"v2"[..])
    );
    assert_eq!(
        db.get(&6u32.to_be_bytes()).unwrap(),
        None,
        "the delete stands"
    );
    assert_eq!(count(&db), 19);
}

#[test]
fn disable_wal_writes_are_lost_in_a_crash_others_are_not() {
    use kv0::kv::WriteOptions;
    let (_d, db_path, wal) = setup();
    let db = Db::open(&db_path, opts(&wal)).unwrap();
    db.put(b"logged", b"1").unwrap();
    let mut b = WriteBatch::new();
    b.put(b"unlogged", b"2");
    db.write_opt(
        b,
        &WriteOptions {
            disable_wal: true,
            ..WriteOptions::default()
        },
    )
    .unwrap();
    assert_eq!(db.get(b"unlogged").unwrap().as_deref(), Some(&b"2"[..]));
    db.abandon();
    let db = Db::open(&db_path, opts(&wal)).unwrap();
    assert_eq!(db.get(b"logged").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(db.get(b"unlogged").unwrap(), None);

    // Flushed before the crash, an unlogged write is committed like any other.
    let mut b = WriteBatch::new();
    b.put(b"unlogged", b"3");
    db.write_opt(
        b,
        &WriteOptions {
            disable_wal: true,
            sync: true,
        },
    )
    .unwrap();
    db.abandon();
    let db = Db::open(&db_path, opts(&wal)).unwrap();
    assert_eq!(db.get(b"unlogged").unwrap().as_deref(), Some(&b"3"[..]));
}

#[test]
fn several_writers_have_separate_wals() {
    let (_d, db_path, wal) = setup();
    let a = Db::open(&db_path, opts(&wal)).unwrap();
    let b = Db::open(&db_path, opts(&wal)).unwrap();
    a.put(b"from-a", b"1").unwrap();
    b.put(b"from-b", b"2").unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"batch-b", b"3");
    b.write(batch).unwrap();
    // A live writer's WAL is not claimed by another open.
    let c = Db::open(&db_path, opts(&wal)).unwrap();
    assert_eq!(c.get(b"from-a").unwrap(), None);
    c.close().unwrap();
    a.abandon();
    b.abandon();

    let d = Db::open(&db_path, opts(&wal)).unwrap();
    assert_eq!(d.get(b"from-a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(d.get(b"from-b").unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(d.get(b"batch-b").unwrap().as_deref(), Some(&b"3"[..]));
    d.close().unwrap();
    assert!(wal_files(&wal).is_empty());
}

#[test]
fn read_only_handles_neither_create_nor_replay_a_wal() {
    let (_d, db_path, wal) = setup();
    let w = Db::open(&db_path, opts(&wal)).unwrap();
    w.put(b"k", b"v").unwrap();
    w.abandon();
    let before = wal_files(&wal);
    let ro = Db::open(
        &db_path,
        Options {
            read_only: true,
            ..opts(&wal)
        },
    )
    .unwrap();
    assert_eq!(ro.get(b"k").unwrap(), None);
    assert_eq!(wal_files(&wal), before);
    drop(ro);
    let w = Db::open(&db_path, opts(&wal)).unwrap();
    assert_eq!(w.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn every_sync_mode_recovers() {
    for mode in [WalSync::None, WalSync::Batch, WalSync::Always] {
        let (_d, db_path, wal) = setup();
        let o = Options {
            wal_sync: mode,
            ..opts(&wal)
        };
        let db = Db::open(&db_path, o.clone()).unwrap();
        for i in 0..50u32 {
            db.put(&i.to_be_bytes(), b"v").unwrap();
        }
        db.abandon();
        let db = Db::open(&db_path, o).unwrap();
        assert_eq!(count(&db), 50, "{mode:?}");
    }
}

#[test]
fn compaction_drops_expired_tombstones_from_memory() {
    let (_d, db_path, wal) = setup();
    let db = Db::open(
        &db_path,
        Options {
            tombstone_grace: std::time::Duration::ZERO,
            ..opts(&wal)
        },
    )
    .unwrap();
    for round in 0..3u32 {
        for i in 0..10_000u32 {
            let k = (round * 10_000 + i).to_be_bytes();
            db.put(&k, b"v").unwrap();
            db.delete(&k).unwrap();
        }
        db.compact().unwrap();
    }
    let s = db.stats();
    assert_eq!(s.live_keys, 0);
    assert_eq!(
        s.view_entries, 0,
        "tombstones past the grace period left the view"
    );
}

#[test]
fn wal_marks_expire_and_a_stale_wal_needs_consent() {
    let (_d, db_path, wal) = setup();
    let ttl = Options {
        mark_ttl: std::time::Duration::from_millis(50),
        ..opts(&wal)
    };
    for i in 0..20u32 {
        let db = Db::open(&db_path, ttl.clone()).unwrap();
        db.put(&i.to_be_bytes(), b"v").unwrap();
        db.close().unwrap();
    }
    // A crashed writer whose mark will expire.
    let crashed = Db::open(&db_path, ttl.clone()).unwrap();
    crashed.put(b"orphan", b"1").unwrap();
    crashed.flush().unwrap();
    crashed.put(b"orphan-2", b"2").unwrap();
    crashed.abandon();
    std::thread::sleep(std::time::Duration::from_millis(120));
    // Another machine's writer compacts: every expired mark is dropped.
    let other_wal = wal.with_extension("other");
    let db = Db::open(
        &db_path,
        Options {
            wal_dir: Some(other_wal),
            ..ttl.clone()
        },
    )
    .unwrap();
    db.compact().unwrap();
    assert_eq!(db.stats().stored_rows, 21, "20 keys and orphan; no marks");
    db.close().unwrap();

    let err = Db::open(&db_path, ttl.clone()).unwrap_err();
    assert!(
        matches!(err, kv0::Error::InvalidArgument(ref m) if m.contains("replay_stale_wal")),
        "{err:?}"
    );
    let db = Db::open(
        &db_path,
        Options {
            replay_stale_wal: true,
            ..ttl
        },
    )
    .unwrap();
    assert_eq!(db.get(b"orphan-2").unwrap().as_deref(), Some(&b"2"[..]));
}

#[test]
fn iterators_stop_after_close() {
    let (_d, db_path, wal) = setup();
    let db = Db::open(&db_path, opts(&wal)).unwrap();
    db.put(b"a", b"1").unwrap();
    let mut it = db.iter();
    db.close().unwrap();
    it.seek_to_first();
    assert!(it.current().is_none());
    it.seek_to_last();
    assert!(it.current().is_none());
}

#[test]
fn reopening_reuses_the_writer_id_so_marks_do_not_accumulate() {
    let (_d, db_path, wal) = setup();
    for i in 0..50u32 {
        let db = Db::open(&db_path, opts(&wal)).unwrap();
        db.put(b"k", &i.to_be_bytes()).unwrap();
        db.close().unwrap();
    }
    let db = Db::open(&db_path, opts(&wal)).unwrap();
    db.compact().unwrap();
    assert_eq!(db.stats().stored_rows, 2, "one key and one writer's mark");
}
