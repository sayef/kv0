//! A reopen starts from the view snapshot saved on close and reads only newer fragments.

use std::time::Duration;

use kv0::kv::{Db, Options};

fn opts(wal: &std::path::Path) -> Options {
    Options {
        flush_interval: None,
        refresh_interval: None,
        snapshot_min_entries: 10,
        lazy_value_threshold: 16,
        wal_dir: Some(wal.to_path_buf()),
        ..Options::default()
    }
}

fn setup() -> (tempfile::TempDir, String, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let uri = d.path().join("db").to_string_lossy().into_owned();
    let wal = d.path().join("wal");
    (d, uri, wal)
}

fn contents(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = db.iter();
    it.seek_to_first();
    it.map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
}

fn fill(db: &Db, from: u32, to: u32) {
    for i in from..to {
        let v = if i % 2 == 0 {
            format!("small{i}")
        } else {
            format!("large-value-{i}-").repeat(4)
        };
        db.put(format!("k{i:04}").as_bytes(), v.as_bytes()).unwrap();
    }
}

fn snap_file(wal: &std::path::Path) -> std::path::PathBuf {
    let dir = std::fs::read_dir(wal)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    dir.join("view.snap")
}

#[test]
fn reopen_loads_the_snapshot_and_catches_up() {
    let (_d, uri, wal) = setup();
    let db = Db::open(&uri, opts(&wal)).unwrap();
    assert!(!db.opened_from_snapshot());
    fill(&db, 0, 300);
    db.delete_range(b"k0010", b"k0020").unwrap();
    db.close().unwrap();
    let want = {
        let db = Db::open(&uri, opts(&wal)).unwrap();
        assert!(db.opened_from_snapshot());
        let c = contents(&db);
        assert_eq!(c.len(), 290);
        assert_eq!(db.stored_values(), 145);
        // Commits after the snapshot: more keys, a delete and an overwrite.
        fill(&db, 300, 350);
        db.delete(b"k0001").unwrap();
        db.put(b"k0002", b"new").unwrap();
        db.flush().unwrap();
        let c = contents(&db);
        db.abandon(); // no new snapshot: the next open must catch up from the old one
        c
    };
    let db = Db::open(&uri, opts(&wal)).unwrap();
    assert!(db.opened_from_snapshot());
    assert_eq!(contents(&db), want);
    // The same as a full scan.
    let full = Db::open(
        &uri,
        Options {
            snapshot_min_entries: usize::MAX,
            read_only: true,
            ..opts(&wal)
        },
    )
    .unwrap();
    assert!(!full.opened_from_snapshot());
    assert_eq!(contents(&full), want);
}

#[test]
fn compaction_invalidates_the_snapshot() {
    let (_d, uri, wal) = setup();
    let db = Db::open(&uri, opts(&wal)).unwrap();
    fill(&db, 0, 100);
    db.close().unwrap();
    // Another handle deletes a key and compacts with no grace, dropping the tombstone: a
    // snapshot that still holds the key must not be used.
    let other = Db::open(
        &uri,
        Options {
            tombstone_grace: Duration::ZERO,
            snapshot_min_entries: usize::MAX,
            ..opts(&wal)
        },
    )
    .unwrap();
    other.delete(b"k0005").unwrap();
    other.compact().unwrap();
    other.close().unwrap();
    let db = Db::open(&uri, opts(&wal)).unwrap();
    assert!(!db.opened_from_snapshot());
    assert_eq!(db.get(b"k0005").unwrap(), None);
    assert_eq!(contents(&db).len(), 99);
}

#[test]
fn a_damaged_or_foreign_snapshot_is_ignored() {
    let (_d, uri, wal) = setup();
    let db = Db::open(&uri, opts(&wal)).unwrap();
    fill(&db, 0, 100);
    db.close().unwrap();
    let path = snap_file(&wal);
    let mut b = std::fs::read(&path).unwrap();
    let n = b.len();
    b[n / 2] ^= 0xff;
    std::fs::write(&path, &b).unwrap();
    let db = Db::open(&uri, opts(&wal)).unwrap();
    assert!(!db.opened_from_snapshot());
    assert_eq!(contents(&db).len(), 100);
    db.close().unwrap();
    assert!(Db::open(&uri, opts(&wal)).unwrap().opened_from_snapshot());

    // A dataset deleted and created again at the same location has new fragments.
    std::fs::remove_dir_all(&uri).unwrap();
    let db = Db::open(&uri, opts(&wal)).unwrap();
    assert!(!db.opened_from_snapshot());
    fill(&db, 500, 505);
    db.flush().unwrap();
    assert_eq!(contents(&db).len(), 5);
}

#[test]
fn a_read_only_handle_saves_a_snapshot_when_dropped() {
    let (_d, uri, wal) = setup();
    let w = Db::open(
        &uri,
        Options {
            snapshot_min_entries: usize::MAX,
            ..opts(&wal)
        },
    )
    .unwrap();
    fill(&w, 0, 50);
    w.close().unwrap();
    assert!(std::fs::read_dir(&wal)
        .map(|mut d| d.all(|e| !e.unwrap().path().join("view.snap").exists()))
        .unwrap_or(true));
    let r = Db::open(
        &uri,
        Options {
            read_only: true,
            ..opts(&wal)
        },
    )
    .unwrap();
    assert!(!r.opened_from_snapshot());
    let want = contents(&r);
    drop(r);
    let r = Db::open(
        &uri,
        Options {
            read_only: true,
            ..opts(&wal)
        },
    )
    .unwrap();
    assert!(r.opened_from_snapshot());
    assert_eq!(contents(&r), want);
}
